# mqtt-session-broker-go

一个本地可运行的 MQTT 消息代理（Broker），作为设备与网关之间的通信中间层。
零第三方依赖，仅使用 Go 标准库。

## 协议版本与兼容范围

- 实现 **MQTT 3.1.1**（`CONNECT` 协议名 `MQTT`、协议级别 4）；其他协议版本（含 MQTT 5.0）在 `CONNECT` 阶段被拒绝，CONNACK 返回码 `0x01`。
- 支持的报文：`CONNECT/CONNACK`、`PUBLISH`、`PUBACK/PUBREC/PUBREL/PUBCOMP`、`SUBSCRIBE/SUBACK`、`UNSUBSCRIBE/UNSUBACK`、`PINGREQ/PINGRESP`、`DISCONNECT`。
- 支持 QoS 0 / 1 / 2 与**保留消息（RETAIN）完整生命周期**；不实现 `$SYS` 之外的系统主题；通配符开头的过滤器不匹配 `$` 开头的主题（符合协议语义）。
- 不实现认证鉴权；`username/password` 字段会被解析但不校验。

## 会话与消息投递规则

### 主题路由
- 发布主题名不允许包含 `+`/`#` 通配符，违反时连接被断开，原因 `ErrInvalidTopic`。
- 订阅过滤器按协议语义校验（`+` 独占一层、`#` 只能在末尾独占一层），非法过滤器在 `SUBACK` 中以 `0x80` 逐项拒绝，不影响同报文中的其他过滤器。
- 投递 QoS = `min(发布QoS, 订阅授予QoS)`；同一会话的出站消息按入队顺序 FIFO 投递（由会话唯一的 drainer goroutine 串行写出，实时消息与保留首投不会互相插队）。

### 保留消息（RETAIN）
按 MQTT 3.1.1 第 3.3.1 节实现，保留状态是**代理层面**的数据，独立于任何会话：

- **写入/覆盖**：`PUBLISH` 的 RETAIN 位为 1 且载荷非空时，该消息成为话题的最新保留状态，按“保留时的发布 QoS”原样记住；同话题再发一条即整体覆盖。
- **清除**：RETAIN 位为 1 且**载荷为空**表示删除该话题的保留状态；该报文不向任何在线订阅者转发（QoS1/2 仍正常回 PUBACK/PUBREC…）。这是 3.1.1 的常见约定（mosquitto 等代理同样行为），MQTT 5 才将其写入标准正文。
- **新订阅首投**：订阅成功时，每个匹配话题向该订阅者**只投当前最新一条**，固定报头 RETAIN 位为 **1**（标识历史状态），投递级别 = `min(保留时QoS, 本次授予QoS)`；通配符一次匹配多个话题时按话题名排序逐个投递，先于 `SUBACK` 到达。同一 `SUBSCRIBE` 里多个过滤器命中同话题只投一次；重复发起订阅会再次首投。
- **在线订阅者**：实时收到保留发布时按普通实时消息处理——RETAIN 位为 **0**、每发布只收一条，不会因为消息被保留而重复投递或事后补推历史副本。
- **与遗嘱一致**：遗嘱消息 RETAIN=1 时，设备异常掉线触发的遗嘱会写入/覆盖对应话题的保留状态；正常 `DISCONNECT` 不发遗嘱，自然也不改保留状态。
- **与会话解耦**：会话被 CleanSession 清掉、被接管或过期，都不删除代理保留状态；保留状态也不会向已清除的会话投递。新连接、新会话随时可拿到当前保留状态。
- **入站 QoS2**：保留发布是 QoS2 时，直到发布端完成 `PUBREL`、消息真正进入路由时才更新保留状态（此前新订阅者看不到它）。

### QoS 状态机
- **QoS 1**：出站 `QUEUED → WAIT_PUBACK →(PUBACK) 完成`；重连后在途消息以 `DUP=1` 重发。
- **QoS 2**：出站 `QUEUED → WAIT_PUBREC →(PUBREC) WAIT_PUBCOMP →(PUBCOMP) 完成`；重连后按状态重发 `PUBLISH(DUP=1)` 或 `PUBREL`，不会重复投递应用消息。
- **入站 QoS 2**：收到 `PUBLISH` 先存会话、回 `PUBREC`；同一 `packetID` 的重传只回 `PUBREC` 不重复投递；收到 `PUBREL` 才路由给订阅者并回 `PUBCOMP`。

### 持久会话与离线消息
- `CleanSession=false`：断线后会话（订阅关系、未完成交接、离线队列）保留；重连时 `CONNACK.SessionPresent=1` 并续传未完成消息。
- `CleanSession=true`：断线即删除会话及其订阅与积压。
- 会话过期（`SessionExpiry>0`）：持久会话断线超过该时长后被清除，离线消息一并丢弃；过期清除后重连视为全新会话（`SessionPresent=0`）。
- 同 `ClientID` 新连接接入时，旧连接被断开（原因 `ErrSessionTaken`），会话状态由新连接继承。
- 遗嘱消息（Will）：异常断开（网络错误、畸形报文、配额断开等）时发布；收到 `DISCONNECT` 的正常断开不发布。

### 资源边界（慢消费者、配额与报文大小）
- **会话级配额** `MaxQueuePerSession`：单会话积压（离线队列 + 在途消息）达到配额后，新投递被拒绝；在线连接以 `ErrQuotaExceeded` 断开，离线会话则丢弃新消息并记录日志。QoS 0 消息对离线会话直接丢弃（协议允许）。保留首投同样占用该配额，超限时以同一原因断开且不发送 `SUBACK`（不留半截状态）。
- **连接级写积压**：每连接异步写出队列容量 256 条，写满即判定为慢消费者，以 `ErrSlowConsumer` 断开。
- **单条报文大小上限** `MaxPacketSize`：按剩余长度（去掉固定报头后的全部字节）计量，默认 1 MiB。在读取报文主体**之前**只解析头部即判定，超限不会把巨大载荷读进内存；超限连接被立即断开，原因为 `ErrPacketTooLarge`（与 `ErrMalformedPacket` 明确区分）。处理是每条连接独立进行的：不阻塞其他连接，也不会在会话里留下半截状态（CONNECT 阶段超限则会话根本不会建立）。

### 可区分的断开/拒绝原因
| 原因 | 含义 |
|---|---|
| `ErrMalformedPacket` | 畸形报文（保留类型、非法标志位、非法 UTF-8、QoS 3、空主题等） |
| `ErrPacketTooLarge` | 报文声明的剩余长度超过 `MaxPacketSize`（单条报文过大） |
| `ErrInvalidTopic` | 发布主题含通配符或非法 |
| `ErrQuotaExceeded` | 会话积压超配额（含保留首投超配额） |
| `ErrSlowConsumer` | 连接写积压超限 |
| `ErrSessionTaken` | 同 ClientID 被新连接接管 |

所有原因可通过 `errors.Is` 判定；`SUBACK` 的 `0x80` 与 CONNACK 返回码用于客户端侧区分。
报文层的 `packet.ErrPacketTooLarge` 包在 `*packet.DecodeError` 中，同样可用 `errors.Is` 与畸形报文区分。

## 关键配置项

`broker.Config` / 命令行标志（`cmd/broker`）：

| 配置 | 标志 | 默认 | 说明 |
|---|---|---|---|
| `MaxQueuePerSession` | `-max-queue` | 1000 | 每会话积压配额（条），`<=0` 不限 |
| `SessionExpiry` | `-session-expiry` | 3600 | 持久会话过期秒数，`<=0` 不过期 |
| `MaxPacketSize` | `-max-packet-size` | 1048576（1 MiB） | 单条入站报文字节上限（按剩余长度），`<=0` 取默认值，超限以 `ErrPacketTooLarge` 断开 |
| 监听地址 | `-addr` | `127.0.0.1:1883` | TCP 监听地址 |

## 本地验证方法

```bash
# 运行全部测试（含竞态检测）
go test ./... -race -v

# 启动代理
go run ./cmd/broker -addr 127.0.0.1:1883 -max-queue 1000 -session-expiry 3600 -max-packet-size 1048576
```

可用 mosquitto 客户端手动验证保留消息：

```bash
# 设备端：发布一条保留状态
mosquitto_pub -h 127.0.0.1 -t status/gw1 -m online   -r -q 1
# 后来的订阅者一连上立即收到 status/gw1=online（RETAIN 位为 1）
mosquitto_sub -h 127.0.0.1 -t 'status/#' -q 1 -v -d
# 清除该话题保留状态（空载荷 + retain）
mosquitto_pub -h 127.0.0.1 -t status/gw1 -n -r
# 超过 -max-packet-size 的报文会以可区分原因断开（日志可见 ErrPacketTooLarge）
```

测试覆盖：
- `packet/`：各类报文编解码往返；18 种畸形报文的拒绝原因（日志打印报文类型与判定依据）；`ReadPacketLimit` 的超限边界与 `errors.Is(err, ErrPacketTooLarge)` 判定。
- `topic/`：主题名/过滤器校验与通配符匹配（含 `$` 系统主题规则）。
- `broker/`：并发发布订阅（不丢不重）、QoS2 握手与重传去重、断线重连续传、QoS2 在途恢复、配额耗尽断开、慢消费者断开、遗嘱消息（正常/异常断开）、会话过期与清理；
  新增保留消息：保留状态写入/覆盖/清除、新订阅（含通配符、QoS 降级、RETAIN 标识、多话题排序、QoS2 在 PUBREL 后才生效）的首投、在线订阅者只收实时副本不被补推、空载荷清除不转发、保留遗嘱更新、保留状态独立于会话清理/过期、保留首投与配额并存；
  新增报文上限：CONNECT 前/后的超限断开原因可区分（`ErrPacketTooLarge`，非 `ErrMalformedPacket`）、不留会话且不影响其他连接；
  以及订阅/发布高并发交错下历史与实时不重复、不乱序的压测（20 轮）。日志打印报文类型、会话状态（在线/pre-connect/持久恢复）与判定依据。
- `server/`：真实 TCP 链路的端到端发布订阅。

## 代码结构

```
packet/   MQTT 3.1.1 报文编解码（畸形报文/报文超限给出可区分原因）
topic/    主题校验与通配符匹配
broker/   会话状态机、订阅路由、保留消息存储（retain.go）、单飞 FIFO 出站 drainer、
          配额/慢消费者/报文大小管理、连接读写循环
server/   TCP 接入层
cmd/broker/ 可执行入口
```
