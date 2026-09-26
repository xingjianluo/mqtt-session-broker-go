# mqtt-session-broker-go

一个本地可运行的 MQTT 消息代理（Broker），作为设备与网关之间的通信中间层。
零第三方依赖，仅使用 Go 标准库。

## 协议版本与兼容范围

- 实现 **MQTT 3.1.1**（`CONNECT` 协议名 `MQTT`、协议级别 4）；其他协议版本（含 MQTT 5.0）在 `CONNECT` 阶段被拒绝，CONNACK 返回码 `0x01`。
- 支持的报文：`CONNECT/CONNACK`、`PUBLISH`、`PUBACK/PUBREC/PUBREL/PUBCOMP`、`SUBSCRIBE/SUBACK`、`UNSUBSCRIBE/UNSUBACK`、`PINGREQ/PINGRESP`、`DISCONNECT`。
- 支持 QoS 0 / 1 / 2；**不支持**保留消息（retain 存储）与 `$SYS` 之外的系统主题；通配符开头的过滤器不匹配 `$` 开头的主题（符合协议语义）。
- 不实现认证鉴权；`username/password` 字段会被解析但不校验。

## 会话与消息投递规则

### 主题路由
- 发布主题名不允许包含 `+`/`#` 通配符，违反时连接被断开，原因 `ErrInvalidTopic`。
- 订阅过滤器按协议语义校验（`+` 独占一层、`#` 只能在末尾独占一层），非法过滤器在 `SUBACK` 中以 `0x80` 逐项拒绝，不影响同报文中的其他过滤器。
- 投递 QoS = `min(发布QoS, 订阅授予QoS)`；同一会话的出站消息按入队顺序 FIFO 投递。

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

### 资源边界（慢消费者与配额）
- **会话级配额** `MaxQueuePerSession`：单会话积压（离线队列 + 在途消息）达到配额后，新投递被拒绝；在线连接以 `ErrQuotaExceeded` 断开，离线会话则丢弃新消息并记录日志。QoS 0 消息对离线会话直接丢弃（协议允许）。
- **连接级写积压**：每连接异步写出队列容量 256 条，写满即判定为慢消费者，以 `ErrSlowConsumer` 断开。

### 可区分的断开/拒绝原因
| 原因 | 含义 |
|---|---|
| `ErrMalformedPacket` | 畸形报文（保留类型、非法标志位、非法 UTF-8、QoS 3、空主题等） |
| `ErrInvalidTopic` | 发布主题含通配符或非法 |
| `ErrQuotaExceeded` | 会话积压超配额 |
| `ErrSlowConsumer` | 连接写积压超限 |
| `ErrSessionTaken` | 同 ClientID 被新连接接管 |

所有原因可通过 `errors.Is` 判定；`SUBACK` 的 `0x80` 与 CONNACK 返回码用于客户端侧区分。

## 关键配置项

`broker.Config` / 命令行标志（`cmd/broker`）：

| 配置 | 标志 | 默认 | 说明 |
|---|---|---|---|
| `MaxQueuePerSession` | `-max-queue` | 1000 | 每会话积压配额（条），`<=0` 不限 |
| `SessionExpiry` | `-session-expiry` | 3600 | 持久会话过期秒数，`<=0` 不过期 |
| 监听地址 | `-addr` | `127.0.0.1:1883` | TCP 监听地址 |

## 本地验证方法

```bash
# 运行全部测试（含竞态检测）
go test ./... -race -v

# 启动代理
go run ./cmd/broker -addr 127.0.0.1:1883 -max-queue 1000 -session-expiry 3600
```

测试覆盖：
- `packet/`：各类报文编解码往返；18 种畸形报文的拒绝原因（日志打印报文类型与判定依据）。
- `topic/`：主题名/过滤器校验与通配符匹配（含 `$` 系统主题规则）。
- `broker/`：并发发布订阅（不丢不重）、QoS2 握手与重传去重、断线重连续传、QoS2 在途恢复、配额耗尽断开、慢消费者断开、遗嘱消息（正常/异常断开）、会话过期与清理（日志打印报文类型、会话状态与判定依据）。
- `server/`：真实 TCP 链路的端到端发布订阅。

## 代码结构

```
packet/   MQTT 3.1.1 报文编解码（畸形报文给出可区分原因）
topic/    主题校验与通配符匹配
broker/   会话状态机、订阅路由、配额与慢消费者管理、连接读写循环
server/   TCP 接入层
cmd/broker/ 可执行入口
```
