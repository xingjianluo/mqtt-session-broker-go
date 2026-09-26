# mqtt-session-broker-go

一个本地可运行的 MQTT 消息代理（Go 标准库实现，无第三方依赖），定位为设备与网关之间的通信中间层。
设计目标：在**并发连接、断线重连、异常报文、慢消费者**等条件下，让消息的投递状态**可预期、可验证**。

---

## 1. 支持的协议版本与兼容范围

| 项目 | 支持情况 |
| --- | --- |
| 协议版本 | **MQTT 3.1.1**（协议名 `MQTT`，protocol level = 4） |
| MQTT 3.1（level 3） | 不支持，回 `CONNACK 0x01`（Unacceptable Protocol Version）后断开 |
| MQTT 5.0（level 5） | 不支持，回 MQTT 5.0 风格 `CONNACK`（原因码 `0x84` Unsupported Protocol Version，含零长度属性）后断开 |
| 传输 | TCP 明文（未实现 TLS/WebSocket） |
| QoS 等级 | QoS 0（至多一次）、QoS 1（至少一次）、QoS 2（仅一次） |
| 主题 | 层级 `/`、单层通配 `+`、多层通配 `#`、`$` 开头主题不被顶层通配符匹配 |
| 会话 | Clean Session 0/1、持久会话、离线消息、会话 TTL（可选） |
| 保留消息（Retained） | 支持存储、订阅首投、覆盖、空载荷清除 |
| 遗嘱（Will） | 支持 QoS/retain；正常 DISCONNECT 与连接接管不发布，异常断开发布 |
| 保活（Keep Alive） | 支持 PINGREQ/PINGRESP，按 1.5 倍超时判定 |
| 认证 | 解析 Username/Password 字段；当前不做鉴权（预留 CONNACK 0x05 路径） |
| 明确不支持 | TLS、WebSocket、Shared Subscription、MQTT5 属性、桥接/集群、服务端持久化到磁盘 |

> 兼容性说明：`$SYS/#` 等以 `$` 开头的主题可以被显式过滤器订阅，但不会被顶层 `#`、`+`
> 匹配（MQTT 3.1.1 §4.7.2 的通用语义）。

---

## 2. 会话与消息投递规则

### 2.1 Clean Session

- `CleanSession=1`：每次连接建立全新会话；断开时（无论正常/异常）删除会话、清空在途与离线队列、移除全部订阅。
- `CleanSession=0`：建立持久会话。
  - 首次连接 `CONNACK` 的 Session Present = 0；
  - 之后同 ClientID 重连，只要会话未被清除/过期，Session Present = 1，且：
    - 订阅关系保留；
    - 未确认的 QoS1/2 出站消息保留，重连后续传；
    - 入站 QoS2 去重记录保留，重传的 QoS2 PUBLISH 不会重复交付。

### 2.2 QoS 状态机（出站，服务端 → 客户端）

| QoS | 状态流转 | 重连/重传行为 |
| --- | --- | --- |
| QoS1 | `QUEUED → PUBLISHED（发 PUBLISH, DUP=0）→ COMPLETED（收 PUBACK）` | 未收到 PUBACK：以同一 PacketID 重发，`DUP=1` |
| QoS2 | `QUEUED → PUBLISHED（发 PUBLISH, DUP=0）→ PUBREL_SENT（收 PUBREC 发 PUBREL）→ COMPLETED（收 PUBCOMP）` | 处于 `PUBLISHED` 重发 PUBLISH(DUP=1)；处于 `PUBREL_SENT` **只重发 PUBREL**，不再重发 PUBLISH |

- 每个会话维护一个有界**在途窗口**（默认 32），PacketID 在窗口内唯一、不复用；窗口满时新消息留在队列等待。
- 未确认消息由重传定时器（默认 10s）以及重连动作触发重传。
- **不丢失**：QoS1/2 消息进入会话队列即被持久到内存会话，确认前不移除。
- **不重复投递**：
  - 多个重叠过滤器命中同一客户端时只投递一条，QoS 取订阅授予的最大值；
  - QoS2 入站按 PacketID 去重：重传 PUBLISH 只重发 PUBREC，不再次交付；PUBREL 未知标识符也回 PUBCOMP。

### 2.3 QoS 0

- 在线：尽力而为，直接走连接出站通道；通道长时间阻塞触发慢消费者断开（见 §4）。
- 离线：按 MQTT 3.1.1 §3.1.2-7 **直接丢弃**（产生可观测的 drop 事件），不进入队列。

### 2.4 保留消息（Retained）

- PUBLISH 携带 `RETAIN=1`：消息按主题存储（空载荷清除该主题），再以 `RETAIN=0` 路由给当前订阅者。
- 新订阅建立时，匹配的保留消息**首投携带 `RETAIN=1`**；此后任何重传 `RETAIN=0`（§3.3.1.3）。
- 投递 QoS 取「发布 QoS」与「订阅授予 QoS」的较小值。

### 2.5 遗嘱（Will）

- 以下**异常**路径发布遗嘱：TCP 网络断开、Keep Alive 1.5 倍超时、慢消费者/配额断开。
- 以下路径**不**发布遗嘱：客户端发送 DISCONNECT、被同 ClientID 新连接**接管**（taken over）、服务端关停、协议版本被拒。
- `Will Retain=1` 的遗嘱同时成为保留消息（路由副本 RETAIN=0，新订阅者首投 RETAIN=1）。

### 2.6 会话过期与清除后的离线消息语义（稳定且明确）

- 默认 `SessionTTL=0`：持久会话永不过期（进程重启不保留，见"不支持"）。
- 配置 `SessionTTL>0`：会话离线后开始计时，超过 TTL 被后台 GC 清除，产生 `session_expired` 事件，
  同时移除其全部订阅与消息。**之后**到达该 ClientID/订阅的 QoS1/2 消息因不存在匹配会话而不会积压；
  同 ClientID 重连得到的是 Session Present=0 的全新空会话。
- `CleanSession=1` 断开与会话过期的清除语义一致。

---

## 3. 主题与非法输入的拒绝原因（可区分）

| 情形 | 行为 | 可区分原因 |
| --- | --- | --- |
| PUBLISH 主题为空/含 `+`/`#`/NUL/超长 | 关闭连接 | `protocol_violation` / `bad_topic` |
| SUBSCRIBE 过滤器非法（`#` 不在末级、通配符未独占整级等） | 该过滤器 SUBACK 授予码 `0x80`，其余过滤器照常处理 | `bad_subscription_filter` |
| 订阅 QoS > 2 | 畸形报文，断开 | `malformed_packet` |
| CONNECT 空 ClientID 且 CleanSession=0 | `CONNACK 0x02` 后断开 | `bad_client_id` |
| CONNECT flags 保留位置位、Will 标志矛盾 | 断开 | `malformed_packet` |
| PUBLISH QoS=3、QoS0 携带 DUP、PacketID=0 | 断开 | `malformed_packet` / `protocol_violation` |
| SUBSCRIBE/UNSUBSCRIBE/PUBREL 固定头保留位错误 | 断开 | `protocol_violation` |
| 报文超过大小上限 / Remaining Length 编码 >4 字节 | 读帧阶段断开 | `malformed_packet`（packet too large） |
| 第二 CONNECT、客户端发送服务端专属报文 | 断开 | `protocol_violation` |

---

## 4. 慢消费者与资源边界

| 边界 | 默认值 | 触达后的行为 | 断开/事件原因 |
| --- | --- | --- | --- |
| `max-packet-bytes` 单报文大小 | 1 MiB | 读帧即拒，关闭连接 | `packet_too_large` / `malformed_packet` |
| `max-queue` 每会话「队列 + 在途」总条数 | 1000 | **最新一条** QoS1/2 消息被拒绝；在线则断开，离线仅拒绝并计数 | drop/disconnect：`queue_quota_exceeded` |
| `max-inflight` 在途窗口 | 32 | 窗口满时消息留在队列，确认后推进，不额外占内存 | — |
| 出站通道背压 | 有界 channel（maxInflight+64） | 通道持续满超过 `slow-consumer-timeout`，断开连接 | `slow_consumer` |
| TCP 写阻塞 | `slow-consumer-timeout`（默认 5s） | 写进入"背压态"后给固定窗口，窗口内仍写不完则断开 | `slow_consumer` |
| Keep Alive | 1.5 × KeepAlive | 周期内无任何报文则断开并发布遗嘱 | `keepalive_timeout` |

设计要点：内存占用是**有上界**的——每会话至多 `max-queue` 条排队/在途消息 + 有界出站通道；
触达边界时不会无限缓冲，而是按上表**限流（窗口）/拒绝（计数 + 事件）/断开（可区分原因）**。

---

## 5. 关键配置项

| 命令行参数 | 默认 | 说明 |
| --- | --- | --- |
| `-listen` | `:1883` | TCP 监听地址 |
| `-max-packet-bytes` | `1048576` | 单个 MQTT 报文最大字节数 |
| `-max-queue` | `1000` | 每会话（离线积压 + 在途）消息条数上限 |
| `-max-inflight` | `32` | 每会话未确认 QoS1/2 出站消息窗口大小 |
| `-session-ttl` | `0`（永不过期） | 持久会话离线保留时长，如 `2h` |
| `-slow-consumer-timeout` | `5s` | 出站背压/写阻塞多久判定慢消费者并断开 |
| `-retry-interval` | `10s` | 未确认 QoS1/2 消息的重传间隔 |

---

## 6. 本地构建与验证

### 6.1 构建与运行

```bash
go build ./...
go run ./cmd/mqttbroker -listen :1883
# 或构建后运行
go build -o mqttbroker ./cmd/mqttbroker
./mqttbroker -listen :1883 -max-queue 1000 -max-inflight 32
```

启动后可使用任意标准 MQTT 3.1.1 客户端（mosquitto_pub/sub、paho 等）连接。

### 6.2 运行测试（含竞态检测）

```bash
# 全部单测/集成测试
go test ./... -count=1

# 带竞态检测器（建议）
go test -race ./... -count=1

# 详细查看报文类型、会话状态、判定依据（测试日志）
go test ./internal/broker/ -v -run 'TestPersistentSessionResumeQoS2'
```

测试覆盖矩阵：

- 畸形报文：Remaining Length 溢出、CONNECT flags/版本、PUBLISH QoS/主题、SUBSCRIBE QoS、保留位等；
- 并发发布订阅：多发布者并发 QoS1，订阅者逐条 PUBACK，断言**不丢不重**；
- 通配符路由：`+`/`#`/`$` 语义、重叠过滤器去重、QoS 降级；
- 重连续传：离线消息、QoS1 DUP 重传、QoS2 按 `PUBLISHED`/`PUBREL_SENT` 状态分别续传、CONNACK Session Present；
- 入站 QoS2 去重：重传 PUBLISH 不二次交付；
- 保留消息：首投 RETAIN、覆盖、空载荷清除、路由副本 RETAIN=0；
- 遗嘱：正常 DISCONNECT 不发、TCP 异常关闭发布、接管不发、willRetain 入库；
- 配额：离线队列满拒绝（可计数事件）、在线积压满以 `queue_quota_exceeded` 断开；
- 慢消费者：不读 TCP 的连接以 `slow_consumer` 断开；
- Keep Alive：超时断开（发遗嘱）与 PINGREQ 续命；
- 会话 TTL：过期清除后语义稳定（空会话、不积压）。

### 6.3 黑盒冒烟工具

仓库内置一个手工完成 MQTT 交互的冒烟客户端（真实 TCP）：

```bash
# 终端 1
go run ./cmd/mqttbroker -listen 127.0.0.1:18830
# 终端 2
go run ./cmd/smoketest 127.0.0.1:18830
```

期望输出包含 `CONNACK ... code=0`、`SUBACK ... codes=01`、`publisher got PUBACK`、
`subscriber PUBLISH ...`，最后打印 `SMOKE OK`。

---

## 7. 目录结构

```
cmd/mqttbroker/        代理可执行入口（CLI 配置、优雅退出、事件日志）
cmd/smoketest/         黑盒 TCP 冒烟客户端
internal/mqttproto/    MQTT 3.1.1 报文编解码、畸形报文分类拒绝原因
internal/topic/        主题/过滤器校验、通配符匹配、并发安全订阅 Trie
internal/session/      持久会话状态机：离线队列、在途窗口、QoS2 去重、代际唤醒
internal/broker/       连接生命周期、路由、保留消息、遗嘱、配额/慢消费者边界、TTL GC
```

## 8. 持久化范围声明

当前"持久会话"指**连接生命周期内的内存持久**（覆盖断线重连续传的全部场景），
尚未实现进程重启后的磁盘/WAL 持久化。需要跨重启持久化时，可在 `internal/session`
的队列/在途结构之下追加日志存储，接口不需要改动。
