// Package broker 实现本地 MQTT 消息代理：路由、会话、保留消息、遗嘱与资源边界。
package broker

import "time"

// Config 为 Broker 可调参数。请使用 DefaultConfig 后按需覆写字段。
type Config struct {
	// Listen 为 TCP 监听地址（main 中使用；测试可直接传入 net.Listener）。
	Listen string
	// MaxPacketSize 单个 MQTT 报文（报文体）最大字节数，超出则以 packet_too_large 断开。
	MaxPacketSize int
	// MaxClientQueue 每会话"离线积压队列 + 在途窗口"总条数上限。
	MaxClientQueue int
	// MaxInflight 每会话在途窗口（未确认的 QoS1/2 出站消息）大小。
	MaxInflight int
	// SessionTTL CleanSession=false 会话离线后的保留时长；0 表示永不过期。
	SessionTTL time.Duration
	// KeepAliveFactor keepalive 超时倍数（MQTT 3.1.1 §3.1.2.10 规定 1.5 倍）。
	KeepAliveFactor float64
	// SlowConsumerTimeout 单次向连接写入的最大阻塞时间，超时以 slow_consumer 断开。
	SlowConsumerTimeout time.Duration
	// RetryInterval 未确认的 QoS1/2 出站消息的重传间隔（PUBLISH DUP=1 / PUBREL）。
	RetryInterval time.Duration
	// EventBufferSize 事件通道缓冲；0 表示不发送事件。
	EventBufferSize int
}

// DefaultConfig 返回本地运行的默认配置。
func DefaultConfig() Config {
	return Config{
		Listen:              ":1883",
		MaxPacketSize:       1 << 20, // 1 MiB
		MaxClientQueue:      1000,
		MaxInflight:         32,
		SessionTTL:          0, // 默认持久会话不过期
		KeepAliveFactor:     1.5,
		SlowConsumerTimeout: 5 * time.Second,
		RetryInterval:       10 * time.Second,
		EventBufferSize:     0,
	}
}
