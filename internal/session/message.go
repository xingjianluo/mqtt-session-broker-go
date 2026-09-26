package session

// State 为出站消息（服务端 -> 客户端）在会话状态机中的状态。
type State int

const (
	StateQueued    State = iota // 在离线/积压队列中等待发送，尚未分配报文标识符
	StatePublished              // 已发送 PUBLISH（QoS1/2），等待 PUBACK/PUBREC
	StatePubrec                 // QoS2：已收 PUBREC 并发出 PUBREL，等待 PUBCOMP
	StateCompleted              // 终态：QoS1 收到 PUBACK / QoS2 收到 PUBCOMP
)

func (s State) String() string {
	switch s {
	case StateQueued:
		return "QUEUED"
	case StatePublished:
		return "PUBLISHED"
	case StatePubrec:
		return "PUBREL_SENT"
	case StateCompleted:
		return "COMPLETED"
	default:
		return "UNKNOWN"
	}
}

// QueuedMessage 为会话中排队/在途的一条出站消息。
type QueuedMessage struct {
	PacketID uint16 // 0 表示尚未分配报文标识符（排队中）
	Topic    string
	Payload  []byte
	QoS      byte
	Retain   bool // 仅用于离线队列中的保留消息首投标记
	State    State
}

// Clone 返回深拷贝，避免外部修改队列内部数据。
func (m *QueuedMessage) Clone() *QueuedMessage {
	cp := *m
	if m.Payload != nil {
		cp.Payload = append([]byte(nil), m.Payload...)
	}
	return &cp
}
