// Package packet 实现 MQTT 3.1.1 控制报文的编解码。
package packet

// Type 是 MQTT 控制报文类型（固定报头高 4 位）。
type Type byte

const (
	TypeConnect     Type = 1
	TypeConnAck     Type = 2
	TypePublish     Type = 3
	TypePubAck      Type = 4
	TypePubRec      Type = 5
	TypePubRel      Type = 6
	TypePubComp     Type = 7
	TypeSubscribe   Type = 8
	TypeSubAck      Type = 9
	TypeUnsubscribe Type = 10
	TypeUnsubAck    Type = 11
	TypePingReq     Type = 12
	TypePingResp    Type = 13
	TypeDisconnect  Type = 14
)

// QoS 等级。
const (
	QoS0 byte = 0
	QoS1 byte = 1
	QoS2 byte = 2
)

// Packet 是解码后的控制报文。不同报文类型使用不同字段。
type Packet struct {
	Type      Type
	Flags     byte   // 固定报头低 4 位
	Remaining []byte // 未解析时保留的原始剩余数据（内部使用）

	// CONNECT
	ProtocolName  string
	ProtocolLevel byte
	CleanSession  bool
	KeepAlive     uint16
	ClientID      string
	WillTopic     string
	WillPayload   []byte
	WillQoS       byte
	WillRetain    bool
	HasWill       bool
	Username      string
	Password      []byte

	// CONNACK
	SessionPresent bool
	ReturnCode     byte

	// PUBLISH
	Dup      bool
	Retain   bool
	QoS      byte
	Topic    string
	PacketID uint16
	Payload  []byte

	// SUBSCRIBE / UNSUBSCRIBE
	Subscriptions []Subscription

	// 通用原因码（SUBACK 等）
	ReasonCodes []byte
}

// Subscription 表示一条订阅请求。
type Subscription struct {
	Topic string
	QoS   byte
}

// String 返回报文类型名，用于日志。
func (t Type) String() string {
	switch t {
	case TypeConnect:
		return "CONNECT"
	case TypeConnAck:
		return "CONNACK"
	case TypePublish:
		return "PUBLISH"
	case TypePubAck:
		return "PUBACK"
	case TypePubRec:
		return "PUBREC"
	case TypePubRel:
		return "PUBREL"
	case TypePubComp:
		return "PUBCOMP"
	case TypeSubscribe:
		return "SUBSCRIBE"
	case TypeSubAck:
		return "SUBACK"
	case TypeUnsubscribe:
		return "UNSUBSCRIBE"
	case TypeUnsubAck:
		return "UNSUBACK"
	case TypePingReq:
		return "PINGREQ"
	case TypePingResp:
		return "PINGRESP"
	case TypeDisconnect:
		return "DISCONNECT"
	default:
		return "UNKNOWN"
	}
}
