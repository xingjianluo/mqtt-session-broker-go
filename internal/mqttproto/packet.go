// Package mqttproto 实现 MQTT 3.1.1 报文的编解码（CONNECT 兼容 MQTT 5 拒绝路径）。
package mqttproto

// Type 为 MQTT 控制报文类型（高 4 位）。
type Type byte

const (
	TypeRESERVED    Type = 0
	TypeCONNECT     Type = 1
	TypeCONNACK     Type = 2
	TypePUBLISH     Type = 3
	TypePUBACK      Type = 4
	TypePUBREC      Type = 5
	TypePUBREL      Type = 6
	TypePUBCOMP     Type = 7
	TypeSUBSCRIBE   Type = 8
	TypeSUBACK      Type = 9
	TypeUNSUBSCRIBE Type = 10
	TypeUNSUBACK    Type = 11
	TypePINGREQ     Type = 12
	TypePINGRESP    Type = 13
	TypeDISCONNECT  Type = 14
	TypeAUTH        Type = 15
)

func (t Type) String() string {
	switch t {
	case TypeCONNECT:
		return "CONNECT"
	case TypeCONNACK:
		return "CONNACK"
	case TypePUBLISH:
		return "PUBLISH"
	case TypePUBACK:
		return "PUBACK"
	case TypePUBREC:
		return "PUBREC"
	case TypePUBREL:
		return "PUBREL"
	case TypePUBCOMP:
		return "PUBCOMP"
	case TypeSUBSCRIBE:
		return "SUBSCRIBE"
	case TypeSUBACK:
		return "SUBACK"
	case TypeUNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case TypeUNSUBACK:
		return "UNSUBACK"
	case TypePINGREQ:
		return "PINGREQ"
	case TypePINGRESP:
		return "PINGRESP"
	case TypeDISCONNECT:
		return "DISCONNECT"
	case TypeAUTH:
		return "AUTH"
	default:
		return "RESERVED"
	}
}

// Connect 表示解码后的 CONNECT 报文。
type Connect struct {
	ProtocolName  string
	ProtocolLevel byte // 4 = MQTT 3.1.1，5 = MQTT 5.0
	CleanSession  bool
	WillFlag      bool
	WillQoS       byte
	WillRetain    bool
	PasswordFlag  bool
	UsernameFlag  bool
	KeepAlive     uint16
	ClientID      string
	Username      string
	Password      []byte
	WillTopic     string
	WillMessage   []byte
}

// Publish 表示 PUBLISH 报文（可变头 + 载荷）。
type Publish struct {
	Topic    string
	PacketID uint16
	QoS      byte
	Retain   bool
	Dup      bool
	Payload  []byte
}

// Subscribe 表示 SUBSCRIBE 报文。
type Subscribe struct {
	PacketID uint16
	Filters  []SubFilter
}

// SubFilter 为单个订阅过滤器。
type SubFilter struct {
	Filter string
	QoS    byte
}

// Unsubscribe 表示 UNSUBSCRIBE 报文。
type Unsubscribe struct {
	PacketID uint16
	Filters  []string
}

// PacketIDPacket 表示仅含报文标识符的确认类报文。
type PacketIDPacket struct {
	PacketID uint16
}
