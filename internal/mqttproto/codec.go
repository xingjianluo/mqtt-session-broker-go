package mqttproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// RejectCode 为可区分的拒绝原因码。
type RejectCode string

const (
	RejectNone                RejectCode = ""
	RejectMalformedPacket     RejectCode = "malformed_packet"
	RejectUnsupportedProtoVer RejectCode = "unsupported_protocol_version"
	RejectBadClientID         RejectCode = "bad_client_id"
	RejectBadTopic            RejectCode = "bad_topic"
	RejectBadFilter           RejectCode = "bad_subscription_filter"
	RejectUnsupportedQoS      RejectCode = "unsupported_qos"
	RejectNotAuthorized       RejectCode = "not_authorized"
	RejectServerUnavailable   RejectCode = "server_unavailable"
	RejectQuotaExceeded       RejectCode = "quota_exceeded"
	RejectProtocolViolation   RejectCode = "protocol_violation"
)

// RejectError 携带可区分的拒绝原因，用于测试断言与日志。
type RejectError struct {
	Code   RejectCode
	Detail string
}

func (e *RejectError) Error() string {
	if e.Detail == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Detail
}

// NewRejectError 构造拒绝错误。
func NewRejectError(code RejectCode, detail string) *RejectError {
	return &RejectError{Code: code, Detail: detail}
}

// AsRejectError 从任意错误中提取 *RejectError。
func AsRejectError(err error) (*RejectError, bool) {
	var re *RejectError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

// Frame 为一个已读出固定头的控制报文。
type Frame struct {
	Type   Type
	Flags  byte
	Header byte
	Body   []byte
}

// ReadFrame 从 r 读取一个完整报文帧（固定头 + 剩余长度 + 报文体）。
// remaining length 超过 maxPacketSize 时返回 RejectPacketTooLarge（由上层映射）。
func ReadFrame(r io.Reader, maxPacketSize int) (*Frame, error) {
	var hdr [1]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	remaining, _, err := readVarLen(r)
	if err != nil {
		return nil, err
	}
	if maxPacketSize > 0 && int(remaining) > maxPacketSize {
		return nil, NewRejectError(RejectMalformedPacket,
			fmt.Sprintf("packet size %d exceeds max %d", remaining, maxPacketSize))
	}
	body := make([]byte, remaining)
	if remaining > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
	}
	return &Frame{
		Type:   Type(hdr[0] >> 4),
		Flags:  hdr[0] & 0x0f,
		Header: hdr[0],
		Body:   body,
	}, nil
}

// ReadFrameOrNil 读取一帧，任何错误均返回 nil（供测试中的一次性读取使用）。
func ReadFrameOrNil(r io.Reader) *Frame {
	f, err := ReadFrame(r, 1<<20)
	if err != nil {
		return nil
	}
	return f
}

func readVarLen(r io.Reader) (uint32, int, error) {
	var value uint32
	var multiplier uint32 = 1
	var b [1]byte
	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, 0, err
		}
		value += uint32(b[0]&0x7f) * multiplier
		if b[0]&0x80 == 0 {
			return value, i + 1, nil
		}
		multiplier *= 128
	}
	return 0, 0, NewRejectError(RejectMalformedPacket, "remaining length exceeds 4 bytes")
}

// DecodeConnect 解码 CONNECT 报文体（固定头之外的全部字节）。
func DecodeConnect(body []byte) (*Connect, error) {
	buf := newReader(body)
	name, err := buf.readString()
	if err != nil {
		return nil, err
	}
	c := &Connect{ProtocolName: name}
	level, err := buf.readByte()
	if err != nil {
		return nil, err
	}
	c.ProtocolLevel = level
	switch {
	case name == "MQTT" && level == 4:
		// MQTT 3.1.1，继续完整解码
	case name == "MQTT" && (level == 5 || level == 3):
		// MQTT 5.0 与 MQTT 3.1：本代理仅支持 3.1.1，不透解析载荷，
		// 仅返回 level 供上层发 CONNACK(0x01) 后断开（3.1 兼容探测路径）。
		return c, nil
	default:
		return nil, NewRejectError(RejectUnsupportedProtoVer,
			fmt.Sprintf("protocol %q level %d not supported", name, level))
	}
	flags, err := buf.readByte()
	if err != nil {
		return nil, err
	}
	c.UsernameFlag = flags&0x80 != 0
	c.PasswordFlag = flags&0x40 != 0
	c.WillRetain = flags&0x20 != 0
	c.WillQoS = (flags >> 3) & 0x03
	c.WillFlag = flags&0x04 != 0
	c.CleanSession = flags&0x02 != 0
	if flags&0x01 != 0 {
		return nil, NewRejectError(RejectMalformedPacket, "CONNECT reserved flag bit must be 0")
	}
	if !c.WillFlag && (c.WillQoS != 0 || c.WillRetain) {
		return nil, NewRejectError(RejectMalformedPacket, "will QoS/retain set while will flag is 0")
	}
	if c.WillQoS > 2 {
		return nil, NewRejectError(RejectMalformedPacket, "will QoS must be 0/1/2")
	}
	ka, err := buf.readU16()
	if err != nil {
		return nil, err
	}
	c.KeepAlive = ka

	if c.ClientID, err = buf.readString(); err != nil {
		return nil, err
	}
	if c.WillFlag {
		if c.WillTopic, err = buf.readString(); err != nil {
			return nil, err
		}
		if c.WillMessage, err = buf.readBinary(); err != nil {
			return nil, err
		}
	}
	if c.UsernameFlag {
		if c.Username, err = buf.readString(); err != nil {
			return nil, err
		}
	}
	if c.PasswordFlag {
		if c.Password, err = buf.readBinary(); err != nil {
			return nil, err
		}
	}
	if buf.remaining() != 0 {
		return nil, NewRejectError(RejectMalformedPacket, "CONNECT has trailing bytes")
	}
	return c, nil
}

// DecodePublish 解码 PUBLISH 报文体。
func DecodePublish(flags byte, body []byte) (*Publish, error) {
	p := &Publish{
		Dup:    flags&0x08 != 0,
		QoS:    (flags >> 1) & 0x03,
		Retain: flags&0x01 != 0,
	}
	if p.QoS == 3 {
		return nil, NewRejectError(RejectMalformedPacket, "PUBLISH QoS must be 0/1/2")
	}
	if p.QoS == 0 && p.Dup {
		return nil, NewRejectError(RejectProtocolViolation, "DUP flag must be 0 for QoS 0 PUBLISH")
	}
	buf := newReader(body)
	var err error
	if p.Topic, err = buf.readString(); err != nil {
		return nil, err
	}
	if p.Topic == "" {
		return nil, NewRejectError(RejectMalformedPacket, "PUBLISH topic must not be empty")
	}
	if p.QoS > 0 {
		if p.PacketID, err = buf.readU16(); err != nil {
			return nil, err
		}
		if p.PacketID == 0 {
			return nil, NewRejectError(RejectProtocolViolation, "PUBLISH packet identifier must be non-zero")
		}
	}
	p.Payload = buf.rest()
	return p, nil
}

// DecodeSubscribe 解码 SUBSCRIBE 报文体。
func DecodeSubscribe(body []byte) (*Subscribe, error) {
	buf := newReader(body)
	id, err := buf.readU16()
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, NewRejectError(RejectProtocolViolation, "SUBSCRIBE packet identifier must be non-zero")
	}
	s := &Subscribe{PacketID: id}
	for buf.remaining() > 0 {
		f, err := buf.readString()
		if err != nil {
			return nil, err
		}
		qos, err := buf.readByte()
		if err != nil {
			return nil, err
		}
		if qos > 2 {
			return nil, NewRejectError(RejectMalformedPacket,
				fmt.Sprintf("SUBSCRIBE QoS %d invalid, must be 0/1/2", qos))
		}
		s.Filters = append(s.Filters, SubFilter{Filter: f, QoS: qos})
	}
	if len(s.Filters) == 0 {
		return nil, NewRejectError(RejectMalformedPacket, "SUBSCRIBE must contain at least one filter")
	}
	return s, nil
}

// DecodeUnsubscribe 解码 UNSUBSCRIBE 报文体。
func DecodeUnsubscribe(body []byte) (*Unsubscribe, error) {
	buf := newReader(body)
	id, err := buf.readU16()
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, NewRejectError(RejectProtocolViolation, "UNSUBSCRIBE packet identifier must be non-zero")
	}
	u := &Unsubscribe{PacketID: id}
	for buf.remaining() > 0 {
		f, err := buf.readString()
		if err != nil {
			return nil, err
		}
		u.Filters = append(u.Filters, f)
	}
	if len(u.Filters) == 0 {
		return nil, NewRejectError(RejectMalformedPacket, "UNSUBSCRIBE must contain at least one filter")
	}
	return u, nil
}

// DecodePacketID 解码仅含 2 字节报文标识符的报文体（PUBACK/PUBREC/PUBREL/PUBCOMP）。
func DecodePacketID(body []byte) (uint16, error) {
	if len(body) != 2 {
		return 0, NewRejectError(RejectMalformedPacket,
			fmt.Sprintf("expected 2 bytes packet identifier, got %d", len(body)))
	}
	id := binary.BigEndian.Uint16(body)
	if id == 0 {
		return 0, NewRejectError(RejectProtocolViolation, "packet identifier must be non-zero")
	}
	return id, nil
}

// ----- 编码（服务端出站 + 供测试客户端使用的入站编码） -----

func appendVarLen(b []byte, n int) []byte {
	for {
		digit := byte(n % 128)
		n /= 128
		if n > 0 {
			digit |= 0x80
		}
		b = append(b, digit)
		if n == 0 {
			return b
		}
	}
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

// frame 组装固定头与报文体。
func frame(header byte, body []byte) []byte {
	out := []byte{header}
	out = appendVarLen(out, len(body))
	return append(out, body...)
}

// EncodeConnack 编码 CONNACK 报文。
func EncodeConnack(sessionPresent bool, code byte) []byte {
	flags := byte(0)
	if sessionPresent {
		flags = 1
	}
	return frame(0x20, []byte{flags, code})
}

// EncodeConnack5 编码最小化的 MQTT 5.0 CONNACK（可变头含一个零长度属性）。
// 仅用于拒绝不支持的协议版本：code 为 MQTT 5 原因码（如 0x84 Unsupported Protocol Version）。
func EncodeConnack5(code byte) []byte {
	return frame(0x20, []byte{0, code, 0})
}

// EncodePublish 编码 PUBLISH 报文。
func EncodePublish(p *Publish) []byte {
	header := byte(TypePUBLISH) << 4
	if p.Dup {
		header |= 0x08
	}
	header |= (p.QoS & 0x03) << 1
	if p.Retain {
		header |= 0x01
	}
	var body []byte
	body = appendString(body, p.Topic)
	if p.QoS > 0 {
		body = binary.BigEndian.AppendUint16(body, p.PacketID)
	}
	body = append(body, p.Payload...)
	return frame(header, body)
}

// EncodeAck 编码 PUBACK/PUBREC/PUBREL/PUBCOMP 报文。
func EncodeAck(t Type, packetID uint16) []byte {
	var header byte
	switch t {
	case TypePUBACK:
		header = 0x40
	case TypePUBREC:
		header = 0x50
	case TypePUBREL:
		header = 0x62
	case TypePUBCOMP:
		header = 0x70
	default:
		panic("EncodeAck: unsupported type " + t.String())
	}
	body := binary.BigEndian.AppendUint16(nil, packetID)
	return frame(header, body)
}

// EncodeSuback 编码 SUBACK 报文。
func EncodeSuback(packetID uint16, codes []byte) []byte {
	body := binary.BigEndian.AppendUint16(nil, packetID)
	body = append(body, codes...)
	return frame(0x90, body)
}

// EncodeUnsuback 编码 UNSUBACK 报文。
func EncodeUnsuback(packetID uint16) []byte {
	return frame(0xB0, binary.BigEndian.AppendUint16(nil, packetID))
}

// EncodePingresp 编码 PINGRESP 报文。
func EncodePingresp() []byte { return []byte{0xD0, 0x00} }

// EncodeDisconnect 编码 DISCONNECT 报文。
func EncodeDisconnect() []byte { return []byte{0xE0, 0x00} }

// EncodeConnect 编码客户端 CONNECT 报文（主要供测试客户端使用）。
func EncodeConnect(c *Connect) []byte {
	var vh []byte
	vh = appendString(vh, "MQTT")
	vh = append(vh, 4)
	var flags byte
	if c.UsernameFlag {
		flags |= 0x80
	}
	if c.PasswordFlag {
		flags |= 0x40
	}
	if c.WillRetain {
		flags |= 0x20
	}
	flags |= (c.WillQoS & 0x03) << 3
	if c.WillFlag {
		flags |= 0x04
	}
	if c.CleanSession {
		flags |= 0x02
	}
	vh = append(vh, flags)
	vh = binary.BigEndian.AppendUint16(vh, c.KeepAlive)

	var payload []byte
	payload = appendString(payload, c.ClientID)
	if c.WillFlag {
		payload = appendString(payload, c.WillTopic)
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(c.WillMessage)))
		payload = append(payload, c.WillMessage...)
	}
	if c.UsernameFlag {
		payload = appendString(payload, c.Username)
	}
	if c.PasswordFlag {
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(c.Password)))
		payload = append(payload, c.Password...)
	}
	return frame(0x10, append(vh, payload...))
}

// EncodeSubscribe 编码客户端 SUBSCRIBE 报文（供测试客户端使用）。
func EncodeSubscribe(s *Subscribe) []byte {
	body := binary.BigEndian.AppendUint16(nil, s.PacketID)
	for _, f := range s.Filters {
		body = appendString(body, f.Filter)
		body = append(body, f.QoS)
	}
	return frame(0x82, body)
}

// EncodeUnsubscribe 编码客户端 UNSUBSCRIBE 报文（供测试客户端使用）。
func EncodeUnsubscribe(u *Unsubscribe) []byte {
	body := binary.BigEndian.AppendUint16(nil, u.PacketID)
	for _, f := range u.Filters {
		body = appendString(body, f)
	}
	return frame(0xA2, body)
}

// ----- 内部字节读取器 -----

type reader struct {
	b   []byte
	pos int
}

func newReader(b []byte) *reader { return &reader{b: b} }

func (r *reader) remaining() int { return len(r.b) - r.pos }

func (r *reader) rest() []byte {
	out := r.b[r.pos:]
	r.pos = len(r.b)
	return out
}

func (r *reader) readByte() (byte, error) {
	if r.pos >= len(r.b) {
		return 0, NewRejectError(RejectMalformedPacket, "unexpected end of packet")
	}
	v := r.b[r.pos]
	r.pos++
	return v, nil
}

func (r *reader) readU16() (uint16, error) {
	if r.pos+2 > len(r.b) {
		return 0, NewRejectError(RejectMalformedPacket, "unexpected end of packet reading uint16")
	}
	v := binary.BigEndian.Uint16(r.b[r.pos:])
	r.pos += 2
	return v, nil
}

// readBinary 读取二进制字段（2 字节长度前缀）。
func (r *reader) readBinary() ([]byte, error) {
	n, err := r.readU16()
	if err != nil {
		return nil, err
	}
	if r.pos+int(n) > len(r.b) {
		return nil, NewRejectError(RejectMalformedPacket, fmt.Sprintf(
			"binary field length %d exceeds packet boundary (have %d bytes)", n, len(r.b)-r.pos))
	}
	out := make([]byte, n)
	copy(out, r.b[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return out, nil
}

// readString 读取 UTF-8 字符串字段并做 MQTT 字符串合法性校验。
func (r *reader) readString() (string, error) {
	b, err := r.readBinary()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", NewRejectError(RejectMalformedPacket, "string field is not valid UTF-8")
	}
	for _, ch := range b {
		if ch == 0 {
			return "", NewRejectError(RejectMalformedPacket, "string field contains NUL character")
		}
	}
	return string(b), nil
}
