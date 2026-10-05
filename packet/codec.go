package packet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// ErrPacketTooLarge 表示报文声明的剩余长度超过调用方配置的单条报文上限。
// 它作为 DecodeError 的 Cause，可通过 errors.Is 与畸形报文区分。
var ErrPacketTooLarge = errors.New("packet exceeds configured maximum packet size")

// DecodeError 描述报文解析失败，Reason 可区分失败原因。
// Cause 非 nil 时给出可通过 errors.Is 判定的具体原因（如 ErrPacketTooLarge）。
type DecodeError struct {
	Reason string
	Cause  error
}

func (e *DecodeError) Error() string { return "mqtt decode: " + e.Reason }

func (e *DecodeError) Unwrap() error { return e.Cause }

func decodeErr(format string, args ...any) *DecodeError {
	return &DecodeError{Reason: fmt.Sprintf(format, args...)}
}

func decodeErrCause(cause error, format string, args ...any) *DecodeError {
	return &DecodeError{Reason: fmt.Sprintf(format, args...), Cause: cause}
}

// maxRemainingLength 是协议允许的最大剩余长度。
const maxRemainingLength = 268435455

// 各报文类型要求的固定报头标志位。
var requiredFlags = map[Type]byte{
	TypeConnect:     0,
	TypeConnAck:     0,
	TypePubAck:      0,
	TypePubRec:      0,
	TypePubRel:      2,
	TypePubComp:     0,
	TypeSubscribe:   2,
	TypeSubAck:      0,
	TypeUnsubscribe: 2,
	TypeUnsubAck:    0,
	TypePingReq:     0,
	TypePingResp:    0,
	TypeDisconnect:  0,
}

// ReadPacket 从 r 读取并解析一个完整报文，不做单条大小限制（协议上限 256MiB）。
func ReadPacket(r io.Reader) (*Packet, error) {
	return ReadPacketLimit(r, 0)
}

// ReadPacketLimit 从 r 读取并解析一个完整报文。maxBytes>0 时，若报文声明的
// 剩余长度（即去掉固定报头后的全部字节）超过该上限，立即返回可通过
// errors.Is(err, ErrPacketTooLarge) 判定的错误，且不再读取/缓冲该报文主体。
func ReadPacketLimit(r io.Reader, maxBytes int) (*Packet, error) {
	var hdr [1]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	t := Type(hdr[0] >> 4)
	flags := hdr[0] & 0x0F
	if t < TypeConnect || t > TypeDisconnect {
		return nil, decodeErr("reserved packet type %d", t)
	}
	if t != TypePublish {
		want, ok := requiredFlags[t]
		if !ok {
			return nil, decodeErr("no flag rule for type %d", t)
		}
		if flags != want {
			return nil, decodeErr("%s: invalid fixed header flags 0x%x, want 0x%x", t, flags, want)
		}
	}

	remLen, err := readRemainingLength(r)
	if err != nil {
		return nil, err
	}
	if maxBytes > 0 && remLen > maxBytes {
		return nil, decodeErrCause(ErrPacketTooLarge,
			"%s: remaining length %d exceeds configured max %d bytes", t, remLen, maxBytes)
	}
	body := make([]byte, remLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, decodeErr("short body: %v", err)
	}

	p := &Packet{Type: t, Flags: flags, Remaining: body}
	if err := p.decodeBody(body); err != nil {
		return nil, err
	}
	return p, nil
}

func readRemainingLength(r io.Reader) (int, error) {
	var buf [1]byte
	mult := 1
	val := 0
	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, err
		}
		val += int(buf[0]&127) * mult
		if buf[0]&128 == 0 {
			return val, nil
		}
		mult *= 128
	}
	return 0, decodeErr("malformed remaining length (exceeds 4 bytes)")
}

func (p *Packet) decodeBody(b []byte) error {
	r := &reader{buf: b}
	var err error
	switch p.Type {
	case TypeConnect:
		err = p.decodeConnect(r)
	case TypePublish:
		p.Dup = p.Flags&8 != 0
		p.QoS = (p.Flags >> 1) & 3
		p.Retain = p.Flags&1 != 0
		if p.QoS == 3 {
			return decodeErr("PUBLISH: QoS 3 is reserved")
		}
		if p.Dup && p.QoS == 0 {
			return decodeErr("PUBLISH: DUP set with QoS 0")
		}
		err = p.decodePublish(r)
	case TypePubAck, TypePubRec, TypePubRel, TypePubComp:
		p.PacketID, err = r.uint16()
		if err == nil && p.PacketID == 0 {
			err = decodeErr("%s: packet id must be non-zero", p.Type)
		}
	case TypeSubscribe:
		err = p.decodeSubscribe(r)
	case TypeUnsubscribe:
		err = p.decodeUnsubscribe(r)
	case TypeConnAck:
		var sp byte
		sp, err = r.byte()
		if err == nil {
			if sp > 1 {
				err = decodeErr("CONNACK: session present flag must be 0 or 1, got %d", sp)
			} else {
				p.SessionPresent = sp == 1
				p.ReturnCode, err = r.byte()
			}
		}
	case TypeSubAck:
		if p.PacketID, err = r.uint16(); err == nil {
			p.ReasonCodes = append([]byte(nil), r.buf[r.pos:]...)
			r.pos = len(r.buf)
			if len(p.ReasonCodes) == 0 {
				err = decodeErr("SUBACK: no reason codes")
			}
		}
	case TypeUnsubAck:
		p.PacketID, err = r.uint16()
	case TypePingReq, TypePingResp, TypeDisconnect:
		if len(b) != 0 {
			err = decodeErr("%s: remaining length must be 0, got %d", p.Type, len(b))
		}
	default:
		err = decodeErr("cannot decode packet type %d from wire", p.Type)
	}
	if err != nil {
		return err
	}
	if r.left() != 0 {
		return decodeErr("%s: %d trailing bytes after body", p.Type, r.left())
	}
	return nil
}

func (p *Packet) decodeConnect(r *reader) error {
	var err error
	if p.ProtocolName, err = r.str(); err != nil {
		return err
	}
	if p.ProtocolName != "MQTT" {
		return decodeErr("CONNECT: unsupported protocol name %q", p.ProtocolName)
	}
	if p.ProtocolLevel, err = r.byte(); err != nil {
		return err
	}
	if p.ProtocolLevel != 4 {
		return decodeErr("CONNECT: unsupported protocol level %d (only 3.1.1 supported)", p.ProtocolLevel)
	}
	var flags byte
	if flags, err = r.byte(); err != nil {
		return err
	}
	if flags&0x01 != 0 {
		return decodeErr("CONNECT: reserved connect flag bit set")
	}
	p.CleanSession = flags&0x02 != 0
	p.HasWill = flags&0x04 != 0
	p.WillQoS = (flags >> 3) & 3
	p.WillRetain = flags&0x20 != 0
	hasPassword := flags&0x40 != 0
	hasUsername := flags&0x80 != 0
	if !p.HasWill && (p.WillQoS != 0 || p.WillRetain) {
		return decodeErr("CONNECT: will qos/retain set without will flag")
	}
	if p.WillQoS == 3 {
		return decodeErr("CONNECT: will QoS 3 is reserved")
	}
	if p.KeepAlive, err = r.uint16(); err != nil {
		return err
	}
	if p.ClientID, err = r.str(); err != nil {
		return err
	}
	if p.HasWill {
		if p.WillTopic, err = r.str(); err != nil {
			return err
		}
		if p.WillPayload, err = r.binary(); err != nil {
			return err
		}
	}
	if hasUsername {
		if p.Username, err = r.str(); err != nil {
			return err
		}
	}
	if hasPassword {
		if p.Password, err = r.binary(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Packet) decodePublish(r *reader) error {
	var err error
	if p.Topic, err = r.str(); err != nil {
		return err
	}
	if p.Topic == "" {
		return decodeErr("PUBLISH: empty topic name")
	}
	if p.QoS > 0 {
		if p.PacketID, err = r.uint16(); err != nil {
			return err
		}
		if p.PacketID == 0 {
			return decodeErr("PUBLISH: packet id must be non-zero")
		}
	}
	p.Payload = append([]byte(nil), r.buf[r.pos:]...)
	r.pos = len(r.buf)
	return nil
}

func (p *Packet) decodeSubscribe(r *reader) error {
	var err error
	if p.PacketID, err = r.uint16(); err != nil {
		return err
	}
	if p.PacketID == 0 {
		return decodeErr("SUBSCRIBE: packet id must be non-zero")
	}
	for r.left() > 0 {
		var sub Subscription
		if sub.Topic, err = r.str(); err != nil {
			return err
		}
		var qos byte
		if qos, err = r.byte(); err != nil {
			return err
		}
		if qos > 2 {
			return decodeErr("SUBSCRIBE: requested QoS %d is reserved", qos)
		}
		sub.QoS = qos
		p.Subscriptions = append(p.Subscriptions, sub)
	}
	if len(p.Subscriptions) == 0 {
		return decodeErr("SUBSCRIBE: no topic filters")
	}
	return nil
}

func (p *Packet) decodeUnsubscribe(r *reader) error {
	var err error
	if p.PacketID, err = r.uint16(); err != nil {
		return err
	}
	if p.PacketID == 0 {
		return decodeErr("UNSUBSCRIBE: packet id must be non-zero")
	}
	for r.left() > 0 {
		var sub Subscription
		if sub.Topic, err = r.str(); err != nil {
			return err
		}
		p.Subscriptions = append(p.Subscriptions, sub)
	}
	if len(p.Subscriptions) == 0 {
		return decodeErr("UNSUBSCRIBE: no topic filters")
	}
	return nil
}

// Encode 将报文序列化为字节流。
func Encode(p *Packet) ([]byte, error) {
	body, err := p.encodeBody()
	if err != nil {
		return nil, err
	}
	flags := p.Flags
	if p.Type != TypePublish {
		if want, ok := requiredFlags[p.Type]; ok {
			flags = want
		}
	} else {
		flags = 0
		if p.Dup {
			flags |= 8
		}
		flags |= (p.QoS & 3) << 1
		if p.Retain {
			flags |= 1
		}
	}
	if len(body) > maxRemainingLength {
		return nil, decodeErr("body too large: %d bytes", len(body))
	}
	out := []byte{byte(p.Type)<<4 | flags}
	out = appendRemainingLength(out, len(body))
	return append(out, body...), nil
}

func (p *Packet) encodeBody() ([]byte, error) {
	w := &bytes.Buffer{}
	switch p.Type {
	case TypeConnect:
		putStr(w, "MQTT")
		w.WriteByte(4)
		var flags byte
		if p.CleanSession {
			flags |= 0x02
		}
		if p.HasWill {
			flags |= 0x04 | (p.WillQoS&3)<<3
			if p.WillRetain {
				flags |= 0x20
			}
		}
		if p.Password != nil {
			flags |= 0x40
		}
		if p.Username != "" {
			flags |= 0x80
		}
		w.WriteByte(flags)
		putUint16(w, p.KeepAlive)
		putStr(w, p.ClientID)
		if p.HasWill {
			putStr(w, p.WillTopic)
			putBinary(w, p.WillPayload)
		}
		if p.Username != "" {
			putStr(w, p.Username)
		}
		if p.Password != nil {
			putBinary(w, p.Password)
		}
	case TypeConnAck:
		var sp byte
		if p.SessionPresent {
			sp = 1
		}
		w.WriteByte(sp)
		w.WriteByte(p.ReturnCode)
	case TypePublish:
		if p.Topic == "" {
			return nil, decodeErr("PUBLISH: empty topic name")
		}
		if p.QoS > 0 && p.PacketID == 0 {
			return nil, decodeErr("PUBLISH: packet id required for QoS>0")
		}
		putStr(w, p.Topic)
		if p.QoS > 0 {
			putUint16(w, p.PacketID)
		}
		w.Write(p.Payload)
	case TypePubAck, TypePubRec, TypePubRel, TypePubComp:
		putUint16(w, p.PacketID)
	case TypeSubscribe:
		if len(p.Subscriptions) == 0 {
			return nil, decodeErr("SUBSCRIBE: no topic filters")
		}
		putUint16(w, p.PacketID)
		for _, s := range p.Subscriptions {
			putStr(w, s.Topic)
			w.WriteByte(s.QoS)
		}
	case TypeSubAck:
		putUint16(w, p.PacketID)
		w.Write(p.ReasonCodes)
	case TypeUnsubscribe:
		if len(p.Subscriptions) == 0 {
			return nil, decodeErr("UNSUBSCRIBE: no topic filters")
		}
		putUint16(w, p.PacketID)
		for _, s := range p.Subscriptions {
			putStr(w, s.Topic)
		}
	case TypeUnsubAck:
		putUint16(w, p.PacketID)
	case TypePingReq, TypePingResp, TypeDisconnect:
		// 空主体
	default:
		return nil, decodeErr("cannot encode packet type %d", p.Type)
	}
	return w.Bytes(), nil
}

func appendRemainingLength(out []byte, n int) []byte {
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 128
		}
		out = append(out, d)
		if n == 0 {
			return out
		}
	}
}

// reader 是带边界检查的字节游标。
type reader struct {
	buf []byte
	pos int
}

func (r *reader) left() int { return len(r.buf) - r.pos }

func (r *reader) byte() (byte, error) {
	if r.left() < 1 {
		return 0, decodeErr("unexpected end of packet")
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *reader) uint16() (uint16, error) {
	if r.left() < 2 {
		return 0, decodeErr("unexpected end of packet reading uint16")
	}
	v := uint16(r.buf[r.pos])<<8 | uint16(r.buf[r.pos+1])
	r.pos += 2
	return v, nil
}

func (r *reader) binary() ([]byte, error) {
	n, err := r.uint16()
	if err != nil {
		return nil, err
	}
	if r.left() < int(n) {
		return nil, decodeErr("length-prefixed field overruns packet")
	}
	v := append([]byte(nil), r.buf[r.pos:r.pos+int(n)]...)
	r.pos += int(n)
	return v, nil
}

func (r *reader) str() (string, error) {
	b, err := r.binary()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", decodeErr("string field is not valid UTF-8")
	}
	for _, c := range b {
		if c == 0 {
			return "", decodeErr("string field contains U+0000")
		}
	}
	return string(b), nil
}

func putStr(w *bytes.Buffer, s string) {
	putUint16(w, uint16(len(s)))
	w.WriteString(s)
}

func putBinary(w *bytes.Buffer, b []byte) {
	putUint16(w, uint16(len(b)))
	w.Write(b)
}

func putUint16(w *bytes.Buffer, v uint16) {
	w.WriteByte(byte(v >> 8))
	w.WriteByte(byte(v))
}
