package broker

import (
	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
	"github.com/xingjianluo/mqtt-session-broker-go/internal/session"
	"github.com/xingjianluo/mqtt-session-broker-go/internal/topic"
)

// dispatch 处理握手之后的单个报文。返回非空 reason 表示协议层要求立即断开连接。
func (c *clientConn) dispatch(f *mqttproto.Frame) (DisconnectReason, string) {
	switch f.Type {
	case mqttproto.TypePINGREQ:
		if len(f.Body) != 0 {
			return ReasonMalformedPacket, "PINGREQ must have zero-length body"
		}
		if !c.send(mqttproto.EncodePingresp()) {
			return ReasonClientClosed, "cannot send PINGRESP"
		}
		return "", ""

	case mqttproto.TypeDISCONNECT:
		if len(f.Body) != 0 {
			return ReasonMalformedPacket, "DISCONNECT must have zero-length body"
		}
		c.markNormalDisconnect()
		return ReasonClientClosed, "client sent DISCONNECT"

	case mqttproto.TypePUBLISH:
		return c.handleIncomingPublish(f)

	case mqttproto.TypePUBACK:
		id, err := mqttproto.DecodePacketID(f.Body)
		if err != nil {
			return ReasonMalformedPacket, err.Error()
		}
		c.sess.AckQoS1(id)
		return "", ""

	case mqttproto.TypePUBREC:
		id, err := mqttproto.DecodePacketID(f.Body)
		if err != nil {
			return ReasonMalformedPacket, err.Error()
		}
		if c.sess.RecvPUBREC(id) {
			c.send(mqttproto.EncodeAck(mqttproto.TypePUBREL, id))
		}
		// 未知标识符的 PUBREC：按 §3.5.1 记录即可，不断开。
		return "", ""

	case mqttproto.TypePUBREL:
		// PUBREL 固定头保留位必须为 0010。
		if f.Flags != 0x02 {
			return ReasonProtocolViolation, "PUBREL fixed header reserved bits must be 0010"
		}
		id, err := mqttproto.DecodePacketID(f.Body)
		if err != nil {
			return ReasonMalformedPacket, err.Error()
		}
		// 无论是否认识该标识符都回复 PUBCOMP（§3.6.2-2）；未知标识符不重复交付。
		c.sess.NoteIncomingPubrel(id)
		c.send(mqttproto.EncodeAck(mqttproto.TypePUBCOMP, id))
		return "", ""

	case mqttproto.TypePUBCOMP:
		id, err := mqttproto.DecodePacketID(f.Body)
		if err != nil {
			return ReasonMalformedPacket, err.Error()
		}
		c.sess.RecvPUBCOMP(id)
		return "", ""

	case mqttproto.TypeSUBSCRIBE:
		if f.Flags != 0x02 {
			return ReasonProtocolViolation, "SUBSCRIBE fixed header reserved bits must be 0010"
		}
		return c.handleSubscribe(f)

	case mqttproto.TypeUNSUBSCRIBE:
		if f.Flags != 0x02 {
			return ReasonProtocolViolation, "UNSUBSCRIBE fixed header reserved bits must be 0010"
		}
		return c.handleUnsubscribe(f)

	case mqttproto.TypeCONNECT:
		return ReasonProtocolViolation, "second CONNECT received on same connection"

	case mqttproto.TypeRESERVED, mqttproto.TypeCONNACK, mqttproto.TypeSUBACK,
		mqttproto.TypeUNSUBACK, mqttproto.TypePINGRESP, mqttproto.TypeAUTH:
		return ReasonProtocolViolation, "client must not send " + f.Type.String()

	default:
		return ReasonMalformedPacket, "unknown packet type"
	}
}

// handleIncomingPublish 处理客户端发布的消息（含入站 QoS1/2 状态机）。
func (c *clientConn) handleIncomingPublish(f *mqttproto.Frame) (DisconnectReason, string) {
	p, err := mqttproto.DecodePublish(f.Flags, f.Body)
	if err != nil {
		if re, ok := mqttproto.AsRejectError(err); ok && re.Code == mqttproto.RejectProtocolViolation {
			return ReasonProtocolViolation, err.Error()
		}
		return ReasonMalformedPacket, err.Error()
	}
	if verr := topic.ValidateTopic(p.Topic); verr != nil {
		return ReasonProtocolViolation, "invalid publish topic: " + verr.Error()
	}

	switch p.QoS {
	case 0:
		c.broker.routePublish(p.Topic, p.Payload, 0, p.Retain, c.clientID)
	case 1:
		c.broker.routePublish(p.Topic, p.Payload, 1, p.Retain, c.clientID)
		c.send(mqttproto.EncodeAck(mqttproto.TypePUBACK, p.PacketID))
	case 2:
		// 重传的 QoS2 PUBLISH：不重复交付，仅重发 PUBREC。
		if c.sess.NoteIncomingPublish(p.PacketID) {
			c.broker.routePublish(p.Topic, p.Payload, 2, p.Retain, c.clientID)
		}
		c.send(mqttproto.EncodeAck(mqttproto.TypePUBREC, p.PacketID))
	}
	return "", ""
}

// handleSubscribe 处理订阅：过滤器逐条校验，给出可区分的逐条 SUBACK 原因码。
func (c *clientConn) handleSubscribe(f *mqttproto.Frame) (DisconnectReason, string) {
	s, err := mqttproto.DecodeSubscribe(f.Body)
	if err != nil {
		return ReasonMalformedPacket, err.Error()
	}
	codes := make([]byte, len(s.Filters))
	for i, sf := range s.Filters {
		if verr := topic.ValidateFilter(sf.Filter); verr != nil {
			codes[i] = 0x80 // Failure
			c.broker.emit(Event{Type: "reject", ClientID: c.clientID,
				Detail: "subscribe filter rejected filter=" + sf.Filter + " reason=" + verr.Error()})
			continue
		}
		granted := sf.QoS
		if granted > 2 {
			granted = 2
		}
		c.broker.trie.Subscribe(sf.Filter, c.clientID, granted)
		codes[i] = granted
		// 订阅建立后投递匹配的保留消息（§3.3.1-3）。
		c.deliverRetained(sf.Filter, granted)
	}
	c.send(mqttproto.EncodeSuback(s.PacketID, codes))
	return "", ""
}

func (c *clientConn) deliverRetained(filter string, grantedQoS byte) {
	for _, rm := range c.broker.retained.match(filter) {
		qos := rm.QoS
		if qos > grantedQoS {
			qos = grantedQoS // 投递 QoS 不超过订阅授予 QoS
		}
		c.deliverToThis(rm.Topic, rm.Payload, qos, true, false)
	}
}

// handleUnsubscribe 处理取消订阅；过滤器不合法直接协议违规断开。
func (c *clientConn) handleUnsubscribe(f *mqttproto.Frame) (DisconnectReason, string) {
	u, err := mqttproto.DecodeUnsubscribe(f.Body)
	if err != nil {
		return ReasonMalformedPacket, err.Error()
	}
	for _, fl := range u.Filters {
		if verr := topic.ValidateFilter(fl); verr != nil {
			return ReasonProtocolViolation, "invalid unsubscribe filter: " + verr.Error()
		}
		c.broker.trie.Unsubscribe(fl, c.clientID)
	}
	c.send(mqttproto.EncodeUnsuback(u.PacketID))
	return "", ""
}

// deliverToThis 把一条要投递给本连接的消息写入出站路径。
// retained 仅用于保留消息首投时的 RETAIN 标记。
func (c *clientConn) deliverToThis(topicName string, payload []byte, qos byte, retained, dup bool) {
	if qos == 0 {
		p := mqttproto.EncodePublish(&mqttproto.Publish{
			Topic: topicName, QoS: 0, Retain: retained, Dup: dup, Payload: payload,
		})
		// QoS0 尽力而为：通道阻塞达到慢消费者时限即触发断开判定。
		c.send(p)
		return
	}
	r := c.sess.Enqueue(topicName, payload, qos, bool2byte(retained))
	if r == session.EnqueueQueueFull {
		c.broker.emit(Event{Type: "drop", ClientID: c.clientID, Reason: ReasonQueueQuotaExceeded,
			Detail: "message to " + topicName + " rejected: session queue quota exhausted"})
		c.fail(ReasonQueueQuotaExceeded, "session queue quota exhausted while delivering to "+topicName)
	}
}

func bool2byte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
