// Package broker 实现 MQTT 会话代理核心：订阅路由、QoS 状态机、配额管理。
package broker

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/packet"
	"github.com/xingjianluo/mqtt-session-broker-go/topic"
)

// 可区分的拒绝/断开原因。
var (
	ErrQuotaExceeded   = errors.New("session queue quota exceeded")
	ErrInvalidTopic    = errors.New("invalid topic")
	ErrInvalidQoS      = errors.New("invalid qos")
	ErrSessionTaken    = errors.New("client id taken over by new connection")
	ErrSlowConsumer    = errors.New("connection write backlog full (slow consumer)")
	ErrMalformedPacket = errors.New("malformed packet")
)

// Config 是代理配置。
type Config struct {
	MaxQueuePerSession int   // 每会话积压（队列+在途）配额，<=0 不限
	SessionExpiry      int64 // 持久会话过期秒数，<=0 不过期
}

// Broker 是消息代理核心。
type Broker struct {
	cfg      Config
	mu       sync.Mutex
	sessions map[string]*Session
	router   *router
}

// New 创建代理。
func New(cfg Config) *Broker {
	return &Broker{
		cfg:      cfg,
		sessions: make(map[string]*Session),
		router:   newRouter(),
	}
}

// CONNACK 返回码。
const (
	rcAccepted          byte = 0x00
	rcBadProtocol       byte = 0x01
	rcIdentifierRejected byte = 0x02
)

// SUBACK 失败码。
const subAckFailure byte = 0x80

// connect 处理已解析的 CONNECT 报文，返回会话与 CONNACK。
// 若同 ClientID 已有连接，旧连接被接管断开（会话状态保留）。
func (b *Broker) connect(cp *packet.Packet, c *conn) (*Session, *packet.Packet, error) {
	if cp.ClientID == "" {
		if !cp.CleanSession {
			return nil, connAck(rcIdentifierRejected), errors.New("empty client id requires clean session")
		}
		cp.ClientID = "auto-" + randomID()
	}
	b.mu.Lock()
	sess, exists := b.sessions[cp.ClientID]
	if cp.CleanSession {
		if exists {
			b.dropSessionLocked(sess)
		}
		sess = newSession(cp.ClientID, true, b.cfg.MaxQueuePerSession)
		b.sessions[cp.ClientID] = sess
		exists = false
	} else if !exists {
		sess = newSession(cp.ClientID, false, b.cfg.MaxQueuePerSession)
		b.sessions[cp.ClientID] = sess
	}
	b.mu.Unlock()

	sess.stopExpiry()
	sess.onExpire = b.expireSession

	// 先绑定新连接再关闭旧连接，避免旧连接的 disconnect 回调误清新会话
	old := sess.getConn()
	sess.setConnected(c)
	c.session = sess
	if old != nil && old != c {
		log.Printf("[broker] client=%q takeover: closing old connection reason=%v", sess.ClientID, ErrSessionTaken)
		old.closeWithReason(ErrSessionTaken)
	}

	ack := &packet.Packet{Type: packet.TypeConnAck, SessionPresent: !cp.CleanSession && exists, ReturnCode: rcAccepted}
	log.Printf("[broker] CONNECT client=%q clean=%v sessionPresent=%v", sess.ClientID, cp.CleanSession, ack.SessionPresent)
	return sess, ack, nil
}

func connAck(rc byte) *packet.Packet {
	return &packet.Packet{Type: packet.TypeConnAck, ReturnCode: rc}
}

// disconnect 处理连接结束。graceful 为 true 表示收到 DISCONNECT（不发遗嘱）。
// will 为连接上登记的遗嘱消息。
func (b *Broker) disconnect(sess *Session, c *conn, will *packet.Packet, graceful bool, cause error) {
	if sess == nil {
		return
	}
	// 仅当当前连接仍是绑定连接时才解绑，避免误清接管后的新连接。
	if sess.getConn() != c {
		return
	}
	sess.setConnected(nil)
	log.Printf("[broker] disconnect client=%q graceful=%v cause=%v", sess.ClientID, graceful, cause)

	if will != nil && !graceful {
		log.Printf("[broker] publishing will client=%q topic=%q", sess.ClientID, will.Topic)
		b.Publish(will)
	}
	if sess.Clean {
		b.mu.Lock()
		if cur, ok := b.sessions[sess.ClientID]; ok && cur == sess {
			b.dropSessionLocked(sess)
		}
		b.mu.Unlock()
	} else if b.cfg.SessionExpiry > 0 {
		sess.startExpiry(time.Duration(b.cfg.SessionExpiry) * time.Second)
	}
}

// dropSessionLocked 删除会话及其订阅与积压。调用方需持 b.mu。
func (b *Broker) dropSessionLocked(sess *Session) {
	delete(b.sessions, sess.ClientID)
	b.router.removeAll(sess.ClientID)
	sess.stopExpiry()
	log.Printf("[broker] session dropped client=%q (subscriptions and backlog discarded)", sess.ClientID)
}

// expireSession 会话过期回调。
func (b *Broker) expireSession(clientID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sess, ok := b.sessions[clientID]; ok && !sess.connected {
		log.Printf("[broker] session expired client=%q", clientID)
		b.dropSessionLocked(sess)
	}
}

// subscribe 处理订阅请求，返回每个过滤器对应的授予码（QoS 或 0x80）。
func (b *Broker) subscribe(sess *Session, subs []packet.Subscription) []byte {
	codes := make([]byte, len(subs))
	for i, sub := range subs {
		if err := topic.ValidateFilter(sub.Topic); err != nil {
			log.Printf("[broker] SUBSCRIBE rejected client=%q filter=%q reason=%v", sess.ClientID, sub.Topic, err)
			codes[i] = subAckFailure
			continue
		}
		if sub.QoS > 2 {
			log.Printf("[broker] SUBSCRIBE rejected client=%q filter=%q reason=%v", sess.ClientID, sub.Topic, ErrInvalidQoS)
			codes[i] = subAckFailure
			continue
		}
		sess.mu.Lock()
		sess.subs[sub.Topic] = sub.QoS
		sess.mu.Unlock()
		b.router.add(sess.ClientID, sub.Topic, sub.QoS)
		codes[i] = sub.QoS
		log.Printf("[broker] SUBSCRIBE ok client=%q filter=%q qos=%d", sess.ClientID, sub.Topic, sub.QoS)
	}
	return codes
}

// unsubscribe 处理退订。
func (b *Broker) unsubscribe(sess *Session, subs []packet.Subscription) {
	for _, sub := range subs {
		sess.mu.Lock()
		delete(sess.subs, sub.Topic)
		sess.mu.Unlock()
		b.router.remove(sess.ClientID, sub.Topic)
		log.Printf("[broker] UNSUBSCRIBE client=%q filter=%q", sess.ClientID, sub.Topic)
	}
}

// Publish 将一条应用消息路由给所有匹配订阅者（含离线持久会话排队）。
// 供入站 PUBLISH 与遗嘱发布调用。主题非法时返回 ErrInvalidTopic。
func (b *Broker) Publish(msg *packet.Packet) error {
	if err := topic.ValidateName(msg.Topic); err != nil {
		log.Printf("[broker] PUBLISH rejected topic=%q reason=%v", msg.Topic, err)
		return ErrInvalidTopic
	}
	matches := b.router.match(msg.Topic)
	log.Printf("[broker] PUBLISH topic=%q qos=%d matched=%d subscribers", msg.Topic, msg.QoS, len(matches))
	for clientID, subQoS := range matches {
		b.mu.Lock()
		sess, ok := b.sessions[clientID]
		b.mu.Unlock()
		if !ok {
			continue
		}
		eff := msg.QoS
		if subQoS < eff {
			eff = subQoS
		}
		out := &packet.Packet{
			Type:    packet.TypePublish,
			Topic:   msg.Topic,
			Payload: msg.Payload,
			QoS:     eff,
			Retain:  msg.Retain,
		}
		if err := sess.enqueue(out); err != nil {
			log.Printf("[broker] enqueue failed client=%q reason=%v -> disconnecting", clientID, err)
			if c := sess.getConn(); c != nil {
				c.closeWithReason(err)
			}
			continue
		}
		b.pump(sess)
	}
	return nil
}

// pump 尝试把会话中可发送的出站消息推到当前连接。
func (b *Broker) pump(sess *Session) {
	c := sess.getConn()
	if c == nil {
		return
	}
	sess.mu.Lock()
	pending := sess.pendingLocked()
	sess.mu.Unlock()
	for _, o := range pending {
		// 检查与状态翻转必须在同一把锁内完成，否则并发 pump 会重复投递
		sess.mu.Lock()
		if o.state != outStateQueued {
			sess.mu.Unlock()
			continue
		}
		m := *o.msg
		switch m.QoS {
		case packet.QoS1:
			o.state = outStateWaitAck
		case packet.QoS2:
			o.state = outStateWaitRec
		default: // QoS0 发出即完成
			sess.removeOutboundLocked(m.PacketID)
		}
		sess.mu.Unlock()
		if err := c.send(&m); err != nil {
			log.Printf("[broker] send failed client=%q err=%v", sess.ClientID, err)
			return
		}
	}
}

// resendPending 重连后恢复投递：在途消息按状态机重发，队列消息首发。
func (b *Broker) resendPending(sess *Session, c *conn) {
	sess.mu.Lock()
	pending := sess.pendingLocked()
	sess.mu.Unlock()
	for _, o := range pending {
		var m *packet.Packet
		sess.mu.Lock()
		switch o.state {
		case outStateWaitAck: // QoS1 在途：重发 PUBLISH + DUP
			cp := *o.msg
			cp.Dup = true
			m = &cp
		case outStateWaitRec: // QoS2 已发未确认：重发 PUBLISH + DUP
			cp := *o.msg
			cp.Dup = true
			m = &cp
		case outStateWaitComp: // QoS2 等 PUBCOMP：重发 PUBREL
			m = &packet.Packet{Type: packet.TypePubRel, PacketID: o.msg.PacketID}
		case outStateQueued:
			cp := *o.msg
			m = &cp
			switch m.QoS {
			case packet.QoS1:
				o.state = outStateWaitAck
			case packet.QoS2:
				o.state = outStateWaitRec
			default:
				sess.removeOutboundLocked(m.PacketID)
			}
		}
		sess.mu.Unlock()
		if m == nil {
			continue
		}
		log.Printf("[broker] resume client=%q packetID=%d state=%s type=%s", sess.ClientID, o.msg.PacketID, o.state, m.Type)
		if err := c.send(m); err != nil {
			log.Printf("[broker] resume send failed client=%q err=%v", sess.ClientID, err)
			return
		}
	}
}

// handleInbound 处理已连接会话收到的一条报文（PUBLISH/PUBACK/... 之外由 conn 分发）。
func (b *Broker) handleInboundPublish(sess *Session, p *packet.Packet, c *conn) error {
	if err := topic.ValidateName(p.Topic); err != nil {
		log.Printf("[broker] inbound PUBLISH rejected client=%q topic=%q reason=%v", sess.ClientID, p.Topic, err)
		return ErrInvalidTopic
	}
	switch p.QoS {
	case packet.QoS0:
		return b.Publish(p)
	case packet.QoS1:
		err := b.Publish(p)
		if err == nil {
			err = c.send(&packet.Packet{Type: packet.TypePubAck, PacketID: p.PacketID})
		}
		return err
	case packet.QoS2:
		if dup := sess.storeInboundQoS2(p.PacketID, p); dup {
			log.Printf("[broker] inbound QoS2 dup client=%q packetID=%d -> resend PUBREC, no redelivery", sess.ClientID, p.PacketID)
		}
		return c.send(&packet.Packet{Type: packet.TypePubRec, PacketID: p.PacketID})
	}
	return ErrInvalidQoS
}

// handlePubRel 处理 PUBREL：完成入站 QoS2 投递。
func (b *Broker) handlePubRel(sess *Session, id uint16, c *conn) error {
	msg := sess.releaseInboundQoS2(id)
	if msg != nil {
		if err := b.Publish(msg); err != nil {
			return err
		}
	}
	return c.send(&packet.Packet{Type: packet.TypePubComp, PacketID: id})
}
