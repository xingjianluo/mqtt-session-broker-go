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
	ErrPacketTooLarge  = errors.New("packet exceeds configured maximum packet size")
)

// Config 是代理配置。
type Config struct {
	MaxQueuePerSession int   // 每会话积压（队列+在途）配额，<=0 不限
	SessionExpiry      int64 // 持久会话过期秒数，<=0 不过期
	// MaxPacketSize 是单条入站报文（剩余长度）字节上限，<=0 使用 DefaultMaxPacketSize。
	// 超限的连接被立即断开，原因 ErrPacketTooLarge（区别于 ErrMalformedPacket）。
	MaxPacketSize int
}

// DefaultMaxPacketSize 是未显式配置 MaxPacketSize 时的默认上限（1 MiB）。
const DefaultMaxPacketSize = 1 << 20

// Broker 是消息代理核心。
type Broker struct {
	cfg           Config
	maxPacketSize int
	mu            sync.Mutex
	sessions      map[string]*Session
	router        *router
	retains       *retainStore
}

// New 创建代理。
func New(cfg Config) *Broker {
	maxPkt := cfg.MaxPacketSize
	if maxPkt <= 0 {
		maxPkt = DefaultMaxPacketSize
	}
	return &Broker{
		cfg:           cfg,
		maxPacketSize: maxPkt,
		sessions:      make(map[string]*Session),
		router:        newRouter(),
		retains:       newRetainStore(),
	}
}

// CONNACK 返回码。
const (
	rcAccepted           byte = 0x00
	rcBadProtocol        byte = 0x01
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
		sess = b.newSessionLocked(cp.ClientID, true)
		exists = false
	} else if !exists {
		sess = b.newSessionLocked(cp.ClientID, false)
		b.sessions[cp.ClientID] = sess
	}
	b.mu.Unlock()

	sess.stopExpiry()
	sess.onExpire = b.expireSession

	// 先绑定新连接再关闭旧连接，避免旧连接的 disconnect 回调误清新会话
	old := sess.getConn()
	// 已存在的持久会话重连（exists==true）：让唯一 drainer 的下一轮先重发在途消息。
	// 新会话（刚创建）无在途消息，不标记恢复。
	if exists {
		sess.mu.Lock()
		sess.markResumeLocked()
		sess.mu.Unlock()
	}
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

// newSessionLocked 创建会话并启动单飞 drainer。调用方持 b.mu。
func (b *Broker) newSessionLocked(clientID string, clean bool) *Session {
	sess := newSession(clientID, clean, b.cfg.MaxQueuePerSession)
	sess.setDrainer(b.drainOnce)
	b.sessions[clientID] = sess
	return sess
}

// dropSessionLocked 删除会话及其订阅与积压。调用方需持 b.mu。
func (b *Broker) dropSessionLocked(sess *Session) {
	delete(b.sessions, sess.ClientID)
	b.router.removeAll(sess.ClientID)
	sess.stopExpiry()
	sess.stopDrainer()
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

// subscribe 处理订阅请求。
// 对成功的过滤器，在代理临界区内完成“登记订阅 + 快照匹配保留消息 + 入队首投”，
// 与并发 Publish 的“更新保留 + 匹配 + 入队”严格串行（锁序 b.mu -> sess.mu），
// 因此保留首投与实时发布不重复、不互相插队。
// 保留消息按话题各投最新一条，Retain=1，投递 QoS=min(保留时QoS, 本次授予QoS)；
// SUBACK 作为控制帧交给唯一 drainer，排在全部保留首投之后发出（协议允许的顺序）。
func (b *Broker) subscribe(sess *Session, packetID uint16, subs []packet.Subscription) error {
	codes := make([]byte, len(subs))
	seen := make(map[string]bool) // 同一 SUBSCRIBE 多过滤器命中同话题时只首投一次
	var failErr error

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

		b.mu.Lock()
		sess.mu.Lock()
		sess.subs[sub.Topic] = sub.QoS
		sess.mu.Unlock()
		b.router.add(sess.ClientID, sub.Topic, sub.QoS)
		retained := b.retains.matchingLocked(sub.Topic)

		deliveredHere := 0
		failed := false
		for _, rm := range retained {
			if seen[rm.topic] {
				continue
			}
			seen[rm.topic] = true
			eff := rm.qos
			if sub.QoS < eff {
				eff = sub.QoS
			}
			out := &packet.Packet{
				Type:    packet.TypePublish,
				Topic:   rm.topic,
				Payload: rm.payload,
				QoS:     eff,
				Retain:  true,
			}
			log.Printf("[broker] retained initial delivery client=%q topic=%q retainQos=%d grantedQos=%d -> qos=%d",
				sess.ClientID, rm.topic, rm.qos, sub.QoS, eff)
			if err := sess.enqueue(out); err != nil {
				log.Printf("[broker] retained enqueue failed client=%q reason=%v -> disconnecting", sess.ClientID, err)
				if c := sess.getConn(); c != nil {
					c.closeWithReason(err)
				}
				failed = true
				failErr = err
				break
			}
			deliveredHere++
		}
		b.mu.Unlock()
		if failed {
			return failErr
		}
		codes[i] = sub.QoS
		log.Printf("[broker] SUBSCRIBE ok client=%q filter=%q qos=%d retainedEnqueued=%d", sess.ClientID, sub.Topic, sub.QoS, deliveredHere)
	}

	ack := &packet.Packet{Type: packet.TypeSubAck, PacketID: packetID, ReasonCodes: codes}
	sess.mu.Lock()
	sess.queueCtrlLocked(ack)
	sess.mu.Unlock()
	sess.notify()
	return nil
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
//
// 保留语义（与 b.mu 保护的订阅登记串行化，避免实时/历史重复或乱序）：
//   - msg.Retain=true 且载荷非空：更新/覆盖该话题的代理级保留状态；
//     已在线订阅者只收到 Retain=0 的实时副本（与普通实时消息一致，不重复推历史）。
//   - msg.Retain=true 且载荷为空：清除该话题保留状态，且不向在线订阅者转发
//     （空载荷保留发布只表示“清除状态”，见 MQTT 3.1.1 §3.3.1 的约定）。
//   - msg.Retain=false：完全不触碰保留状态。
func (b *Broker) Publish(msg *packet.Packet) error {
	if err := topic.ValidateName(msg.Topic); err != nil {
		log.Printf("[broker] PUBLISH rejected topic=%q reason=%v", msg.Topic, err)
		return ErrInvalidTopic
	}

	// 保留状态更新 + 路由匹配 + 各会话入队全部在同一临界区完成，
	// 与 SUBSCRIBE 的“登记+保留快照+入队”严格串行：
	// 实时副本与历史首投因此不会重复，也不会互相插队。
	b.mu.Lock()
	if msg.Retain {
		if len(msg.Payload) == 0 {
			b.retains.storeLocked(msg.Topic, nil, msg.QoS)
			log.Printf("[broker] PUBLISH topic=%q retain=1 empty payload -> retained state cleared, not forwarded", msg.Topic)
			b.mu.Unlock()
			return nil
		}
		b.retains.storeLocked(msg.Topic, msg.Payload, msg.QoS)
		log.Printf("[broker] PUBLISH topic=%q qos=%d retain=1 -> retained state stored/updated", msg.Topic, msg.QoS)
	}
	matches := b.router.match(msg.Topic)
	log.Printf("[broker] PUBLISH topic=%q qos=%d retain=%d matched=%d subscribers", msg.Topic, msg.QoS, boolBit(msg.Retain), len(matches))
	queued := make([]*Session, 0, len(matches))
	for clientID, subQoS := range matches {
		sess, ok := b.sessions[clientID]
		if !ok {
			continue
		}
		eff := msg.QoS
		if subQoS < eff {
			eff = subQoS
		}
		// 在线订阅者收到的始终是实时消息：Retain 位恒为 0，
		// 即使该发布同时更新了保留状态，也不会把历史再补推一遍。
		out := &packet.Packet{
			Type:    packet.TypePublish,
			Topic:   msg.Topic,
			Payload: msg.Payload,
			QoS:     eff,
			Retain:  false,
		}
		if err := sess.enqueue(out); err != nil {
			log.Printf("[broker] enqueue failed client=%q reason=%v -> disconnecting", clientID, err)
			if c := sess.getConn(); c != nil {
				c.closeWithReason(err)
			}
			continue
		}
		queued = append(queued, sess)
	}
	b.mu.Unlock()

	// 实际写出由会话唯一 drainer 完成（锁外、FIFO）。
	for _, sess := range queued {
		sess.notify()
	}
	return nil
}

func boolBit(v bool) int {
	if v {
		return 1
	}
	return 0
}

// drainOnce 由会话唯一的 drainer 调用：严格按 sess.order 的 FIFO 顺序写出。
//   - 常规轮：只首发 QUEUED 消息；在途消息（等 ACK/REC/COMP）保持不动，绝不重复重发。
//   - 重连后的第一个恢复轮：按顺序重发在途消息（QoS1/2 的 PUBLISH 带 DUP；
//     WAIT_PUBCOMP 阶段重发 PUBREL），并首发 QUEUED 消息，随后标志清零。
func (b *Broker) drainOnce(sess *Session) {
	c := sess.getConn()
	if c == nil {
		return
	}
	sess.mu.Lock()
	resuming := sess.takeResumeLocked()
	pending := sess.pendingLocked()
	sess.mu.Unlock()
	for _, o := range pending {
		var m *packet.Packet
		isResumeMsg := false
		sess.mu.Lock()
		switch o.state {
		case outStateWaitAck, outStateWaitRec, outStateWaitComp:
			if !resuming {
				sess.mu.Unlock()
				continue // 常规轮不碰在途消息
			}
			if o.state == outStateWaitComp {
				m = &packet.Packet{Type: packet.TypePubRel, PacketID: o.msg.PacketID}
			} else {
				cp := *o.msg
				cp.Dup = true
				m = &cp
			}
			isResumeMsg = true
		case outStateQueued:
			cp := *o.msg
			m = &cp
			switch m.QoS {
			case packet.QoS1:
				o.state = outStateWaitAck
			case packet.QoS2:
				o.state = outStateWaitRec
			default: // QoS0 发出即完成
				sess.removeOutboundLocked(m.PacketID)
			}
		}
		sess.mu.Unlock()
		if m == nil {
			continue
		}
		if isResumeMsg {
			log.Printf("[broker] resume client=%q packetID=%d state=%s type=%s", sess.ClientID, m.PacketID, o.state, m.Type)
		}
		if err := c.send(m); err != nil {
			log.Printf("[broker] send failed client=%q err=%v", sess.ClientID, err)
			return
		}
	}

	// 数据消息（含保留首投）全部发出后，再发挂起的控制帧（如 SUBACK）。
	sess.mu.Lock()
	ctrl := sess.takeCtrlLocked()
	sess.mu.Unlock()
	if ctrl != nil {
		if err := c.send(ctrl); err != nil {
			log.Printf("[broker] send control frame failed client=%q err=%v", sess.ClientID, err)
		}
	}
}

// resumeDelivery 重连后触发一次按 FIFO 的恢复投递（在途重发 + 队列首发）。
func (b *Broker) resumeDelivery(sess *Session) { sess.notify() }

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
