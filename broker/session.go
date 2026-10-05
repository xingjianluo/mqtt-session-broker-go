package broker

import (
	"sync"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/packet"
)

// outState 是会话内出站 QoS1/2 消息的状态。
type outState int

const (
	outStateQueued   outState = iota // 在队列中，尚未发送
	outStateWaitAck                  // QoS1: 已发 PUBLISH，等 PUBACK
	outStateWaitRec                  // QoS2: 已发 PUBLISH，等 PUBREC
	outStateWaitComp                 // QoS2: 已发 PUBREL，等 PUBCOMP
)

func (s outState) String() string {
	switch s {
	case outStateQueued:
		return "QUEUED"
	case outStateWaitAck:
		return "WAIT_PUBACK"
	case outStateWaitRec:
		return "WAIT_PUBREC"
	case outStateWaitComp:
		return "WAIT_PUBCOMP"
	default:
		return "UNKNOWN"
	}
}

// outbound 是会话中的一条待投递消息。
type outbound struct {
	msg   *packet.Packet
	state outState
}

// Session 保存一个客户端会话的全部状态。
type Session struct {
	mu        sync.Mutex
	ClientID  string
	Clean     bool
	createdAt time.Time

	subs map[string]byte // filter -> qos

	outbox map[uint16]*outbound      // packetID -> 出站消息（含队列与在途）
	inQoS2 map[uint16]*packet.Packet // packetID -> 入站 QoS2 消息（等 PUBREL）
	order  []uint16                  // 出站消息入队顺序（保证 FIFO）

	quota int // outbox 配额（条数），<=0 表示不限

	connected bool
	conn      *conn // 当前绑定连接

	expireTimer *time.Timer
	onExpire    func(clientID string)
}

func newSession(clientID string, clean bool, quota int) *Session {
	return &Session{
		ClientID:  clientID,
		Clean:     clean,
		createdAt: time.Now(),
		subs:      make(map[string]byte),
		outbox:    make(map[uint16]*outbound),
		inQoS2:    make(map[uint16]*packet.Packet),
		quota:     quota,
	}
}

// backlogLocked 返回当前积压条数（队列 + 在途）。调用方需持锁。
func (s *Session) backlogLocked() int { return len(s.outbox) }

// backlogSize 返回当前积压条数（线程安全，供日志/测试观测）。
func (s *Session) backlogSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.outbox)
}

// isConnected 返回当前会话是否绑定在线连接（供日志/测试观测）。
func (s *Session) isConnected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected
}

// allocPacketIDLocked 分配未使用的出站 packetID。调用方需持锁。
func (s *Session) allocPacketIDLocked() uint16 {
	for i := 0; i < 0xFFFF; i++ {
		id := uint16((int(s.createdAt.UnixNano()) + i) % 0xFFFF)
		id++
		if _, used := s.outbox[id]; !used {
			return id
		}
	}
	return 0 // 全部占用（配额应先于此触发）
}

// enqueue 将一条投递消息加入会话。effective QoS 已在 msg.QoS 中。
// QoS0 且未连接时直接丢弃（协议允许）；超配额返回 ErrQuotaExceeded。
// 返回是否入队成功。
func (s *Session) enqueue(msg *packet.Packet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.QoS == 0 && !s.connected {
		return nil // QoS0 离线即丢弃
	}
	if s.quota > 0 && s.backlogLocked() >= s.quota {
		return ErrQuotaExceeded
	}
	// 所有入队消息都分配内部 packetID 作为 outbox 键（QoS0 的 ID 不上报文）
	msg.PacketID = s.allocPacketIDLocked()
	s.outbox[msg.PacketID] = &outbound{msg: msg, state: outStateQueued}
	s.order = append(s.order, msg.PacketID)
	return nil
}

// pendingLocked 按序返回所有待发送/待重传的出站消息。调用方需持锁。
func (s *Session) pendingLocked() []*outbound {
	out := make([]*outbound, 0, len(s.order))
	for _, id := range s.order {
		if o, ok := s.outbox[id]; ok {
			out = append(out, o)
		}
	}
	return out
}

// ackOutbound 处理 PUBACK：完成 QoS1 出站消息。返回是否存在。
func (s *Session) ackOutbound(id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.outbox[id]
	if !ok || o.state != outStateWaitAck {
		return false
	}
	s.removeOutboundLocked(id)
	return true
}

// recOutbound 处理 PUBREC：QoS2 出站进入等 PUBCOMP。
// 返回是否需要发送 PUBREL。
func (s *Session) recOutbound(id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.outbox[id]
	if !ok || o.state != outStateWaitRec {
		return false
	}
	o.state = outStateWaitComp
	return true
}

// compOutbound 处理 PUBCOMP：完成 QoS2 出站消息。
func (s *Session) compOutbound(id uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.outbox[id]
	if !ok || o.state != outStateWaitComp {
		return false
	}
	s.removeOutboundLocked(id)
	return true
}

func (s *Session) removeOutboundLocked(id uint16) {
	delete(s.outbox, id)
	for i, v := range s.order {
		if v == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// storeInboundQoS2 记录入站 QoS2 消息。返回 true 表示是重复包（已存在）。
func (s *Session) storeInboundQoS2(id uint16, msg *packet.Packet) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inQoS2[id]; ok {
		return true
	}
	s.inQoS2[id] = msg
	return false
}

// releaseInboundQoS2 处理 PUBREL：取出并删除入站 QoS2 消息。
// 返回 nil 表示未知 packetID。
func (s *Session) releaseInboundQoS2(id uint16) *packet.Packet {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.inQoS2[id]
	if !ok {
		return nil
	}
	delete(s.inQoS2, id)
	return m
}

// setConnected 绑定/解绑连接。
func (s *Session) setConnected(c *conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = c
	s.connected = c != nil
}

// getConn 返回当前绑定连接。
func (s *Session) getConn() *conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// startExpiry 在 d 后触发会话过期；d<=0 时不过期。
func (s *Session) startExpiry(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopExpiryLocked()
	if d > 0 && s.onExpire != nil {
		s.expireTimer = time.AfterFunc(d, func() { s.onExpire(s.ClientID) })
	}
}

// stopExpiry 取消过期计时。
func (s *Session) stopExpiry() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopExpiryLocked()
}

func (s *Session) stopExpiryLocked() {
	if s.expireTimer != nil {
		s.expireTimer.Stop()
		s.expireTimer = nil
	}
}
