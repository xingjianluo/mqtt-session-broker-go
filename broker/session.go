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

	// resumeNeeded 为 true 时，drainer 的下一轮先重发在途消息（重连恢复）；
	// 恢复轮结束后清零，此后在途消息不会被普通 kick 重复重发。
	resumeNeeded bool

	expireTimer *time.Timer
	onExpire    func(clientID string)

	// 单飞出站驱动：所有入队路径只 kick，由唯一 drainer 按 order FIFO 写出。
	// 这避免了多个 goroutine（如 SUBSCRIBE 与 PUBLISH）并发 pump 时
	// QoS0 消息在写出通道上互相插队。
	kickCh   chan struct{}
	stopCh   chan struct{}
	runnerMu sync.Mutex
	running  bool
	onKick   func(*Session) // 由 Broker 注入：驱动一次 FIFO 写出

	// ctrl 是紧跟在数据消息之后由 drainer 串行发出的控制帧（如 SUBACK），
	// 保证“保留首投先于 SUBACK”的协议顺序。当前最多挂一个，均在 s.mu 下操作。
	ctrl     *packet.Packet
	ctrlSent bool
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
		kickCh:    make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
}

// setDrainer 注入出站驱动回调并启动单飞 drainer。
func (s *Session) setDrainer(fn func(*Session)) {
	s.runnerMu.Lock()
	defer s.runnerMu.Unlock()
	s.onKick = fn
	if !s.running {
		s.running = true
		go s.drainLoop()
	}
}

// stopDrainer 停止 drainer（会话被删除时调用）。
func (s *Session) stopDrainer() {
	s.runnerMu.Lock()
	defer s.runnerMu.Unlock()
	if s.running {
		close(s.stopCh)
		s.running = false
	}
}

func (s *Session) drainLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.kickCh:
		}
		if fn := s.onKick; fn != nil {
			fn(s)
		}
	}
}

// notify 唤醒 drainer 尝试写出（非阻塞、可合并）。
func (s *Session) notify() {
	select {
	case s.kickCh <- struct{}{}:
	default:
	}
}

// markResumeLocked 标记当前连接需要一次恢复投递（在途消息重发）。
// 重连绑定新连接后调用。调用方需持锁。
func (s *Session) markResumeLocked() { s.resumeNeeded = true }

// takeResumeLocked 原子地取出并清除恢复标志。调用方需持锁。
func (s *Session) takeResumeLocked() bool {
	v := s.resumeNeeded
	s.resumeNeeded = false
	return v
}

// queueCtrlLocked 挂一个由 drainer 在数据消息之后发出的控制帧。调用方需持 s.mu。
func (s *Session) queueCtrlLocked(p *packet.Packet) {
	s.ctrl = p
	s.ctrlSent = false
}

// takeCtrlLocked 取出待发控制帧；已取走/不存在返回 nil。调用方需持 s.mu。
func (s *Session) takeCtrlLocked() *packet.Packet {
	if s.ctrl == nil || s.ctrlSent {
		return nil
	}
	s.ctrlSent = true
	p := s.ctrl
	s.ctrl = nil
	return p
}

// backlogLocked 返回当前积压条数（队列 + 在途）。调用方需持锁。
func (s *Session) backlogLocked() int { return len(s.outbox) }

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
	s.notifyLocked()
	return nil
}

// notifyLocked 唤醒 drainer。调用方持锁也安全（通道发送不回调）。
func (s *Session) notifyLocked() {
	select {
	case s.kickCh <- struct{}{}:
	default:
	}
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
