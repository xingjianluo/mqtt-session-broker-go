package session

import "sync"

// EnqueueResult 为入队结果，语义可区分（供事件日志与测试断言）。
type EnqueueResult int

const (
	// EnqueueAccepted 消息已被会话接受（在队列或立即发送路径上）。
	EnqueueAccepted EnqueueResult = iota
	// EnqueueDroppedOfflineQoS0 客户端离线时 QoS0 消息按 MQTT 3.1.1 §3.1.2 直接丢弃。
	EnqueueDroppedOfflineQoS0
	// EnqueueQueueFull 队列+在途总量达到配额，消息（最新一条）被拒绝。
	EnqueueQueueFull
)

func (r EnqueueResult) String() string {
	switch r {
	case EnqueueAccepted:
		return "accepted"
	case EnqueueDroppedOfflineQoS0:
		return "dropped_offline_qos0"
	case EnqueueQueueFull:
		return "queue_full"
	default:
		return "unknown"
	}
}

// InState 为入站（客户端 -> 服务端）QoS2 流程状态。
type InState int

const (
	// InGotPublish 已收到 PUBLISH 并发 PUBREC，等待 PUBREL。
	InGotPublish InState = iota
	// InGotPubrel 已收到 PUBREL 并发 PUBCOMP；保留记录以防重复 PUBLISH 再交付。
	InGotPubrel
)

// Session 为单个客户端的持久会话状态。所有方法均并发安全。
type Session struct {
	mu sync.Mutex

	clientID    string
	online      bool
	maxQueue    int
	maxInflight int

	queue       []*QueuedMessage          // 状态 QUEUED，等待分配窗口
	inflight    map[uint16]*QueuedMessage // 状态 PUBLISHED / PUBREL_SENT
	inflightIDs []uint16                  // 按进入在途的先后记录 PacketID，保证重传顺序确定
	nextID      uint16

	incoming map[uint16]InState // 入站 QoS2 去重表
	dropped  int64

	// wake 采用"代际广播通道"：notify 关闭当前 wake 并换一个新通道。
	// 与容量为 1 的信号通道不同，任何等待者持有的旧通道一定会被关闭，
	// 因此在"检查队列"与"开始等待"之间发生的入队不会丢失唤醒。
	wake chan struct{}
}

// NewSession 创建会话。maxQueue/maxInflight 小于等于 0 时使用默认值。
func NewSession(clientID string, maxQueue, maxInflight int) *Session {
	if maxQueue <= 0 {
		maxQueue = 1000
	}
	if maxInflight <= 0 {
		maxInflight = 32
	}
	return &Session{
		clientID:    clientID,
		maxQueue:    maxQueue,
		maxInflight: maxInflight,
		inflight:    make(map[uint16]*QueuedMessage),
		incoming:    make(map[uint16]InState),
		nextID:      1,
		wake:        make(chan struct{}),
	}
}

// notify 广播一次状态变更（调用方必须持有 s.mu）。
func (s *Session) notify() {
	close(s.wake)
	s.wake = make(chan struct{})
}

// WakeCh 返回当前的唤醒通道；配合 Snapshot 等在持锁检查后获取，
// 即可保证"检查—等待"窗口内不丢信号。
func (s *Session) WakeCh() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wake
}

// SetOnline 标记连接上线/下线。
func (s *Session) SetOnline(online bool) {
	s.mu.Lock()
	was := s.online
	s.online = online
	if online && !was {
		s.notify()
	}
	s.mu.Unlock()
}

// IsOnline 返回会话当前是否有活动连接。
func (s *Session) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online
}

func (s *Session) storedCount() int { return len(s.queue) + len(s.inflight) }

// Enqueue 将一条 QoS1/2 出站消息加入会话队列。
// 调用方需保证 qos 为 1 或 2；QoS0 与离线丢弃策略由连接层处理。
func (s *Session) Enqueue(topic string, payload []byte, qos, retain byte) EnqueueResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.online && qos == 0 {
		s.dropped++
		return EnqueueDroppedOfflineQoS0
	}
	if s.storedCount() >= s.maxQueue {
		s.dropped++
		return EnqueueQueueFull
	}
	s.queue = append(s.queue, &QueuedMessage{
		Topic:   topic,
		Payload: append([]byte(nil), payload...),
		QoS:     qos,
		Retain:  retain != 0,
		State:   StateQueued,
	})
	s.notify()
	return EnqueueAccepted
}

// allocID 分配一个当前未在途的报文标识符；全部占用时返回 0。
func (s *Session) allocID() uint16 {
	start := s.nextID
	for {
		id := s.nextID
		s.nextID++
		if s.nextID == 0 {
			s.nextID = 1
		}
		if _, used := s.inflight[id]; !used {
			return id
		}
		if s.nextID == start {
			return 0
		}
	}
}

// TakeReady 将队列中可发送的消息移入在途窗口并返回（深拷贝快照，报文标识符已分配）。
// 窗口满或队列空时返回空切片。调用方据此编码 PUBLISH（DUP=false）。
func (s *Session) TakeReady() []QueuedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []QueuedMessage
	for len(s.inflight) < s.maxInflight && len(s.queue) > 0 {
		msg := s.queue[0]
		s.queue = s.queue[1:]
		if msg.PacketID == 0 {
			id := s.allocID()
			if id == 0 {
				// 标识符全部在途：放回队首，等确认后再试。
				s.queue = append([]*QueuedMessage{msg}, s.queue...)
				break
			}
			msg.PacketID = id
		}
		msg.State = StatePublished
		s.inflight[msg.PacketID] = msg
		s.inflightIDs = append(s.inflightIDs, msg.PacketID)
		first := msg.Clone() // 首投携带 RETAIN（若为保留消息）
		msg.Retain = false   // 存储副本立即清除；后续重传 RETAIN 恒为 0（§3.3.1.3）
		out = append(out, *first)
	}
	return out
}

// ResendInflight 返回重连或重传定时器触发时需要重发的在途消息快照（DUP 由调用方置 1）。
// 按 MQTT 3.1.1 §3.3.1.3，重传消息的 RETAIN 标志恒为 0（首投之后即清除）。
func (s *Session) ResendInflight() []QueuedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]QueuedMessage, 0, len(s.inflight))
	// 按进入在途的先后顺序重传，避免 map 遍历随机序导致重连后报文顺序不确定。
	for _, id := range s.inflightIDs {
		msg := s.inflight[id]
		cp := msg.Clone()
		cp.Retain = false
		out = append(out, *cp)
	}
	return out
}

// InflightPubrels 返回处于 PUBREL_SENT 状态的 QoS2 报文标识符（有序），
// 供重传定时器重发 PUBREL（§4.3.3 传输保证用例）。
func (s *Session) InflightPubrels() []uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []uint16
	for _, id := range s.inflightIDs {
		msg := s.inflight[id]
		if msg.QoS == 2 && msg.State == StatePubrec {
			ids = append(ids, id)
		}
	}
	return ids
}

// removeInflightID 从有序在途 ID 列表中删除一项（调用方持锁）。
func (s *Session) removeInflightID(id uint16) {
	for i, v := range s.inflightIDs {
		if v == id {
			s.inflightIDs = append(s.inflightIDs[:i], s.inflightIDs[i+1:]...)
			return
		}
	}
}

// AckQoS1 处理出站 QoS1 的 PUBACK。返回 true 表示在途消息已完成移除。
func (s *Session) AckQoS1(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.inflight[packetID]
	if !ok {
		return false
	}
	msg.State = StateCompleted
	delete(s.inflight, packetID)
	s.removeInflightID(packetID)
	s.notify()
	return true
}

// RecvPUBREC 处理出站 QoS2 的 PUBREC：推进到 PUBREL_SENT。
// 返回 true 表示该在途消息存在（调用方应发送/重发 PUBREL）；重复 PUBREC 也返回 true。
func (s *Session) RecvPUBREC(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.inflight[packetID]
	if !ok {
		return false
	}
	if msg.State == StatePublished {
		msg.State = StatePubrec
	}
	return true
}

// RecvPUBCOMP 完成出站 QoS2 流程，移除在途消息。
func (s *Session) RecvPUBCOMP(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.inflight[packetID]
	if !ok {
		return false
	}
	msg.State = StateCompleted
	delete(s.inflight, packetID)
	s.removeInflightID(packetID)
	s.notify()
	return true
}

// NoteIncomingPublish 登记入站 QoS2 PUBLISH。
// 返回 true 表示首次收到（调用方应交付给订阅者并发 PUBREC）；
// 返回 false 表示重传（不得重复交付，仅重发 PUBREC）。
func (s *Session) NoteIncomingPublish(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.incoming[packetID]; ok {
		return false
	}
	s.incoming[packetID] = InGotPublish
	return true
}

// NoteIncomingPubrel 登记入站 QoS2 PUBREL。
// 返回 true 表示此前见过该标识符的 PUBLISH；false 表示未知标识符（调用方仍回复 PUBCOMP）。
func (s *Session) NoteIncomingPubrel(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	known := false
	if st, ok := s.incoming[packetID]; ok {
		known = true
		if st == InGotPublish {
			s.incoming[packetID] = InGotPubrel
		}
	}
	return known
}

// Stats 为会话可观测状态快照（供日志与测试）。
type Stats struct {
	ClientID     string
	Online       bool
	QueueLen     int
	InflightLen  int
	DroppedCount int64
	NextPacketID uint16
	IncomingQoS2 int
}

// Snapshot 返回会话状态快照。
func (s *Session) Snapshot() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		ClientID:     s.clientID,
		Online:       s.online,
		QueueLen:     len(s.queue),
		InflightLen:  len(s.inflight),
		DroppedCount: s.dropped,
		NextPacketID: s.nextID,
		IncomingQoS2: len(s.incoming),
	}
}

// Clear 清空会话全部状态（CleanSession=true 断开或会话过期时调用）。
func (s *Session) Clear() {
	s.mu.Lock()
	s.queue = nil
	s.inflight = make(map[uint16]*QueuedMessage)
	s.inflightIDs = nil
	s.incoming = make(map[uint16]InState)
	s.dropped = 0
	s.nextID = 1
	s.notify()
	s.mu.Unlock()
}
