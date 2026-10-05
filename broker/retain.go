package broker

import (
	"sort"
	"sync"

	"github.com/xingjianluo/mqtt-session-broker-go/packet"
	"github.com/xingjianluo/mqtt-session-broker-go/topic"
)

// retainedStore 是代理级保留消息存储：话题名 -> 最近一条保留消息。
// 与会话独立：会话清理/过期不影响保留状态，保留状态也不会复活会话。
type retainedStore struct {
	mu   sync.RWMutex
	msgs map[string]*packet.Packet
}

// newRetainedStore 创建空的保留消息存储。
func newRetainedStore() *retainedStore {
	return &retainedStore{msgs: make(map[string]*packet.Packet)}
}

// cloneRetained 深拷贝一条保留消息：保留载荷与内部队列/包 ID 解耦，
// 避免同一底层数组被多个会话修改。
func cloneRetained(msg *packet.Packet) *packet.Packet {
	payload := append([]byte(nil), msg.Payload...)
	return &packet.Packet{
		Type:    packet.TypePublish,
		Topic:   msg.Topic,
		Payload: payload,
		QoS:     msg.QoS, // 保留“保留时的级别”，订阅投递时再与订阅级别取 min
	}
}

// store 写入或清除某话题的保留消息：
// 非空载荷 -> 覆盖为最新状态（携带保留时的 QoS）；空载荷 -> 清除该话题状态。
func (s *retainedStore) store(msg *packet.Packet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(msg.Payload) == 0 {
		if _, ok := s.msgs[msg.Topic]; ok {
			delete(s.msgs, msg.Topic)
		}
		return
	}
	s.msgs[msg.Topic] = cloneRetained(msg)
}

// matching 返回快照中所有匹配过滤器的保留消息（按话题名排序，顺序确定）。
// 每条返回的是独立拷贝，调用方可直接改写（如降级 QoS、置位首投标记）。
func (s *retainedStore) matching(filter string) []*packet.Packet {
	s.mu.RLock()
	names := make([]string, 0, len(s.msgs))
	for name := range s.msgs {
		if topic.Match(filter, name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([]*packet.Packet, 0, len(names))
	for _, name := range names {
		out = append(out, cloneRetained(s.msgs[name]))
	}
	s.mu.RUnlock()
	return out
}
