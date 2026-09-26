package broker

import (
	"sync"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/topic"
)

// retainedStore 保存各主题最新的保留消息（空载荷表示清除）。
type retainedStore struct {
	mu sync.RWMutex
	m  map[string]RetainedMessage
}

// RetainedMessage 为一条保留消息。
type RetainedMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
}

func newRetainedStore() *retainedStore {
	return &retainedStore{m: make(map[string]RetainedMessage)}
}

// set 写入保留消息；payload 为空时按 MQTT 3.1.1 §3.3.1.3 从存储中清除该主题。
// 对同一主题的多次写入以最新一次为准（原子替换，不保留历史）。
func (s *retainedStore) set(topicName string, payload []byte, qos byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(payload) == 0 {
		delete(s.m, topicName)
		return
	}
	s.m[topicName] = RetainedMessage{
		Topic:   topicName,
		Payload: append([]byte(nil), payload...),
		QoS:     qos,
	}
}

// match 返回与过滤器匹配的全部保留消息。QoS 按订阅 QoS 降级由调用方在投递时处理。
func (s *retainedStore) match(filter string) []RetainedMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RetainedMessage, 0)
	for _, rm := range s.m {
		if topic.Match(filter, rm.Topic) {
			out = append(out, RetainedMessage{
				Topic:   rm.Topic,
				Payload: append([]byte(nil), rm.Payload...),
				QoS:     rm.QoS,
			})
		}
	}
	return out
}
