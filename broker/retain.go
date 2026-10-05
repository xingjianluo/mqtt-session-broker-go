package broker

import (
	"sort"

	"github.com/xingjianluo/mqtt-session-broker-go/topic"
)

// retainedMsg 是一个话题上被记住的最新一条保留消息。
type retainedMsg struct {
	topic   string
	payload []byte
	qos     byte // 保留时的发布 QoS（不随订阅授予等级变化）
}

// retainStore 是代理层面的保留消息表（与任何会话无关）：
// 会话清理/过期/接管都不会影响这里的保留状态，保留状态也不会复活会话。
// 空载荷的保留发布表示清除该话题的保留状态（MQTT 3.1.1 第 3.3.1 节约定）。
type retainStore struct {
	msgs map[string]*retainedMsg // topic -> 最新保留消息
}

func newRetainStore() *retainStore {
	return &retainStore{msgs: make(map[string]*retainedMsg)}
}

// storeLocked 写入/覆盖/清除（payload 为空时清除）一个话题的保留状态。
// payload 会被复制，避免调用方复用切片造成的别名问题。调用方需持 Broker.mu。
func (s *retainStore) storeLocked(topicName string, payload []byte, qos byte) {
	if len(payload) == 0 {
		delete(s.msgs, topicName)
		return
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	s.msgs[topicName] = &retainedMsg{topic: topicName, payload: cp, qos: qos}
}

// matchingLocked 返回匹配过滤器的全部保留消息，按话题名排序（确定的投递顺序）。
// 通配符开头的过滤器不匹配 $ 话题的规则由 topic.Match 统一处理。
// 调用方需持 Broker.mu。
func (s *retainStore) matchingLocked(filter string) []*retainedMsg {
	out := make([]*retainedMsg, 0)
	for name, m := range s.msgs {
		if topic.Match(filter, name) {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].topic < out[j].topic })
	return out
}
