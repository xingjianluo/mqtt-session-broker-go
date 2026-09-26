package broker

import (
	"github.com/xingjianluo/mqtt-session-broker-go/internal/session"
	"github.com/xingjianluo/mqtt-session-broker-go/internal/topic"
)

// routePublish 将一条入站 PUBLISH 路由给全部匹配订阅者，并处理保留消息存储。
// 匹配结果已由 trie 按客户端去重（重叠过滤器只命中一次，QoS 取最大订阅 QoS），
// 因此同一条消息对同一客户端至多投递一次。
//
// retainAsStored 语义：
//   - 普通 PUBLISH(retain=1) 与遗嘱(willRetain=1)：更新保留存储，但**路由中的副本
//     RETAIN=0**（MQTT 3.1.1 §3.3.1.3：RETAIN=1 只在订阅后的保留消息首投中出现）；
//   - 保留消息首投（来自订阅动作）携带 RETAIN=1，见 clientConn.deliverRetained。
func (b *Broker) routePublish(topicName string, payload []byte, qos byte, retain bool, senderID string) {
	if retain {
		b.retained.set(topicName, payload, qos)
	}
	matches := b.trie.Match(topicName)
	for _, m := range matches {
		effQoS := qos
		if effQoS > m.QoS {
			effQoS = m.QoS // 投递 QoS 取发布 QoS 与订阅授予 QoS 的较小值
		}
		b.deliverTo(m.ClientID, topicName, payload, effQoS, false)
	}
}

// deliverWill 发布遗嘱：遗嘱按匹配订阅路由，并遵守 willRetain 的保留语义。
func (b *Broker) deliverWill(topicName string, payload []byte, qos byte, retain bool) {
	if verr := topic.ValidateTopic(topicName); verr != nil {
		b.emit(Event{Type: "drop", Detail: "will message dropped: " + verr.Error()})
		return
	}
	b.routePublish(topicName, payload, qos, retain, "")
}

// deliverTo 将消息投递给指定客户端会话。
// 客户端离线时：QoS1/2 进入持久会话离线队列；QoS0 按 MQTT 3.1.1 §3.1.2-7 直接丢弃。
// 在线时：QoS0 直接走连接出站通道；QoS1/2 进入会话队列由消息泵按窗口发送。
func (b *Broker) deliverTo(clientID, topicName string, payload []byte, qos byte, retained bool) {
	b.mu.Lock()
	e := b.sessions[clientID]
	cc := (*clientConn)(nil)
	if e != nil {
		cc = e.conn
	}
	b.mu.Unlock()
	if e == nil {
		return
	}

	if qos == 0 {
		if cc == nil {
			b.emit(Event{Type: "drop", ClientID: clientID,
				Detail: "qos0 message discarded for offline session topic=" + topicName})
			return
		}
		cc.deliverToThis(topicName, payload, 0, retained, false)
		return
	}

	// QoS1/2：在线、离线统一入队（离线队列语义即"等会话恢复后续传"）。
	r := e.sess.Enqueue(topicName, payload, qos, bool2byte(retained))
	if r == session.EnqueueQueueFull {
		b.emit(Event{Type: "drop", ClientID: clientID, Reason: ReasonQueueQuotaExceeded,
			Detail: "message rejected: session queue quota exhausted topic=" + topicName})
		if cc != nil {
			cc.fail(ReasonQueueQuotaExceeded, "session queue quota exhausted: "+topicName)
		}
	}
}
