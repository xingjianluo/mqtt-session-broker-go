package broker

// 保留消息生命周期与报文大小上限的行为测试。

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/packet"
)

// publishRetained 通过连接发送一条 RETAIN PUBLISH；QoS1/2 完成相应握手。
// 注意：QoS2 的保留状态在收到 PUBREL（入站 QoS2 完成）时才写入。
func (c *testClient) publishRetained(topic, payload string, qos byte) {
	c.t.Helper()
	id := uint16(0)
	if qos > 0 {
		id = c.nextID()
	}
	c.send(&packet.Packet{Type: packet.TypePublish, Topic: topic, Payload: []byte(payload),
		QoS: qos, PacketID: id, Retain: true})
	switch qos {
	case 1:
		if ack := c.read(); ack.Type != packet.TypePubAck || ack.PacketID != id {
			c.t.Fatalf("retained publish qos1: expected PUBACK id=%d got %s id=%d", id, ack.Type, ack.PacketID)
		}
	case 2:
		if rec := c.read(); rec.Type != packet.TypePubRec || rec.PacketID != id {
			c.t.Fatalf("retained publish qos2: expected PUBREC id=%d got %s id=%d", id, rec.Type, rec.PacketID)
		}
		c.send(&packet.Packet{Type: packet.TypePubRel, PacketID: id})
		if comp := c.read(); comp.Type != packet.TypePubComp || comp.PacketID != id {
			c.t.Fatalf("retained publish qos2: expected PUBCOMP id=%d got %s id=%d", id, comp.Type, comp.PacketID)
		}
	}
}

// TestRetainStoreOverwriteClear 验证保留消息写入、覆盖、清除（存储层 + 端到端首投）。
func TestRetainStoreOverwriteClear(t *testing.T) {
	// 存储层直接验证
	s := newRetainedStore()
	if got := s.matching("dev/state"); len(got) != 0 {
		t.Fatalf("empty store must have no retained, got %d", len(got))
	}
	s.store(&packet.Packet{Type: packet.TypePublish, Topic: "dev/state", Payload: []byte("v1"), QoS: 1})
	got := s.matching("dev/state")
	if len(got) != 1 || string(got[0].Payload) != "v1" || got[0].QoS != 1 {
		t.Fatalf("after store: want v1/qos1, got %+v", got)
	}
	t.Logf("store: retained written topic=%q payload=%q qos=%d", got[0].Topic, got[0].Payload, got[0].QoS)

	// 同话题再发一条 -> 覆盖（payload 与保留级别都更新）
	s.store(&packet.Packet{Type: packet.TypePublish, Topic: "dev/state", Payload: []byte("v2"), QoS: 2})
	got = s.matching("dev/state")
	if len(got) != 1 || string(got[0].Payload) != "v2" || got[0].QoS != 2 {
		t.Fatalf("after overwrite: want v2/qos2 (single latest), got %+v", got)
	}
	t.Logf("overwrite: retained replaced payload=%q qos=%d (single latest retained per topic)", got[0].Payload, got[0].QoS)

	// 空载荷 -> 清除
	s.store(&packet.Packet{Type: packet.TypePublish, Topic: "dev/state", Payload: []byte(""), QoS: 0, Retain: true})
	if got := s.matching("dev/state"); len(got) != 0 {
		t.Fatalf("empty payload must clear retained, still got %d", len(got))
	}
	t.Log("clear: empty-payload retained publish removed the state")

	// 端到端：清除后新订阅者首投拿不到任何东西
	b := newBroker()
	pub, _ := dial(t, b, "pub-roc", true)
	pub.publishRetained("roc/x", "first", 0)
	sub1, _ := dial(t, b, "sub-roc1", true)
	sub1.subscribe("roc/x", 1)
	m := sub1.read()
	if !m.Retain || string(m.Payload) != "first" {
		t.Fatalf("first-delivery must be retained 'first', got retain=%v payload=%q", m.Retain, m.Payload)
	}
	t.Logf("subscriber1 first-delivery: type=%s retain=true payload=%q (历史保留状态)", m.Type, m.Payload)

	pub.publishRetained("roc/x", "", 0) // 清除
	sub2, _ := dial(t, b, "sub-roc2", true)
	sub2.subscribe("roc/x", 1)
	if extra := sub2.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("after clear, new subscriber must not receive retained, got %+v", extra)
	}
	t.Log("subscriber2 after clear: no first-delivery (保留状态已清除) -> ok")
}

// TestRetainNewSubscriberFirstDelivery 验证新订阅者（含通配符）首投、级别降级与 RETAIN 标记。
func TestRetainNewSubscriberFirstDelivery(t *testing.T) {
	b := newBroker()
	pub, _ := dial(t, b, "pub-fd", true)
	// 三个话题，分别以不同级别保留；再准备一个不匹配话题
	pub.publishRetained("fd/a", "A", 0)
	pub.publishRetained("fd/b", "B", 1)
	pub.publishRetained("fd/c", "C", 2)
	pub.publishRetained("other/x", "X", 0)

	// 通配符订阅，请求 QoS1：
	// fd/a 保留级别0 -> 投 qos0；fd/b 保留级别1 -> 投 qos1；fd/c 保留级别2 -> 降级为 qos1
	sub, _ := dial(t, b, "sub-fd", true)
	id := sub.nextID()
	sub.send(&packet.Packet{Type: packet.TypeSubscribe, PacketID: id,
		Subscriptions: []packet.Subscription{{Topic: "fd/+", QoS: 1}}})
	// 先收到 SUBACK，之后才是各话题保留首投
	ack := sub.read()
	if ack.Type != packet.TypeSubAck || ack.PacketID != id {
		t.Fatalf("SUBACK must precede retained delivery, got %s id=%d", ack.Type, ack.PacketID)
	}
	t.Logf("SUBACK id=%d received before retained first-delivery (顺序确定)", id)

	got := map[string]*packet.Packet{}
	for i := 0; i < 3; i++ {
		m := sub.read()
		if m.Type != packet.TypePublish || !m.Retain {
			t.Fatalf("first-delivery must be PUBLISH with RETAIN=1, got %s retain=%v", m.Type, m.Retain)
		}
		if m.Dup {
			t.Fatalf("first-delivery must not have DUP set, topic=%q", m.Topic)
		}
		got[m.Topic] = m
	}
	want := map[string]byte{"fd/a": 0, "fd/b": 1, "fd/c": 1}
	for topic, qos := range want {
		m, ok := got[topic]
		if !ok {
			t.Fatalf("missing retained first-delivery for %q", topic)
		}
		if m.QoS != qos {
			t.Fatalf("topic %q first-delivery qos=%d want %d (min(stored, granted=1))", topic, m.QoS, qos)
		}
		t.Logf("first-delivery topic=%q payload=%q retain=true qos=%d (判定依据: min(保留级别, 订阅级别))",
			topic, m.Payload, m.QoS)
	}
	if _, ok := got["other/x"]; ok {
		t.Fatal("non-matching retained message must not be delivered")
	}
	if extra := sub.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("exactly one retained per topic, got extra: %+v", extra)
	}

	// 同一 SUBSCRIBE 多个过滤器命中同一话题 -> 只投一条，授予级取最大值
	pub.publishRetained("dup/n", "D", 2)
	sub2, _ := dial(t, b, "sub-dup", true)
	sub2.send(&packet.Packet{Type: packet.TypeSubscribe, PacketID: 7,
		Subscriptions: []packet.Subscription{{Topic: "dup/+", QoS: 0}, {Topic: "dup/n", QoS: 2}}})
	if a := sub2.read(); a.Type != packet.TypeSubAck {
		t.Fatalf("expected SUBACK, got %s", a.Type)
	}
	m := sub2.read()
	if m.Topic != "dup/n" || !m.Retain || m.QoS != 2 {
		t.Fatalf("dedup first-delivery: want dup/n retain qos2 (max granted), got %+v", m)
	}
	t.Logf("multi-filter dedup: topic=%q delivered once qos=%d retain=true (取最高授予级)", m.Topic, m.QoS)
	if extra := sub2.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("same topic matched by two filters must deliver once, got extra: %+v", extra)
	}

	// 重新订阅同一过滤器会再次首投（协议允许，新订阅动作）
	codes := sub2.subscribe("dup/n", 2)
	if codes[0] != 2 {
		t.Fatalf("resubscribe grant=%d want 2", codes[0])
	}
	m2 := sub2.read()
	if m2.Topic != "dup/n" || !m2.Retain {
		t.Fatalf("re-subscribe must first-deliver retained again, got %+v", m2)
	}
	t.Log("re-subscribe first-delivers retained again -> ok")
}

// TestRetainOnlineSubscriberNoDuplicate 验证在线订阅者只收 RETAIN=0 的实时消息，不被重复/补推。
func TestRetainOnlineSubscriberNoDuplicate(t *testing.T) {
	b := newBroker()
	sub, _ := dial(t, b, "sub-nd", true)
	sub.subscribe("nd/x", 1)

	pub, _ := dial(t, b, "pub-nd", true)

	// 1) 订阅建立后发布 RETAIN 消息：在线订阅者只收到一条 RETAIN=0 的实时消息
	pub.publishRetained("nd/x", "live-retained", 1)
	m := sub.read()
	if m.Topic != "nd/x" || string(m.Payload) != "live-retained" {
		t.Fatalf("online subscriber must receive the live message, got %+v", m)
	}
	if m.Retain {
		t.Fatalf("real-time delivery to existing subscriber must have RETAIN=0 (MQTT-3.3.1-12), got retain=true")
	}
	t.Logf("online real-time delivery: type=%s topic=%q retain=false (判定依据: 实时消息 RETAIN 必须为 0)", m.Type, m.Topic)
	sub.send(&packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})
	if extra := sub.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("online subscriber must not get a duplicate/historical copy, got %+v", extra)
	}
	t.Log("no duplicate/historical re-push after retained publish -> ok")

	// 2) 该话题再被普通（非 RETAIN）发布覆盖前，新订阅者拿到的仍是上次保留的状态；
	//    先验证保留状态确实存在
	sub2, _ := dial(t, b, "sub-nd2", true)
	sub2.subscribe("nd/x", 1)
	h := sub2.read()
	if !h.Retain || string(h.Payload) != "live-retained" {
		t.Fatalf("new subscriber must see retained 'live-retained' retain=true, got retain=%v payload=%q", h.Retain, h.Payload)
	}
	t.Logf("late subscriber sees historical retained: payload=%q retain=true (与实时链路互不干扰)", h.Payload)
	sub2.send(&packet.Packet{Type: packet.TypePubAck, PacketID: h.PacketID})

	// 3) 顺序不插队：订阅建立瞬间，首投保留必须先于订阅生效后的实时消息（使用独立话题避免干扰）
	pub.publishRetained("ord/z", "retained-z", 0)
	sub3, _ := dial(t, b, "sub-ord", true)
	sub3.subscribe("ord/z", 0) // 触发 ord/z 的保留首投
	// 紧接着实时发布同话题
	pub.publish("ord/z", "realtime-z", 0)
	first := sub3.read()
	second := sub3.read()
	if !first.Retain || string(first.Payload) != "retained-z" {
		t.Fatalf("retained first-delivery must come first, got retain=%v payload=%q", first.Retain, first.Payload)
	}
	if second.Retain || string(second.Payload) != "realtime-z" {
		t.Fatalf("real-time message must follow first-delivery with retain=false, got retain=%v payload=%q",
			second.Retain, second.Payload)
	}
	t.Logf("ordering: [%q retain=%v] then [%q retain=%v] (历史首投不被实时消息插队)",
		first.Payload, first.Retain, second.Payload, second.Retain)
	if extra := sub3.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("unexpected extra delivery: %+v", extra)
	}
}

// dialRaw 建立一条原始连接（用于自定义 CONNECT，如遗嘱），完成 CONNACK 握手。
func dialRaw(t *testing.T, b *Broker, cp *packet.Packet) (*testClient, error) {
	t.Helper()
	c1, c2 := net.Pipe()
	tc := &testClient{t: t, nc: c1, done: make(chan error, 1)}
	go func() { tc.done <- b.HandleConnection(c2) }()
	tc.send(cp)
	ack := tc.read()
	if ack.Type != packet.TypeConnAck {
		t.Fatalf("expected CONNACK, got %s", ack.Type)
	}
	return tc, nil
}

// TestRetainWillMessage 验证遗嘱（含保留遗嘱）触发的保留更新。
func TestRetainWillMessage(t *testing.T) {
	b := newBroker()

	// 设备先把状态话题保留为 online
	dev, _ := dial(t, b, "dev-will-r", true)
	dev.publishRetained("wstat/dev1", "online", 0)

	// 在线观察者：订阅时先收到先前保留的 online 首投
	watcher, _ := dial(t, b, "watcher-w", true)
	watcher.subscribe("wstat/+", 1)
	first := watcher.read()
	if !first.Retain || string(first.Payload) != "online" {
		t.Fatalf("watcher first-delivery must be retained 'online', got retain=%v payload=%q", first.Retain, first.Payload)
	}
	t.Logf("watcher first-delivery retained=%q retain=true (订阅瞬间的历史状态)", first.Payload)

	// 以“保留遗嘱”接入的设备
	wdev, _ := dialRaw(t, b, &packet.Packet{
		Type: packet.TypeConnect, ClientID: "dev1", CleanSession: true, KeepAlive: 30,
		HasWill: true, WillTopic: "wstat/dev1", WillPayload: []byte("offline"),
		WillQoS: 1, WillRetain: true,
	})
	// 遗嘱只在异常掉线时发布；接入期间不应有任何推送
	if m := watcher.readMaybe(200 * time.Millisecond); m != nil {
		t.Fatalf("will must not publish on connect, got %+v", m)
	}
	// 异常掉线（不发 DISCONNECT）-> 遗嘱发布：实时 RETAIN=0 + 保留状态更新
	wdev.close()
	<-wdev.done
	m := watcher.read()
	if m.Topic != "wstat/dev1" || string(m.Payload) != "offline" || m.QoS != 1 {
		t.Fatalf("watchers want real-time will offline qos1, got %+v", m)
	}
	if m.Retain {
		t.Fatalf("real-time will delivery must have RETAIN=0")
	}
	t.Logf("watcher: will delivered in real time topic=%q payload=%q retain=false (判定依据: 遗嘱走普通实时发布)",
		m.Topic, m.Payload)
	watcher.send(&packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})
	if extra := watcher.readMaybe(200 * time.Millisecond); extra != nil {
		t.Fatalf("will must not be delivered twice: %+v", extra)
	}

	// 后来的订阅者必须直接看到遗嘱留下的“离线”保留状态
	late, _ := dial(t, b, "late-w", true)
	late.subscribe("wstat/+", 1)
	h := late.read()
	if !h.Retain || string(h.Payload) != "offline" || h.QoS != 1 {
		t.Fatalf("new subscriber must see retained will state offline/retain=true/qos1, got %+v", h)
	}
	t.Logf("late subscriber first-delivery: payload=%q retain=true qos=%d (遗嘱异常掉线即登记为状态)",
		h.Payload, h.QoS)
	late.send(&packet.Packet{Type: packet.TypePubAck, PacketID: h.PacketID})

	// 正常 DISCONNECT 不发布遗嘱，也不改变保留状态
	gdev, _ := dialRaw(t, b, &packet.Packet{
		Type: packet.TypeConnect, ClientID: "dev2", CleanSession: true, KeepAlive: 30,
		HasWill: true, WillTopic: "wstat/dev2", WillPayload: []byte("offline2"), WillRetain: true,
	})
	gdev.send(&packet.Packet{Type: packet.TypeDisconnect})
	if err := <-gdev.done; err != nil {
		t.Fatalf("graceful disconnect should return nil, got %v", err)
	}
	late2, _ := dial(t, b, "late-w2", true)
	late2.subscribe("wstat/+", 1)
	h2 := late2.read()
	if h2.Topic != "wstat/dev1" || !h2.Retain || string(h2.Payload) != "offline" {
		t.Fatalf("graceful disconnect must not publish will; only dev1 offline retained expected, got %+v", h2)
	}
	t.Logf("graceful DISCONNECT: will NOT published; retained state unchanged (%q) -> ok", h2.Payload)
	if extra := late2.readMaybe(200 * time.Millisecond); extra != nil {
		t.Fatalf("unexpected retained for graceful-disconnect client: %+v", extra)
	}
}

// TestRetainSurvivesSessionCleanupExpiry 验证保留状态独立于会话清理/过期，且不复活会话。
func TestRetainSurvivesSessionCleanupExpiry(t *testing.T) {
	// 1) CleanSession 设备断开（会删除会话与订阅），保留状态仍在
	b := newBroker()
	dev, _ := dial(t, b, "dev-clean", true)
	dev.subscribe("surv/a", 0) // 发布者顺带订阅无意义，仅制造会话内订阅关系
	dev.publishRetained("surv/a", "kept", 0)
	dev.close()
	<-dev.done
	t.Log("clean-session publisher disconnected: session/subscriptions dropped; retained state must survive")

	late, ack := dial(t, b, "late-clean", true)
	if ack.SessionPresent {
		t.Fatal("brand new client must have sessionPresent=false")
	}
	late.subscribe("surv/a", 1)
	m := late.read()
	if !m.Retain || string(m.Payload) != "kept" {
		t.Fatalf("retained state must survive clean-session removal, got retain=%v payload=%q", m.Retain, m.Payload)
	}
	t.Logf("retained %q survives clean session removal; new session sessionPresent=false (保留不复活会话)", m.Payload)

	// 2) 持久会话过期后，保留状态仍在，且重连是全新会话
	b2 := New(Config{MaxQueuePerSession: 100, SessionExpiry: 1})
	pub, _ := dial(t, b2, "dev-exp-pub", true)
	pub.publishRetained("surv/b", "kept2", 0)

	sub, _ := dial(t, b2, "sub-exp2", false)
	if ack.SessionPresent {
		t.Fatal("first connect must not report session present")
	}
	sub.subscribe("surv/b", 1)
	h := sub.read()
	if !h.Retain || string(h.Payload) != "kept2" {
		t.Fatalf("expected retained kept2, got %+v", h)
	}
	sub.send(&packet.Packet{Type: packet.TypePubAck, PacketID: h.PacketID})
	sub.close()
	<-sub.done
	t.Log("persistent subscriber disconnected; waiting for session expiry (1s)")
	time.Sleep(1200 * time.Millisecond)

	sub2, ack2 := dial(t, b2, "sub-exp2", false)
	if ack2.SessionPresent {
		t.Fatal("expired session must not be resumed (retained state must not resurrect it)")
	}
	t.Log("reconnect after expiry: sessionPresent=false — retained store did not revive the session")
	sub2.subscribe("surv/b", 1)
	h2 := sub2.read()
	if !h2.Retain || string(h2.Payload) != "kept2" {
		t.Fatalf("retained must survive session expiry, got retain=%v payload=%q", h2.Retain, h2.Payload)
	}
	t.Log("retained first-delivery still works after session expiry -> retained store is broker-global")

	// 3) 保留状态也不会让已清除会话“收到”实时消息（router 已随过期会话移除）。
	//    用另一个持久客户端订阅后任其过期且不重连，再向其原订阅话题实时发布。
	gone, _ := dial(t, b2, "sub-gone", false)
	gone.subscribe("surv/gone", 1)
	gone.close()
	<-gone.done
	time.Sleep(1200 * time.Millisecond)      // 等其过期
	pub.publishRetained("surv/gone", "g", 0) // 仅写保留，不验证投递
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "surv/gone", PacketID: 91,
		Payload: []byte("live-nowhere")})
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}
	// 一个全新客户端只应收到保留首投，收不到发给已死会话的那条实时消息
	fresh, _ := dial(t, b2, "fresh", true)
	fresh.subscribe("surv/gone", 1)
	fm := fresh.read()
	if !fm.Retain || string(fm.Payload) != "g" {
		t.Fatalf("fresh client first-delivery must be retained 'g', got retain=%v payload=%q", fm.Retain, fm.Payload)
	}
	if extra := fresh.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("real-time message must not be delivered to the expired session, got %+v", extra)
	}
	t.Log("real-time message after expiry delivered nowhere; retained first-delivery unaffected -> ok")
	sub2.close()
	<-sub2.done
}

// TestPacketTooLargeRejected 验证超限报文被以可区分原因拒绝，且不留半截状态、不影响其他连接。
func TestPacketTooLargeRejected(t *testing.T) {
	const limit = 64 // 很小的上限，便于构造
	b := New(Config{MaxQueuePerSession: 100, SessionExpiry: 0, MaxPacketSize: limit})

	// 其他连接上的“参照客户端”：整个过程中必须持续正常工作
	ref, _ := dial(t, b, "ref", true)
	ref.subscribe("pl/ref", 0)

	victim, _ := dial(t, b, "victim", true)

	// 合法小报文不受影响
	victim.subscribe("pl/x", 0)

	// 构造超限 PUBLISH（remaining length = 2+topic+payload > 64），直接写字节
	big := &packet.Packet{Type: packet.TypePublish, QoS: 0, Topic: "pl/x",
		Payload: make([]byte, 200)}
	raw, err := packet.Encode(big)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	t.Logf("sending oversized packet: type=PUBLISH totalBytes=%d remaining>%d (判定依据: 声明长度即超限)",
		len(raw), limit)
	go func() { _, _ = victim.nc.Write(raw) }()
	victim.expectConnError(ErrPacketTooLarge)
	t.Logf("victim disconnected with ErrPacketTooLarge; errors.Is 区分于 ErrMalformedPacket=%v / ErrQuotaExceeded=%v",
		errors.Is(ErrPacketTooLarge, ErrMalformedPacket), errors.Is(ErrPacketTooLarge, ErrQuotaExceeded))
	if errors.Is(ErrPacketTooLarge, ErrMalformedPacket) {
		t.Fatal("oversize reason must be distinguishable from malformed")
	}

	// 不留半截状态：用同 ClientID 重连必须是干净的新会话
	v2, ack2 := dial(t, b, "victim", true)
	if ack2.SessionPresent {
		t.Fatal("clean-session reconnect after oversize rejection must be a fresh session")
	}
	t.Log("reconnect with same clientID: fresh session, no half state left behind")
	v2.close()

	// 其他连接不受影响：参照客户端仍能收发
	refPub, _ := dial(t, b, "ref-pub", true)
	refPub.publish("pl/ref", "still-alive", 0)
	m := ref.read()
	if string(m.Payload) != "still-alive" {
		t.Fatalf("reference connection disturbed by oversize rejection: %+v", m)
	}
	t.Log("reference connection still pub/sub normally — oversize handling does not stall other connections")

	// 超限与畸形报文是两条不同路径：畸形报文仍是 ErrMalformedPacket
	c1, c2 := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- b.HandleConnection(c2) }()
	go func() { _, _ = c1.Write([]byte{0x00, 0x00}) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("garbage bytes reason=%v, want ErrMalformedPacket", err)
		}
		t.Logf("malformed path still reason=%v — 与超限路径相互独立", ErrMalformedPacket)
	case <-time.After(3 * time.Second):
		t.Fatal("malformed connection not closed")
	}
	c1.Close()

	// CONNECT 阶段超限同样以 ErrPacketTooLarge 拒绝（首包即超限，无 CONNACK、无会话）
	c3, c4 := net.Pipe()
	done2 := make(chan error, 1)
	go func() { done2 <- b.HandleConnection(c4) }()
	hugeConnect := &packet.Packet{Type: packet.TypeConnect, ClientID: "too-big",
		CleanSession: true, Password: make([]byte, 500)}
	rawC, err := packet.Encode(hugeConnect)
	if err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	go func() { _, _ = c3.Write(rawC) }()
	select {
	case err := <-done2:
		if !errors.Is(err, ErrPacketTooLarge) {
			t.Fatalf("oversized CONNECT reason=%v, want ErrPacketTooLarge", err)
		}
		t.Log("oversized CONNECT rejected with ErrPacketTooLarge before any session state (会话状态: 未创建)")
	case <-time.After(3 * time.Second):
		t.Fatal("oversized CONNECT connection not closed")
	}
	c3.Close()

	// 被拒 ClientID 没有残留会话：以正常小 CONNECT 重连是全新会话
	okClient, ack3 := dial(t, b, "too-big", true)
	if ack3.SessionPresent {
		t.Fatal("rejected oversized CONNECT must not have created a session")
	}
	okClient.close()
}

var _ = net.Pipe
var _ = errors.Is

// TestRetainFirstDeliveryQuota 验证保留首投也受会话积压配额约束：
// 会话已被未确认消息占满时，新订阅触发的保留首投按既有 ErrQuotaExceeded 路径断开，
// 与单条报文上限 ErrPacketTooLarge 相互独立。
func TestRetainFirstDeliveryQuota(t *testing.T) {
	b := New(Config{MaxQueuePerSession: 2, SessionExpiry: 0, MaxPacketSize: 1 << 20})
	pub, _ := dial(t, b, "pub-rq", true)
	// 写入 3 个不同话题的保留
	pub.publishRetained("rq/a", "a", 0)
	pub.publishRetained("rq/b", "b", 0)
	pub.publishRetained("rq/c", "c", 0)

	// 先用 qos1 订阅别的话题并占满积压（2 条在途，不回 PUBACK）
	sub, _ := dial(t, b, "sub-rq", true)
	sub.subscribe("fill/+", 1)
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "fill/1", PacketID: 201, Payload: []byte("f1")})
	pub.read() // PUBACK
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "fill/2", PacketID: 202, Payload: []byte("f2")})
	pub.read()
	// 订阅端读出 2 条填充消息但不确认，使其处于在途占满状态
	f1 := sub.read()
	f2 := sub.read()
	if f1.QoS != 1 || f2.QoS != 1 {
		t.Fatalf("fill messages should be qos1, got %d/%d", f1.QoS, f2.QoS)
	}
	t.Log("session backlog full: 2 inflight unacked (会话状态: backlog=2/quota=2)")

	// 新订阅触发 3 条保留首投 -> 第三条必然撞配额 -> 连接被 ErrQuotaExceeded 断开
	sub.send(&packet.Packet{Type: packet.TypeSubscribe, PacketID: 9,
		Subscriptions: []packet.Subscription{{Topic: "rq/+", QoS: 0}}})
	select {
	case err := <-sub.done:
		if !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("retained first-delivery quota breach reason=%v, want ErrQuotaExceeded", err)
		}
		if errors.Is(err, ErrPacketTooLarge) {
			t.Fatal("quota breach must be distinguishable from packet-size breach")
		}
		t.Logf("retained first-delivery breached session quota -> %v (判定依据: errors.Is)", err)
	case <-time.After(3 * time.Second):
		t.Fatal("connection not closed after retained first-delivery exhausted quota")
	}
}
