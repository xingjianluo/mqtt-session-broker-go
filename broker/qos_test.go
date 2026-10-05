package broker

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/packet"
)

// TestQoS2HandshakeNoRedelivery 验证 QoS2 完整握手且重复 PUBLISH 不重复投递。
func TestQoS2HandshakeNoRedelivery(t *testing.T) {
	b := newBroker()
	sub, _ := dial(t, b, "sub-q2", true)
	sub.subscribe("q2/topic", 2)
	pub, _ := dial(t, b, "pub-q2", true)

	// 发布 QoS2 消息：PUBLISH -> PUBREC
	pid := uint16(41)
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 2, Topic: "q2/topic", PacketID: pid, Payload: []byte("m1")})
	ack := pub.read()
	t.Logf("publisher: PUBLISH qos=2 id=%d -> recv %s (判定依据: 应为 PUBREC)", pid, ack.Type)
	if ack.Type != packet.TypePubRec || ack.PacketID != pid {
		t.Fatalf("expected PUBREC id=%d, got %s id=%d", pid, ack.Type, ack.PacketID)
	}

	// 发布端重传同一 packetID 的 PUBLISH（DUP）：应只回 PUBREC，消息不重复入队
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 2, Dup: true, Topic: "q2/topic", PacketID: pid, Payload: []byte("m1")})
	ack2 := pub.read()
	if ack2.Type != packet.TypePubRec || ack2.PacketID != pid {
		t.Fatalf("dup PUBLISH: expected PUBREC id=%d, got %s", pid, ack2.Type)
	}
	t.Logf("publisher: dup PUBLISH id=%d -> PUBREC again (判定依据: 入站 QoS2 去重)", pid)

	// 完成发布端握手：PUBREL -> PUBCOMP，此时消息才投递给订阅端
	pub.send(&packet.Packet{Type: packet.TypePubRel, PacketID: pid})
	comp := pub.read()
	if comp.Type != packet.TypePubComp || comp.PacketID != pid {
		t.Fatalf("expected PUBCOMP id=%d, got %s", pid, comp.Type)
	}
	t.Logf("publisher: PUBREL id=%d -> PUBCOMP, inbound QoS2 complete -> message delivered once", pid)

	// 订阅端收到恰好一条 PUBLISH（重传未造成重复投递），完成出站 QoS2 握手
	m := sub.read()
	if m.Type != packet.TypePublish || m.QoS != 2 || string(m.Payload) != "m1" {
		t.Fatalf("subscriber: expected PUBLISH qos2 payload=m1, got %s qos=%d payload=%q", m.Type, m.QoS, m.Payload)
	}
	t.Logf("subscriber: recv PUBLISH id=%d qos=2 state=WAIT_PUBREC -> send PUBREC", m.PacketID)
	sub.send(&packet.Packet{Type: packet.TypePubRec, PacketID: m.PacketID})
	rel := sub.read()
	if rel.Type != packet.TypePubRel || rel.PacketID != m.PacketID {
		t.Fatalf("subscriber: expected PUBREL, got %s", rel.Type)
	}
	t.Logf("subscriber: recv PUBREL id=%d state=WAIT_PUBCOMP -> send PUBCOMP", rel.PacketID)
	sub.send(&packet.Packet{Type: packet.TypePubComp, PacketID: rel.PacketID})

	if extra := sub.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("QoS2 duplicate PUBLISH caused redelivery: %s id=%d", extra.Type, extra.PacketID)
	}
	t.Log("subscriber: exactly one delivery despite publisher retransmit -> QoS2 会话内不重复投递 confirmed")
}

// TestReconnectResume 验证持久会话断线重连后续传离线消息与 QoS2 在途状态。
func TestReconnectResume(t *testing.T) {
	b := newBroker()
	sub, ack := dial(t, b, "sub-persist", false)
	if ack.SessionPresent {
		t.Fatal("first connect must not have session present")
	}
	sub.subscribe("offline/+", 1)

	// 异常断开（直接关连接，不发 DISCONNECT）
	sub.nc.Close()
	<-sub.done
	t.Log("subscriber disconnected abruptly; session must persist (clean=false)")

	// 离线期间发布 3 条 QoS1 消息
	pub, _ := dial(t, b, "pub-persist", true)
	for i, payload := range []string{"o1", "o2", "o3"} {
		id := uint16(100 + i)
		pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "offline/a", PacketID: id, Payload: []byte(payload)})
		ack := pub.read()
		if ack.Type != packet.TypePubAck {
			t.Fatalf("publisher: expected PUBACK, got %s", ack.Type)
		}
	}
	t.Log("published 3 QoS1 messages while subscriber offline -> must be queued")

	// 重连：sessionPresent=1，并收到排队的 3 条消息
	sub2, ack2 := dial(t, b, "sub-persist", false)
	if !ack2.SessionPresent {
		t.Fatal("reconnect must report session present")
	}
	t.Logf("reconnect: CONNACK sessionPresent=true (判定依据: 持久会话恢复)")
	received := map[string]bool{}
	for i := 0; i < 3; i++ {
		m := sub2.read()
		if m.Type != packet.TypePublish || m.QoS != 1 {
			t.Fatalf("expected queued PUBLISH qos1, got %s qos=%d", m.Type, m.QoS)
		}
		t.Logf("resumed delivery: payload=%q packetID=%d state=QUEUED->WAIT_PUBACK", m.Payload, m.PacketID)
		received[string(m.Payload)] = true
		sub2.send(&packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})
	}
	for _, want := range []string{"o1", "o2", "o3"} {
		if !received[want] {
			t.Fatalf("missing queued message %q after reconnect", want)
		}
	}
	if extra := sub2.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("unexpected extra message after resume: %s", extra.Type)
	}
	t.Log("all 3 offline messages resumed exactly once -> ok")
}

// TestQoS2InflightResume 验证 QoS2 出站消息在等 PUBCOMP 时断线，重连后重发 PUBREL 而非 PUBLISH。
func TestQoS2InflightResume(t *testing.T) {
	b := newBroker()
	sub, _ := dial(t, b, "sub-q2r", false)
	sub.subscribe("q2r/#", 2)
	pub, _ := dial(t, b, "pub-q2r", true)

	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 2, Topic: "q2r/x", PacketID: 55, Payload: []byte("inflight")})
	if a := pub.read(); a.Type != packet.TypePubRec {
		t.Fatalf("expected PUBREC, got %s", a.Type)
	}
	// 完成发布端入站握手，消息才会投递给订阅端
	pub.send(&packet.Packet{Type: packet.TypePubRel, PacketID: 55})
	if a := pub.read(); a.Type != packet.TypePubComp {
		t.Fatalf("expected PUBCOMP, got %s", a.Type)
	}

	m := sub.read()
	if m.Type != packet.TypePublish || m.QoS != 2 {
		t.Fatalf("expected PUBLISH qos2, got %s", m.Type)
	}
	sub.send(&packet.Packet{Type: packet.TypePubRec, PacketID: m.PacketID})
	rel := sub.read()
	if rel.Type != packet.TypePubRel {
		t.Fatalf("expected PUBREL, got %s", rel.Type)
	}
	t.Logf("subscriber: state=WAIT_PUBCOMP id=%d; disconnecting WITHOUT PUBCOMP", rel.PacketID)

	// 不回 PUBCOMP，直接断开并重连
	sub.nc.Close()
	<-sub.done
	sub2, ack := dial(t, b, "sub-q2r", false)
	if !ack.SessionPresent {
		t.Fatal("session must be present after reconnect")
	}
	m2 := sub2.read()
	t.Logf("after reconnect: recv %s id=%d (判定依据: WAIT_PUBCOMP 状态应重发 PUBREL 而非 PUBLISH)", m2.Type, m2.PacketID)
	if m2.Type != packet.TypePubRel || m2.PacketID != rel.PacketID {
		t.Fatalf("expected re-sent PUBREL id=%d, got %s id=%d", rel.PacketID, m2.Type, m2.PacketID)
	}
	sub2.send(&packet.Packet{Type: packet.TypePubComp, PacketID: m2.PacketID})
	if extra := sub2.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("unexpected extra packet after QoS2 resume: %s", extra.Type)
	}
	t.Log("QoS2 inflight resumed via PUBREL, no duplicate PUBLISH -> ok")
}

// TestQuotaExhaustion 验证积压超配额时连接被断开且原因可区分。
func TestQuotaExhaustion(t *testing.T) {
	b := New(Config{MaxQueuePerSession: 2, SessionExpiry: 0})
	sub, _ := dial(t, b, "sub-quota", true)
	sub.subscribe("quota/#", 1)
	pub, _ := dial(t, b, "pub-quota", true)

	// 订阅端不回 PUBACK，积压（在途）将累积；第 3 条触发配额
	for i := 0; i < 3; i++ {
		pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "quota/x", PacketID: uint16(10 + i), Payload: []byte("q")})
	}
	// 订阅端应被代理以 ErrQuotaExceeded 断开
	select {
	case err := <-sub.done:
		t.Logf("subscriber disconnected: err=%v (判定依据: errors.Is ErrQuotaExceeded)", err)
		if !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("disconnect reason = %v, want ErrQuotaExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscriber connection not closed after quota exhaustion")
	}
	t.Log("quota exhaustion -> deterministic disconnect with ErrQuotaExceeded")
}

// TestWillMessage 验证遗嘱消息在异常断开时发布、正常断开时不发布。
func TestWillMessage(t *testing.T) {
	b := newBroker()
	watcher, _ := dial(t, b, "watcher", true)
	watcher.subscribe("status/+", 0)

	// 异常断开 -> 遗嘱发布
	c1, c2 := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- b.HandleConnection(c2) }()
	wc := &testClient{t: t, nc: c1, done: done}
	wc.send(&packet.Packet{Type: packet.TypeConnect, ClientID: "dev-will", CleanSession: true,
		HasWill: true, WillTopic: "status/dev-will", WillPayload: []byte("offline"), WillQoS: 0})
	wc.read()     // CONNACK
	wc.nc.Close() // 异常断开
	<-done
	m := watcher.read()
	t.Logf("watcher: recv %s topic=%q payload=%q (判定依据: 异常断开必须发布遗嘱)", m.Type, m.Topic, m.Payload)
	if m.Type != packet.TypePublish || m.Topic != "status/dev-will" || string(m.Payload) != "offline" {
		t.Fatalf("will not delivered correctly: %+v", m)
	}

	// 正常断开（DISCONNECT）-> 遗嘱不发布
	c3, c4 := net.Pipe()
	done2 := make(chan error, 1)
	go func() { done2 <- b.HandleConnection(c4) }()
	wc2 := &testClient{t: t, nc: c3, done: done2}
	wc2.send(&packet.Packet{Type: packet.TypeConnect, ClientID: "dev-will2", CleanSession: true,
		HasWill: true, WillTopic: "status/dev-will2", WillPayload: []byte("offline")})
	wc2.read()
	wc2.send(&packet.Packet{Type: packet.TypeDisconnect})
	if err := <-done2; err != nil {
		t.Fatalf("graceful disconnect should return nil, got %v", err)
	}
	if m := watcher.readMaybe(300 * time.Millisecond); m != nil {
		t.Fatalf("will published on graceful disconnect: %+v", m)
	}
	t.Log("graceful DISCONNECT -> will NOT published -> ok")
}

// TestInvalidInputs 验证非法主题/过滤器/畸形报文的拒绝原因可区分。
func TestInvalidInputs(t *testing.T) {
	b := newBroker()

	// 非法订阅过滤器 -> SUBACK 0x80
	c, _ := dial(t, b, "sub-invalid", true)
	codes := c.subscribe("a/#/b", 1)
	t.Logf("subscribe filter=%q -> SUBACK codes=%v (判定依据: 0x80=失败)", "a/#/b", codes)
	if len(codes) != 1 || codes[0] != 0x80 {
		t.Fatalf("invalid filter must be rejected with 0x80, got %v", codes)
	}

	// 非法发布主题（含通配符）-> 连接断开，原因 ErrInvalidTopic
	pub, _ := dial(t, b, "pub-invalid", true)
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 0, Topic: "a/+/b", Payload: []byte("x")})
	select {
	case err := <-pub.done:
		t.Logf("publish to wildcard topic -> disconnect err=%v (判定依据: errors.Is ErrInvalidTopic)", err)
		if !errors.Is(err, ErrInvalidTopic) {
			t.Fatalf("reason = %v, want ErrInvalidTopic", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connection not closed after invalid publish topic")
	}

	// 畸形报文 -> 连接断开，原因 ErrMalformedPacket
	c1, c2 := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- b.HandleConnection(c2) }()
	// 服务端读到首字节即拒绝，写端需异步避免 net.Pipe 上阻塞
	go func() { _, _ = c1.Write([]byte{0x00, 0x00}) }() // 保留报文类型 0
	select {
	case err := <-done:
		t.Logf("garbage bytes -> disconnect err=%v (判定依据: errors.Is ErrMalformedPacket)", err)
		if !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("reason = %v, want ErrMalformedPacket", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connection not closed after malformed packet")
	}
	c1.Close()
}

// TestSessionExpiry 验证持久会话过期后离线消息被丢弃、会话不恢复。
func TestSessionExpiry(t *testing.T) {
	b := New(Config{MaxQueuePerSession: 100, SessionExpiry: 1}) // 1 秒过期
	sub, _ := dial(t, b, "sub-exp", false)
	sub.subscribe("exp/#", 1)
	sub.nc.Close()
	<-sub.done
	t.Log("persistent session disconnected; waiting for expiry (1s)")

	time.Sleep(1200 * time.Millisecond)

	// 过期后发布：会话已删，消息不应排队
	pub, _ := dial(t, b, "pub-exp", true)
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "exp/a", PacketID: 1, Payload: []byte("late")})
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}

	// 重连：sessionPresent=0，且无离线消息
	sub2, ack := dial(t, b, "sub-exp", false)
	if ack.SessionPresent {
		t.Fatal("expired session must not be resumed")
	}
	t.Log("reconnect after expiry: sessionPresent=false (判定依据: 会话已过期清除)")
	if m := sub2.readMaybe(300 * time.Millisecond); m != nil {
		t.Fatalf("offline message delivered after session expiry: %+v", m)
	}
	t.Log("no offline messages after expiry -> semantics stable")
}

// TestCleanSessionRemoval 验证 CleanSession=true 断开后会话与订阅被清除。
func TestCleanSessionRemoval(t *testing.T) {
	b := newBroker()
	sub, _ := dial(t, b, "sub-clean", true)
	sub.subscribe("clean/#", 1)
	sub.nc.Close()
	<-sub.done

	pub, _ := dial(t, b, "pub-clean", true)
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "clean/a", PacketID: 1, Payload: []byte("gone")})
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}

	sub2, ack := dial(t, b, "sub-clean", true)
	if ack.SessionPresent {
		t.Fatal("clean session must never be resumed")
	}
	if m := sub2.readMaybe(300 * time.Millisecond); m != nil {
		t.Fatalf("message delivered to cleaned session: %+v", m)
	}
	t.Log("clean session: subscriptions and session state removed on disconnect -> ok")
}

// TestSlowConsumer 验证连接写积压超限（订阅端从不读取）时以可区分原因断开。
func TestSlowConsumer(t *testing.T) {
	b := New(Config{MaxQueuePerSession: 10000, SessionExpiry: 0}) // 会话配额足够高，单独触发连接级写积压
	sub, _ := dial(t, b, "sub-slow", true)
	sub.subscribe("slow/#", 1)
	// 订阅端从此不再读取任何报文

	pub, _ := dial(t, b, "pub-slow", true)
	// 发布超过连接写缓冲（outBuf）的消息；订阅端不读取，写通道必然填满
	for i := 0; i < outBuf+50; i++ {
		pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "slow/x",
			PacketID: uint16(i + 1), Payload: []byte("backpressure")})
		if a := pub.read(); a.Type != packet.TypePubAck {
			t.Fatalf("publisher: expected PUBACK, got %s", a.Type)
		}
	}
	select {
	case err := <-sub.done:
		t.Logf("slow consumer disconnected: err=%v (判定依据: errors.Is ErrSlowConsumer)", err)
		if !errors.Is(err, ErrSlowConsumer) {
			t.Fatalf("disconnect reason = %v, want ErrSlowConsumer", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("slow consumer connection not closed")
	}
	t.Log("write backlog > outBuf -> deterministic disconnect with ErrSlowConsumer")
}
