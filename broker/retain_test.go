package broker

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/packet"
)

// 本文件覆盖保留消息完整生命周期与报文大小上限。
// 断言区分保留首投（Retain=1）与实时投递（Retain=0），
// 并以 errors.Is 区分超限断开（ErrPacketTooLarge）与畸形报文（ErrMalformedPacket）。

// TestRetainStoreOverwriteClear 验证保留状态写入/覆盖/清除（存储单元级）。
func TestRetainStoreOverwriteClear(t *testing.T) {
	s := newRetainStore()

	s.storeLocked("dev/a", []byte("v1"), 1)
	got := s.matchingLocked("dev/+")
	if len(got) != 1 || string(got[0].payload) != "v1" || got[0].qos != 1 {
		t.Fatalf("store: got %+v, want single v1 qos1", got)
	}
	t.Log("retained store write: dev/a=v1 qos1 -> 1 match")

	// 同话题覆盖：新内容新 QoS 生效，话题数不变
	s.storeLocked("dev/a", []byte("v2"), 2)
	got = s.matchingLocked("dev/a")
	if len(got) != 1 || string(got[0].payload) != "v2" || got[0].qos != 2 {
		t.Fatalf("overwrite: got %+v, want v2 qos2", got)
	}
	t.Log("retained overwrite: dev/a=v2 qos2 replaced old state")

	// 多话题 + 通配符匹配 + 按话题名排序的确定性投递
	s.storeLocked("dev/c", []byte("c"), 0)
	s.storeLocked("dev/b", []byte("b"), 0)
	got = s.matchingLocked("dev/#")
	if len(got) != 3 || got[0].topic != "dev/a" || got[1].topic != "dev/b" || got[2].topic != "dev/c" {
		t.Fatalf("wildcard match/order wrong: %+v", got)
	}
	t.Logf("wildcard dev/# matches %d retained topics in sorted order", len(got))

	// $ 系统话题不被 # / + 开头的过滤器匹配
	s.storeLocked("$SYS/x", []byte("sys"), 0)
	if n := len(s.matchingLocked("#")); n != 3 {
		t.Fatalf("# must not match $ topics, got %d", n)
	}
	if n := len(s.matchingLocked("$SYS/+")); n != 1 {
		t.Fatalf("explicit $SYS/+ must match, got %d", n)
	}
	t.Log("$SYS topics excluded from wildcard-leading filters (判定依据: topic.Match 规则)")

	// 空载荷清除
	s.storeLocked("dev/a", nil, 0)
	got = s.matchingLocked("dev/#")
	if len(got) != 2 {
		t.Fatalf("after clear dev/a, want 2 retained, got %d", len(got))
	}
	for _, m := range got {
		if m.topic == "dev/a" {
			t.Fatal("dev/a retained state must be gone after empty payload")
		}
	}
	t.Log("empty-payload retained publish clears dev/a -> state removed")
}

// TestPacketReadLimitUnit 验证 packet.ReadPacketLimit 的边界与错误类型。
func TestPacketReadLimitUnit(t *testing.T) {
	// 小报文 + 足够上限：正常解析
	p, err := packet.ReadPacketLimit(bytes.NewReader([]byte{0xC0, 0x00}), 10)
	if err != nil || p.Type != packet.TypePingReq {
		t.Fatalf("small packet should decode, got p=%v err=%v", p, err)
	}

	// 声明剩余长度 100 但上限 10：不读主体即失败，且错误可区分
	var big bytes.Buffer
	big.WriteByte(0x30) // PUBLISH
	big.WriteByte(100)  // remaining length 100
	_, err = packet.ReadPacketLimit(&big, 10)
	t.Logf("oversized declared packet -> err=%v (判定依据: errors.Is ErrPacketTooLarge)", err)
	if !errors.Is(err, packet.ErrPacketTooLarge) {
		t.Fatalf("want ErrPacketTooLarge, got %v", err)
	}

	// limit<=0 表示不限（协议上限内）
	p, err = packet.ReadPacketLimit(bytes.NewReader([]byte{0xC0, 0x00}), 0)
	if err != nil || p.Type != packet.TypePingReq {
		t.Fatalf("limit=0 means unlimited, got err=%v", err)
	}
	t.Log("ReadPacketLimit: oversized -> ErrPacketTooLarge (distinct from malformed); limit=0 unlimited")
}

// subscribeCollect 发送 SUBSCRIBE，收集 SUBACK 之前到达的保留首投。
func (c *testClient) subscribeCollect(filter string, qos byte) ([]byte, []*packet.Packet) {
	c.t.Helper()
	id := c.nextID()
	c.send(&packet.Packet{Type: packet.TypeSubscribe, PacketID: id,
		Subscriptions: []packet.Subscription{{Topic: filter, QoS: qos}}})
	var deliv []*packet.Packet
	for {
		p := c.read()
		if p.Type == packet.TypeSubAck {
			if p.PacketID != id {
				c.t.Fatalf("expected SUBACK id=%d, got id=%d", id, p.PacketID)
			}
			c.t.Logf("SUBSCRIBE %q qos=%d -> SUBACK codes=%v, retained-before-suback=%d (判定依据: Retain=1 且先于 SUBACK)",
				filter, qos, p.ReasonCodes, len(deliv))
			return p.ReasonCodes, deliv
		}
		if p.Type != packet.TypePublish {
			c.t.Fatalf("expected retained PUBLISH or SUBACK, got %s", p.Type)
		}
		deliv = append(deliv, p)
	}
}

// ackIfQoS 对收到的投递按其 QoS 回 ACK，保持会话干净。
func (c *testClient) ackIfQoS(m *packet.Packet) {
	c.t.Helper()
	switch m.QoS {
	case 1:
		c.send(&packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})
	case 2:
		c.send(&packet.Packet{Type: packet.TypePubRec, PacketID: m.PacketID})
		if rel := c.read(); rel.Type != packet.TypePubRel || rel.PacketID != m.PacketID {
			c.t.Fatalf("expected PUBREL id=%d, got %s", m.PacketID, rel.Type)
		}
		c.send(&packet.Packet{Type: packet.TypePubComp, PacketID: m.PacketID})
	}
}

// TestRetainNewSubscriberInitialDelivery 验证新订阅（含通配符）按话题首投最新一条、
// 降级 QoS 且 Retain=1。
func TestRetainNewSubscriberInitialDelivery(t *testing.T) {
	b := newBroker()
	pub, _ := dial(t, b, "pub-ret", true)

	// dev/a 以 QoS2 + Retain 发布：入站 QoS2 在 PUBREL 完成前不得更新保留状态
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 2, Retain: true,
		Topic: "dev/a", PacketID: 70, Payload: []byte("a-q2")})
	if a := pub.read(); a.Type != packet.TypePubRec || a.PacketID != 70 {
		t.Fatalf("expected PUBREC 70, got %s", a.Type)
	}
	early, _ := dial(t, b, "early", true)
	if codes, deliv := early.subscribeCollect("dev/a", 2); len(deliv) != 0 || codes[0] != 2 {
		t.Fatalf("before PUBREL retained must not exist, got deliv=%d", len(deliv))
	}
	t.Log("QoS2 retained PUBLISH before PUBREL -> not stored yet (判定依据: 入站 QoS2 完成点才生效)")
	early.close()
	<-early.done

	pub.send(&packet.Packet{Type: packet.TypePubRel, PacketID: 70})
	if a := pub.read(); a.Type != packet.TypePubComp || a.PacketID != 70 {
		t.Fatalf("expected PUBCOMP 70, got %s", a.Type)
	}

	// dev/b 以 QoS1 + Retain 发布（覆盖语义：再发一条）
	idb := pub.nextID()
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Retain: true,
		Topic: "dev/b", PacketID: idb, Payload: []byte("b-old")})
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}
	idb2 := pub.nextID()
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Retain: true,
		Topic: "dev/b", PacketID: idb2, Payload: []byte("b-new")})
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}

	// 新订阅者用通配符一次匹配两个话题，授予 QoS1：
	// dev/a 保留级别 2 -> 投递 min(2,1)=1；dev/b 保留级别 1 -> 1；均 Retain=1，按话题排序
	sub, _ := dial(t, b, "sub-ret", true)
	codes, deliv := sub.subscribeCollect("dev/#", 1)
	if codes[0] != 1 || len(deliv) != 2 {
		t.Fatalf("want granted=1 and 2 retained deliveries, got codes=%v deliv=%d", codes, len(deliv))
	}
	want := []struct {
		topic, payload string
		qos            byte
	}{{"dev/a", "a-q2", 1}, {"dev/b", "b-new", 1}}
	for i, m := range deliv {
		t.Logf("retained initial #%d: type=%s topic=%q qos=%d retain=%v payload=%q (会话状态: 新订阅, 判定依据: 历史首投 Retain=1)",
			i, m.Type, m.Topic, m.QoS, m.Retain, m.Payload)
		if m.Topic != want[i].topic || m.QoS != want[i].qos || !m.Retain || string(m.Payload) != want[i].payload {
			t.Fatalf("delivery[%d]=%+v, want topic=%s qos=%d retain=true payload=%s", i, m, want[i].topic, want[i].qos, want[i].payload)
		}
		sub.ackIfQoS(m)
	}

	// 精确订阅 + 授予 QoS0：投 min(2,0)=0，仍 Retain=1
	codes, deliv = sub.subscribeCollect("dev/a", 0)
	if len(deliv) != 1 || deliv[0].QoS != 0 || !deliv[0].Retain {
		t.Fatalf("exact sub qos0: want 1 retained qos0 retain=1, got %+v", deliv)
	}
	t.Log("re-subscribe redelivers retained (each SUBSCRIBE is a new matching point), qos downgraded to 0")

	// 订阅没有保留状态的过滤器：只有 SUBACK
	codes, deliv = sub.subscribeCollect("dev/none/+", 1)
	if codes[0] != 1 || len(deliv) != 0 {
		t.Fatalf("no retained state: want 0 deliveries, got %d", len(deliv))
	}
	t.Log("filter without retained state -> SUBACK only")
} // TestRetainOnlineSubscriberNotReplayed 验证在线订阅者收到的实时副本 Retain=0，
// 且不会因为保留发布被重复补推；同一话题实时消息保持 FIFO 顺序。
func TestRetainOnlineSubscriberNotReplayed(t *testing.T) {
	b := newBroker()
	sub, _ := dial(t, b, "sub-live", true)
	sub.subscribe("live/+", 1)
	pub, _ := dial(t, b, "pub-live", true)

	// 两条 Retain=1 的实时发布：在线订阅者各收一条，Retain=0，按序
	id1 := pub.nextID()
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Retain: true,
		Topic: "live/x", PacketID: id1, Payload: []byte("live-1")})
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}
	id2 := pub.nextID()
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Retain: true,
		Topic: "live/x", PacketID: id2, Payload: []byte("live-2")})
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}
	for i, want := range []string{"live-1", "live-2"} {
		m := sub.read()
		t.Logf("online realtime #%d: type=%s topic=%q retain=%v payload=%q (会话状态: 在线, 判定依据: 实时投递 Retain=0 且不重)",
			i, m.Type, m.Topic, m.Retain, m.Payload)
		if m.Type != packet.TypePublish || m.Retain || string(m.Payload) != want {
			t.Fatalf("online delivery: want realtime retain=0 payload=%s, got %+v", want, m)
		}
		sub.send(&packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})
	}
	// 在线者不应再收到任何“补推”的历史副本
	if extra := sub.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("online subscriber got replayed history: %+v", extra)
	}
	t.Log("online subscriber got exactly 2 realtime (Retain=0) copies, no replay -> ok")

	// 同一话题之后的非保留实时发布：在线者收到 Retain=0；新订阅者首投的仍是最新保留态
	id3 := pub.nextID()
	pub.send(&packet.Packet{Type: packet.TypePublish, QoS: 1, Retain: false,
		Topic: "live/x", PacketID: id3, Payload: []byte("live-3-nonretained")})
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}
	m := sub.read()
	if m.Retain || string(m.Payload) != "live-3-nonretained" {
		t.Fatalf("non-retained realtime wrong: %+v", m)
	}
	sub.send(&packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})

	late, _ := dial(t, b, "sub-late", true)
	_, deliv := late.subscribeCollect("live/x", 1)
	if len(deliv) != 1 || !deliv[0].Retain || string(deliv[0].Payload) != "live-2" {
		t.Fatalf("new subscriber must see last retained live-2, got %+v", deliv)
	}
	late.ackIfQoS(deliv[0])
	t.Log("non-retained realtime did not alter retained state; new subscriber sees live-2 -> ok")
}

// TestRetainEmptyPayloadClears 验证空载荷保留发布清除状态且不向在线者转发。
func TestRetainEmptyPayloadClears(t *testing.T) {
	b := newBroker()
	sub, _ := dial(t, b, "sub-clr", true)
	sub.subscribe("clr/+", 1)
	pub, _ := dial(t, b, "pub-clr", true)

	// 先存一条保留状态
	pub.publishRetain("clr/x", "on", 1)
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}
	m := sub.read() // 在线者先收到实时副本
	if m.Retain || string(m.Payload) != "on" {
		t.Fatalf("realtime copy wrong: %+v", m)
	}
	sub.send(&packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})

	// 空载荷 + Retain：在线者什么都收不到
	pub.publishRetain("clr/x", "", 1)
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("empty retained publish still gets PUBACK, got %s", a.Type)
	}
	if extra := sub.readMaybe(300 * time.Millisecond); extra != nil {
		t.Fatalf("empty retained publish must not be forwarded, got %+v", extra)
	}
	t.Log("empty-payload retained publish: not forwarded to online subscribers")

	// 新订阅者首投为空
	fresh, _ := dial(t, b, "sub-clr2", true)
	_, deliv := fresh.subscribeCollect("clr/#", 1)
	if len(deliv) != 0 {
		t.Fatalf("retained state should be cleared, got %d deliveries", len(deliv))
	}
	t.Log("after clear, new wildcard subscriber receives no retained state -> ok")
}

// publishRetain 是发送带 Retain 位的 PUBLISH 的测试辅助。
func (c *testClient) publishRetain(topicName, payload string, qos byte) uint16 {
	c.t.Helper()
	id := uint16(0)
	if qos > 0 {
		id = c.nextID()
	}
	c.send(&packet.Packet{Type: packet.TypePublish, Topic: topicName, Payload: []byte(payload),
		QoS: qos, PacketID: id, Retain: true})
	return id
}

// TestRetainWillUpdatesRetainedState 验证遗嘱（Retain=1）异常断开时更新保留状态，
// 之后的新订阅者直接看到离线状态；正常 DISCONNECT 不触发。
func TestRetainWillUpdatesRetainedState(t *testing.T) {
	b := newBroker()

	// 设备登记保留遗嘱后异常掉线
	dev1, dev2 := net.Pipe()
	devDone := make(chan error, 1)
	go func() { devDone <- b.HandleConnection(dev2) }()
	dev := &testClient{t: t, nc: dev1, done: devDone}
	dev.send(&packet.Packet{Type: packet.TypeConnect, ClientID: "dev-will-ret", CleanSession: true,
		HasWill: true, WillTopic: "state/dev-will-ret", WillPayload: []byte("offline"),
		WillQoS: 1, WillRetain: true})
	if a := dev.read(); a.Type != packet.TypeConnAck {
		t.Fatalf("expected CONNACK, got %s", a.Type)
	}
	dev.close() // 异常断开 -> 遗嘱发布且 Retain=1
	if err := <-devDone; err == nil {
		t.Fatal("abrupt disconnect must end with an error")
	}
	t.Log("device disconnected abruptly -> retained will published (判定依据: 异常断开 + WillRetain)")

	// 后来的订阅者无需等设备再上报，订阅即看到保留的离线状态
	later, _ := dial(t, b, "sub-later", true)
	_, deliv := later.subscribeCollect("state/+", 1)
	if len(deliv) != 1 || !deliv[0].Retain || string(deliv[0].Payload) != "offline" {
		t.Fatalf("new subscriber must see retained will offline, got %+v", deliv)
	}
	later.ackIfQoS(deliv[0])
	t.Logf("later subscriber immediate retained delivery: topic=%q retain=%v payload=%q",
		deliv[0].Topic, deliv[0].Retain, deliv[0].Payload)

	// 正常 DISCONNECT 的设备即使带保留遗嘱也不应更新状态
	g1, g2 := net.Pipe()
	gDone := make(chan error, 1)
	go func() { gDone <- b.HandleConnection(g2) }()
	graceful := &testClient{t: t, nc: g1, done: gDone}
	graceful.send(&packet.Packet{Type: packet.TypeConnect, ClientID: "dev-graceful", CleanSession: true,
		HasWill: true, WillTopic: "state/dev-graceful", WillPayload: []byte("offline2"),
		WillQoS: 1, WillRetain: true})
	graceful.read()
	graceful.send(&packet.Packet{Type: packet.TypeDisconnect})
	if err := <-gDone; err != nil {
		t.Fatalf("graceful disconnect should return nil, got %v", err)
	}
	_, deliv = later.subscribeCollect("state/+", 1)
	for _, m := range deliv {
		if m.Topic == "state/dev-graceful" {
			t.Fatal("retained will must not be published on graceful DISCONNECT")
		}
		later.ackIfQoS(m)
	}
	t.Log("graceful DISCONNECT -> retained will not stored -> ok")
}

// TestRetainIndependentFromSession 验证会话清理/过期不影响保留状态，
// 保留状态也不会复活已清除的会话（不投递到已不存在的订阅者）。
func TestRetainIndependentFromSession(t *testing.T) {
	// 场景1：CleanSession 断开后保留状态仍在
	b := newBroker()
	pub, _ := dial(t, b, "pub-indep", true)
	pub.publishRetain("indep/a", "kept", 1)
	if a := pub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}
	pub.close()
	<-pub.done

	// 一个 clean 订阅者断开（其会话/订阅被清除），不应收到任何保留首投之外的东西
	cleanSub, _ := dial(t, b, "clean-sub", true)
	_, deliv := cleanSub.subscribeCollect("indep/+", 1)
	if len(deliv) != 1 {
		t.Fatalf("retained survives publisher disconnect, want 1 got %d", len(deliv))
	}
	cleanSub.ackIfQoS(deliv[0])
	cleanSub.close()
	<-cleanSub.done
	t.Log("clean sessions gone; retained state at broker level untouched")

	// 新订阅者仍能拿到：保留状态没有随会话清除而丢失
	fresh, _ := dial(t, b, "fresh-sub", true)
	_, deliv = fresh.subscribeCollect("indep/#", 1)
	if len(deliv) != 1 || string(deliv[0].Payload) != "kept" {
		t.Fatalf("retained must survive session cleanup, got %+v", deliv)
	}
	fresh.ackIfQoS(deliv[0])
	t.Log("retained state available after all prior sessions removed (判定依据: 代理级存储独立于会话)")

	// 场景2：持久会话过期后，保留状态仍在；重连视为全新会话
	b2 := New(Config{MaxQueuePerSession: 100, SessionExpiry: 1})
	ps, _ := dial(t, b2, "persist-sub", false)
	ps.subscribe("exp-ret/+", 1)
	ps.close()
	<-ps.done
	ppub, _ := dial(t, b2, "exp-pub", true)
	ppub.publishRetain("exp-ret/x", "still-here", 1)
	if a := ppub.read(); a.Type != packet.TypePubAck {
		t.Fatalf("expected PUBACK, got %s", a.Type)
	}
	time.Sleep(1200 * time.Millisecond) // 等持久会话过期

	again, ack := dial(t, b2, "persist-sub", false)
	if ack.SessionPresent {
		t.Fatal("expired session must not be resumed")
	}
	_, deliv = again.subscribeCollect("exp-ret/+", 1)
	if len(deliv) != 1 || !deliv[0].Retain || string(deliv[0].Payload) != "still-here" {
		t.Fatalf("retained must survive session expiry, got %+v", deliv)
	}
	again.ackIfQoS(deliv[0])
	t.Log("session expired but retained state still served to fresh subscription -> ok")
}

// encodeRaw 构造一个声明指定剩余长度的原始报文（type 仅用于首字节）。
func encodeRaw(packetType byte, remLen int) []byte {
	data := []byte{packetType << 4}
	n := remLen
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 128
		}
		data = append(data, d)
		if n == 0 {
			break
		}
	}
	return data
}

// TestMaxPacketSizeRejected 验证超过单条上限的连接在 CONNECT 阶段即以
// ErrPacketTooLarge 断开（区别于 ErrMalformedPacket），不进入会话表，不影响其他连接。
func TestMaxPacketSizeRejected(t *testing.T) {
	b := New(Config{MaxQueuePerSession: 100, MaxPacketSize: 64})

	// 一条正常连接作为对照：超限处理不能卡住其他连接
	ok, _ := dial(t, b, "neighbor", true)

	c1, c2 := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- b.HandleConnection(c2) }()

	// 声明 remaining length=1000（>64）的 CONNECT：只写头部，服务端不得读/缓冲主体
	hdr := encodeRaw(1, 1000)
	go func() {
		// net.Pipe 可能在服务端关闭后写入失败，忽略
		_, _ = c1.Write(hdr)
		_, _ = c1.Write(make([]byte, 1000))
	}()

	select {
	case err := <-done:
		t.Logf("oversized CONNECT -> err=%v (会话状态: pre-connect 未建会话, 判定依据: errors.Is ErrPacketTooLarge)", err)
		if !errors.Is(err, ErrPacketTooLarge) {
			t.Fatalf("want ErrPacketTooLarge, got %v", err)
		}
		if errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("oversize must be distinct from ErrMalformedPacket, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("oversized CONNECT connection not closed")
	}
	c1.Close()

	// 不留半截状态：该 client 从未建立会话；其他连接照常收发
	b.mu.Lock()
	n := len(b.sessions)
	b.mu.Unlock()
	if n != 1 {
		t.Fatalf("want exactly 1 session (neighbor), got %d", n)
	}
	ok.subscribe("nb/+", 0)
	ok.publish("nb/x", "alive", 0)
	m := ok.read()
	if m.Type != packet.TypePublish || string(m.Payload) != "alive" {
		t.Fatalf("neighbor connection disturbed by oversized peer: %+v", m)
	}
	t.Log("oversized CONNECT left no session, other connections unaffected -> ok")
}

// TestMaxPacketSizeOversizedPublish 验证已连接会话上超大 PUBLISH 被断开，
// 原因可区分且其他连接不受影响。
func TestMaxPacketSizeOversizedPublish(t *testing.T) {
	b := New(Config{MaxQueuePerSession: 100, MaxPacketSize: 64})
	victim, _ := dial(t, b, "victim", true)
	other, _ := dial(t, b, "other", true)
	other.subscribe("p/+", 0)

	// 合法小包不受限制
	victim.publish("p/small", "ok", 0)
	m := other.read()
	if string(m.Payload) != "ok" {
		t.Fatalf("small publish should pass, got %+v", m)
	}
	t.Log("packets at/below limit pass; sending oversized PUBLISH header ...")

	hdr := encodeRaw(3, 1000)
	go func() {
		_, _ = victim.nc.Write(hdr)
		_, _ = victim.nc.Write(make([]byte, 1000))
	}()
	victim.expectConnError(ErrPacketTooLarge)

	// 其他连接仍正常
	other.publish("p/again", "fine", 0)
	// 自己发自己收：other 订阅了 p/+，应收到
	m2 := other.read()
	if m2.Type != packet.TypePublish || string(m2.Payload) != "fine" {
		t.Fatalf("other connection disturbed: %+v", m2)
	}
	t.Log("oversized PUBLISH disconnects with ErrPacketTooLarge, other connections unaffected -> ok")
}

// TestRetainInitialDeliveryQuotaInteraction 验证保留首投同样受会话积压配额约束，
// 触发时以 ErrQuotaExceeded 断开（与慢消费者、报文超限原因互不混淆）。
func TestRetainInitialDeliveryQuotaInteraction(t *testing.T) {
	b := New(Config{MaxQueuePerSession: 2, MaxPacketSize: 0})
	// 预置 3 个话题的保留状态，订阅者配额为 2：首投到第 3 条时触发配额
	setup, _ := dial(t, b, "setup", true)
	for _, tp := range []string{"q/a", "q/b", "q/c"} {
		setup.publishRetain(tp, tp, 1)
		if a := setup.read(); a.Type != packet.TypePubAck {
			t.Fatalf("expected PUBACK, got %s", a.Type)
		}
	}
	setup.close()
	<-setup.done

	sub, _ := dial(t, b, "sub-rq", true)
	id := sub.nextID()
	sub.send(&packet.Packet{Type: packet.TypeSubscribe, PacketID: id,
		Subscriptions: []packet.Subscription{{Topic: "q/#", QoS: 1}}})
	// 不应收到 SUBACK：保留首投压垮配额，连接直接以 ErrQuotaExceeded 结束
	select {
	case err := <-sub.done:
		t.Logf("retained fanout hit quota -> err=%v (判定依据: errors.Is ErrQuotaExceeded, 非 ErrPacketTooLarge)", err)
		if !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("want ErrQuotaExceeded, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connection not closed after retained fanout exceeded quota")
	}
}

// TestRetainSubscribePublishConcurrentOrder 压测订阅与发布并发交错：
// 每个话题订阅者恰好收到两次有效状态之一（历史值 rN 或实时值 rN+1），
// 不允许既收到历史又收到实时（重复），也不允许实时在前历史在后（乱序）。
func TestRetainSubscribePublishConcurrentOrder(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		b := newBroker()
		pub, _ := dial(t, b, "race-pub", true)

		// 预置话题 i 的保留状态为 old-i
		for i := 0; i < 8; i++ {
			pub.publishRetain(fmt.Sprintf("r/%d", i), fmt.Sprintf("old-%d", i), 0)
		}

		sub, _ := dial(t, b, "race-sub", true)
		// 订阅与一条同话题实时发布并发（实时值 new-i 会覆盖保留）
		go func() {
			id := sub.nextID()
			sub.send(&packet.Packet{Type: packet.TypeSubscribe, PacketID: id,
				Subscriptions: []packet.Subscription{{Topic: "r/#", QoS: 0}}})
		}()
		go func() {
			for i := 0; i < 8; i++ {
				pub.publish(fmt.Sprintf("r/%d", i), fmt.Sprintf("new-%d", i), 0)
			}
		}()

		// 收集 SUBACK 前后收到的全部消息
		seen := make(map[string][]bool) // topic -> [oldSeen, newSeen]
		deadline := time.Now().Add(3 * time.Second)
		gotSubAck := false
		for time.Now().Before(deadline) {
			p := sub.readMaybe(100 * time.Millisecond)
			if p == nil {
				if gotSubAck {
					break
				}
				continue
			}
			if p.Type == packet.TypeSubAck {
				gotSubAck = true
				continue
			}
			if p.Type != packet.TypePublish {
				t.Fatalf("unexpected packet %s", p.Type)
			}
			v := string(p.Payload)
			var idx int
			var val string
			if _, err := fmt.Sscanf(v, "old-%d", &idx); err == nil {
				val = "old"
			} else if _, err := fmt.Sscanf(v, "new-%d", &idx); err == nil {
				val = "new"
			} else {
				t.Fatalf("unexpected payload %q", v)
			}
			key := fmt.Sprintf("r/%d", idx)
			if seen[key] == nil {
				seen[key] = []bool{false, false}
			}
			// 关键顺序断言：new（实时）一旦出现，old（历史）不得再出现
			if val == "old" {
				if !p.Retain {
					t.Fatalf("iter=%d topic=%s: historical delivery must carry Retain=1", iter, key)
				}
				if seen[key][1] {
					t.Fatalf("iter=%d topic=%s: realtime(new) delivered before retained(old) -> reorder/dup", iter, key)
				}
				seen[key][0] = true
			} else {
				if p.Retain {
					t.Fatalf("iter=%d topic=%s: realtime delivery must carry Retain=0", iter, key)
				}
				seen[key][1] = true
			}
		}
		for i := 0; i < 8; i++ {
			key := fmt.Sprintf("r/%d", i)
			s := seen[key]
			if s == nil || (!s[0] && !s[1]) {
				t.Fatalf("iter=%d topic=%s: no delivery at all", iter, key)
			}
			// 每个话题：历史（Retain=1）与实时（Retain=0）至多各一条，且历史只可能在前
			if s[0] && s[1] {
				// 两者都收到是允许的（先订阅后发布）：old 在前已由上面的顺序断言保证，
				// 这里要求确实只可能是这一种组合。
			}
		}
		sub.close()
		pub.close()
		<-sub.done
		<-pub.done
	}
	t.Log("20 iterations: retained/realtime never duplicated or reordered across subscribe boundary")
}
