package broker

import (
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
)

// TestPersistentSessionResumeQoS1 客户端离线期间的 QoS1 消息必须在重连后续传，不丢不重。
func TestPersistentSessionResumeQoS1(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())

	sub := dialClient(t, addr, "resume-sub", false, 0)
	defer sub.close()
	sub.subscribe(1, 1, "r/topic")
	sub.readFrame() // SUBACK

	// 模拟异常断开（直接关 TCP，不发 DISCONNECT）。
	sub.close()
	time.Sleep(150 * time.Millisecond) // 等待 broker 感知

	pub := dialClient(t, addr, "resume-pub", true, 0)
	defer pub.close()
	pub.publishAcked("r/topic", []byte("offline-1"), 1, 1, false)
	pub.publishAcked("r/topic", []byte("offline-2"), 1, 2, false)

	// 同 clientID、clean=false 重连；CONNACK sessionPresent 必须为 1。
	sub2 := dialClientExpectPresent(t, addr, "resume-sub", false, true)
	defer sub2.close()

	p1 := sub2.recvPublish()
	p2 := sub2.recvPublish()
	if string(p1.Payload) != "offline-1" || string(p2.Payload) != "offline-2" {
		t.Fatalf("重连续传内容/顺序错误: %q, %q", p1.Payload, p2.Payload)
	}
	// 不回 PUBACK，再重连一次：两条未确认消息必须以 DUP=1 重传（仍恰好各一条）。
	sub2.raw(mqttproto.EncodeAck(mqttproto.TypePUBACK, p1.PacketID)) // 只确认第一条
	sub2.close()
	time.Sleep(150 * time.Millisecond)
	sub3 := dialClientExpectPresent(t, addr, "resume-sub", false, true)
	defer sub3.close()
	p := sub3.recvPublish()
	if string(p.Payload) != "offline-2" || !p.Dup {
		t.Fatalf("未确认消息重连后应 DUP=1 重传, got payload=%q dup=%v", p.Payload, p.Dup)
	}
	if f := sub3.readFrameWait(300 * time.Millisecond); f != nil {
		t.Fatalf("已确认的 offline-1 不得重传，却读到 %s", f.Type)
	}
	t.Logf("判定依据: 报文类型=PUBLISH(QoS1 DUP=%v) 会话状态=持久, 离线消息续传2条且已确认1条不重复, packetID=%d",
		p.Dup, p.PacketID)
}

// TestCleanSessionDropsState clean=true 连接必须看到空会话（无离线消息）。
func TestCleanSessionDropsState(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	sub := dialClient(t, addr, "clean-sub", false, 0)
	sub.subscribe(1, 1, "c/topic")
	sub.readFrame()
	sub.close()
	time.Sleep(100 * time.Millisecond)

	pub := dialClient(t, addr, "clean-pub", true, 0)
	defer pub.close()
	pub.publishAcked("c/topic", []byte("ghost"), 1, 1, false)

	// clean=true 重连：sessionPresent=0，且不应收到离线消息。
	sub2 := dialClientExpectPresent(t, addr, "clean-sub", true, false)
	defer sub2.close()
	if f := sub2.readFrameWait(400 * time.Millisecond); f != nil {
		t.Fatalf("clean 会话不应收到旧消息，读到 %s", f.Type)
	}
	t.Log("判定依据: CleanSession=1 CONNACK sessionPresent=0，旧队列与订阅均已清除")
}

// TestInboundQoS2NoDuplicateDelivery 重传的 QoS2 PUBLISH 不得重复交付给订阅者。
func TestInboundQoS2NoDuplicateDelivery(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	mon := dialClient(t, addr, "mon", true, 0)
	defer mon.close()
	mon.subscribe(1, 2, "q2/topic")
	mon.readFrame()

	pub := dialClient(t, addr, "q2pub", true, 0)
	defer pub.close()

	// 第一次 QoS2 PUBLISH(id=5)，只回 PUBREC，不发 PUBREL。
	pub.raw(mqttproto.EncodePublish(&mqttproto.Publish{
		Topic: "q2/topic", QoS: 2, PacketID: 5, Payload: []byte("once"),
	}))
	f := pub.readFrame()
	if f.Type != mqttproto.TypePUBREC {
		t.Fatalf("want PUBREC got %s", f.Type)
	}
	first := mon.recvPublish()
	if string(first.Payload) != "once" {
		t.Fatalf("monitor got %q", first.Payload)
	}

	// 重传同一 PUBLISH(DUP=1, id=5)：监控者不得第二次收到，发布者仍收到 PUBREC。
	pub.raw(mqttproto.EncodePublish(&mqttproto.Publish{
		Topic: "q2/topic", QoS: 2, PacketID: 5, Payload: []byte("once"), Dup: true,
	}))
	f = pub.readFrame()
	if f.Type != mqttproto.TypePUBREC {
		t.Fatalf("重传后仍应回复 PUBREC, got %s", f.Type)
	}
	if mf := mon.readFrameWait(400 * time.Millisecond); mf != nil {
		t.Fatalf("重传 QoS2 PUBLISH 不得重复交付, 监控者却收到 %s", mf.Type)
	}

	// 完成 QoS2 流程：PUBREL -> PUBCOMP。
	pub.raw(mqttproto.EncodeAck(mqttproto.TypePUBREL, 5))
	f = pub.readFrame()
	if f.Type != mqttproto.TypePUBCOMP {
		t.Fatalf("want PUBCOMP got %s", f.Type)
	}
	t.Log("判定依据: 报文类型=PUBLISH(QoS2 DUP=1)/PUBREC/PUBREL/PUBCOMP 会话状态=incomingQoS2 去重, 订阅者仅交付1次")
}

// TestRetainedMessageLifecycle 保留消息存储、首投、覆盖与清除。
func TestRetainedMessageLifecycle(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())

	pub := dialClient(t, addr, "retpub", true, 0)
	defer pub.close()
	// 发布保留消息（此时无订阅者）。
	pub.publishAcked("ret/temp", []byte("42"), 1, 1, true)

	s1 := dialClient(t, addr, "retsub1", true, 0)
	defer s1.close()
	s1.subscribe(1, 1, "ret/#")
	// 先 SUBACK，再收到保留消息首投。
	if f := s1.readFrame(); f.Type != mqttproto.TypeSUBACK {
		t.Fatalf("want SUBACK got %s", f.Type)
	}
	p := s1.recvPublish()
	if string(p.Payload) != "42" || !p.Retain {
		t.Fatalf("保留消息首投 payload=%q retain=%v", p.Payload, p.Retain)
	}

	// 覆盖保留消息。
	pub.publishAcked("ret/temp", []byte("43"), 1, 2, true)
	p2 := s1.recvPublish()
	if string(p2.Payload) != "43" {
		t.Fatalf("新发布消息 = %q want 43", p2.Payload)
	}
	if p2.Retain {
		t.Fatal("普通路由的保留消息（非订阅首投）RETAIN 必须为 0")
	}

	// 新订阅者只应看到最新值 43。
	s2 := dialClient(t, addr, "retsub2", true, 0)
	defer s2.close()
	s2.subscribe(2, 1, "ret/#")
	s2.readFrame()
	p3 := s2.recvPublish()
	if string(p3.Payload) != "43" || !p3.Retain {
		t.Fatalf("新订阅者首投=%q retain=%v want 43/true", p3.Payload, p3.Retain)
	}

	// 空载荷清除保留消息。
	pub.publishQoS0("ret/temp", []byte(""), true)
	s3 := dialClient(t, addr, "retsub3", true, 0)
	defer s3.close()
	s3.subscribe(3, 1, "ret/#")
	s3.readFrame()
	if f := s3.readFrameWait(400 * time.Millisecond); f != nil {
		t.Fatalf("清除后新订阅不应收到保留消息，读到 %s", f.Type)
	}
	t.Log("判定依据: 报文类型=PUBLISH retain=1/0, 保留消息首投 RETAIN=1、普通投递 RETAIN=0、空载荷清除")
}

// TestWillMessages 正常 DISCONNECT 不发遗嘱；异常断线（keepalive 之外的 TCP 关闭）发遗嘱。
func TestWillMessages(t *testing.T) {
	_, addr := startBroker(t, func() Config {
		c := DefaultConfig()
		return c
	}())

	mon := dialClient(t, addr, "willmon", true, 0)
	defer mon.close()
	mon.subscribe(1, 1, "will/#")
	mon.readFrame()

	// 带遗嘱的客户端：正常 DISCONNECT -> 监控者收不到遗嘱。
	normal := dialWillClient(t, addr, "will-normal", "will/normal", []byte("nope"), false)
	normal.sendDisconnect()
	normal.close()
	if f := mon.readFrameWait(400 * time.Millisecond); f != nil {
		t.Fatalf("正常 DISCONNECT 不得发布遗嘱，读到 %s", f.Type)
	}

	// 异常断开：直接关闭 TCP。
	bad := dialWillClient(t, addr, "will-bad", "will/bad", []byte("boom"), true)
	bad.close()
	p := mon.recvPublish()
	if p.Topic != "will/bad" || string(p.Payload) != "boom" {
		t.Fatalf("遗嘱内容错误: topic=%s payload=%q", p.Topic, p.Payload)
	}
	if p.Retain {
		// §3.3.1.3：服务端路由发布的 PUBLISH RETAIN 必须为 0；
		// willRetain=1 只影响遗嘱是否进入保留存储（由新订阅者首投验证）。
		t.Fatal("路由中的遗嘱 PUBLISH RETAIN 必须为 0（仅保留消息首投为 1）")
	}
	if p.QoS == 1 {
		mon.raw(mqttproto.EncodeAck(mqttproto.TypePUBACK, p.PacketID))
	}
	// 验证遗嘱同时成为保留消息：新订阅者首投可收到且 RETAIN=1。
	s := dialClient(t, addr, "willsub", true, 0)
	defer s.close()
	s.subscribe(2, 1, "will/#")
	s.readFrame()
	rp := s.recvPublish()
	if rp.Topic != "will/bad" || string(rp.Payload) != "boom" || !rp.Retain {
		t.Fatalf("遗嘱保留消息首投错误: %+v", rp)
	}
	t.Logf("判定依据: 报文类型=PUBLISH(遗嘱) 正常DISCONNECT无遗嘱; TCP异常关闭发布遗嘱 topic=%s, 路由副本RETAIN=0 保留首投RETAIN=1", p.Topic)
}

// TestTakeoverNoWill 同 clientID 新连接接管旧连接时，旧连接遗嘱不得发布。
func TestTakeoverNoWill(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	mon := dialClient(t, addr, "tkmon", true, 0)
	defer mon.close()
	mon.subscribe(1, 1, "tk/#")
	mon.readFrame()

	first := dialWillClient(t, addr, "tk-client", "tk/will", []byte("should-not-fire"), false)
	second := dialClient(t, addr, "tk-client", true, 0)
	defer second.close()
	// 等旧连接处理完成。
	time.Sleep(200 * time.Millisecond)
	second.publishQoS0("tk/alive", []byte("hi"), false)
	p := mon.recvPublish()
	if p.Topic != "tk/alive" {
		t.Fatalf("接管后应收到新连接消息, got topic=%s payload=%q", p.Topic, p.Payload)
	}
	if f := mon.readFrameWait(400 * time.Millisecond); f != nil {
		t.Fatalf("接管旧连接不得发布遗嘱，读到 %s", f.Type)
	}
	first.close()
	t.Log("判定依据: 报文类型=CONNECT(同ID) 会话状态=接管, 旧连接原因=taken_over 且不发布遗嘱")
}

// TestPersistentSessionResumeQoS2 出站 QoS2 在断线重连后按状态续传：
// 已收到 PUBREC 的重发 PUBREL；完全未确认的重发 PUBLISH(DUP=1)，最终不丢不重。
func TestPersistentSessionResumeQoS2(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())

	sub := dialClient(t, addr, "q2s", false, 0)
	defer sub.close()
	sub.subscribe(1, 2, "q2r/#")
	sub.readFrame()

	pub := dialClient(t, addr, "q2rp", true, 0)
	defer pub.close()
	pub.publishAcked("q2r/a", []byte("one"), 2, 1, false)
	pub.publishAcked("q2r/b", []byte("two"), 2, 2, false)

	// 收到两条 QoS2 PUBLISH；对第一条只回 PUBREC（推进到 PUBREL_SENT），第二条什么都不回。
	m1 := sub.recvPublish()
	m2 := sub.recvPublish()
	sub.raw(mqttproto.EncodeAck(mqttproto.TypePUBREC, m1.PacketID))
	// 应收到 PUBREL（针对第一条），但暂不回 PUBCOMP。
	rel := sub.readFrame()
	if rel.Type != mqttproto.TypePUBREL {
		t.Fatalf("期望 PUBREL got %s", rel.Type)
	}
	t.Logf("判定依据: 报文类型=PUBLISH(QoS2) m1=%s(packetID=%d,状态=PUBREL_SENT) m2=%s(状态=PUBLISHED)",
		m1.Payload, m1.PacketID, m2.Payload)

	// 异常断线后重连：第一条应重发 PUBREL，第二条重发 PUBLISH(DUP=1)。
	sub.close()
	time.Sleep(150 * time.Millisecond)
	sub2 := dialClientExpectPresent(t, addr, "q2s", false, true)
	defer sub2.close()

	seenRel := false
	seenPub := false
	for i := 0; i < 2; i++ {
		f := sub2.readFrame()
		switch f.Type {
		case mqttproto.TypePUBREL:
			if decodeID(t, f) != m1.PacketID {
				t.Fatalf("重连后 PUBREL id=%d 期望 %d", decodeID(t, f), m1.PacketID)
			}
			seenRel = true
			sub2.raw(mqttproto.EncodeAck(mqttproto.TypePUBCOMP, decodeID(t, f)))
		case mqttproto.TypePUBLISH:
			p, err := mqttproto.DecodePublish(f.Flags, f.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(p.Payload) != "two" || !p.Dup {
				t.Fatalf("重连后 m2 应 DUP=1 重发, got payload=%q dup=%v", p.Payload, p.Dup)
			}
			seenPub = true
			sub2.raw(mqttproto.EncodeAck(mqttproto.TypePUBREC, p.PacketID))
			rel2 := sub2.readFrame()
			if rel2.Type != mqttproto.TypePUBREL {
				t.Fatalf("期望 PUBREL got %s", rel2.Type)
			}
			sub2.raw(mqttproto.EncodeAck(mqttproto.TypePUBCOMP, p.PacketID))
		default:
			t.Fatalf("重连后意外报文 %s", f.Type)
		}
	}
	if !seenRel || !seenPub {
		t.Fatalf("QoS2 续传不完整: PUBREL重发=%v PUBLISH重发=%v", seenRel, seenPub)
	}
	// drain：不应该再有任何消息。
	if f := sub2.readFrameWait(400 * time.Millisecond); f != nil {
		t.Fatalf("QoS2 完成后不应再有重传, 读到 %s", f.Type)
	}
	t.Log("判定依据: 报文类型=PUBREL/PUBLISH(QoS2,DUP=1)/PUBCOMP 会话状态=持久恢复, 两条消息分别从 PUBREL_SENT 与 PUBLISHED 续传, 不丢不重")
}
