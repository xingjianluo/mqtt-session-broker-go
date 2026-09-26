package session

import "testing"

func TestEnqueueQuota(t *testing.T) {
	s := NewSession("c", 3, 2)
	s.SetOnline(true)
	for i := 0; i < 3; i++ {
		if r := s.Enqueue("t", []byte{byte(i)}, 1, 0); r != EnqueueAccepted {
			t.Fatalf("enqueue #%d result=%s want accepted", i, r)
		}
	}
	if r := s.Enqueue("t", []byte("x"), 1, 0); r != EnqueueQueueFull {
		t.Fatalf("第4条结果=%s, 期望 queue_full（队列+在途配额=3）", r)
	}
	st := s.Snapshot()
	t.Logf("判定依据: 报文类型=入站PUBLISH(QoS1) 会话状态=%+v, 队列满拒绝原因=queue_full, dropped=%d",
		st, st.DroppedCount)
}

func TestOfflineQoS0Dropped(t *testing.T) {
	s := NewSession("c", 10, 2)
	// 不上线
	if r := s.Enqueue("t", nil, 0, 0); r != EnqueueDroppedOfflineQoS0 {
		t.Fatalf("离线 QoS0 结果=%s 期望 dropped_offline_qos0", r)
	}
	t.Log("判定依据: 会话离线时 QoS0 按 MQTT 3.1.1 §3.1.2-7 直接丢弃，结果可区分")
}

func TestQoS1Flow(t *testing.T) {
	s := NewSession("c", 10, 2)
	s.SetOnline(true)
	s.Enqueue("a", []byte("m1"), 1, 0)
	s.Enqueue("a", []byte("m2"), 1, 0)
	ready := s.TakeReady()
	if len(ready) != 2 {
		t.Fatalf("TakeReady=%d want 2", len(ready))
	}
	if ready[0].PacketID == ready[1].PacketID || ready[0].PacketID == 0 {
		t.Fatalf("packet ids must be distinct nonzero: %d,%d", ready[0].PacketID, ready[1].PacketID)
	}
	// 第三条因在途窗口=2 而无法发送，仍在队列。
	s.Enqueue("a", []byte("m3"), 1, 0)
	if r := s.TakeReady(); len(r) != 0 {
		t.Fatalf("window full 时应取 0 条, got %d", len(r))
	}
	if !s.AckQoS1(ready[0].PacketID) {
		t.Fatal("PUBACK 应完成第一条在途消息")
	}
	next := s.TakeReady()
	if len(next) != 1 || string(next[0].Payload) != "m3" {
		t.Fatalf("窗口释放后应发送 m3, got %+v", next)
	}
	st := s.Snapshot()
	t.Logf("判定依据: 报文类型=PUBLISH/PUBACK 会话状态 queue=%d inflight=%d nextID=%d; 确认后窗口推进",
		st.QueueLen, st.InflightLen, st.NextPacketID)
}

func TestQoS2NoDuplicateDelivery(t *testing.T) {
	s := NewSession("c", 10, 4)
	// 入站 QoS2：同一 packetID 两次 PUBLISH，只有第一次允许交付。
	if !s.NoteIncomingPublish(7) {
		t.Fatal("首次 QoS2 PUBLISH 应返回 true（交付）")
	}
	if s.NoteIncomingPublish(7) {
		t.Fatal("重传的同 packetID QoS2 PUBLISH 必须返回 false（不得重复交付），仍重发 PUBREC")
	}
	if !s.NoteIncomingPubrel(7) {
		t.Fatal("已知 packetID 的 PUBREL 应返回 true 并发 PUBCOMP")
	}
	if s.NoteIncomingPubrel(99) {
		t.Fatal("未知 packetID 的 PUBREL 返回 false，但协议上仍回复 PUBCOMP")
	}
	st := s.Snapshot()
	t.Logf("判定依据: 报文类型=PUBLISH(QoS2)/PUBREL 会话状态 incomingQoS2=%d; 同会话同 packetID 不重复投递",
		st.IncomingQoS2)
}

func TestOutboundQoS2Resume(t *testing.T) {
	s := NewSession("c", 10, 4)
	s.SetOnline(true)
	s.Enqueue("a", []byte("m"), 2, 0)
	ready := s.TakeReady()
	if len(ready) != 1 || ready[0].QoS != 2 {
		t.Fatalf("ready=%+v", ready)
	}
	id := ready[0].PacketID

	// 断线重连场景：会话保留、在途未确认。
	s.SetOnline(false)
	st := s.Snapshot()
	if st.InflightLen != 1 {
		t.Fatalf("离线时 inflight=%d want 1（未完成消息必须保留以备续传）", st.InflightLen)
	}
	s.SetOnline(true)
	resent := s.ResendInflight()
	if len(resent) != 1 || resent[0].PacketID != id {
		t.Fatalf("重连后重发列表=%+v, 期望同一 packetID=%d DUP 重传", resent, id)
	}
	if resent[0].Retain {
		t.Fatal("重传消息 RETAIN 必须为 0")
	}
	// 收到 PUBREC -> 发 PUBREL；再收到 PUBCOMP 完成。
	if !s.RecvPUBREC(id) {
		t.Fatal("PUBREC 应推进出站 QoS2 状态")
	}
	if ids := s.InflightPubrels(); len(ids) != 1 || ids[0] != id {
		t.Fatalf("PUBREL_SENT 列表=%v", ids)
	}
	if !s.RecvPUBCOMP(id) {
		t.Fatal("PUBCOMP 应完成出站 QoS2")
	}
	st = s.Snapshot()
	if st.InflightLen != 0 {
		t.Fatalf("完成后 inflight=%d want 0", st.InflightLen)
	}
	t.Logf("判定依据: 报文类型=PUBLISH(QoS2)/PUBREC/PUBREL/PUBCOMP 会话状态=%+v; 重连续传且不丢不重", st)
}

func TestRetainFlagClearedAfterFirstSend(t *testing.T) {
	s := NewSession("c", 10, 4)
	s.SetOnline(true)
	s.Enqueue("a", []byte("m"), 1, 1)
	first := s.TakeReady()
	if !first[0].Retain {
		t.Fatal("保留消息首投 RETAIN 必须为 1")
	}
	again := s.ResendInflight()
	if again[0].Retain {
		t.Fatal("首投之后重传 RETAIN 必须为 0")
	}
	t.Log("判定依据: §3.3.1.3 保留消息仅在订阅后首投携带 RETAIN")
}

func TestClearResetsState(t *testing.T) {
	s := NewSession("c", 10, 4)
	s.SetOnline(true)
	s.Enqueue("a", []byte("m"), 1, 0)
	s.TakeReady()
	s.NoteIncomingPublish(3)
	s.Clear()
	st := s.Snapshot()
	if st.QueueLen != 0 || st.InflightLen != 0 || st.IncomingQoS2 != 0 {
		t.Fatalf("Clear 后状态非空: %+v", st)
	}
	t.Logf("判定依据: Clear（clean 会话断开/会话过期）后消息与去重表全部清空: %+v", st)
}
