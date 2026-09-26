package broker

import (
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
)

// TestQueueQuotaExceededOffline 离线持久会话积压超过配额：
// 超过 maxQueue 的消息被拒绝并产生 queue_quota_exceeded 事件，连接重连后
// 恰好收到前 maxQueue 条（有界占用内存，拒绝原因可区分）。
func TestQueueQuotaExceededOffline(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxClientQueue = 5
	cfg.MaxInflight = 2
	cfg.EventBufferSize = 256
	b, addr := startBroker(t, cfg)

	sub := dialClient(t, addr, "qs", false, 0)
	sub.subscribe(1, 1, "q/#")
	sub.readFrame()
	sub.close()
	time.Sleep(150 * time.Millisecond)

	pub := dialClient(t, addr, "qpub", true, 0)
	defer pub.close()
	// 发布 8 条 QoS1：只有前 5 条入队（队列+在途总量上限=5，离线时在途为 0）。
	for i := 0; i < 8; i++ {
		pub.publishAcked("q/n", []byte{byte('A' + i)}, 1, uint16(i+1), false)
	}

	// 收集 drop 事件（离线时 cc==nil，不断开，只拒绝入队）。
	var drops int
	deadline := time.After(1500 * time.Millisecond)
collect:
	for {
		select {
		case ev := <-b.Events():
			if ev.Type == "drop" && ev.Reason == ReasonQueueQuotaExceeded {
				drops++
			}
		case <-deadline:
			break collect
		}
	}
	if drops != 3 {
		t.Fatalf("队列满拒绝事件=%d 期望 3（8-5），判定依据: 拒绝原因可区分", drops)
	}

	sub2 := dialClientExpectPresent(t, addr, "qs", false, true)
	defer sub2.close()
	got := map[byte]bool{}
	for i := 0; i < 5; i++ {
		p := sub2.recvPublishWait(2 * time.Second)
		if p == nil {
			t.Fatalf("只收到 %d/5 条入队消息", i)
		}
		got[p.Payload[0]] = true
		sub2.raw(mqttproto.EncodeAck(mqttproto.TypePUBACK, p.PacketID))
	}
	if f := sub2.readFrameWait(400 * time.Millisecond); f != nil {
		t.Fatalf("不应收到被配额拒绝的消息，读到 %s", f.Type)
	}
	t.Logf("判定依据: 报文类型=PUBLISH(QoS1) 会话状态=离线, 配额=5 拒绝=3(queue_quota_exceeded) 重连后收到=%d 条", len(got))
}

// TestQueueQuotaExceededOnlineDisconnects 在线会话队列打满时连接以
// queue_quota_exceeded 原因断开（不 ACK 的慢订阅者），会话仍持久保留在途消息。
func TestQueueQuotaExceededOnlineDisconnects(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxClientQueue = 4
	cfg.MaxInflight = 2
	cfg.EventBufferSize = 256
	b, addr := startBroker(t, cfg)

	sub := dialClient(t, addr, "qo", false, 0)
	defer sub.close()
	sub.subscribe(1, 1, "qo/#")
	sub.readFrame()

	pub := dialClient(t, addr, "qopub", true, 0)
	defer pub.close()
	// 订阅者不回 PUBACK：2 条占满在途窗口，再来消息入队直到总量=4，之后触发断开。
	for i := 0; i < 6; i++ {
		pub.publishAcked("qo/n", []byte{byte('A' + i)}, 1, uint16(i+1), false)
	}

	var reason DisconnectReason
	deadline := time.After(3 * time.Second)
collect:
	for {
		select {
		case ev := <-b.Events():
			if ev.Type == "disconnect" && ev.ClientID == "qo" {
				reason = ev.Reason
				break collect
			}
			if ev.Type == "drop" && ev.Reason == ReasonQueueQuotaExceeded {
				reason = ev.Reason
				break collect
			}
		case <-deadline:
			break collect
		}
	}
	if reason != ReasonQueueQuotaExceeded {
		t.Fatalf("期望断开/拒绝原因 queue_quota_exceeded, 实际=%q", reason)
	}
	t.Logf("判定依据: 报文类型=PUBLISH(QoS1) 会话状态=在线但不ACK, 积压达到配额 4 后原因=%q", reason)
}

// TestSlowConsumerDisconnect 客户端不读取 TCP 数据（慢消费者）：
// QoS0 消息直接走连接出站通道（不占会话队列），通道持续阻塞超过 SlowConsumerTimeout
// 后以 slow_consumer 原因断开——该原因与 queue_quota_exceeded 明确可区分。
func TestSlowConsumerDisconnect(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SlowConsumerTimeout = 600 * time.Millisecond
	cfg.EventBufferSize = 256
	b, addr := startBroker(t, cfg)

	conn, err := netDial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeRaw := func(p []byte) {
		if _, err := conn.Write(p); err != nil {
			t.Fatal(err)
		}
	}
	writeRaw(mqttproto.EncodeConnect(&mqttproto.Connect{ClientID: "slow", CleanSession: false}))
	writeRaw(mqttproto.EncodeSubscribe(&mqttproto.Subscribe{
		PacketID: 1,
		Filters:  []mqttproto.SubFilter{{Filter: "slow/#", QoS: 0}},
	}))
	time.Sleep(150 * time.Millisecond) // 等待订阅生效

	pub := dialClient(t, addr, "slowpub", true, 0)
	defer pub.close()
	// 大量 QoS0 消息（1400B * 4000 ≈ 5.6MB，超过 loopback 发送/接收缓冲）：
	// 发送放到独立 goroutine，避免路由背压反压阻塞测试主流程的事件等待。
	go func() {
		for i := 0; i < 4000; i++ {
			pub.publishQoS0("slow/n", makePayload(i), false)
		}
	}()

	var reason DisconnectReason
	deadline := time.After(5 * time.Second)
loop:
	for {
		select {
		case ev := <-b.Events():
			if ev.ClientID == "slow" && ev.Type == "disconnect" {
				reason = ev.Reason
				break loop
			}
		case <-deadline:
			break loop
		}
	}
	if reason != ReasonSlowConsumer {
		t.Fatalf("期望 slow_consumer 断开, 实际=%q", reason)
	}
	t.Logf("判定依据: 报文类型=PUBLISH(QoS0) 会话状态=慢消费者(不读TCP), outCh 阻塞超时后原因=%q", reason)
}

func makePayload(i int) []byte {
	b := make([]byte, 1400)
	b[0] = byte(i)
	return b
}
