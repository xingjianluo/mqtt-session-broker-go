package broker

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
)

// TestWildcardRouting 验证通配符路由与 QoS 降级。
func TestWildcardRouting(t *testing.T) {
	b, addr := startBroker(t, DefaultConfig())
	_ = b
	pub := dialClient(t, addr, "pub", true, 0)
	sub1 := dialClient(t, addr, "sub-plus", true, 0)
	sub2 := dialClient(t, addr, "sub-hash", true, 0)
	sub3 := dialClient(t, addr, "sub-exact", true, 0)
	defer pub.close()
	defer sub1.close()
	defer sub2.close()
	defer sub3.close()

	sub1.subscribe(1, 1, "dev/+/temp")
	sub2.subscribe(2, 2, "dev/#")
	sub3.subscribe(3, 2, "dev/42/temp")
	sub1.readFrame() // SUBACK
	sub2.readFrame()
	sub3.readFrame()

	pub.publishAcked("dev/42/temp", []byte("23.5"), 1, 1, false)

	got := map[string]*mqttproto.Publish{}
	got["sub-plus"] = sub1.recvPublish()
	got["sub-hash"] = sub2.recvPublish()
	got["sub-exact"] = sub3.recvPublish()
	for name, p := range got {
		if p.Topic != "dev/42/temp" || string(p.Payload) != "23.5" {
			t.Fatalf("%s 收到 %+v", name, p)
		}
		t.Logf("判定依据: 报文类型=PUBLISH 客户端=%s 会话状态=在线, 命中过滤器 topic=%s qos=%d dup=%v",
			name, p.Topic, p.QoS, p.Dup)
	}
	if got["sub-plus"].QoS != 1 {
		t.Fatalf("sub-plus 订阅 QoS1，发布 QoS1，但投递 QoS=%d", got["sub-plus"].QoS)
	}
}

// TestPublishToWildcardTopicRejected 发布主题含通配符必须被断开拒绝。
func TestPublishToWildcardTopicRejected(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	c := dialClient(t, addr, "badpub", true, 0)
	defer c.close()
	c.publishQoS0("a/+/c", []byte("x"), false)
	// 服务端应关闭连接。
	if f := c.readFrameWait(500 * time.Millisecond); f != nil {
		t.Fatalf("非法主题后期望连接被关闭，却读到报文 %s", f.Type)
	}
	t.Log("判定依据: PUBLISH 主题含 '+' -> protocol_violation 断开，无后续报文")
}

// TestSubscribeRejectReasons 非法订阅过滤器逐条返回 0x80，合法过滤器仍授予。
func TestSubscribeRejectReasons(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	c := dialClient(t, addr, "subbad", true, 0)
	defer c.close()
	// 同一 SUBSCRIBE 内含一个非法、一个合法过滤器。
	c.subscribe(10, 1, "a/#/b", "ok/topic")
	f := c.readFrame()
	if f.Type != mqttproto.TypeSUBACK {
		t.Fatalf("want SUBACK got %s", f.Type)
	}
	codes := f.Body[2:]
	if len(codes) != 2 || codes[0] != 0x80 || codes[1] != 1 {
		t.Fatalf("SUBACK codes=%x, want [0x80 0x01]（非法与合法原因可区分）", codes)
	}
	t.Logf("判定依据: 报文类型=SUBSCRIBE/SUBACK 逐条授予码=%x: 非法过滤器=0x80, 合法=QoS1", codes)
}

// TestConcurrentPubSub 多发布者并发发布，订阅者对每条消息恰好收到一次。
func TestConcurrentPubSub(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	sub := dialClient(t, addr, "sub", true, 0)
	defer sub.close()
	sub.subscribe(1, 1, "loadtest/+")
	sub.readFrame()

	const publishers = 8
	const perPub = 25
	var wg sync.WaitGroup
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pc := dialClient(t, addr, "pub-"+itoaPub(idx), true, 0)
			defer pc.close()
			for j := 0; j < perPub; j++ {
				pc.publishAcked("loadtest/"+itoaPub(idx), []byte(itoaPub(j)), 1, uint16(j+1), false)
			}
		}(i)
	}
	wg.Wait()

	total := publishers * perPub
	counts := make(map[string]int)
	var countMu sync.Mutex
	var readErr error
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			p := sub.recvPublishWait(12 * time.Second)
			if p == nil {
				return // 超时后安静退出，由主 goroutine 判定数量
			}
			if p.QoS == 1 {
				sub.raw(mqttproto.EncodeAck(mqttproto.TypePUBACK, p.PacketID))
			}
			key := p.Topic + ":" + string(p.Payload)
			countMu.Lock()
			counts[key]++
			n := len(counts)
			countMu.Unlock()
			if n >= total {
				return
			}
		}
	}()
	wg.Wait()
	done := make(chan struct{})
	go func() { readerWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
	countMu.Lock()
	defer countMu.Unlock()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(counts) != total {
		t.Fatalf("只收到 %d/%d 条，判定依据: 并发发布下消息丢失", len(counts), total)
	}
	for k, n := range counts {
		if n != 1 {
			t.Fatalf("消息 %s 收到 %d 次，判定依据: 并发下不得重复投递", k, n)
		}
	}
	t.Logf("判定依据: 报文类型=PUBLISH(QoS1) 会话状态=inflight 已逐条 PUBACK, 并发发布者=%d 不重复消息=%d 条",
		publishers, len(counts))
}

// TestMalformedPacketDisconnects 畸形固定头必须导致连接被可区分地关闭。
func TestMalformedPacketDisconnects(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	c := dialClient(t, addr, "mal", true, 0)
	defer c.close()
	// PUBREL 保留位错误（应为 0x62，这里发 0x60）。
	c.raw([]byte{0x60, 0x02, 0x00, 0x01})
	if f := c.readFrameWait(500 * time.Millisecond); f != nil {
		t.Fatalf("畸形报文后期望断连，却读到 %s", f.Type)
	}
	t.Log("判定依据: 报文类型=PUBREL 固定头保留位!=0010 -> protocol_violation 断开")
}

// TestConnectRejections 覆盖 CONNECT 拒绝路径。
func TestConnectRejections(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())

	// 空 ClientID + clean=0 -> 0x02
	code, err := rawConnect(addr, mqttproto.EncodeConnect(&mqttproto.Connect{CleanSession: false}))
	if err != nil || code != 0x02 {
		t.Fatalf("empty-id clean=0: code=0x%02x err=%v, want 0x02", code, err)
	}
	t.Log("判定依据: 空 ClientID 且 CleanSession=0 -> CONNACK 0x02 identifier rejected")

	// 协议级别 3（MQTT 3.1）-> 0x01 不支持的协议版本
	code, err = rawConnect(addr, buildLevel3Connect())
	if err != nil || code != 0x01 {
		t.Fatalf("protocol level 3: code=0x%02x err=%v, want 0x01", code, err)
	}
	t.Log("判定依据: protocol level=3 -> CONNACK 0x01 unsupported protocol version")

	// level=5：服务端以 MQTT5 原因码 0x84 拒绝
	conn5, err := netDial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn5.Close()
	if _, err := conn5.Write(buildLevelConnect(5)); err != nil {
		t.Fatal(err)
	}
	f := readOne(conn5)
	if f == nil || f.Type != mqttproto.TypeCONNACK || len(f.Body) < 2 || f.Body[1] != 0x84 {
		t.Fatalf("level5: frame=%v, want CONNACK reason 0x84", f)
	}
	t.Log("判定依据: MQTT 5.0 CONNECT -> MQTT5 CONNACK 0x84 unsupported protocol version 后断开")
}

// TestUnsubscribeStopsDelivery 取消订阅后不再收到消息。
func TestUnsubscribeStopsDelivery(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	pub := dialClient(t, addr, "pub2", true, 0)
	sub := dialClient(t, addr, "sub2", true, 0)
	defer pub.close()
	defer sub.close()
	sub.subscribe(1, 1, "u/topic")
	sub.readFrame()
	pub.publishAcked("u/topic", []byte("first"), 1, 1, false)
	if p := sub.recvPublish(); string(p.Payload) != "first" {
		t.Fatalf("first = %q", p.Payload)
	}
	sub.unsubscribe(2, "u/topic")
	sub.readFrame() // UNSUBACK
	pub.publishQoS0("u/topic", []byte("second"), false)
	if f := sub.readFrameWait(400 * time.Millisecond); f != nil {
		t.Fatalf("取消订阅后不应再收到，却读到 %s", f.Type)
	}
	t.Log("判定依据: UNSUBSCRIBE+UNSUBACK 后同主题消息不再投递")
}

// TestInvalidUTF8Topic CONNECT 之外，PUBLISH 的 UTF-8/长度校验。
func TestEmptyPublishTopic(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	c := dialClient(t, addr, "empt", true, 0)
	defer c.close()
	c.raw(mqttproto.EncodePublish(&mqttproto.Publish{Topic: "", QoS: 0, Payload: []byte("x")}))
	if f := c.readFrameWait(500 * time.Millisecond); f != nil {
		t.Fatalf("空主题后期望断连，读到 %s", f.Type)
	}
	t.Log("判定依据: PUBLISH 空主题 -> malformed/protocol_violation 断开")
}

var _ = strings.Contains
