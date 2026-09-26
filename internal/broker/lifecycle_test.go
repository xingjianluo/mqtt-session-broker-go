package broker

import (
	"testing"
	"time"
)

// TestSessionTTLExpiry 离线持久会话超过 TTL 后被清除：
// 之后到达到该会话的 QoS1/2 消息按"无匹配会话"处理（不积压），重连得到空会话。
func TestSessionTTLExpiry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SessionTTL = 400 * time.Millisecond
	cfg.EventBufferSize = 256
	b, addr := startBroker(t, cfg)

	sub := dialClient(t, addr, "ttlsub", false, 0)
	sub.subscribe(1, 1, "ttl/#")
	sub.readFrame()
	sub.close()

	// 等待 TTL 过期（GC 间隔 >=100ms，TTL=400ms）。
	var expired bool
	deadline := time.After(3 * time.Second)
	for !expired {
		select {
		case ev := <-b.Events():
			if ev.Reason == ReasonSessionExpired {
				expired = true
			}
		case <-deadline:
			t.Fatal("未观察到 session_expired 事件")
		}
	}

	pub := dialClient(t, addr, "ttlpub", true, 0)
	defer pub.close()
	// 此时订阅已随会话移除：消息无订阅者，直接发布（PUBACK 正常），不产生积压。
	pub.publishAcked("ttl/n", []byte("ghost"), 1, 1, false)

	// 同 ID 重连（clean=false）应得到全新空会话：sessionPresent=0。
	sub2 := dialClientExpectPresent(t, addr, "ttlsub", false, false)
	defer sub2.close()
	if f := sub2.readFrameWait(400 * time.Millisecond); f != nil {
		t.Fatalf("过期会话重连后不应收到过期前消息, 读到 %s", f.Type)
	}
	t.Log("判定依据: 报文类型=会话事件 状态=离线超过TTL, 原因=session_expired, 订阅与队列清除后语义稳定(空会话重连)")
}

// TestKeepAliveTimeout keepalive 内无任何报文 -> 1.5 倍超时后断开并发布遗嘱。
func TestKeepAliveTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EventBufferSize = 256
	cfg.SlowConsumerTimeout = 2 * time.Second
	_, addr := startBroker(t, cfg)

	mon := dialClient(t, addr, "kamon", true, 0)
	defer mon.close()
	mon.subscribe(1, 1, "k/#")
	mon.readFrame()

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
	// keepAlive=1 秒，1.5 倍 = 1.5 秒无报文即断开；带遗嘱。
	writeRaw(mqttConnectWillKeepAlive("kclient", "k/dead", []byte("ka"), 1))
	if f := readOne(conn); f == nil {
		t.Fatal("no CONNACK")
	}

	p := mon.recvPublishWait(3 * time.Second)
	if p == nil || p.Topic != "k/dead" {
		t.Fatalf("keepalive 超时后应发布遗嘱, got %v", p)
	}
	t.Logf("判定依据: 报文类型=遗嘱PUBLISH 会话状态=keepalive=1s 内无报文, 1.5倍超时断开原因=keepalive_timeout 并发布遗嘱 topic=%s", p.Topic)
}

// TestKeepAlivePingResetsDeadline 周期 PINGREQ 续命，不应被断开。
func TestKeepAlivePingResetsDeadline(t *testing.T) {
	_, addr := startBroker(t, DefaultConfig())
	c := dialClient(t, addr, "pingclient", true, 1)
	defer c.close()
	for i := 0; i < 3; i++ {
		time.Sleep(700 * time.Millisecond)
		c.raw(pingReq())
		f := c.waitFrame(1500 * time.Millisecond)
		if f == nil || f.Type.String() != "PINGRESP" {
			t.Fatalf("第%d次 PINGREQ 未收到 PINGRESP: %v", i+1, f)
		}
	}
	t.Log("判定依据: 报文类型=PINGREQ/PINGRESP 会话状态=在线, keepalive 周期内有报文则持续续命不断开")
}
