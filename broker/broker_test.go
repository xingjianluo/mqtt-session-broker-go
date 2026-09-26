package broker

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/packet"
)

// testClient 是基于 net.Pipe 的内存 MQTT 客户端。
type testClient struct {
	t    *testing.T
	nc   net.Conn
	done chan error // HandleConnection 返回结果（断开原因）
	pid  uint16
}

func newBroker() *Broker {
	return New(Config{MaxQueuePerSession: 100, SessionExpiry: 0})
}

// dial 建立连接并完成 CONNECT/CONNACK。
func dial(t *testing.T, b *Broker, clientID string, clean bool) (*testClient, *packet.Packet) {
	t.Helper()
	c1, c2 := net.Pipe()
	tc := &testClient{t: t, nc: c1, done: make(chan error, 1)}
	go func() { tc.done <- b.HandleConnection(c2) }()
	tc.send(&packet.Packet{Type: packet.TypeConnect, ClientID: clientID, CleanSession: clean, KeepAlive: 30})
	ack := tc.read()
	if ack.Type != packet.TypeConnAck {
		t.Fatalf("expected CONNACK, got %s", ack.Type)
	}
	t.Logf("CONNECT client=%q clean=%v -> CONNACK sessionPresent=%v rc=%d",
		clientID, clean, ack.SessionPresent, ack.ReturnCode)
	return tc, ack
}

func (c *testClient) nextID() uint16 {
	c.pid++
	return c.pid
}

func (c *testClient) send(p *packet.Packet) {
	c.t.Helper()
	data, err := packet.Encode(p)
	if err != nil {
		c.t.Fatalf("encode %s: %v", p.Type, err)
	}
	if _, err := c.nc.Write(data); err != nil {
		c.t.Fatalf("write %s: %v", p.Type, err)
	}
}

func (c *testClient) read() *packet.Packet {
	c.t.Helper()
	_ = c.nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	p, err := packet.ReadPacket(c.nc)
	if err != nil {
		c.t.Fatalf("read packet: %v", err)
	}
	return p
}

// readMaybe 在超时内尝试读一个报文，超时返回 nil。
func (c *testClient) readMaybe(d time.Duration) *packet.Packet {
	_ = c.nc.SetReadDeadline(time.Now().Add(d))
	p, err := packet.ReadPacket(c.nc)
	if err != nil {
		return nil
	}
	return p
}

func (c *testClient) subscribe(filter string, qos byte) []byte {
	c.t.Helper()
	id := c.nextID()
	c.send(&packet.Packet{Type: packet.TypeSubscribe, PacketID: id,
		Subscriptions: []packet.Subscription{{Topic: filter, QoS: qos}}})
	ack := c.read()
	if ack.Type != packet.TypeSubAck || ack.PacketID != id {
		c.t.Fatalf("expected SUBACK id=%d, got %s id=%d", id, ack.Type, ack.PacketID)
	}
	c.t.Logf("SUBSCRIBE filter=%q qos=%d -> SUBACK codes=%v (判定依据: 授予码)", filter, qos, ack.ReasonCodes)
	return ack.ReasonCodes
}

func (c *testClient) publish(topic string, payload string, qos byte) uint16 {
	c.t.Helper()
	id := uint16(0)
	if qos > 0 {
		id = c.nextID()
	}
	c.send(&packet.Packet{Type: packet.TypePublish, Topic: topic, Payload: []byte(payload), QoS: qos, PacketID: id})
	return id
}

func (c *testClient) close() { _ = c.nc.Close() }

// expectConnError 断言连接以指定原因结束。
func (c *testClient) expectConnError(want error) {
	c.t.Helper()
	select {
	case err := <-c.done:
		c.t.Logf("connection ended: err=%v (判定依据: errors.Is == %v)", err, want)
		if !errors.Is(err, want) {
			c.t.Fatalf("disconnect reason = %v, want %v", err, want)
		}
	case <-time.After(3 * time.Second):
		c.t.Fatalf("connection did not end, want reason %v", want)
	}
}

// TestPubSubWildcard 验证通配符路由与 QoS 降级（min(pub, sub)）。
func TestPubSubWildcard(t *testing.T) {
	b := newBroker()
	sub, _ := dial(t, b, "sub-1", true)
	if codes := sub.subscribe("sensors/+", 1); codes[0] != 1 {
		t.Fatalf("subscribe granted=%d want 1", codes[0])
	}
	pub, _ := dial(t, b, "pub-1", true)
	pub.publish("sensors/temp", "22C", 0)

	m := sub.read()
	t.Logf("recv type=%s topic=%q qos=%d payload=%q (判定依据: QoS=min(0,1)=0)", m.Type, m.Topic, m.QoS, m.Payload)
	if m.Type != packet.TypePublish || m.Topic != "sensors/temp" || m.QoS != 0 || string(m.Payload) != "22C" {
		t.Fatalf("unexpected delivery: %+v", m)
	}
	// 不匹配的层级不应投递
	pub.publish("sensors/temp/humidity", "x", 0)
	if m := sub.readMaybe(200 * time.Millisecond); m != nil {
		t.Fatalf("unexpected message for non-matching topic: %+v", m)
	}
	t.Log("non-matching topic not delivered -> ok")
}

// TestConcurrentPubSub 验证并发发布订阅下消息不丢不重。
func TestConcurrentPubSub(t *testing.T) {
	b := New(Config{MaxQueuePerSession: 1000, SessionExpiry: 0}) // 配额需大于在途峰值
	sub, _ := dial(t, b, "sub-c", true)
	sub.subscribe("data/#", 1)

	const publishers = 8
	const perPub = 25
	total := publishers * perPub

	// 先启动接收协程持续消费，避免被判定为慢消费者
	recv := make(chan *packet.Packet, total)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_ = sub.nc.SetReadDeadline(time.Now().Add(3 * time.Second))
			p, err := packet.ReadPacket(sub.nc)
			if err != nil {
				return
			}
			recv <- p
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p, _ := dial(t, b, fmt.Sprintf("pub-%d", n), true)
			for j := 0; j < perPub; j++ {
				id := p.publish("data/x", fmt.Sprintf("%d-%d", n, j), 1)
				ack := p.read()
				if ack.Type != packet.TypePubAck || ack.PacketID != id {
					t.Errorf("pub-%d: expected PUBACK id=%d got %s id=%d", n, id, ack.Type, ack.PacketID)
					return
				}
			}
		}(i)
	}
	// 订阅方收满并逐条 PUBACK
	got := make(map[string]int)
	for len(got) < total {
		m := <-recv
		if m.Type != packet.TypePublish {
			t.Fatalf("expected PUBLISH, got %s", m.Type)
		}
		if m.QoS != 1 {
			t.Fatalf("delivered QoS=%d want 1 (min(pub=1,sub=1))", m.QoS)
		}
		key := string(m.Payload)
		got[key]++
		if got[key] > 1 {
			t.Fatalf("duplicate delivery of %q", key)
		}
		sub.send(&packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})
	}
	wg.Wait()
	t.Logf("received %d distinct messages, no loss/dup (判定依据: 唯一 payload 计数 == %d)", len(got), total)
	select {
	case m := <-recv:
		t.Fatalf("extra message after all delivered: %+v", m)
	case <-time.After(200 * time.Millisecond):
	}
	sub.close()
	<-readDone
}
