package server

import (
	"net"
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/broker"
	"github.com/xingjianluo/mqtt-session-broker-go/packet"
)

// TestTCPEndToEnd 通过真实 TCP 连接验证 订阅->发布->接收 链路。
func TestTCPEndToEnd(t *testing.T) {
	b := broker.New(broker.Config{MaxQueuePerSession: 100, SessionExpiry: 60})
	s := New(b)
	errCh := make(chan error, 1)
	go func() { errCh <- s.ListenAndServe("127.0.0.1:0") }()
	// 等待监听就绪
	var addr string
	for i := 0; i < 100; i++ {
		if addr = s.Addr(); addr != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("server did not start listening")
	}
	defer s.Close()
	t.Logf("server listening on %s", addr)

	// 订阅端
	sub := mustDial(t, addr, "tcp-sub")
	sendPkt(t, sub, &packet.Packet{Type: packet.TypeSubscribe, PacketID: 1,
		Subscriptions: []packet.Subscription{{Topic: "tcp/#", QoS: 1}}})
	if ack := readPkt(t, sub); ack.Type != packet.TypeSubAck {
		t.Fatalf("expected SUBACK, got %s", ack.Type)
	}

	// 发布端
	pub := mustDial(t, addr, "tcp-pub")
	sendPkt(t, pub, &packet.Packet{Type: packet.TypePublish, QoS: 1, Topic: "tcp/hello", PacketID: 9, Payload: []byte("over-tcp")})
	if ack := readPkt(t, pub); ack.Type != packet.TypePubAck || ack.PacketID != 9 {
		t.Fatalf("expected PUBACK id=9, got %s id=%d", ack.Type, ack.PacketID)
	}

	// 订阅端收到
	m := readPkt(t, sub)
	t.Logf("subscriber recv type=%s topic=%q qos=%d payload=%q (判定依据: TCP 链路投递)", m.Type, m.Topic, m.QoS, m.Payload)
	if m.Type != packet.TypePublish || m.Topic != "tcp/hello" || string(m.Payload) != "over-tcp" {
		t.Fatalf("unexpected delivery: %+v", m)
	}
	sendPkt(t, sub, &packet.Packet{Type: packet.TypePubAck, PacketID: m.PacketID})

	// 正常断开
	sendPkt(t, pub, &packet.Packet{Type: packet.TypeDisconnect})
	sendPkt(t, sub, &packet.Packet{Type: packet.TypeDisconnect})
	sub.Close()
	pub.Close()
}

func mustDial(t *testing.T, addr, clientID string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	sendPkt(t, c, &packet.Packet{Type: packet.TypeConnect, ClientID: clientID, CleanSession: true, KeepAlive: 30})
	ack := readPkt(t, c)
	if ack.Type != packet.TypeConnAck || ack.ReturnCode != 0 {
		t.Fatalf("connect rejected: %+v", ack)
	}
	t.Logf("CONNECT client=%q -> CONNACK rc=0", clientID)
	return c
}

func sendPkt(t *testing.T, c net.Conn, p *packet.Packet) {
	t.Helper()
	data, err := packet.Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := c.Write(data); err != nil {
		t.Fatalf("write %s: %v", p.Type, err)
	}
}

func readPkt(t *testing.T, c net.Conn) *packet.Packet {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	p, err := packet.ReadPacket(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return p
}
