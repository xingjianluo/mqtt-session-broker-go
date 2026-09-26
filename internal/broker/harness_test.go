package broker

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
)

// startBroker 在随机本地端口启动 broker，测试结束自动关闭。
func startBroker(t *testing.T, cfg Config) (*Broker, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if cfg.MaxPacketSize == 0 {
		cfg = DefaultConfig()
	}
	cfg.Listen = l.Addr().String()
	b := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		_ = b.Close()
		_ = l.Close()
	})
	return b, l.Addr().String()
}

// testClient 为最小化 MQTT 3.1.1 测试客户端。
// 所有报文由单独的 reader goroutine 读取并投入 frames 通道，杜绝多 goroutine 共享读取竞态。
type testClient struct {
	t       *testing.T
	conn    net.Conn
	frames  chan *mqttproto.Frame
	readErr chan error
	writeMu sync.Mutex
	closed  bool
	closeMu sync.Mutex
	done    chan struct{}
}

func newTestClient(t *testing.T, conn net.Conn) *testClient {
	c := &testClient{
		t:       t,
		conn:    conn,
		frames:  make(chan *mqttproto.Frame, 128),
		readErr: make(chan error, 1),
		done:    make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *testClient) readLoop() {
	defer close(c.done)
	r := newByteReader(c.conn)
	for {
		f, err := mqttproto.ReadFrame(r, 1<<20)
		if err != nil {
			c.readErr <- err
			return
		}
		c.frames <- f
	}
}

func dialClient(t *testing.T, addr, clientID string, clean bool, keepAlive uint16) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := newTestClient(t, conn)
	c.raw(mqttproto.EncodeConnect(&mqttproto.Connect{
		ClientID: clientID, CleanSession: clean, KeepAlive: keepAlive,
	}))
	f := c.mustFrame()
	if f.Type != mqttproto.TypeCONNACK || len(f.Body) < 2 {
		t.Fatalf("expected CONNACK, got %s body=%x", f.Type, f.Body)
	}
	if f.Body[1] != 0 {
		t.Fatalf("CONNACK return code=0x%02x, want 0 (accepted); 判定依据: 握手拒绝原因码", f.Body[1])
	}
	return c
}

// dialClientExpectPresent 拨号并断言 CONNACK 连接确认标志（sessionPresent）。
func dialClientExpectPresent(t *testing.T, addr, clientID string, clean, wantPresent bool) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := newTestClient(t, conn)
	c.raw(mqttproto.EncodeConnect(&mqttproto.Connect{
		ClientID: clientID, CleanSession: clean,
	}))
	f := c.mustFrame()
	if f.Type != mqttproto.TypeCONNACK || len(f.Body) < 2 {
		t.Fatalf("expected CONNACK, got %s body=%x", f.Type, f.Body)
	}
	if f.Body[1] != 0 {
		t.Fatalf("CONNACK return code=0x%02x, want 0", f.Body[1])
	}
	if (f.Body[0]&0x01 == 1) != wantPresent {
		t.Fatalf("CONNACK sessionPresent=%v, want %v（判定依据: 持久会话存在性）",
			f.Body[0]&0x01 == 1, wantPresent)
	}
	return c
}

// dialWillClient 拨号一个带遗嘱的客户端。
func dialWillClient(t *testing.T, addr, clientID, willTopic string, willPayload []byte, willRetain bool) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := newTestClient(t, conn)
	c.raw(mqttproto.EncodeConnect(&mqttproto.Connect{
		ClientID: clientID, CleanSession: true,
		WillFlag: true, WillTopic: willTopic, WillMessage: willPayload,
		WillQoS: 1, WillRetain: willRetain,
	}))
	f := c.mustFrame()
	if f.Type != mqttproto.TypeCONNACK || (len(f.Body) >= 2 && f.Body[1] != 0) {
		t.Fatalf("will client CONNACK unexpected: %s body=%x", f.Type, f.Body)
	}
	return c
}

func (c *testClient) raw(p []byte) {
	c.closeMu.Lock()
	closed := c.closed
	c.closeMu.Unlock()
	if closed {
		c.t.Fatalf("write on closed test client")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.conn.Write(p); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// mustFrame 无限等待下一帧；连接关闭时测试失败。
func (c *testClient) mustFrame() *mqttproto.Frame {
	select {
	case f := <-c.frames:
		return f
	case err := <-c.readErr:
		c.t.Fatalf("read frame: %v", err)
	}
	return nil
}

// waitFrame 在 d 内等待下一帧；超时返回 nil（不 Fatal，连接关闭也返回 nil）。
func (c *testClient) waitFrame(d time.Duration) *mqttproto.Frame {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case f := <-c.frames:
		return f
	case <-c.readErr:
		return nil
	case <-timer.C:
		return nil
	}
}

func (c *testClient) subscribe(id uint16, qos byte, filters ...string) {
	s := &mqttproto.Subscribe{PacketID: id}
	for _, f := range filters {
		s.Filters = append(s.Filters, mqttproto.SubFilter{Filter: f, QoS: qos})
	}
	c.raw(mqttproto.EncodeSubscribe(s))
}

func (c *testClient) unsubscribe(id uint16, filters ...string) {
	c.raw(mqttproto.EncodeUnsubscribe(&mqttproto.Unsubscribe{PacketID: id, Filters: filters}))
}

func (c *testClient) publishQoS0(topicName string, payload []byte, retain bool) {
	c.raw(mqttproto.EncodePublish(&mqttproto.Publish{
		Topic: topicName, QoS: 0, Payload: payload, Retain: retain,
	}))
}

func (c *testClient) publishAcked(topicName string, payload []byte, qos byte, id uint16, retain bool) {
	c.raw(mqttproto.EncodePublish(&mqttproto.Publish{
		Topic: topicName, QoS: qos, PacketID: id, Payload: payload, Retain: retain,
	}))
	for {
		f := c.mustFrame()
		switch f.Type {
		case mqttproto.TypePUBACK:
			if qos != 1 {
				c.t.Fatalf("got PUBACK for qos=%d", qos)
			}
			return
		case mqttproto.TypePUBREC:
			c.raw(mqttproto.EncodeAck(mqttproto.TypePUBREL, decodeID(c.t, f)))
		case mqttproto.TypePUBCOMP:
			if qos != 2 {
				c.t.Fatalf("got PUBCOMP for qos=%d", qos)
			}
			return
		default:
			c.t.Fatalf("while awaiting publish ack, unexpected packet %s", f.Type)
		}
	}
}

// recvPublish 读取并解码一条 PUBLISH，跳过 SUBACK/UNSUBACK/PINGRESP 等控制帧。
func (c *testClient) recvPublish() *mqttproto.Publish {
	return c.recvPublishWait(0)
}

// recvPublishWait 在 d 内等待 PUBLISH；d=0 表示无限等待。超时或断连返回 nil。
func (c *testClient) recvPublishWait(d time.Duration) *mqttproto.Publish {
	for {
		var f *mqttproto.Frame
		if d > 0 {
			f = c.waitFrame(d)
			if f == nil {
				return nil
			}
		} else {
			f = c.mustFrame()
		}
		switch f.Type {
		case mqttproto.TypePUBLISH:
			p, err := mqttproto.DecodePublish(f.Flags, f.Body)
			if err != nil {
				c.t.Fatalf("decode publish: %v", err)
			}
			return p
		case mqttproto.TypeSUBACK, mqttproto.TypeUNSUBACK, mqttproto.TypePINGRESP:
			continue
		default:
			c.t.Fatalf("expected PUBLISH, got %s", f.Type)
		}
	}
}

// readFrame / readFrameWait 为旧调用点保留的别名。
func (c *testClient) readFrame() *mqttproto.Frame { return c.mustFrame() }
func (c *testClient) readFrameWait(d time.Duration) *mqttproto.Frame {
	return c.waitFrame(d)
}

func (c *testClient) sendDisconnect() {
	c.raw(mqttproto.EncodeDisconnect())
}

func (c *testClient) close() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if !c.closed {
		c.closed = true
		_ = c.conn.Close()
	}
}

func decodeID(t *testing.T, f *mqttproto.Frame) uint16 {
	t.Helper()
	id, err := mqttproto.DecodePacketID(f.Body)
	if err != nil {
		t.Fatalf("decode packet id: %v", err)
	}
	return id
}

// rawConnect 发送任意 CONNECT 字节后读取 CONNACK 返回码。
func rawConnect(addr string, connectPacket []byte) (byte, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if _, err := conn.Write(connectPacket); err != nil {
		return 0, err
	}
	f := mqttproto.ReadFrameOrNil(newByteReader(conn))
	if f == nil || f.Type != mqttproto.TypeCONNACK || len(f.Body) < 2 {
		return 0, errNoConnack
	}
	return f.Body[1], nil
}
