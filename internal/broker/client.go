package broker

import (
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
	"github.com/xingjianluo/mqtt-session-broker-go/internal/session"
	"github.com/xingjianluo/mqtt-session-broker-go/internal/topic"
)

func itoaLevel(n int) string { return strconv.Itoa(n) }

// outItem 为发往单个连接的串行出站指令。
type outItem struct {
	packet []byte
}

// clientConn 处理单个客户端连接的协议状态机。
type clientConn struct {
	broker *Broker
	conn   net.Conn

	protoLevel   byte
	clientID     string
	cleanSession bool
	keepAlive    time.Duration

	willTopic   string
	willPayload []byte
	willQoS     byte
	willRetain  bool
	willFlag    bool

	sess *session.Session

	outCh     chan outItem
	closeCh   chan struct{}
	closeOnce sync.Once

	// normalDisconnect 表示客户端发送了 DISCONNECT 报文（正常断开，不发布遗嘱）。
	normalDisconnect bool

	deadline atomic.Int64 // keepalive 绝对截止时间（unix nano）；0 表示不检查

	reason   DisconnectReason
	reasonMu sync.Mutex
	detail   string
	closed   bool
}

// serve 阻塞处理该连接直到其关闭。
func (c *clientConn) serve() {
	// 通道在任何可能触发 fail 的读操作之前初始化，避免握手失败路径上的 nil channel 竞态。
	c.outCh = make(chan outItem, c.broker.cfg.MaxInflight+64)
	c.closeCh = make(chan struct{})
	defer c.cleanup()

	frame, err := mqttproto.ReadFrame(c.conn, c.broker.cfg.MaxPacketSize)
	if err != nil {
		c.fail(ReasonMalformedPacket, "read CONNECT failed: "+err.Error())
		return
	}
	if frame.Type != mqttproto.TypeCONNECT {
		c.fail(ReasonProtocolViolation, "first packet must be CONNECT, got "+frame.Type.String())
		return
	}
	connect, derr := mqttproto.DecodeConnect(frame.Body)
	if derr != nil {
		c.fail(ReasonMalformedPacket, derr.Error())
		return
	}
	c.protoLevel = connect.ProtocolLevel

	// MQTT 5.0 不在支持范围内：直接 CONNACK 返回"不支持的协议版本"后断开（无遗嘱）。
	if connect.ProtocolLevel == 5 {
		_, _ = c.conn.Write(mqttproto.EncodeConnack5(0x84)) // Unsupported Protocol Version
		c.fail(ReasonUnsupportedVersion, "MQTT 5.0 is not supported")
		return
	}
	// MQTT 3.1（level 3）及其他 level：回 3.1.1 CONNACK 0x01 后断开。
	if connect.ProtocolLevel != 4 {
		_, _ = c.conn.Write(mqttproto.EncodeConnack(false, 0x01))
		c.fail(ReasonUnsupportedVersion, "only MQTT 3.1.1 (level 4) supported, got level "+itoaLevel(int(connect.ProtocolLevel)))
		return
	}

	sess, resolvedID, present, old, rerr := c.broker.beginConnect(connect)
	if rerr != nil {
		_, _ = c.conn.Write(mqttproto.EncodeConnack(false, connackCode(rerr)))
		c.fail(reasonFromReject(rerr), "CONNECT rejected: "+rerr.Error())
		return
	}

	c.sess = sess
	c.clientID = resolvedID
	c.cleanSession = connect.CleanSession
	c.willFlag = connect.WillFlag
	c.willTopic = connect.WillTopic
	c.willPayload = connect.WillMessage
	c.willQoS = connect.WillQoS
	c.willRetain = connect.WillRetain
	if connect.KeepAlive > 0 {
		c.keepAlive = time.Duration(connect.KeepAlive) * time.Second
		d := time.Now().Add(time.Duration(float64(connect.KeepAlive) * c.broker.cfg.KeepAliveFactor * float64(time.Second)))
		c.deadline.Store(d.UnixNano())
	}

	// 接管同 ID 旧连接（按 §3.1.4 不发布旧连接遗嘱）。
	if old != nil {
		old.takeover()
	}

	c.broker.attachConn(c.clientID, c)
	c.sess.SetOnline(true)

	if _, err := c.conn.Write(mqttproto.EncodeConnack(present && !connect.CleanSession, 0x00)); err != nil {
		c.fail(ReasonClientClosed, "write CONNACK failed: "+err.Error())
		return
	}
	c.broker.emit(Event{Type: "connect", ClientID: c.clientID,
		Detail: "clean=" + boolStr(connect.CleanSession) + " sessionPresent=" + boolStr(present && !connect.CleanSession)})

	var wg sync.WaitGroup
	wg.Add(3)
	go c.writerLoop(&wg)
	go c.readLoop(&wg)
	go c.pumpLoop(&wg)
	wg.Wait()
}

// readLoop 读取并分发客户端报文。
func (c *clientConn) readLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}
		if err := c.conn.SetReadDeadline(c.nextReadDeadline()); err != nil {
			c.fail(ReasonClientClosed, err.Error())
			return
		}
		f, err := mqttproto.ReadFrame(c.conn, c.broker.cfg.MaxPacketSize)
		if err != nil {
			c.handleReadError(err)
			return
		}
		c.touchDeadline()
		if reason, detail := c.dispatch(f); reason != "" {
			c.fail(reason, detail)
			return
		}
	}
}

// handleReadError 区分网络 EOF、超时与畸形报文。
func (c *clientConn) handleReadError(err error) {
	var re *mqttproto.RejectError
	if errors.As(err, &re) {
		if re.Code == mqttproto.RejectMalformedPacket {
			c.fail(ReasonMalformedPacket, "malformed frame: "+re.Detail)
			return
		}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		c.fail(ReasonKeepAliveTimeout, "no packet within 1.5x keepalive")
		return
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		// 对端直接关连接属于"网络连接异常关闭"（§3.1.2-6），遗嘱按异常路径发布。
		// 客户端正常断开必须先发 DISCONNECT，该路径由 normalDisconnect 单独标记。
		c.fail(ReasonNetworkClosed, "connection closed by peer/network")
		return
	}
	c.fail(ReasonNetworkClosed, "read error: "+err.Error())
}

func (c *clientConn) nextReadDeadline() time.Time {
	d := c.deadline.Load()
	if d == 0 {
		return time.Time{}
	}
	return time.Unix(0, d)
}

func (c *clientConn) touchDeadline() {
	if c.keepAlive > 0 {
		d := time.Now().Add(time.Duration(float64(c.keepAlive) * c.broker.cfg.KeepAliveFactor))
		c.deadline.Store(d.UnixNano())
	}
}

// writerLoop 将 outCh 中的报文串行写回客户端。
// 慢消费者判定：以"背压开始"（一次写未能快速完成）的时刻为起点给固定截止时间，
// 而不是每条消息重置，避免 TCP 缓冲渐进填满期间截止时间被无限顺延。
func (c *clientConn) writerLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-c.closeCh:
			return
		case item, ok := <-c.outCh:
			if !ok {
				return
			}
			if err := c.writeWithBackpressure(item.packet); err != nil {
				if isTimeoutErr(err) {
					c.fail(ReasonSlowConsumer, "tcp write blocked beyond slow consumer timeout")
				} else {
					c.fail(ReasonNetworkClosed, "write failed: "+err.Error())
				}
				return
			}
		}
	}
}

// writeWithBackpressure 先以零截止时间探测一次；不能立即写完则进入"背压态"，
// 从探测失败时刻起给 SlowConsumerTimeout 的固定窗口，窗口内仍未写完即判定慢消费者。
func (c *clientConn) writeWithBackpressure(p []byte) error {
	timeout := c.broker.cfg.SlowConsumerTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	// 第一次探测：给一个极短窗口判断是否立即可写。
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Millisecond))
	_, err := c.conn.Write(p)
	if err == nil {
		return nil
	}
	if !isTimeoutErr(err) {
		return err
	}
	// 背压态：从现在起固定 timeout，不再顺延。
	_ = c.conn.SetWriteDeadline(time.Now().Add(timeout))
	_, err = c.conn.Write(p)
	return err
}

// send 将报文投递到该连接的串行出站通道。
// 语义（慢消费者资源边界）：
//   - outCh 有空位：立即入队，返回 true；
//   - outCh 已满：以 SlowConsumerTimeout 为上限等待；仍满则判定该连接为慢消费者，
//     以 slow_consumer 原因断开并返回 false，绝不无限期占用调用方（路由）goroutine。
func (c *clientConn) send(p []byte) bool {
	if c.isClosing() {
		return false
	}
	timeout := c.broker.cfg.SlowConsumerTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-c.closeCh:
		return false
	case c.outCh <- outItem{packet: p}:
		return true
	case <-t.C:
		c.fail(ReasonSlowConsumer, "outbound backlog full for "+timeout.String()+" (channel capacity="+itoaLevel(cap(c.outCh))+")")
		return false
	}
}

func (c *clientConn) isClosing() bool {
	select {
	case <-c.closeCh:
		return true
	default:
		return false
	}
}

// pumpLoop 在会话有就绪消息（或重连、确认推进）时驱动出站，并定时重传未确认消息。
func (c *clientConn) pumpLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	// 上线后先按状态重传在途，再发送新消息，保证断线期间在途消息可续传。
	c.resendInflight()
	c.drainReady(false)

	retry := c.broker.cfg.RetryInterval
	if retry <= 0 {
		retry = 10 * time.Second
	}
	ticker := time.NewTicker(retry)
	defer ticker.Stop()
	for {
		// 先排空可发送的新消息。
		c.drainReady(false)
		// 获取"本次检查之后"的唤醒通道；获取后再排空一次，以与入队操作线性化：
		// 入队若发生在第二次排空之前，消息会被取走；若发生在其后，必然关闭该通道。
		// 因此不存在"消息已入队但泵仍阻塞"的丢唤醒窗口。
		wake := c.sess.WakeCh()
		c.drainReady(false)
		select {
		case <-c.closeCh:
			return
		case <-wake:
			// 新消息入队、在途确认推进、上线等，回到循环顶部排空。
		case <-ticker.C:
			// 重传由 resendInflight 内部按 QoS/状态区分（PUBLISH DUP=1 或 PUBREL）。
			if !c.resendInflight() {
				return
			}
		}
	}
}

// resendInflight 按状态重传在途消息（重连或重传定时器触发）：
//   - QoS1 在途：重发 PUBLISH(DUP=1)；
//   - QoS2 处于 PUBLISHED（仅收到/未收到 PUBREC 之前）：重发 PUBLISH(DUP=1)；
//   - QoS2 处于 PUBREL_SENT（已发 PUBREL）：只重发 PUBREL，绝不重发 PUBLISH。
//
// 返回 false 表示连接已关闭，应停止后续发送。
func (c *clientConn) resendInflight() bool {
	for _, m := range c.sess.ResendInflight() {
		if m.QoS == 2 && m.State == session.StatePubrec {
			if !c.send(mqttproto.EncodeAck(mqttproto.TypePUBREL, m.PacketID)) {
				return false
			}
			continue
		}
		p := mqttproto.EncodePublish(&mqttproto.Publish{
			Topic: m.Topic, PacketID: m.PacketID, QoS: m.QoS, Dup: true, Payload: m.Payload,
		})
		if !c.send(p) {
			return false
		}
	}
	return true
}

func (c *clientConn) drainReady(dup bool) {
	for _, m := range c.sess.TakeReady() {
		p := mqttproto.EncodePublish(&mqttproto.Publish{
			Topic: m.Topic, PacketID: m.PacketID, QoS: m.QoS, Retain: m.Retain, Dup: dup, Payload: m.Payload,
		})
		if !c.send(p) {
			return
		}
	}
}

// sendWill 发布遗嘱消息（仅在异常断开路径由 cleanup 调用）。
func (c *clientConn) sendWill() {
	if !c.willFlag {
		return
	}
	c.broker.deliverWill(c.willTopic, c.willPayload, c.willQoS, c.willRetain)
}

// takeover 标记旧连接被同 ID 新连接接管（无遗嘱、无 CONNACK/DISCONNECT）。
func (c *clientConn) takeover() {
	c.fail(ReasonTakenOver, "session taken over by new connection with same client id")
}

// markNormalDisconnect 记录客户端已发送 DISCONNECT（受 reasonMu 保护，
// 与 cleanup 中读取该标志的遗嘱判定同步）。
func (c *clientConn) markNormalDisconnect() {
	c.reasonMu.Lock()
	c.normalDisconnect = true
	c.reasonMu.Unlock()
}

// fail 设置关闭原因并关闭连接，幂等。必须在 conn.Close 之前关闭 closeCh，
// 使 pump/writer 立即停止，避免向已关闭连接写入。
func (c *clientConn) fail(reason DisconnectReason, detail string) {
	c.reasonMu.Lock()
	if c.closed {
		c.reasonMu.Unlock()
		return
	}
	c.closed = true
	c.reason = reason
	c.detail = detail
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.reasonMu.Unlock()
	_ = c.conn.Close()
}

// closeWith 供 Broker 层（关停/接管）调用。
func (c *clientConn) closeWith(reason DisconnectReason, detail string) {
	c.fail(reason, detail)
}

// cleanup 在连接 goroutine 退出时执行：收尾会话、按需发布遗嘱。
func (c *clientConn) cleanup() {
	c.reasonMu.Lock()
	reason := c.reason
	detail := c.detail
	closed := c.closed
	normal := c.normalDisconnect
	c.reasonMu.Unlock()
	if !closed {
		// serve 在读到 CONNECT 之前即返回等极端路径：确保通道关闭。
		c.closeOnce.Do(func() { close(c.closeCh) })
	}

	// 未显式设置原因时视为对端/网络关闭（异常路径，遗嘱应发布）。
	if reason == "" {
		reason = ReasonNetworkClosed
	}
	// 遗嘱发布条件（MQTT 3.1.1 §3.14.4-4 异常断开；§3.14.6-4 正常 DISCONNECT 不发布）：
	// 正常 DISCONNECT、被接管、服务端关停、协议版本拒绝均不发布；
	// keepalive 超时、慢消费者/配额断开、网络异常断开均发布。
	will := !normal &&
		reason != ReasonTakenOver &&
		reason != ReasonServerShutdown &&
		reason != ReasonUnsupportedVersion
	if c.sess != nil && will {
		c.sendWill()
	}
	if c.sess != nil {
		c.broker.connClosed(c)
	}
	c.broker.emit(Event{Type: "disconnect", ClientID: c.clientID, Reason: reason, Detail: detail})
}

func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func reasonFromReject(e *mqttproto.RejectError) DisconnectReason {
	switch e.Code {
	case mqttproto.RejectBadClientID:
		return ReasonIDRejected
	case mqttproto.RejectNotAuthorized:
		return ReasonNotAuthorized
	case mqttproto.RejectUnsupportedProtoVer:
		return ReasonUnsupportedVersion
	case mqttproto.RejectProtocolViolation:
		return ReasonProtocolViolation
	default:
		return ReasonProtocolViolation
	}
}

var _ = topic.Match
