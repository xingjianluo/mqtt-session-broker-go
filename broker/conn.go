package broker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/xingjianluo/mqtt-session-broker-go/packet"
)

func randomID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// outBuf 是每连接写出通道缓冲，写积压超出即判定为慢消费者并断开。
const outBuf = 256

// conn 表示一个已接入的客户端连接，驱动报文读写循环。
type conn struct {
	broker  *Broker
	session *Session
	rw      io.ReadWriter

	out  chan []byte   // 异步写出队列
	done chan struct{} // 写协程结束

	writeMu sync.Mutex // 串行化入队与关闭
	wclosed bool

	closeOnce sync.Once
	reasonMu  sync.Mutex
	reason    error // 断开原因（可区分）
}

// HandleConnection 处理一条连接上的 MQTT 会话，直到连接结束或出错。
// 返回的 error 可用于区分断开原因；正常 DISCONNECT 返回 nil。
func (b *Broker) HandleConnection(rw io.ReadWriter) error {
	c := &conn{broker: b, rw: rw, out: make(chan []byte, outBuf), done: make(chan struct{})}
	return c.run()
}

func (c *conn) run() error {
	go c.writer()
	defer func() {
		c.writeMu.Lock()
		c.wclosed = true
		close(c.out)
		c.writeMu.Unlock()
		<-c.done
	}()

	// 首包必须是 CONNECT
	p, err := packet.ReadPacket(c.rw)
	if err != nil {
		return c.fail(ErrMalformedPacket, "first packet read: %v", err)
	}
	if p.Type != packet.TypeConnect {
		return c.fail(ErrMalformedPacket, "first packet must be CONNECT, got %s", p.Type)
	}
	var will *packet.Packet
	if p.HasWill {
		will = &packet.Packet{
			Type:    packet.TypePublish,
			Topic:   p.WillTopic,
			Payload: p.WillPayload,
			QoS:     p.WillQoS,
			Retain:  p.WillRetain,
		}
	}
	sess, ack, err := c.broker.connect(p, c)
	if err != nil {
		_ = c.send(ack) // 拒绝码 CONNACK
		return c.fail(err, "connect rejected")
	}
	if err := c.send(ack); err != nil {
		c.broker.disconnect(sess, c, will, false, err)
		return err
	}
	// 恢复未完成投递
	c.broker.resendPending(sess, c)

	graceful := false
	var cause error
	defer func() {
		c.broker.disconnect(sess, c, will, graceful, cause)
	}()

	for {
		p, err := packet.ReadPacket(c.rw)
		if err != nil {
			if reason := c.closeReason(); reason != nil {
				cause = reason // 被代理主动断开（配额/接管/慢消费者）
			} else if de, ok := err.(*packet.DecodeError); ok {
				cause = c.failErr(ErrMalformedPacket, de)
			} else {
				cause = err // EOF 等对端断开
			}
			log.Printf("[broker] read loop end client=%q cause=%v", sess.ClientID, cause)
			return cause
		}
		log.Printf("[broker] recv client=%q type=%s packetID=%d", sess.ClientID, p.Type, p.PacketID)
		if err := c.dispatch(sess, p); err != nil {
			cause = err
			return err
		}
		if p.Type == packet.TypeDisconnect {
			graceful = true
			return nil
		}
	}
}

// writer 把写出队列中的数据写入底层连接。
func (c *conn) writer() {
	defer close(c.done)
	for data := range c.out {
		if _, err := c.rw.Write(data); err != nil {
			c.closeWithReason(err)
			return
		}
	}
}

// dispatch 分发已连接会话的报文。
func (c *conn) dispatch(sess *Session, p *packet.Packet) error {
	switch p.Type {
	case packet.TypeConnect:
		return c.fail(ErrMalformedPacket, "second CONNECT on same connection")
	case packet.TypePublish:
		return c.broker.handleInboundPublish(sess, p, c)
	case packet.TypePubAck:
		if !sess.ackOutbound(p.PacketID) {
			log.Printf("[broker] PUBACK for unknown/inactive id=%d client=%q (ignored)", p.PacketID, sess.ClientID)
		}
	case packet.TypePubRec:
		if sess.recOutbound(p.PacketID) {
			return c.send(&packet.Packet{Type: packet.TypePubRel, PacketID: p.PacketID})
		}
		log.Printf("[broker] PUBREC for unknown/inactive id=%d client=%q (ignored)", p.PacketID, sess.ClientID)
	case packet.TypePubRel:
		return c.broker.handlePubRel(sess, p.PacketID, c)
	case packet.TypePubComp:
		if !sess.compOutbound(p.PacketID) {
			log.Printf("[broker] PUBCOMP for unknown/inactive id=%d client=%q (ignored)", p.PacketID, sess.ClientID)
		}
	case packet.TypeSubscribe:
		codes := c.broker.subscribe(sess, p.Subscriptions)
		return c.send(&packet.Packet{Type: packet.TypeSubAck, PacketID: p.PacketID, ReasonCodes: codes})
	case packet.TypeUnsubscribe:
		c.broker.unsubscribe(sess, p.Subscriptions)
		return c.send(&packet.Packet{Type: packet.TypeUnsubAck, PacketID: p.PacketID})
	case packet.TypePingReq:
		return c.send(&packet.Packet{Type: packet.TypePingResp})
	case packet.TypeDisconnect:
		return nil
	default:
		return c.fail(ErrMalformedPacket, "unexpected packet type %s from client", p.Type)
	}
	return nil
}

// send 将报文放入异步写出队列。队列满判定为慢消费者并断开。
func (c *conn) send(p *packet.Packet) error {
	data, err := packet.Encode(p)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.wclosed {
		return io.ErrClosedPipe
	}
	select {
	case c.out <- data:
		return nil
	default:
		go c.closeWithReason(ErrSlowConsumer)
		return ErrSlowConsumer
	}
}

// closeWithReason 记录断开原因并关闭底层连接（若可关闭），使读循环退出。
func (c *conn) closeWithReason(err error) {
	c.closeOnce.Do(func() {
		c.reasonMu.Lock()
		c.reason = err
		c.reasonMu.Unlock()
		if cl, ok := c.rw.(io.Closer); ok {
			_ = cl.Close()
		}
	})
}

func (c *conn) closeReason() error {
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	return c.reason
}

// fail 记录协议违规等原因并返回组合错误。
func (c *conn) fail(reason error, format string, args ...any) error {
	log.Printf("[broker] protocol violation: "+format, args...)
	return &connError{reason: reason, detail: fmt.Sprintf(format, args...)}
}

func (c *conn) failErr(reason error, detail error) error {
	return &connError{reason: reason, detail: detail.Error()}
}

// connError 包装断开原因，errors.Is 可区分。
type connError struct {
	reason error
	detail string
}

func (e *connError) Error() string { return e.reason.Error() + ": " + e.detail }
func (e *connError) Unwrap() error { return e.reason }

var _ = errors.Is // 保持 errors 导入（供调用方语义说明）
