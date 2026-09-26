package broker

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/session"
	"github.com/xingjianluo/mqtt-session-broker-go/internal/topic"
)

// DisconnectReason 为连接关闭的可区分原因（用于日志与测试断言）。
type DisconnectReason string

const (
	ReasonClientClosed       DisconnectReason = "client_closed"
	ReasonMalformedPacket    DisconnectReason = "malformed_packet"
	ReasonProtocolViolation  DisconnectReason = "protocol_violation"
	ReasonUnsupportedVersion DisconnectReason = "unsupported_protocol_version"
	ReasonIDRejected         DisconnectReason = "identifier_rejected"
	ReasonNotAuthorized      DisconnectReason = "not_authorized"
	ReasonKeepAliveTimeout   DisconnectReason = "keepalive_timeout"
	ReasonSlowConsumer       DisconnectReason = "slow_consumer"
	ReasonQueueQuotaExceeded DisconnectReason = "queue_quota_exceeded"
	ReasonPacketTooLarge     DisconnectReason = "packet_too_large"
	ReasonServerShutdown     DisconnectReason = "server_shutdown"
	ReasonTakenOver          DisconnectReason = "taken_over"
	ReasonSessionExpired     DisconnectReason = "session_expired"
	ReasonNetworkClosed      DisconnectReason = "network_closed"
)

// Event 为可观测事件，供测试与日志使用。
type Event struct {
	Type     string // "connect" | "disconnect" | "reject" | "drop"
	ClientID string
	Reason   DisconnectReason
	Detail   string
}

// sessionEntry 为会话注册表中的一项。
type sessionEntry struct {
	sess     *session.Session
	conn     *clientConn // 当前活动连接；nil 表示离线
	lastSeen time.Time   // 最近一次下线时间（用于 TTL 清理）
}

// Broker 为 MQTT 代理核心。
type Broker struct {
	cfg Config

	mu       sync.Mutex
	sessions map[string]*sessionEntry
	trie     *topic.Trie
	retained *retainedStore
	closed   bool

	events chan Event
	wg     sync.WaitGroup
}

// New 创建 Broker。
func New(cfg Config) *Broker {
	b := &Broker{
		cfg:      cfg,
		sessions: make(map[string]*sessionEntry),
		trie:     topic.NewTrie(),
		retained: newRetainedStore(),
	}
	if cfg.EventBufferSize > 0 {
		b.events = make(chan Event, cfg.EventBufferSize)
	}
	return b
}

// Events 返回事件通道（EventBufferSize=0 时为 nil）。
func (b *Broker) Events() <-chan Event { return b.events }

func (b *Broker) emit(ev Event) {
	if b.events == nil {
		return
	}
	select {
	case b.events <- ev:
	default: // 事件满不阻塞协议处理
	}
}

// Serve 在 l 上接受连接，直到 ctx 取消或 Close。
func (b *Broker) Serve(ctx context.Context, l net.Listener) error {
	if l == nil {
		var err error
		l, err = net.Listen("tcp", b.cfg.Listen)
		if err != nil {
			return err
		}
	}
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()
	if b.cfg.SessionTTL > 0 {
		b.wg.Add(1)
		go b.sessionGC(ctx)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || b.isClosed() {
				b.shutdownAll()
				b.wg.Wait()
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			cc := &clientConn{broker: b, conn: c}
			cc.serve()
		}()
	}
}

func (b *Broker) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// Close 停止接受新连接并断开全部现有连接（用于测试与优雅退出）。
func (b *Broker) Close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.shutdownAll()
	return nil
}

func (b *Broker) shutdownAll() {
	b.mu.Lock()
	conns := make([]*clientConn, 0, len(b.sessions))
	for _, e := range b.sessions {
		if e.conn != nil {
			conns = append(conns, e.conn)
		}
	}
	b.mu.Unlock()
	for _, c := range conns {
		c.closeWith(ReasonServerShutdown, "server shutdown")
	}
}

// sessionGC 周期性清除超过 TTL 的离线持久会话。
func (b *Broker) sessionGC(ctx context.Context) {
	defer b.wg.Done()
	// 扫描周期取 TTL/2 与 100ms 的较大值，避免 TTL 过小时 NewTicker 触发 panic。
	interval := b.cfg.SessionTTL / 2
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			b.expireSessions(now)
		}
	}
}

func (b *Broker) expireSessions(now time.Time) {
	b.mu.Lock()
	var expired []string
	for id, e := range b.sessions {
		if e.conn == nil && now.Sub(e.lastSeen) >= b.cfg.SessionTTL {
			expired = append(expired, id)
		}
	}
	b.mu.Unlock()
	for _, id := range expired {
		b.destroySession(id, "session ttl expired")
	}
}

// destroySession 移除会话及其全部订阅并清空状态。
// 调用时该会话必须无活动连接（过期仅发生在离线期间）。
func (b *Broker) destroySession(clientID, detail string) {
	b.mu.Lock()
	e := b.sessions[clientID]
	if e != nil {
		delete(b.sessions, clientID)
	}
	b.mu.Unlock()
	if e != nil {
		n := b.trie.RemoveClient(clientID)
		e.sess.Clear()
		b.emit(Event{Type: "session_expired", ClientID: clientID, Reason: ReasonSessionExpired,
			Detail: detail + "; subscriptions removed=" + itoa(n)})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
