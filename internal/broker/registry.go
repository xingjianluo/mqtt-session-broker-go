package broker

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
	"github.com/xingjianluo/mqtt-session-broker-go/internal/session"
)

// beginConnect 处理 CONNECT 的会话语义：
//   - clean=true：丢弃同 ID 的历史会话（清空消息与订阅），返回全新会话，present 恒为 false；
//   - clean=false：复用已存在的会话（在线或离线），present 表示是否存在历史会话；
//   - 同 ID 已有活动连接时返回 old，由调用方接管（旧连接不发布遗嘱）。
//
// 返回的 *RejectError 对应 CONNACK 拒绝码（见 connackCode）。
func (b *Broker) beginConnect(c *mqttproto.Connect) (sess *session.Session, clientID string, present bool, old *clientConn, err *mqttproto.RejectError) {
	clientID = c.ClientID
	if clientID == "" {
		if !c.CleanSession {
			return nil, "", false, nil, mqttproto.NewRejectError(mqttproto.RejectBadClientID,
				"empty client identifier requires clean session=1")
		}
		clientID = "auto-" + randID()
	}
	if !validClientID(clientID) {
		return nil, "", false, nil, mqttproto.NewRejectError(mqttproto.RejectBadClientID,
			"client identifier contains invalid characters")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.sessions[clientID]; e != nil {
		if c.CleanSession {
			// 同 ID 旧会话先标记摘除；旧连接若在线也一并接管关闭。
			old = e.conn
			delete(b.sessions, clientID)
			if old != nil {
				b.trie.RemoveClient(clientID)
			}
			e.sess.Clear()
		} else {
			// 复用持久会话：保留离线队列、在途消息与入站 QoS2 记录。
			present = true
			old = e.conn
			sess = e.sess
		}
	}
	if sess == nil {
		sess = session.NewSession(clientID, b.cfg.MaxClientQueue, b.cfg.MaxInflight)
		b.sessions[clientID] = &sessionEntry{sess: sess, lastSeen: time.Now()}
	}
	return sess, clientID, present, old, nil
}

// attachConn 把活动连接绑定到其会话表项。
func (b *Broker) attachConn(clientID string, cc *clientConn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.sessions[clientID]; e != nil {
		e.conn = cc
		e.lastSeen = time.Now()
	}
}

// connClosed 在连接处理结束时执行会话收尾：
//   - clean=true：删除会话、清空消息与订阅；
//   - clean=false：保留会话与在途消息，等待重连续传。
func (b *Broker) connClosed(cc *clientConn) {
	b.mu.Lock()
	e := b.sessions[cc.clientID]
	if e != nil && e.conn == cc {
		e.conn = nil
		e.lastSeen = time.Now()
	}
	clean := cc.cleanSession
	clientID := cc.clientID
	b.mu.Unlock()

	cc.sess.SetOnline(false)
	if clean {
		b.mu.Lock()
		delete(b.sessions, clientID)
		b.mu.Unlock()
		b.trie.RemoveClient(clientID)
		cc.sess.Clear()
		b.emit(Event{Type: "drop", ClientID: clientID, Detail: "clean session removed on disconnect"})
	}
}

func randID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// validClientID 按 MQTT 3.1.1 §2.3.1-3 的允许字符集校验，长度上限 65535。
func validClientID(id string) bool {
	if len(id) == 0 || len(id) > 65535 {
		return false
	}
	for i := 0; i < len(id); i++ {
		ch := id[i]
		if ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' {
			continue
		}
		switch ch {
		case '-', '_', '.', ':', '%', '@': // 允许的扩展字符
			continue
		default:
			// 服务端分配的 auto- 前缀 ID 为十六进制，不会走到这里；
			// 其余非 ASCII/控制字符一律拒绝，保证可区分。
			if ch < 0x20 || ch == 0x7f {
				return false
			}
		}
	}
	return true
}

// connackCode 把拒绝原因映射为 3.1.1 CONNACK 返回码。
func connackCode(e *mqttproto.RejectError) byte {
	switch e.Code {
	case mqttproto.RejectUnsupportedProtoVer:
		return 0x01
	case mqttproto.RejectBadClientID:
		return 0x02
	case mqttproto.RejectServerUnavailable:
		return 0x03
	case mqttproto.RejectNotAuthorized:
		return 0x05
	default:
		return 0x02
	}
}
