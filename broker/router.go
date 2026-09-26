package broker

import (
	"sync"

	"github.com/xingjianluo/mqtt-session-broker-go/topic"
)

// router 维护订阅表并做通配符路由。
// 采用扁平表 + 全量匹配：本地代理规模下足够简单且行为确定。
type router struct {
	mu   sync.RWMutex
	subs map[string]map[string]byte // clientID -> filter -> qos
}

func newRouter() *router { return &router{subs: make(map[string]map[string]byte)} }

// add 添加或更新订阅。
func (r *router) add(clientID, filter string, qos byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.subs[clientID]
	if !ok {
		m = make(map[string]byte)
		r.subs[clientID] = m
	}
	m[filter] = qos
}

// remove 移除订阅。
func (r *router) remove(clientID, filter string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.subs[clientID]; ok {
		delete(m, filter)
		if len(m) == 0 {
			delete(r.subs, clientID)
		}
	}
}

// removeAll 移除某客户端全部订阅。
func (r *router) removeAll(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subs, clientID)
}

// match 返回匹配某主题的所有订阅：clientID -> 授予的 QoS（取各过滤器最大值）。
func (r *router) match(name string) map[string]byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]byte)
	for clientID, filters := range r.subs {
		var best byte
		matched := false
		for f, q := range filters {
			if topic.Match(f, name) {
				if !matched || q > best {
					best = q
				}
				matched = true
			}
		}
		if matched {
			out[clientID] = best
		}
	}
	return out
}
