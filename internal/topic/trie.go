package topic

import (
	"strings"
	"sync"
)

// Trie 为按主题层级组织的订阅前缀树。
// 节点上的 subs 记录在该过滤器终点订阅的客户端及其授予 QoS。
// 所有方法均并发安全（内部持有单一 RWMutex；订阅/路由均为内存操作，竞争很轻）。
type Trie struct {
	mu   sync.RWMutex
	root *node
}

type node struct {
	children map[string]*node // 普通层级键（不含 "+" "#"）
	wild     *node            // "+" 单层通配
	hash     *node            // "#" 多层通配
	subs     map[string]byte  // clientID -> 授予 QoS
}

func newNode() *node {
	return &node{children: make(map[string]*node), subs: make(map[string]byte)}
}

// MatchResult 为一次匹配命中的订阅。
type MatchResult struct {
	ClientID string
	QoS      byte
}

// NewTrie 创建空订阅树。
func NewTrie() *Trie {
	return &Trie{root: newNode()}
}

// child 取得（或创建）某层级对应的子节点。
func (n *node) child(level string, create bool) *node {
	switch level {
	case "+":
		if n.wild == nil && create {
			n.wild = newNode()
		}
		return n.wild
	case "#":
		if n.hash == nil && create {
			n.hash = newNode()
		}
		return n.hash
	default:
		c := n.children[level]
		if c == nil && create {
			c = newNode()
			n.children[level] = c
		}
		return c
	}
}

// Subscribe 添加订阅。重复订阅以新 QoS 替换旧订阅（MQTT 3.1.1 §3.8.4）。
// 调用前必须通过 ValidateFilter。
func (t *Trie) Subscribe(filter, clientID string, qos byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.root
	for _, level := range strings.Split(filter, "/") {
		cur = cur.child(level, true)
	}
	cur.subs[clientID] = qos
}

// Unsubscribe 移除订阅；过滤器不存在时为空操作。
func (t *Trie) Unsubscribe(filter, clientID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.root
	for _, level := range strings.Split(filter, "/") {
		cur = cur.child(level, false)
		if cur == nil {
			return
		}
	}
	delete(cur.subs, clientID)
}

// Match 返回主题名命中的全部订阅。
// 同一客户端若被多个重叠过滤器命中，只返回一条且 QoS 取最大值，避免重复投递。
// 以 '$' 开头的主题不与首层为 '+'/'#' 的过滤器匹配（MQTT 3.1.1 §4.7.2 语义）。
func (t *Trie) Match(topicName string) []MatchResult {
	levels := strings.Split(topicName, "/")
	dollar := topicName != "" && topicName[0] == '$'
	out := make(map[string]byte)
	seen := make(map[string]bool)
	// add 记录一次命中：seen 区分"未命中"与"授予 QoS=0"。
	add := func(id string, q byte) {
		if !seen[id] || q > out[id] {
			out[id] = q
			seen[id] = true
		}
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	var walk func(n *node, idx int)
	walk = func(n *node, idx int) {
		if n == nil {
			return
		}
		// 该节点上的 "#" 订阅匹配从 idx 开始的任意剩余层级；
		// 根节点(idx==0)上的 "#" 不得匹配 '$' 开头主题。
		if n.hash != nil && !(idx == 0 && dollar) {
			for id, q := range n.hash.subs {
				add(id, q)
			}
		}
		if idx == len(levels) {
			for id, q := range n.subs {
				add(id, q)
			}
			return
		}
		lv := levels[idx]
		// 根节点(idx==0)上的 "+" 不得匹配 '$' 开头层级。
		if !(idx == 0 && dollar) {
			walk(n.wild, idx+1)
		}
		walk(n.children[lv], idx+1)
	}
	walk(t.root, 0)
	results := make([]MatchResult, 0, len(out))
	for id, q := range out {
		results = append(results, MatchResult{ClientID: id, QoS: q})
	}
	return results
}

// RemoveClient 移除某客户端在整棵树上的全部订阅，返回移除条数（会话清除时使用）。
func (t *Trie) RemoveClient(clientID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	count := 0
	var walk func(n *node)
	walk = func(n *node) {
		if n == nil {
			return
		}
		if _, ok := n.subs[clientID]; ok {
			delete(n.subs, clientID)
			count++
		}
		if n.hash != nil {
			walk(n.hash)
		}
		if n.wild != nil {
			walk(n.wild)
		}
		for _, c := range n.children {
			walk(c)
		}
	}
	walk(t.root)
	return count
}
