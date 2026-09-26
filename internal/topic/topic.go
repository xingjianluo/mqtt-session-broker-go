// Package topic 实现 MQTT 主题名/主题过滤器的校验与通配符匹配。
package topic

import (
	"strings"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
)

// MaxTopicLength 为主题字段的最大字节数（MQTT 字符串字段上限）。
const MaxTopicLength = 65535

// RejectCode 转发协议层拒绝码，避免上层重复定义。
type RejectCode = mqttproto.RejectCode

// ValidateTopic 校验 PUBLISH 主题名：非空、不含通配符、长度受限、不含 NUL。
func ValidateTopic(name string) error {
	if len(name) == 0 {
		return mqttproto.NewRejectError(mqttproto.RejectBadTopic, "topic name must not be empty")
	}
	if len(name) > MaxTopicLength {
		return mqttproto.NewRejectError(mqttproto.RejectBadTopic, "topic name exceeds 65535 bytes")
	}
	if strings.ContainsRune(name, 0) {
		return mqttproto.NewRejectError(mqttproto.RejectBadTopic, "topic name contains NUL")
	}
	if strings.ContainsAny(name, "+#") {
		return mqttproto.NewRejectError(mqttproto.RejectBadTopic,
			"topic name in PUBLISH must not contain wildcard '+' or '#'")
	}
	return nil
}

// ValidateFilter 校验订阅过滤器：非空、长度受限、'#' 只能作为最后一级且独占该级，
// 普通层级不得包含通配符。
func ValidateFilter(filter string) error {
	if len(filter) == 0 {
		return mqttproto.NewRejectError(mqttproto.RejectBadFilter, "topic filter must not be empty")
	}
	if len(filter) > MaxTopicLength {
		return mqttproto.NewRejectError(mqttproto.RejectBadFilter, "topic filter exceeds 65535 bytes")
	}
	if strings.ContainsRune(filter, 0) {
		return mqttproto.NewRejectError(mqttproto.RejectBadFilter, "topic filter contains NUL")
	}
	levels := strings.Split(filter, "/")
	for i, lv := range levels {
		switch {
		case lv == "#":
			if i != len(levels)-1 {
				return mqttproto.NewRejectError(mqttproto.RejectBadFilter,
					"'#' multi-level wildcard must be the last level")
			}
		case lv == "+":
			// 单层通配独占一级，合法
		case strings.ContainsAny(lv, "+#"):
			return mqttproto.NewRejectError(mqttproto.RejectBadFilter,
				"wildcard '+'/'#' must occupy an entire level")
		}
	}
	return nil
}

// Match 判断主题名是否匹配过滤器（调用前二者均已校验合法）。
// 语义：'+' 匹配恰好一级；'#' 匹配其父级及任意后续层级；
// 以 '$' 开头的系统主题不与首层为通配符的过滤器匹配。
func Match(filter, name string) bool {
	if name == "" {
		return false
	}
	// $SYS 主题不被顶层通配订阅匹配。
	if name[0] == '$' {
		if strings.HasPrefix(filter, "+") || strings.HasPrefix(filter, "#") {
			return false
		}
	}
	f := strings.Split(filter, "/")
	n := strings.Split(name, "/")
	for i, fl := range f {
		if fl == "#" {
			return true
		}
		if i >= len(n) {
			return false
		}
		if fl == "+" {
			continue
		}
		if fl != n[i] {
			return false
		}
	}
	return len(f) == len(n)
}
