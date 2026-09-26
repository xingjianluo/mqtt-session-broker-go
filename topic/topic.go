// Package topic 实现 MQTT 主题名校验与通配符匹配。
package topic

import (
	"errors"
	"strings"
)

// 可区分的校验失败原因。
var (
	ErrEmpty          = errors.New("topic is empty")
	ErrTooLong        = errors.New("topic exceeds 65535 bytes")
	ErrWildcardInName = errors.New("publish topic must not contain wildcards")
	ErrBadPlus        = errors.New("'+' must occupy an entire level")
	ErrBadHash        = errors.New("'#' must be the last level and stand alone")
	ErrNullChar       = errors.New("topic contains U+0000")
)

const maxTopicLen = 65535

func checkCommon(s string) error {
	if len(s) == 0 {
		return ErrEmpty
	}
	if len(s) > maxTopicLen {
		return ErrTooLong
	}
	if strings.IndexByte(s, 0) >= 0 {
		return ErrNullChar
	}
	return nil
}

// ValidateName 校验发布用主题名（不允许通配符）。
func ValidateName(name string) error {
	if err := checkCommon(name); err != nil {
		return err
	}
	if strings.ContainsAny(name, "+#") {
		return ErrWildcardInName
	}
	return nil
}

// ValidateFilter 校验订阅用主题过滤器（允许通配符）。
func ValidateFilter(filter string) error {
	if err := checkCommon(filter); err != nil {
		return err
	}
	levels := strings.Split(filter, "/")
	for i, lv := range levels {
		if strings.Contains(lv, "#") {
			if lv != "#" || i != len(levels)-1 {
				return ErrBadHash
			}
		}
		if strings.Contains(lv, "+") && lv != "+" {
			return ErrBadPlus
		}
	}
	return nil
}

// Match 判断过滤器是否匹配主题名（协议语义的通配符匹配）。
// 约定：以 '$' 开头的系统主题不被以通配符开头的过滤器匹配。
func Match(filter, name string) bool {
	if ValidateFilter(filter) != nil || ValidateName(name) != nil {
		return false
	}
	if strings.HasPrefix(name, "$") &&
		(strings.HasPrefix(filter, "+") || strings.HasPrefix(filter, "#")) {
		return false
	}
	fl := strings.Split(filter, "/")
	nl := strings.Split(name, "/")
	for i, f := range fl {
		if f == "#" {
			return true // '#' 可匹配其父级在内的任意剩余层级
		}
		if i >= len(nl) {
			return false
		}
		if f != "+" && f != nl[i] {
			return false
		}
	}
	return len(fl) == len(nl)
}
