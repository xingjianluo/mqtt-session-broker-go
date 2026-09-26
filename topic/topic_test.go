package topic

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	cases := []struct {
		name string
		want error
	}{
		{"a/b/c", nil},
		{"a", nil},
		{"/a/", nil},
		{"$SYS/status", nil},
		{"", ErrEmpty},
		{strings.Repeat("x", 65536), ErrTooLong},
		{"a/+/b", ErrWildcardInName},
		{"a/#", ErrWildcardInName},
		{"a/\x00b", ErrNullChar},
	}
	for _, tc := range cases {
		got := ValidateName(tc.name)
		t.Logf("ValidateName(%q) -> %v (判定依据: errors.Is == %v)", trunc(tc.name), got, tc.want)
		if !errors.Is(got, tc.want) {
			t.Errorf("ValidateName(%q) = %v, want %v", trunc(tc.name), got, tc.want)
		}
	}
}

func TestValidateFilter(t *testing.T) {
	cases := []struct {
		filter string
		want   error
	}{
		{"a/b", nil},
		{"a/+/c", nil},
		{"+", nil},
		{"#", nil},
		{"a/#", nil},
		{"a/+/#", nil},
		{"", ErrEmpty},
		{"a/#/b", ErrBadHash},
		{"a/b#", ErrBadHash},
		{"a#", ErrBadHash},
		{"a/b+", ErrBadPlus},
		{"a/+b/c", ErrBadPlus},
		{"a/\x00b", ErrNullChar},
	}
	for _, tc := range cases {
		got := ValidateFilter(tc.filter)
		t.Logf("ValidateFilter(%q) -> %v (判定依据: errors.Is == %v)", tc.filter, got, tc.want)
		if !errors.Is(got, tc.want) {
			t.Errorf("ValidateFilter(%q) = %v, want %v", tc.filter, got, tc.want)
		}
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		filter, name string
		want         bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"a/+", "a", false},
		{"+/+", "a/b", true},
		{"a/#", "a", true},        // '#' 匹配父级本身
		{"a/#", "a/b/c", true},    // '#' 匹配任意深度
		{"#", "a/b/c", true},
		{"#", "$SYS/x", false},    // 通配符不匹配 $ 系统主题
		{"+/x", "$SYS/x", false},
		{"$SYS/#", "$SYS/x", true},
		{"sport/+/player1", "sport/tennis/player1", true},
		{"sport/tennis#", "sport/tennis", false}, // "tennis#" 非法过滤器
		{"a/+", "a/#", false},                    // 名称含通配符非法
	}
	for _, tc := range cases {
		got := Match(tc.filter, tc.name)
		t.Logf("Match(%q, %q) -> %v (判定依据: 协议通配符语义, want %v)", tc.filter, tc.name, got, tc.want)
		if got != tc.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tc.filter, tc.name, got, tc.want)
		}
	}
}

func trunc(s string) string {
	if len(s) > 20 {
		return s[:20] + "..."
	}
	return s
}
