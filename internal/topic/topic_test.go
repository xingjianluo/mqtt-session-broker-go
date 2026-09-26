package topic

import "testing"

func TestValidateTopic(t *testing.T) {
	good := []string{"a", "a/b/c", "dev/42/temp", "a//b", "$SYS/load", "中文/主题"}
	for _, s := range good {
		if err := ValidateTopic(s); err != nil {
			t.Errorf("topic %q should be valid, got %v", s, err)
		}
	}
	bad := []string{"", "a/+", "a/#", "a/b+", "a\x00b"}
	for _, s := range bad {
		err := ValidateTopic(s)
		if err == nil {
			t.Errorf("topic %q should be rejected", s)
			continue
		}
		t.Logf("判定依据: 非法 PUBLISH 主题 %q 被拒绝 -> %v", s, err)
	}
}

func TestValidateFilter(t *testing.T) {
	good := []string{"#", "+/b", "a/+/c", "a/#", "sport/tennis/+", "$SYS/#"}
	for _, f := range good {
		if err := ValidateFilter(f); err != nil {
			t.Errorf("filter %q should be valid, got %v", f, err)
		}
	}
	bad := []string{"", "a/#/b", "a/b#/c", "a+/b", "a/b+", "#/x"}
	for _, f := range bad {
		if err := ValidateFilter(f); err == nil {
			t.Errorf("filter %q should be rejected", f)
		} else {
			t.Logf("判定依据: 非法订阅过滤器 %q 被拒绝 -> %v", f, err)
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
		{"a/#", "a/b/c", true},
		{"a/#", "a", true}, // # 可匹配父级（零个后续层级）
		{"a/+", "a", false},
		{"#", "anything/here", true},
		{"+", "a", true},
		{"+", "a/b", false},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/x/c", true},
		{"a/+/c", "a/b/d", false},
		{"#", "$SYS/load", false},     // $SYS 不被顶层 # 匹配
		{"+/x", "$SYS/x", false},      // $SYS 不被顶层 + 匹配
		{"$SYS/#", "$SYS/load", true}, // 显式 $SYS 过滤器可以匹配
	}
	for _, tc := range cases {
		if got := Match(tc.filter, tc.name); got != tc.want {
			t.Errorf("Match(%q,%q)=%v want %v", tc.filter, tc.name, got, tc.want)
		} else {
			t.Logf("判定依据: Match filter=%q name=%q => %v", tc.filter, tc.name, got)
		}
	}
}

func TestTrieDedup(t *testing.T) {
	tr := NewTrie()
	tr.Subscribe("a/+", "c1", 1)
	tr.Subscribe("a/#", "c1", 2)
	tr.Subscribe("a/b", "c2", 0)
	got := tr.Match("a/b")
	qos := map[string]byte{}
	for _, r := range got {
		qos[r.ClientID] = r.QoS
	}
	if len(qos) != 2 {
		t.Fatalf("matched clients=%v, want c1,c2 各一次（重叠过滤器去重）", qos)
	}
	if qos["c1"] != 2 {
		t.Fatalf("c1 qos=%d, want 取重叠过滤器最大值 2", qos["c1"])
	}
	if qos["c2"] != 0 {
		t.Fatalf("c2 qos=%d, want 0", qos["c2"])
	}
	t.Logf("判定依据: c1 同时订阅 a/+ 与 a/#, 命中 a/b 仅投递一次且 QoS=max(1,2)=2; 命中=%v", qos)

	tr.Unsubscribe("a/#", "c1")
	got = tr.Match("a/b")
	qos = map[string]byte{}
	for _, r := range got {
		qos[r.ClientID] = r.QoS
	}
	if qos["c1"] != 1 {
		t.Fatalf("取消 a/# 后 c1 应仍由 a/+ 命中 qos=1, got=%v", qos)
	}
	if n := tr.RemoveClient("c2"); n != 1 {
		t.Fatalf("RemoveClient c2 removed=%d want 1", n)
	}
	if r := tr.Match("a/b"); len(r) != 1 {
		t.Fatalf("移除 c2 后命中数=%d want 1", len(r))
	}
}
