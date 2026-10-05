package packet

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestRoundTrip 验证各类报文编码后可无损解码。
func TestRoundTrip(t *testing.T) {
	cases := []*Packet{
		{Type: TypeConnect, ClientID: "dev-1", CleanSession: true, KeepAlive: 60,
			HasWill: true, WillTopic: "dev/1/status", WillPayload: []byte("offline"), WillQoS: 1, WillRetain: true,
			Username: "u", Password: []byte("p")},
		{Type: TypeConnAck, SessionPresent: true, ReturnCode: 0},
		{Type: TypePublish, QoS: QoS0, Topic: "a/b", Payload: []byte("hello")},
		{Type: TypePublish, QoS: QoS1, Dup: true, Topic: "a/b", PacketID: 7, Payload: []byte("q1")},
		{Type: TypePublish, QoS: QoS2, Retain: true, Topic: "x", PacketID: 9, Payload: []byte("q2")},
		{Type: TypePubAck, PacketID: 7},
		{Type: TypePubRec, PacketID: 8},
		{Type: TypePubRel, PacketID: 8},
		{Type: TypePubComp, PacketID: 8},
		{Type: TypeSubscribe, PacketID: 3, Subscriptions: []Subscription{{Topic: "a/+"}, {Topic: "b/#", QoS: 2}}},
		{Type: TypeSubAck, PacketID: 3, ReasonCodes: []byte{0, 2, 0x80}},
		{Type: TypeUnsubscribe, PacketID: 4, Subscriptions: []Subscription{{Topic: "a/+"}}},
		{Type: TypeUnsubAck, PacketID: 4},
		{Type: TypePingReq},
		{Type: TypePingResp},
		{Type: TypeDisconnect},
	}
	for _, want := range cases {
		data, err := Encode(want)
		if err != nil {
			t.Fatalf("encode %s: %v", want.Type, err)
		}
		got, err := ReadPacket(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("decode %s: %v", want.Type, err)
		}
		t.Logf("roundtrip type=%s packetID=%d topic=%q subs=%d -> ok", got.Type, got.PacketID, got.Topic, len(got.Subscriptions))
		if got.Type != want.Type {
			t.Errorf("type: got %v want %v", got.Type, want.Type)
		}
		if got.PacketID != want.PacketID || got.Topic != want.Topic || got.QoS != want.QoS {
			t.Errorf("fields mismatch: got %+v want %+v", got, want)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Errorf("payload: got %q want %q", got.Payload, want.Payload)
		}
		if len(got.Subscriptions) != len(want.Subscriptions) {
			t.Errorf("subs: got %d want %d", len(got.Subscriptions), len(want.Subscriptions))
		}
	}
}

// TestMalformed 验证畸形报文被拒绝且原因可区分。
func TestMalformed(t *testing.T) {
	cases := []struct {
		name   string
		data   []byte
		reason string // 期望错误信息包含的子串（判定依据）
	}{
		{"reserved-type", []byte{0x00, 0x00}, "reserved packet type"},
		{"bad-flags-subscribe", []byte{0x80, 0x00}, "invalid fixed header flags"},
		{"bad-flags-pubrel", []byte{0x60, 0x02, 0x00, 0x01}, "invalid fixed header flags"},
		{"remaining-len-overflow", []byte{0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}, "malformed remaining length"},
		{"connect-bad-protocol", connectBytes("MQIs", 4, 0x02), "unsupported protocol name"},
		{"connect-bad-level", connectBytes("MQTT", 5, 0x02), "unsupported protocol level"},
		{"connect-reserved-flag", connectBytes("MQTT", 4, 0x03), "reserved connect flag"},
		{"connect-will-qos3", connectBytes("MQTT", 4, 0x04|0x18), "will QoS 3"},
		{"publish-qos3", []byte{0x36, 0x05, 0x00, 0x01, 'a', 0x00, 0x01}, "QoS 3 is reserved"},
		{"publish-dup-qos0", []byte{0x38, 0x03, 0x00, 0x01, 'a'}, "DUP set with QoS 0"},
		{"publish-empty-topic", []byte{0x30, 0x02, 0x00, 0x00}, "empty topic"},
		{"publish-qos1-no-packetid", []byte{0x32, 0x03, 0x00, 0x01, 'a'}, "unexpected end"},
		{"publish-packetid-zero", []byte{0x32, 0x05, 0x00, 0x01, 'a', 0x00, 0x00}, "packet id must be non-zero"},
		{"subscribe-empty", []byte{0x82, 0x02, 0x00, 0x01}, "no topic filters"},
		{"subscribe-bad-qos", []byte{0x82, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x03}, "requested QoS 3"},
		{"pingreq-nonempty", []byte{0xC0, 0x01, 0x00}, "remaining length must be 0"},
		{"bad-utf8-topic", []byte{0x30, 0x04, 0x00, 0x02, 0xFF, 0xFE}, "not valid UTF-8"},
		{"trailing-bytes", []byte{0x40, 0x03, 0x00, 0x01, 0x00}, "trailing bytes"},
	}
	for _, tc := range cases {
		_, err := ReadPacket(bytes.NewReader(tc.data))
		if err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
			continue
		}
		de, ok := err.(*DecodeError)
		if !ok {
			t.Errorf("%s: error type %T, want *DecodeError", tc.name, err)
			continue
		}
		t.Logf("malformed case=%q -> reason=%q (判定依据: 包含 %q)", tc.name, de.Reason, tc.reason)
		if !strings.Contains(de.Reason, tc.reason) {
			t.Errorf("%s: reason %q does not contain %q", tc.name, de.Reason, tc.reason)
		}
	}
}

// connectBytes 构造一个最小 CONNECT 报文。
func connectBytes(proto string, level byte, flags byte) []byte {
	body := []byte{0x00, byte(len(proto))}
	body = append(body, proto...)
	body = append(body, level, flags, 0x00, 0x3C) // keepalive=60
	body = append(body, 0x00, 0x01, 'c')          // clientID="c"
	out := []byte{0x10, byte(len(body))}
	return append(out, body...)
}

// TestReadPacketBounded 验证单条报文大小上限：超限在缓冲报文体前拒绝，
// 错误可被 errors.Is(err, ErrPacketTooLarge) 区分；未超限正常解析。
func TestReadPacketBounded(t *testing.T) {
	big := &Packet{Type: TypePublish, QoS: QoS0, Topic: "pl/x", Payload: make([]byte, 500)}
	raw, err := Encode(big)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// 上限 128：报文剩余长度远超上限 -> ErrPacketTooLarge，且不读/缓冲报文体
	if _, err := ReadPacketBounded(bytes.NewReader(raw), 128); err == nil {
		t.Fatal("expected error for oversized packet, got nil")
	} else if !errors.Is(err, ErrPacketTooLarge) {
		var de *DecodeError
		if !errors.As(err, &de) {
			t.Fatalf("want *DecodeError wrapping ErrPacketTooLarge, got %T: %v", err, err)
		}
		t.Fatalf("errors.Is(err, ErrPacketTooLarge)=false, reason=%q", de.Reason)
	}
	t.Logf("oversized packet rejected before buffering body: %v (判定依据: errors.Is ErrPacketTooLarge)", err)

	// 上限 0 表示仅受协议上限约束 -> 正常解析
	p, err := ReadPacketBounded(bytes.NewReader(raw), 0)
	if err != nil {
		t.Fatalf("maxBytes=0 must accept protocol-sized packets: %v", err)
	}
	if p.Type != TypePublish || len(p.Payload) != 500 {
		t.Fatalf("unexpected parse: type=%s payloadLen=%d", p.Type, len(p.Payload))
	}

	// 上限恰好等于报文剩余长度 -> 边界为“超过才拒绝”，等于不拒绝
	remLen := len(big.Payload) + 2 + len("pl/x") // payload + 主题长度前缀(2) + 主题
	if _, err := ReadPacketBounded(bytes.NewReader(raw), remLen); err != nil {
		t.Fatalf("size == limit must be accepted, got %v", err)
	}
	if _, err := ReadPacketBounded(bytes.NewReader(raw), remLen-1); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("size == limit-1 must be rejected with ErrPacketTooLarge, got %v", err)
	}
	t.Logf("boundary: remaining==%d accepted, %d rejected (严格超过上限才拒绝)", remLen, remLen-1)
}
