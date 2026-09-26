package mqttproto

import (
	"bytes"
	"testing"
)

func TestConnectRoundTrip311(t *testing.T) {
	orig := &Connect{
		CleanSession: true, KeepAlive: 60, ClientID: "dev-1",
		WillFlag: true, WillQoS: 1, WillRetain: true,
		WillTopic: "will/topic", WillMessage: []byte("bye"),
	}
	raw := EncodeConnect(orig)
	f, err := ReadFrame(bytes.NewReader(raw), 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if f.Type != TypeCONNECT {
		t.Fatalf("packet type = %s, want CONNECT", f.Type)
	}
	c, err := DecodeConnect(f.Body)
	if err != nil {
		t.Fatalf("DecodeConnect: %v", err)
	}
	if c.ProtocolLevel != 4 || c.ClientID != "dev-1" || c.WillTopic != "will/topic" ||
		!c.WillFlag || c.WillQoS != 1 || !c.WillRetain || c.KeepAlive != 60 {
		t.Fatalf("decoded connect mismatch: %+v", c)
	}
	t.Logf("判定依据: CONNECT 往返一致, level=%d client=%s will=%s qos=%d",
		c.ProtocolLevel, c.ClientID, c.WillTopic, c.WillQoS)
}

func TestDecodeMalformedFrames(t *testing.T) {
	cases := []struct {
		name   string
		raw    []byte
		code   RejectCode
		pktTyp Type
	}{
		{
			name:   "remaining length exceeds 4 bytes",
			raw:    []byte{0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
			code:   RejectMalformedPacket,
			pktTyp: TypeCONNECT,
		},
		{
			name:   "connect truncated after protocol name length",
			raw:    []byte{0x10, 0x04, 0x00, 0x04, 'M', 'Q'},
			code:   RejectMalformedPacket,
			pktTyp: TypeCONNECT,
		},
		{
			name:   "connect reserved bit set",
			raw:    buildConnectWithFlags(0x01),
			code:   RejectMalformedPacket,
			pktTyp: TypeCONNECT,
		},
		{
			name:   "unknown protocol level 7 returns typed reject",
			raw:    buildConnectLevel(7),
			code:   RejectUnsupportedProtoVer,
			pktTyp: TypeCONNECT,
		},
		{
			name:   "publish bad qos 3",
			raw:    frame(0x36, appendString(nil, "a/b")), // flags=0110 => QoS=3
			code:   RejectMalformedPacket,
			pktTyp: TypePUBLISH,
		},
		{
			name:   "publish qos1 zero packet id",
			raw:    frame(0x32, append(appendString(nil, "a/b"), 0x00, 0x00)),
			code:   RejectProtocolViolation,
			pktTyp: TypePUBLISH,
		},
		{
			name:   "subscribe without filters",
			raw:    frame(0x82, []byte{0x00, 0x01}),
			code:   RejectMalformedPacket,
			pktTyp: TypeSUBSCRIBE,
		},
		{
			name:   "subscribe qos 3",
			raw:    frame(0x82, []byte{0x00, 0x01, 0x00, 0x01, 'a', 0x03}),
			code:   RejectMalformedPacket,
			pktTyp: TypeSUBSCRIBE,
		},
		{
			name:   "packet id body wrong length",
			raw:    frame(0x40, []byte{0x00}),
			code:   RejectMalformedPacket,
			pktTyp: TypePUBACK,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := ReadFrame(bytes.NewReader(tc.raw), 1<<20)
			var derr error
			if err == nil {
				switch f.Type {
				case TypeCONNECT:
					_, derr = DecodeConnect(f.Body)
				case TypePUBLISH:
					_, derr = DecodePublish(f.Flags, f.Body)
				case TypeSUBSCRIBE:
					_, derr = DecodeSubscribe(f.Body)
				default:
					_, derr = DecodePacketID(f.Body)
				}
			} else {
				derr = err
			}
			re, ok := AsRejectError(derr)
			if !ok || re.Code != tc.code {
				t.Fatalf("报文类型=%s 期望拒绝原因=%s, 实际=%v", tc.pktTyp, tc.code, derr)
			}
			t.Logf("判定依据: 报文类型=%s 畸形输入被拒绝 reason=%s detail=%q", tc.pktTyp, re.Code, re.Detail)
		})
	}
}

func TestMQTT5LevelReject(t *testing.T) {
	for _, lvl := range []byte{3, 5} {
		raw := buildConnectLevel(lvl)
		f, err := ReadFrame(bytes.NewReader(raw), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		c, err := DecodeConnect(f.Body)
		if err != nil {
			t.Fatalf("level%d connect should parse level only, got %v", lvl, err)
		}
		if c.ProtocolLevel != lvl {
			t.Fatalf("level=%d want %d", c.ProtocolLevel, lvl)
		}
	}
	if got := EncodeConnack5(0x84); len(got) != 5 {
		t.Fatalf("mqtt5 connack length=%d want 5", len(got))
	}
	t.Logf("判定依据: level 3/5 CONNECT 仅解析 level 并透出, 由上层分别回 CONNACK 0x01 / MQTT5 0x84 后断开")
}

func TestPacketSizeLimit(t *testing.T) {
	raw := EncodeConnect(&Connect{ClientID: string(make([]byte, 100)), CleanSession: true})
	if _, err := ReadFrame(bytes.NewReader(raw), 10); err == nil {
		t.Fatal("expected packet-too-large reject, got nil")
	} else if re, ok := AsRejectError(err); !ok || re.Code != RejectMalformedPacket {
		t.Fatalf("err=%v want malformed(size limit)", err)
	}
	t.Logf("判定依据: 超过 maxPacketSize 的报文在读帧阶段即拒绝")
}

func TestAckEncodings(t *testing.T) {
	if got := EncodeAck(TypePUBREL, 0x1234); !bytes.Equal(got, []byte{0x62, 0x02, 0x12, 0x34}) {
		t.Fatalf("pubrel encoding = % x", got)
	}
	if got := EncodePingresp(); !bytes.Equal(got, []byte{0xD0, 0x00}) {
		t.Fatalf("pingresp = % x", got)
	}
	if got := EncodeDisconnect(); !bytes.Equal(got, []byte{0xE0, 0x00}) {
		t.Fatalf("disconnect = % x", got)
	}
	if got := EncodeSuback(7, []byte{0, 1, 0x80}); !bytes.Equal(got, []byte{0x90, 0x05, 0x00, 0x07, 0x00, 0x01, 0x80}) {
		t.Fatalf("suback = % x", got)
	}
	t.Log("判定依据: PUBREL 固定头 0x62、SUBACK 逐条授予码编码正确")
}

// buildConnectWithFlags 构造一个 CONNECT 帧，connect flags 使用给定字节（用于畸形用例）。
func buildConnectWithFlags(flags byte) []byte {
	var vh []byte
	vh = appendString(vh, "MQTT")
	vh = append(vh, 4, flags, 0x00, 0x3c) // level, flags, keepalive=60
	payload := appendString(nil, "dev-x")
	return frame(0x10, append(vh, payload...))
}

// buildConnectLevel 构造指定 protocol level 的最小 CONNECT 帧。
func buildConnectLevel(level byte) []byte {
	var vh []byte
	vh = appendString(vh, "MQTT")
	vh = append(vh, level, 0x02, 0x00, 0x3c) // clean=1 keepalive=60
	payload := appendString(nil, "dev-x")
	return frame(0x10, append(vh, payload...))
}
