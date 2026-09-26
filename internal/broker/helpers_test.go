package broker

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
)

func itoaPub(n int) string { return strconv.Itoa(n) }

func netDial(addr string) (net.Conn, error) { return net.Dial("tcp", addr) }

func newByteReader(r io.Reader) *bufio.Reader { return bufio.NewReaderSize(r, 1<<20) }

func readOne(conn net.Conn) *mqttproto.Frame {
	return mqttproto.ReadFrameOrNil(bufio.NewReaderSize(conn, 1<<20))
}

// errNoConnack 表示未读到 CONNACK。
var errNoConnack = errors.New("no CONNACK received")

// buildLevelConnect 手工构造指定 protocol level 的 CONNECT 帧。
func buildLevelConnect(level byte) []byte {
	var vh []byte
	vh = appendStr(vh, "MQTT")
	vh = append(vh, level, 0x02, 0x00, 0x3c) // flags=clean, keepalive=60
	payload := appendStr(nil, "level-client")
	return frameRaw(0x10, append(vh, payload...))
}

func buildLevel3Connect() []byte { return buildLevelConnect(3) }

func appendStr(b []byte, s string) []byte {
	b = append(b, byte(len(s)>>8), byte(len(s)))
	return append(b, s...)
}

func frameRaw(header byte, body []byte) []byte {
	out := []byte{header}
	n := len(body)
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		out = append(out, d)
		if n == 0 {
			break
		}
	}
	return append(out, body...)
}

// mqttConnectWillKeepAlive 构造带 keepAlive 与遗嘱的 CONNECT 帧。
func mqttConnectWillKeepAlive(clientID, willTopic string, willPayload []byte, keepAlive uint16) []byte {
	return mqttproto.EncodeConnect(&mqttproto.Connect{
		ClientID:     clientID,
		CleanSession: true,
		KeepAlive:    keepAlive,
		WillFlag:     true,
		WillQoS:      1,
		WillTopic:    willTopic,
		WillMessage:  willPayload,
	})
}

// pingReq 返回 PINGREQ 固定帧。
func pingReq() []byte { return []byte{0xC0, 0x00} }
