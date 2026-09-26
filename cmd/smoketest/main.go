// smoketest 是对真实 TCP 二进制的黑盒冒烟客户端（手工完成 MQTT 最小交互）。
package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/mqttproto"
)

func readFull(c net.Conn, b []byte) {
	got := 0
	for got < len(b) {
		n, err := c.Read(b[got:])
		if err != nil {
			panic(err)
		}
		got += n
	}
}

func readFrame(c net.Conn) *mqttproto.Frame {
	hdr := make([]byte, 1)
	readFull(c, hdr)
	var rem, mult uint32
	mult = 1
	for i := 0; i < 4; i++ {
		b := make([]byte, 1)
		readFull(c, b)
		rem += uint32(b[0]&0x7f) * mult
		if b[0]&0x80 == 0 {
			break
		}
		mult *= 128
	}
	body := make([]byte, rem)
	if rem > 0 {
		readFull(c, body)
	}
	return &mqttproto.Frame{Type: mqttproto.Type(hdr[0] >> 4), Flags: hdr[0] & 0x0f, Body: body}
}

func main() {
	addr := "127.0.0.1:18830"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	write := func(p []byte) {
		if _, err := conn.Write(p); err != nil {
			panic(err)
		}
	}
	write(mqttproto.EncodeConnect(&mqttproto.Connect{ClientID: "smoke-sub", CleanSession: true, KeepAlive: 30}))
	ca := readFrame(conn)
	fmt.Printf("CONNACK type=%s code=%d present=%d\n", ca.Type, ca.Body[1], ca.Body[0])
	if ca.Body[1] != 0 {
		os.Exit(1)
	}
	write(mqttproto.EncodeSubscribe(&mqttproto.Subscribe{PacketID: 1,
		Filters: []mqttproto.SubFilter{{Filter: "sensors/+/temp", QoS: 1}}}))
	sa := readFrame(conn)
	fmt.Printf("SUBACK type=%s codes=%x\n", sa.Type, sa.Body[2:])

	pc, _ := net.Dial("tcp", addr)
	defer pc.Close()
	pc.Write(mqttproto.EncodeConnect(&mqttproto.Connect{ClientID: "smoke-pub", CleanSession: true}))
	readFrame(pc)
	pc.Write(mqttproto.EncodePublish(&mqttproto.Publish{Topic: "sensors/42/temp", QoS: 1, PacketID: 9, Payload: []byte("21.7")}))
	pa := readFrame(pc)
	fmt.Printf("publisher got %s body=%x\n", pa.Type, pa.Body)
	if pa.Type != mqttproto.TypePUBACK {
		os.Exit(1)
	}

	pub := readFrame(conn)
	p, _ := mqttproto.DecodePublish(pub.Flags, pub.Body)
	fmt.Printf("subscriber PUBLISH topic=%s payload=%s qos=%d id=%d -> PUBACK\n", p.Topic, p.Payload, p.QoS, p.PacketID)
	write(mqttproto.EncodeAck(mqttproto.TypePUBACK, p.PacketID))
	time.Sleep(100 * time.Millisecond)
	fmt.Println("SMOKE OK")
}
