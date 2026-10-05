package main

import (
	"flag"
	"log"

	"github.com/xingjianluo/mqtt-session-broker-go/broker"
	"github.com/xingjianluo/mqtt-session-broker-go/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "listen address")
	maxQueue := flag.Int("max-queue", 1000, "max queued/inflight messages per session (<=0 unlimited)")
	expiry := flag.Int64("session-expiry", 3600, "persistent session expiry in seconds (<=0 never)")
	maxPacket := flag.Int("max-packet-size", 1024*1024, "max inbound packet size in bytes (remaining length); <=0 only protocol limit")
	flag.Parse()

	b := broker.New(broker.Config{
		MaxQueuePerSession: *maxQueue,
		SessionExpiry:      *expiry,
		MaxPacketSize:      *maxPacket,
	})
	s := server.New(b)
	log.Printf("mqtt broker listening on %s (max-queue=%d session-expiry=%ds max-packet-size=%d)",
		*addr, *maxQueue, *expiry, *maxPacket)
	if err := s.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
