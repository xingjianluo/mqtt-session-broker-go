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
	flag.Parse()

	b := broker.New(broker.Config{
		MaxQueuePerSession: *maxQueue,
		SessionExpiry:      *expiry,
	})
	s := server.New(b)
	log.Printf("mqtt broker listening on %s (max-queue=%d session-expiry=%ds)", *addr, *maxQueue, *expiry)
	if err := s.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
