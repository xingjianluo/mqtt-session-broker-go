// 命令 mqttbroker 启动一个本地 MQTT 3.1.1 消息代理。
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xingjianluo/mqtt-session-broker-go/internal/broker"
)

func main() {
	listen := flag.String("listen", ":1883", "TCP 监听地址")
	maxPacket := flag.Int("max-packet-bytes", 1<<20, "单个 MQTT 报文最大字节数")
	maxQueue := flag.Int("max-queue", 1000, "每会话(离线积压+在途)消息条数上限")
	maxInflight := flag.Int("max-inflight", 32, "每会话未确认 QoS1/2 出站消息窗口大小")
	sessionTTL := flag.Duration("session-ttl", 0, "持久会话离线保留时长(0=永不过期, 如 2h)")
	slowTimeout := flag.Duration("slow-consumer-timeout", 5*time.Second, "出站写阻塞多久判定为慢消费者并断开")
	retry := flag.Duration("retry-interval", 10*time.Second, "未确认 QoS1/2 消息重传间隔")
	flag.Parse()

	cfg := broker.DefaultConfig()
	cfg.Listen = *listen
	cfg.MaxPacketSize = *maxPacket
	cfg.MaxClientQueue = *maxQueue
	cfg.MaxInflight = *maxInflight
	cfg.SessionTTL = *sessionTTL
	cfg.SlowConsumerTimeout = *slowTimeout
	cfg.RetryInterval = *retry
	cfg.EventBufferSize = 256

	b := broker.New(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	l, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.Listen, err)
	}
	log.Printf("mqtt broker listening on %s (MQTT 3.1.1; maxQueue=%d maxInflight=%d sessionTTL=%s)",
		cfg.Listen, cfg.MaxClientQueue, cfg.MaxInflight, cfg.SessionTTL)

	// 打印关键事件：报文类型之外的连接/断开/丢弃原因。
	go func() {
		for ev := range b.Events() {
			log.Printf("event type=%s client=%s reason=%s detail=%s", ev.Type, ev.ClientID, ev.Reason, ev.Detail)
		}
	}()

	errCh := make(chan error, 1)
	go func() { errCh <- b.Serve(ctx, l) }()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, closing broker")
		_ = b.Close()
		<-errCh
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			log.Printf("broker stopped: %v", err)
			os.Exit(1)
		}
	}
	log.Printf("broker stopped cleanly")
}
