// Package server 提供 TCP 监听与连接接入。
package server

import (
	"errors"
	"log"
	"net"
	"sync"

	"github.com/xingjianluo/mqtt-session-broker-go/broker"
)

// Server 是 TCP 接入层。
type Server struct {
	b  *broker.Broker
	ln net.Listener

	mu     sync.Mutex
	closed bool
}

// New 创建服务器。
func New(b *broker.Broker) *Server { return &Server{b: b} }

// ListenAndServe 在 addr 上监听并服务，直到 Close 或出错。
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	return s.Serve(ln)
}

// Serve 在已有监听器上服务。
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go func() {
			defer nc.Close()
			if err := s.b.HandleConnection(nc); err != nil {
				log.Printf("[server] connection %s ended: %v", nc.RemoteAddr(), err)
			}
		}()
	}
}

// Addr 返回实际监听地址。
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Close 关闭监听。
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}
