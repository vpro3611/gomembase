package server

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/pubsub"
)

type Server struct {
	mux        *multiplexer.Multiplexer
	addr       string
	listener   net.Listener
	conns      map[*ClientConn]struct{}
	connsMutex sync.Mutex
	wg         sync.WaitGroup
	quit       chan struct{}
	stopOnce   sync.Once
	hub        *pubsub.Hub
}

// Generate a random RFC-4122 compliant UUID
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	// Set version 4 and variant 1
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func NewServer(mux *multiplexer.Multiplexer, hub *pubsub.Hub, addr string) *Server {
	return &Server{
		mux:   mux,
		addr:  addr,
		conns: make(map[*ClientConn]struct{}),
		quit:  make(chan struct{}),
		hub:   hub,
	}
}

func (s *Server) Start() error {
	s.connsMutex.Lock()
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.connsMutex.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = l
	s.connsMutex.Unlock()

	for {
		s.connsMutex.Lock()
		listener := s.listener
		s.connsMutex.Unlock()

		if listener == nil {
			return nil
		}

		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				// Accept error (could be temporary, but log and check if closed)
				continue
			}
		}

		s.connsMutex.Lock()
		clientConn := NewClientConn(conn, generateUUID())
		s.conns[clientConn] = struct{}{}
		s.connsMutex.Unlock()

		s.wg.Add(1)
		go s.handleConnection(clientConn)
	}
}

func (s *Server) Addr() net.Addr {
	s.connsMutex.Lock()
	defer s.connsMutex.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

func (s *Server) Stop() error {
	s.stopOnce.Do(func() {
		close(s.quit)
	})

	s.connsMutex.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	s.connsMutex.Unlock()

	// Close all active connections to unblock goroutines
	s.connsMutex.Lock()
	for conn := range s.conns {
		conn.Close()
	}
	s.connsMutex.Unlock()

	s.wg.Wait()
	return nil
}

func (s *Server) handleConnection(clientConn *ClientConn) {
	defer func() {
		clientConn.Close()
		if s.hub != nil {
			s.hub.UnsubscribeAll(clientConn)
		}
		s.connsMutex.Lock()
		delete(s.conns, clientConn)
		s.connsMutex.Unlock()
		s.wg.Done()
	}()

	reader := bufio.NewReader(clientConn.conn)
	var txBuilder *multiplexer.TxBuilder
	subscriberMode := false
	subCount := 0

	for {
		select {
		case <-s.quit:
			return
		default:
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return // Client disconnected
			}
			return // Connection error (e.g. closed by server)
		}

		var req multiplexer.Request
		if err := json.Unmarshal(line, &req); err != nil {
			s.sendResponse(clientConn, multiplexer.Response{
				OK:    false,
				Error: fmt.Sprintf("invalid json payload: %v", err),
			})
			continue
		}

		// INFO Command
		if req.Method == "INFO" {
			info := s.mux.GetInfo(req.UUID)
			b, err := json.Marshal(info)
			if err != nil {
				s.sendResponse(clientConn, multiplexer.Response{OK: false, Error: "failed to marshal info response"})
				continue
			}
			b = append(b, '\n')

			// Write directly to the connection for custom response type,
			// bypassing the standard multiplexer.Response serialization
			clientConn.writeCh <- b
			continue
		}

		// Pub/Sub Commands (Top Priority)
		if req.Method == "PUBLISH" {
			if len(req.Args) < 2 {
				s.sendResponse(clientConn, multiplexer.Response{OK: false, Error: "PUBLISH requires channel and message"})
				continue
			}
			var channel string
			if err := json.Unmarshal(req.Args[0], &channel); err != nil {
				s.sendResponse(clientConn, multiplexer.Response{OK: false, Error: "invalid channel"})
				continue
			}
			recipients := s.hub.Publish(channel, req.Args[1])
			s.sendResponse(clientConn, multiplexer.Response{OK: true, Data: []json.RawMessage{json.RawMessage(fmt.Sprintf("%d", recipients))}})
			continue
		}

		if req.Method == "SUBSCRIBE" {
			subscriberMode = true
			for _, arg := range req.Args {
				var channel string
				if err := json.Unmarshal(arg, &channel); err == nil && channel != "" {
					subCount = s.hub.Subscribe(channel, clientConn)
					s.sendResponse(clientConn, multiplexer.Response{OK: true, Data: []json.RawMessage{
						json.RawMessage(`"subscribe"`),
						json.RawMessage(fmt.Sprintf("%q", channel)),
						json.RawMessage(fmt.Sprintf("%d", subCount)),
					}})
				}
			}
			continue
		}

		if req.Method == "UNSUBSCRIBE" {
			for _, arg := range req.Args {
				var channel string
				if err := json.Unmarshal(arg, &channel); err == nil {
					subCount = s.hub.Unsubscribe(channel, clientConn)
					s.sendResponse(clientConn, multiplexer.Response{OK: true, Data: []json.RawMessage{
						json.RawMessage(`"unsubscribe"`),
						json.RawMessage(fmt.Sprintf("%q", channel)),
						json.RawMessage(fmt.Sprintf("%d", subCount)),
					}})
				}
			}
			if subCount == 0 {
				subscriberMode = false
			}
			continue
		}

		if req.Method == "PSUBSCRIBE" {
			subscriberMode = true
			for _, arg := range req.Args {
				var pattern string
				if err := json.Unmarshal(arg, &pattern); err == nil && pattern != "" {
					subCount = s.hub.PSubscribe(pattern, clientConn)
					s.sendResponse(clientConn, multiplexer.Response{OK: true, Data: []json.RawMessage{
						json.RawMessage(`"psubscribe"`),
						json.RawMessage(fmt.Sprintf("%q", pattern)),
						json.RawMessage(fmt.Sprintf("%d", subCount)),
					}})
				}
			}
			continue
		}

		if req.Method == "PUNSUBSCRIBE" {
			for _, arg := range req.Args {
				var pattern string
				if err := json.Unmarshal(arg, &pattern); err == nil {
					subCount = s.hub.PUnsubscribe(pattern, clientConn)
					s.sendResponse(clientConn, multiplexer.Response{OK: true, Data: []json.RawMessage{
						json.RawMessage(`"punsubscribe"`),
						json.RawMessage(fmt.Sprintf("%q", pattern)),
						json.RawMessage(fmt.Sprintf("%d", subCount)),
					}})
				}
			}
			if subCount == 0 {
				subscriberMode = false
			}
			continue
		}

		if subscriberMode {
			s.sendResponse(clientConn, multiplexer.Response{OK: false, Error: "connection is in subscriber mode, use a different connection for commands"})
			continue
		}

		if req.Method == "MULTI" {
			if txBuilder != nil {
				s.sendResponse(clientConn, multiplexer.Response{OK: false, Error: "MULTI calls can not be nested"})
				continue
			}
			txBuilder = multiplexer.NewTxBuilder(s.mux)
			s.sendResponse(clientConn, multiplexer.Response{OK: true})
			continue
		}

		if req.Method == "EXEC" {
			if txBuilder == nil {
				s.sendResponse(clientConn, multiplexer.Response{OK: false, Error: "EXEC without MULTI"})
				continue
			}

			txID := "tx-" + generateUUID()
			responses, err := txBuilder.Exec(txID)

			if err != nil {
				s.sendResponse(clientConn, multiplexer.Response{OK: false, Error: err.Error()})
			} else {
				var data []json.RawMessage
				for _, r := range responses {
					b, _ := json.Marshal(r)
					data = append(data, json.RawMessage(b))
				}
				s.sendResponse(clientConn, multiplexer.Response{OK: true, Data: data})
			}
			txBuilder = nil
			continue
		}

		if req.Method == "DISCARD" {
			if txBuilder == nil {
				s.sendResponse(clientConn, multiplexer.Response{OK: false, Error: "DISCARD without MULTI"})
				continue
			}
			txBuilder = nil
			s.sendResponse(clientConn, multiplexer.Response{OK: true})
			continue
		}

		if txBuilder != nil {
			txBuilder.Queue(req)
			s.sendResponse(clientConn, multiplexer.Response{OK: true, Data: []json.RawMessage{json.RawMessage(`"QUEUED"`)}})
			continue
		}

		resp := s.mux.Execute(req)
		s.sendResponse(clientConn, resp)
	}
}

func (s *Server) sendResponse(clientConn *ClientConn, resp multiplexer.Response) {
	clientConn.WriteResponse(resp)
}
