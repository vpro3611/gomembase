package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
)

type Server struct {
	mux        *multiplexer.Multiplexer
	addr       string
	listener   net.Listener
	conns      map[net.Conn]struct{}
	connsMutex sync.Mutex
	wg         sync.WaitGroup
	quit       chan struct{}
	stopOnce   sync.Once
}

func NewServer(mux *multiplexer.Multiplexer, addr string) *Server {
	return &Server{
		mux:   mux,
		addr:  addr,
		conns: make(map[net.Conn]struct{}),
		quit:  make(chan struct{}),
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
		s.conns[conn] = struct{}{}
		s.connsMutex.Unlock()

		s.wg.Add(1)
		go s.handleConnection(conn)
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
		_ = conn.Close()
	}
	s.connsMutex.Unlock()

	s.wg.Wait()
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.connsMutex.Lock()
		delete(s.conns, conn)
		s.connsMutex.Unlock()
		s.wg.Done()
	}()

	reader := bufio.NewReader(conn)
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
			s.sendResponse(conn, multiplexer.Response{
				OK:    false,
				Error: fmt.Sprintf("invalid json payload: %v", err),
			})
			continue
		}

		resp := s.mux.Execute(req)
		s.sendResponse(conn, resp)
	}
}

func (s *Server) sendResponse(conn net.Conn, resp multiplexer.Response) {
	data, err := json.Marshal(resp)
	if err != nil {
		// Log or write raw fallback
		_, _ = conn.Write([]byte(`{"ok":false,"error":"failed to serialize response"}` + "\n"))
		return
	}
	data = append(data, '\n')
	_, _ = conn.Write(data)
}
