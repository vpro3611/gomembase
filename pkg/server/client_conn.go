package server

import (
	"encoding/json"
	"net"
	"sync"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/pubsub"
)

type ClientConn struct {
	conn    net.Conn
	id       string
	writeCh  chan []byte
	done     chan struct{}
	stopOnce sync.Once
}

func NewClientConn(conn net.Conn, id string) *ClientConn {
	cc := &ClientConn{
		conn:    conn,
		id:      id,
		writeCh: make(chan []byte, 256), // Buffer for high-throughput pubsub
		done:    make(chan struct{}),
	}
	go cc.writeLoop()
	return cc
}

func (cc *ClientConn) writeLoop() {
	for {
		select {
		case <-cc.done:
			return
		case data := <-cc.writeCh:
			_, _ = cc.conn.Write(data)
		}
	}
}

// WriteResponse is blocking (we don't want to drop command responses)
func (cc *ClientConn) WriteResponse(resp multiplexer.Response) {
	data, err := json.Marshal(resp)
	if err != nil {
		data = []byte(`{"ok":false,"error":"failed to serialize response"}`)
	}
	data = append(data, '\n')
	
	select {
	case <-cc.done:
	case cc.writeCh <- data:
	}
}

// Send implements pubsub.Subscriber (non-blocking)
func (cc *ClientConn) Send(msg pubsub.PushMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	data = append(data, '\n')

	select {
	case cc.writeCh <- data:
		// success
	default:
		// buffer full, drop message (slow consumer)
	}
}

func (cc *ClientConn) ID() string {
	return cc.id
}

func (cc *ClientConn) Close() {
	cc.stopOnce.Do(func() {
		close(cc.done)
		_ = cc.conn.Close()
	})
}
