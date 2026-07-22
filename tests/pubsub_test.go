package tests

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/pubsub"
	"github.com/vpro3611/gomembase.git/pkg/server"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

func setupPubSubServer(t *testing.T) (*server.Server, func()) {
	tempDir, err := os.MkdirTemp("", "pubsub_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	w, _ := wal.NewWal(filepath.Join(tempDir, "test.wal"))
	snap := snapshot.NewSnapshot(filepath.Join(tempDir, "test.snap"))
	pm := persistence.NewPersistenceManager(w, &snap)
	mux := multiplexer.NewMultiplexer(pm, 10)
	hub := pubsub.NewHub()

	srv := server.NewServer(mux, hub, "127.0.0.1:0")
	go srv.Start()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	return srv, func() {
		srv.Stop()
		os.RemoveAll(tempDir)
	}
}

func readResponse(t *testing.T, reader *bufio.Reader) map[string]interface{} {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	return resp
}

func readPushMessage(t *testing.T, reader *bufio.Reader) pubsub.PushMessage {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("Failed to read push message: %v", err)
	}
	var msg pubsub.PushMessage
	if err := json.Unmarshal(line, &msg); err != nil {
		t.Fatalf("Failed to unmarshal push message: %v\nLine: %s", err, string(line))
	}
	return msg
}

func sendRequest(t *testing.T, conn net.Conn, req string) {
	t.Helper()
	if !strings.HasSuffix(req, "\n") {
		req += "\n"
	}
	_, err := conn.Write([]byte(req))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
}

func TestPubSub_BasicSubscribePublish(t *testing.T) {
	srv, cleanup := setupPubSubServer(t)
	defer cleanup()

	addr := srv.Addr().String()
	subConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer subConn.Close()
	subReader := bufio.NewReader(subConn)

	pubConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer pubConn.Close()
	pubReader := bufio.NewReader(pubConn)

	// Subscribe
	sendRequest(t, subConn, `{"method":"SUBSCRIBE","args":["chat"]}`)
	subResp := readResponse(t, subReader)
	if subResp["ok"] != true {
		t.Fatalf("Subscribe failed: %v", subResp)
	}

	// Publish
	sendRequest(t, pubConn, `{"method":"PUBLISH","args":["chat","hello"]}`)
	pubResp := readResponse(t, pubReader)
	if pubResp["ok"] != true {
		t.Fatalf("Publish failed: %v", pubResp)
	}
	
	// Check push message
	msg := readPushMessage(t, subReader)
	if msg.Type != "message" || msg.Channel != "chat" || string(msg.Data) != `"hello"` {
		t.Fatalf("Unexpected push message: %+v", msg)
	}
}

func TestPubSub_PSubscribeGlob(t *testing.T) {
	srv, cleanup := setupPubSubServer(t)
	defer cleanup()

	addr := srv.Addr().String()
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	subReader := bufio.NewReader(subConn)

	pubConn, _ := net.Dial("tcp", addr)
	defer pubConn.Close()
	pubReader := bufio.NewReader(pubConn)

	// PSubscribe
	sendRequest(t, subConn, `{"method":"PSUBSCRIBE","args":["news.*"]}`)
	readResponse(t, subReader)

	// Publish
	sendRequest(t, pubConn, `{"method":"PUBLISH","args":["news.sports","goal"]}`)
	readResponse(t, pubReader)

	// Check push message
	msg := readPushMessage(t, subReader)
	if msg.Type != "pmessage" || msg.Pattern != "news.*" || msg.Channel != "news.sports" || string(msg.Data) != `"goal"` {
		t.Fatalf("Unexpected push message: %+v", msg)
	}
}

func TestPubSub_SubscriberModeBlocksSET(t *testing.T) {
	srv, cleanup := setupPubSubServer(t)
	defer cleanup()

	addr := srv.Addr().String()
	subConn, _ := net.Dial("tcp", addr)
	defer subConn.Close()
	subReader := bufio.NewReader(subConn)

	sendRequest(t, subConn, `{"method":"SUBSCRIBE","args":["chat"]}`)
	readResponse(t, subReader)

	sendRequest(t, subConn, `{"method":"SET","args":["key","val"]}`)
	resp := readResponse(t, subReader)
	if resp["ok"] == true {
		t.Fatalf("Expected SET to fail in subscriber mode")
	}
	if !strings.Contains(resp["error"].(string), "subscriber mode") {
		t.Fatalf("Expected subscriber mode error, got: %v", resp["error"])
	}
}
