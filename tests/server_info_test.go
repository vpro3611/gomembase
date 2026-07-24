package tests

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/pubsub"
	"github.com/vpro3611/gomembase.git/pkg/server"
)

func TestInfoCommand(t *testing.T) {
	// Setup real server with in-memory persistence mock
	pm := persistence.NewPersistenceManager(&MockWal{}, nil)
	mux := multiplexer.NewMultiplexer(pm, 10)
	hub := pubsub.NewHub()

	srv := server.NewServer(mux, hub, "127.0.0.1:0") // Random port
	go func() {
		err := srv.Start()
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(100 * time.Millisecond) // wait for listen
	defer srv.Stop()

	// Connect client
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Helper to send request and read raw line
	sendRequest := func(req multiplexer.Request) string {
		b, _ := json.Marshal(req)
		b = append(b, '\n')
		conn.Write(b)
		line, _ := reader.ReadString('\n')
		return line
	}

	// 1. Create a KV instance
	createRespLine := sendRequest(multiplexer.Request{Method: "CREATE", DS: "kv"})
	var createResp multiplexer.Response
	if err := json.Unmarshal([]byte(createRespLine), &createResp); err != nil {
		t.Fatalf("failed to unmarshal create response: %v", err)
	}
	if !createResp.OK {
		t.Fatalf("failed to create instance: %v", createResp.Error)
	}
	uuid := createResp.UUID

	// 2. Set some data in it to bump memory usage and key count
	setReq := multiplexer.Request{
		Method: "SET",
		DS:     "kv",
		UUID:   uuid,
		Args:   []json.RawMessage{[]byte(`"test_key"`), []byte(`"test_value_with_some_length_to_use_bytes"`)},
	}
	sendRequest(setReq)

	// 3. Test INFO without UUID (all instances)
	infoReq := multiplexer.Request{Method: "INFO"}
	infoRespLine := sendRequest(infoReq)

	var infoResp multiplexer.Response
	if err := json.Unmarshal([]byte(infoRespLine), &infoResp); err != nil {
		t.Fatalf("failed to unmarshal info response: %v\nLine: %s", err, infoRespLine)
	}

	if !infoResp.OK || infoResp.Info == nil {
		t.Fatalf("info request failed")
	}

	if infoResp.Info.Server.TotalInstances != 1 {
		t.Errorf("expected 1 total instance, got %d", infoResp.Info.Server.TotalInstances)
	}
	if infoResp.Info.Server.MemoryAllocBytes == 0 {
		t.Errorf("expected MemoryAllocBytes > 0")
	}
	if len(infoResp.Info.Users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(infoResp.Info.Users))
	}
	if len(infoResp.Info.Users[0].Instances) != 1 {
		t.Fatalf("expected 1 instance under user, got %d", len(infoResp.Info.Users[0].Instances))
	}

	inst := infoResp.Info.Users[0].Instances[0]
	if inst.UUID != uuid {
		t.Errorf("expected instance UUID %s, got %s", uuid, inst.UUID)
	}
	if inst.KeyCount != 1 {
		t.Errorf("expected 1 key count, got %d", inst.KeyCount)
	}
	if inst.MemoryUsageBytes == 0 {
		t.Errorf("expected MemoryUsageBytes > 0")
	}

	// 4. Test INFO WITH UUID
	infoReqWithUUID := multiplexer.Request{Method: "INFO", UUID: uuid}
	infoRespWithUUIDLine := sendRequest(infoReqWithUUID)

	var infoRespWithUUID multiplexer.Response
	if err := json.Unmarshal([]byte(infoRespWithUUIDLine), &infoRespWithUUID); err != nil {
		t.Fatalf("failed to unmarshal info response with UUID: %v", err)
	}

	if infoRespWithUUID.Info == nil {
		t.Fatalf("expected info payload for UUID response, got %+v", infoRespWithUUID)
	}
	if len(infoRespWithUUID.Info.Users[0].Instances) != 1 {
		t.Fatalf("expected 1 instance when passing UUID, got %d", len(infoRespWithUUID.Info.Users[0].Instances))
	}

	// 5. Test INFO with non-existent UUID (should return empty instances array)
	infoReqEmpty := multiplexer.Request{Method: "INFO", UUID: "non-existent-uuid"}
	infoRespEmptyLine := sendRequest(infoReqEmpty)

	var infoRespEmpty multiplexer.Response
	json.Unmarshal([]byte(infoRespEmptyLine), &infoRespEmpty)

	if infoRespEmpty.Info == nil {
		t.Fatalf("expected info payload for empty UUID response, got %+v", infoRespEmpty)
	}
	if len(infoRespEmpty.Info.Users[0].Instances) != 0 {
		t.Errorf("expected 0 instances when passing invalid UUID, got %d", len(infoRespEmpty.Info.Users[0].Instances))
	}
}
