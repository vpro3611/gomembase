package tests

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/server"
)

func sendRecv(t *testing.T, conn net.Conn, reader *bufio.Reader, req multiplexer.Request) multiplexer.Response {
	reqData, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}
	_, err = conn.Write(append(reqData, '\n'))
	if err != nil {
		t.Fatalf("failed to write to connection: %v", err)
	}
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	var resp multiplexer.Response
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	return resp
}

func mustValue(t *testing.T, resp multiplexer.Response) json.RawMessage {
	t.Helper()
	if len(resp.Value) == 0 {
		t.Fatalf("expected value payload, got %+v", resp)
	}
	return resp.Value
}

func mustItems(t *testing.T, resp multiplexer.Response, expected int) []json.RawMessage {
	t.Helper()
	if len(resp.Items) != expected {
		t.Fatalf("expected %d items, got %+v", expected, resp)
	}
	return resp.Items
}

type User struct {
	Name    string   `json:"name"`
	Age     int      `json:"age"`
	Student bool     `json:"student"`
	Skills  []string `json:"skills"`
}

func TestServer_E2E(t *testing.T) {
	mockWal := &MockWal{}
	mux := multiplexer.NewMultiplexer(mockWal, 10)
	srv := server.NewServer(mux, nil, "127.0.0.1:0")

	// Start server in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start()
	}()

	// Wait a moment for listener to bind
	time.Sleep(50 * time.Millisecond)

	addr := srv.Addr()
	if addr == nil {
		t.Fatalf("Server listener failed to bind")
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// 1. KV Engine Test with Raw String
	kvCreate := sendRecv(t, conn, reader, multiplexer.Request{DS: "kv", Method: "CREATE"})
	if !kvCreate.OK || kvCreate.UUID == "" {
		t.Fatalf("failed to create KV instance")
	}
	kvUuid := kvCreate.UUID

	argKey := json.RawMessage(`"k1"`)
	argVal := json.RawMessage(`"v1"`)

	setResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "kv",
		UUID:   kvUuid,
		Method: "SET",
		Args:   []json.RawMessage{argKey, argVal},
	})
	if !setResp.OK {
		t.Fatalf("KV SET failed: %s", setResp.Error)
	}

	getResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "kv",
		UUID:   kvUuid,
		Method: "GET",
		Args:   []json.RawMessage{argKey},
	})
	if !getResp.OK {
		t.Fatalf("KV GET failed: error=%s", getResp.Error)
	}
	var gotStr string
	_ = json.Unmarshal(mustValue(t, getResp), &gotStr)
	if gotStr != "v1" {
		t.Errorf("expected v1, got %s", gotStr)
	}

	// 2. KV Engine Test with Arbitrary Complex Object
	userKey := json.RawMessage(`"user:101"`)
	user := User{
		Name:    "Alice",
		Age:     28,
		Student: false,
		Skills:  []string{"Go", "Python"},
	}
	userBytes, _ := json.Marshal(user)
	userVal := json.RawMessage(userBytes)

	setObjResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "kv",
		UUID:   kvUuid,
		Method: "SET",
		Args:   []json.RawMessage{userKey, userVal},
	})
	if !setObjResp.OK {
		t.Fatalf("KV SET object failed: %s", setObjResp.Error)
	}

	getObjResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "kv",
		UUID:   kvUuid,
		Method: "GET",
		Args:   []json.RawMessage{userKey},
	})
	if !getObjResp.OK {
		t.Fatalf("KV GET object failed: error=%s", getObjResp.Error)
	}
	var gotUser User
	err = json.Unmarshal(mustValue(t, getObjResp), &gotUser)
	if err != nil {
		t.Fatalf("failed to unmarshal retrieved object: %v", err)
	}
	if gotUser.Name != "Alice" || gotUser.Age != 28 || gotUser.Student || len(gotUser.Skills) != 2 || gotUser.Skills[0] != "Go" {
		t.Errorf("retrieved user mismatch: %+v", gotUser)
	}

	// 3. List Engine Test
	listCreate := sendRecv(t, conn, reader, multiplexer.Request{DS: "list", Method: "CREATE"})
	if !listCreate.OK || listCreate.UUID == "" {
		t.Fatalf("failed to create List instance")
	}
	listUuid := listCreate.UUID

	lpushResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "list",
		UUID:   listUuid,
		Method: "LPUSH",
		Args:   []json.RawMessage{json.RawMessage(`"lkey"`), json.RawMessage(`{"id":1}`), json.RawMessage(`{"id":2}`)},
	})
	if !lpushResp.OK {
		t.Fatalf("LPUSH failed: %s", lpushResp.Error)
	}

	lrangeResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "list",
		UUID:   listUuid,
		Method: "LRANGE",
		Args:   []json.RawMessage{json.RawMessage(`"lkey"`), json.RawMessage(`0`), json.RawMessage(`-1`)},
	})
	if !lrangeResp.OK {
		t.Fatalf("LRANGE failed: error=%s", lrangeResp.Error)
	}
	items := mustItems(t, lrangeResp, 2)
	var id2, id1 map[string]int
	_ = json.Unmarshal(items[0], &id2)
	_ = json.Unmarshal(items[1], &id1)
	if id2["id"] != 2 || id1["id"] != 1 {
		t.Errorf("expected list elements [id:2, id:1], got [%+v, %+v]", id2, id1)
	}

	// 4. Set Engine Test
	setCreate := sendRecv(t, conn, reader, multiplexer.Request{DS: "set", Method: "CREATE"})
	if !setCreate.OK || setCreate.UUID == "" {
		t.Fatalf("failed to create Set instance")
	}
	setUuid := setCreate.UUID

	saddResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "set",
		UUID:   setUuid,
		Method: "SADD",
		Args:   []json.RawMessage{json.RawMessage(`"skey"`), json.RawMessage(`"member1"`), json.RawMessage(`"member2"`)},
	})
	if !saddResp.OK || saddResp.Integer == nil {
		t.Fatalf("SADD failed: %+v", saddResp)
	}
	added := *saddResp.Integer
	if added != 2 {
		t.Errorf("expected 2 members added, got %d", added)
	}

	// 5. ZSet Engine Test
	zsetCreate := sendRecv(t, conn, reader, multiplexer.Request{DS: "zset", Method: "CREATE"})
	if !zsetCreate.OK || zsetCreate.UUID == "" {
		t.Fatalf("failed to create ZSet instance")
	}
	zsetUuid := zsetCreate.UUID

	zaddResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "zset",
		UUID:   zsetUuid,
		Method: "ZADD",
		Args:   []json.RawMessage{json.RawMessage(`"zkey"`), json.RawMessage(`1.5`), json.RawMessage(`"m1"`), json.RawMessage(`2.5`), json.RawMessage(`"m2"`)},
	})
	if !zaddResp.OK || zaddResp.Integer == nil {
		t.Fatalf("ZADD failed: %+v", zaddResp)
	}
	zadded := *zaddResp.Integer
	if zadded != 2 {
		t.Errorf("expected 2 zset members added, got %d", zadded)
	}

	zrangeResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "zset",
		UUID:   zsetUuid,
		Method: "ZRANGE",
		Args:   []json.RawMessage{json.RawMessage(`"zkey"`), json.RawMessage(`0`), json.RawMessage(`-1`)},
	})
	if !zrangeResp.OK || len(zrangeResp.Scored) != 2 {
		t.Fatalf("ZRANGE failed: %+v", zrangeResp)
	}
	var m1 string
	_ = json.Unmarshal(zrangeResp.Scored[0].Member, &m1)
	if m1 != "m1" || zrangeResp.Scored[0].Score != 1.5 {
		t.Errorf("expected m1 score 1.5, got %s score %f", m1, zrangeResp.Scored[0].Score)
	}

	// 6. Test invalid command
	reqInvalidCmd := multiplexer.Request{
		DS:     "kv",
		UUID:   kvUuid,
		Method: "NONEXISTENT",
	}
	respInvalidCmd := sendRecv(t, conn, reader, reqInvalidCmd)
	if respInvalidCmd.OK {
		t.Error("expected ok=false for invalid command")
	}

	// Close client connection and stop server
	_ = conn.Close()
	_ = srv.Stop()

	// Wait for server goroutine to terminate
	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Server Start returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("timed out waiting for Server Start to return")
	}
}

func TestServer_TransactionTypedResponses(t *testing.T) {
	mockWal := &MockWal{}
	mux := multiplexer.NewMultiplexer(mockWal, 10)
	srv := server.NewServer(mux, nil, "127.0.0.1:0")

	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start()
	}()
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	createResp := sendRecv(t, conn, reader, multiplexer.Request{DS: "kv", Method: "CREATE"})
	if !createResp.OK || createResp.UUID == "" {
		t.Fatalf("failed to create KV instance: %+v", createResp)
	}

	multiResp := sendRecv(t, conn, reader, multiplexer.Request{Method: "MULTI"})
	if !multiResp.OK {
		t.Fatalf("expected OK MULTI response, got %+v", multiResp)
	}

	setReq := multiplexer.Request{
		DS:     "kv",
		UUID:   createResp.UUID,
		Method: "SET",
		Args:   []json.RawMessage{json.RawMessage(`"tx-key"`), json.RawMessage(`"tx-val"`)},
	}
	queuedResp := sendRecv(t, conn, reader, setReq)
	if !queuedResp.OK || queuedResp.Queued == nil || !*queuedResp.Queued {
		t.Fatalf("expected queued SET response, got %+v", queuedResp)
	}

	execResp := sendRecv(t, conn, reader, multiplexer.Request{Method: "EXEC"})
	if !execResp.OK || len(execResp.Responses) != 1 {
		t.Fatalf("expected EXEC typed responses, got %+v", execResp)
	}
	if !execResp.Responses[0].OK {
		t.Fatalf("expected nested exec response to succeed, got %+v", execResp.Responses[0])
	}

	_ = conn.Close()
	_ = srv.Stop()
	<-errChan
}
