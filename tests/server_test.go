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

type User struct {
	Name    string   `json:"name"`
	Age     int      `json:"age"`
	Student bool     `json:"student"`
	Skills  []string `json:"skills"`
}

func TestServer_E2E(t *testing.T) {
	mockWal := &MockWal{}
	mux := multiplexer.NewMultiplexer(mockWal, 10)
	srv := server.NewServer(mux, "127.0.0.1:0")

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
	if !getResp.OK || len(getResp.Data) != 1 {
		t.Fatalf("KV GET failed: data=%v, error=%s", getResp.Data, getResp.Error)
	}
	var gotStr string
	_ = json.Unmarshal(getResp.Data[0], &gotStr)
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
	if !getObjResp.OK || len(getObjResp.Data) != 1 {
		t.Fatalf("KV GET object failed: data=%v, error=%s", getObjResp.Data, getObjResp.Error)
	}
	var gotUser User
	err = json.Unmarshal(getObjResp.Data[0], &gotUser)
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
	if !lrangeResp.OK || len(lrangeResp.Data) != 2 {
		t.Fatalf("LRANGE failed: data=%v, error=%s", lrangeResp.Data, lrangeResp.Error)
	}
	var id2, id1 map[string]int
	_ = json.Unmarshal(lrangeResp.Data[0], &id2)
	_ = json.Unmarshal(lrangeResp.Data[1], &id1)
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
	if !saddResp.OK || len(saddResp.Data) != 1 {
		t.Fatalf("SADD failed: data=%v, error=%s", saddResp.Data, saddResp.Error)
	}
	var added int64
	_ = json.Unmarshal(saddResp.Data[0], &added)
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
	if !zaddResp.OK || len(zaddResp.Data) != 1 {
		t.Fatalf("ZADD failed: data=%v, error=%s", zaddResp.Data, zaddResp.Error)
	}
	var zadded int64
	_ = json.Unmarshal(zaddResp.Data[0], &zadded)
	if zadded != 2 {
		t.Errorf("expected 2 zset members added, got %d", zadded)
	}

	zrangeResp := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "zset",
		UUID:   zsetUuid,
		Method: "ZRANGE",
		Args:   []json.RawMessage{json.RawMessage(`"zkey"`), json.RawMessage(`0`), json.RawMessage(`-1`)},
	})
	if !zrangeResp.OK || len(zrangeResp.Data) != 4 {
		t.Fatalf("ZRANGE failed: data=%v, error=%s", zrangeResp.Data, zrangeResp.Error)
	}
	var m1 string
	var s1 float64
	_ = json.Unmarshal(zrangeResp.Data[0], &m1)
	_ = json.Unmarshal(zrangeResp.Data[1], &s1)
	if m1 != "m1" || s1 != 1.5 {
		t.Errorf("expected m1 score 1.5, got %s score %f", m1, s1)
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
