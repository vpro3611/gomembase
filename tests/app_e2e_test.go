package tests

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/server"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

type TestTask struct {
	Task     string `json:"task"`
	Priority int    `json:"priority"`
	Done     bool   `json:"done"`
}

type TestRole struct {
	Role  string   `json:"role"`
	Perms []string `json:"perms"`
}

type TestUser struct {
	User    string `json:"user"`
	Country string `json:"country"`
}

func startServerHelper(t *testing.T, w wal.WalInterface, snap *snapshot.Snapshot, maxInstances int) (*server.Server, chan error) {
	pm := persistence.NewPersistenceManager(w, snap)
	mux := multiplexer.NewMultiplexer(pm, maxInstances)
	pm.RegisterEngine(mux)
	pm.RegisterFallbackEngine(mux)

	if err := pm.Restore(nil); err != nil {
		t.Fatalf("failed to restore db: %v", err)
	}

	srv := server.NewServer(mux, nil, "127.0.0.1:0")
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start()
	}()

	// Wait for listener to bind
	time.Sleep(50 * time.Millisecond)
	return srv, errChan
}

func TestApp_E2E_MultiTenantIsolation(t *testing.T) {
	mockWal := &MockWal{}
	srv, errChan := startServerHelper(t, mockWal, nil, 5)
	defer srv.Stop()

	addr := srv.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create Instance 1
	res1 := sendRecv(t, conn, reader, multiplexer.Request{DS: "kv", Method: "CREATE"})
	if !res1.OK || res1.UUID == "" {
		t.Fatalf("failed to create instance 1")
	}
	uuid1 := res1.UUID

	// Create Instance 2
	res2 := sendRecv(t, conn, reader, multiplexer.Request{DS: "kv", Method: "CREATE"})
	if !res2.OK || res2.UUID == "" {
		t.Fatalf("failed to create instance 2")
	}
	uuid2 := res2.UUID

	// Set user:profile on Instance 1 to Alice
	userAlice, _ := json.Marshal(TestUser{User: "Alice", Country: "US"})
	set1 := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "kv",
		UUID:   uuid1,
		Method: "SET",
		Args:   []json.RawMessage{json.RawMessage(`"user:profile"`), json.RawMessage(userAlice)},
	})
	if !set1.OK {
		t.Fatalf("SET instance 1 failed: %s", set1.Error)
	}

	// Set user:profile on Instance 2 to Bob
	userBob, _ := json.Marshal(TestUser{User: "Bob", Country: "UK"})
	set2 := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "kv",
		UUID:   uuid2,
		Method: "SET",
		Args:   []json.RawMessage{json.RawMessage(`"user:profile"`), json.RawMessage(userBob)},
	})
	if !set2.OK {
		t.Fatalf("SET instance 2 failed: %s", set2.Error)
	}

	// Retrieve user:profile from Instance 1
	get1 := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "kv",
		UUID:   uuid1,
		Method: "GET",
		Args:   []json.RawMessage{json.RawMessage(`"user:profile"`)},
	})
	if !get1.OK || len(get1.Data) != 1 {
		t.Fatalf("GET instance 1 failed: %s", get1.Error)
	}
	var u1 TestUser
	_ = json.Unmarshal(get1.Data[0], &u1)
	if u1.User != "Alice" || u1.Country != "US" {
		t.Errorf("expected Alice in instance 1, got %+v", u1)
	}

	// Retrieve user:profile from Instance 2
	get2 := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "kv",
		UUID:   uuid2,
		Method: "GET",
		Args:   []json.RawMessage{json.RawMessage(`"user:profile"`)},
	})
	if !get2.OK || len(get2.Data) != 1 {
		t.Fatalf("GET instance 2 failed: %s", get2.Error)
	}
	var u2 TestUser
	_ = json.Unmarshal(get2.Data[0], &u2)
	if u2.User != "Bob" || u2.Country != "UK" {
		t.Errorf("expected Bob in instance 2, got %+v", u2)
	}

	_ = srv.Stop()
	<-errChan
}

func TestApp_E2E_ListComplexPayloads(t *testing.T) {
	mockWal := &MockWal{}
	srv, errChan := startServerHelper(t, mockWal, nil, 5)
	defer srv.Stop()

	addr := srv.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create list sub-instance
	res := sendRecv(t, conn, reader, multiplexer.Request{DS: "list", Method: "CREATE"})
	uuid := res.UUID

	t1, _ := json.Marshal(TestTask{Task: "Buy milk", Priority: 1, Done: false})
	t2, _ := json.Marshal(TestTask{Task: "Run tests", Priority: 3, Done: true})
	t3, _ := json.Marshal(TestTask{Task: "Fix race condition", Priority: 2, Done: false})

	// Push 3 tasks
	push := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "list",
		UUID:   uuid,
		Method: "LPUSH",
		Args:   []json.RawMessage{json.RawMessage(`"tasks"`), json.RawMessage(t1), json.RawMessage(t2), json.RawMessage(t3)},
	})
	if !push.OK {
		t.Fatalf("LPUSH failed: %s", push.Error)
	}

	// Retrieve LRANGE
	rng := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "list",
		UUID:   uuid,
		Method: "LRANGE",
		Args:   []json.RawMessage{json.RawMessage(`"tasks"`), json.RawMessage(`0`), json.RawMessage(`-1`)},
	})
	if !rng.OK || len(rng.Data) != 3 {
		t.Fatalf("LRANGE failed: count=%d, error=%s", len(rng.Data), rng.Error)
	}

	// Verify order: t3, t2, t1 (since LPUSH prepends)
	var task3, task2, task1 TestTask
	_ = json.Unmarshal(rng.Data[0], &task3)
	_ = json.Unmarshal(rng.Data[1], &task2)
	_ = json.Unmarshal(rng.Data[2], &task1)

	if task3.Task != "Fix race condition" || task2.Task != "Run tests" || task1.Task != "Buy milk" {
		t.Errorf("expected tasks order Fix, Run, Buy, got %+v, %+v, %+v", task3, task2, task1)
	}

	// LINDEX check
	idx := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "list",
		UUID:   uuid,
		Method: "LINDEX",
		Args:   []json.RawMessage{json.RawMessage(`"tasks"`), json.RawMessage(`1`)},
	})
	if !idx.OK || len(idx.Data) != 1 {
		t.Fatalf("LINDEX failed: %s", idx.Error)
	}
	var taskIndex1 TestTask
	_ = json.Unmarshal(idx.Data[0], &taskIndex1)
	if taskIndex1.Task != "Run tests" {
		t.Errorf("expected Run tests, got %s", taskIndex1.Task)
	}

	_ = srv.Stop()
	<-errChan
}

func TestApp_E2E_SetComplexPayloads(t *testing.T) {
	mockWal := &MockWal{}
	srv, errChan := startServerHelper(t, mockWal, nil, 5)
	defer srv.Stop()

	addr := srv.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create set sub-instance
	res := sendRecv(t, conn, reader, multiplexer.Request{DS: "set", Method: "CREATE"})
	uuid := res.UUID

	r1, _ := json.Marshal(TestRole{Role: "admin", Perms: []string{"read", "write"}})
	r2, _ := json.Marshal(TestRole{Role: "editor", Perms: []string{"read", "edit"}})

	// SADD
	add := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "set",
		UUID:   uuid,
		Method: "SADD",
		Args:   []json.RawMessage{json.RawMessage(`"roles"`), json.RawMessage(r1), json.RawMessage(r2)},
	})
	if !add.OK || len(add.Data) != 1 {
		t.Fatalf("SADD failed: %s", add.Error)
	}
	var added int64
	_ = json.Unmarshal(add.Data[0], &added)
	if added != 2 {
		t.Errorf("expected 2 members added, got %d", added)
	}

	// SISMEMBER (admin should be member)
	isMem := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "set",
		UUID:   uuid,
		Method: "SISMEMBER",
		Args:   []json.RawMessage{json.RawMessage(`"roles"`), json.RawMessage(r1)},
	})
	if !isMem.OK || len(isMem.Data) != 1 {
		t.Fatalf("SISMEMBER failed: %s", isMem.Error)
	}
	var member bool
	_ = json.Unmarshal(isMem.Data[0], &member)
	if !member {
		t.Error("expected r1 to be member of roles")
	}

	// SMEMBERS
	mems := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "set",
		UUID:   uuid,
		Method: "SMEMBERS",
		Args:   []json.RawMessage{json.RawMessage(`"roles"`)},
	})
	if !mems.OK || len(mems.Data) != 2 {
		t.Fatalf("SMEMBERS failed: data=%v, error=%s", mems.Data, mems.Error)
	}

	_ = srv.Stop()
	<-errChan
}

func TestApp_E2E_ZSetRankingObjects(t *testing.T) {
	mockWal := &MockWal{}
	srv, errChan := startServerHelper(t, mockWal, nil, 5)
	defer srv.Stop()

	addr := srv.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create zset sub-instance
	res := sendRecv(t, conn, reader, multiplexer.Request{DS: "zset", Method: "CREATE"})
	uuid := res.UUID

	uAlice, _ := json.Marshal(TestUser{User: "Alice", Country: "US"})
	uBob, _ := json.Marshal(TestUser{User: "Bob", Country: "UK"})
	uCharlie, _ := json.Marshal(TestUser{User: "Charlie", Country: "FR"})

	// ZADD
	add := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "zset",
		UUID:   uuid,
		Method: "ZADD",
		Args: []json.RawMessage{
			json.RawMessage(`"users_zset"`),
			json.RawMessage(`100.0`), json.RawMessage(uAlice),
			json.RawMessage(`150.0`), json.RawMessage(uBob),
			json.RawMessage(`80.0`), json.RawMessage(uCharlie),
		},
	})
	if !add.OK {
		t.Fatalf("ZADD failed: %s", add.Error)
	}

	// ZRANGE (ascending order: Charlie (80), Alice (100), Bob (150))
	rng := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "zset",
		UUID:   uuid,
		Method: "ZRANGE",
		Args:   []json.RawMessage{json.RawMessage(`"users_zset"`), json.RawMessage(`0`), json.RawMessage(`-1`)},
	})
	if !rng.OK || len(rng.Data) != 6 {
		t.Fatalf("ZRANGE failed: data=%v, error=%s", rng.Data, rng.Error)
	}

	var firstUser, secondUser, thirdUser TestUser
	_ = json.Unmarshal(rng.Data[0], &firstUser)
	_ = json.Unmarshal(rng.Data[2], &secondUser)
	_ = json.Unmarshal(rng.Data[4], &thirdUser)

	if firstUser.User != "Charlie" || secondUser.User != "Alice" || thirdUser.User != "Bob" {
		t.Errorf("expected ZRANGE sorted order Charlie, Alice, Bob, got %+v, %+v, %+v", firstUser, secondUser, thirdUser)
	}

	// ZRANGEBYSCORE (range 90 to 200: Alice, Bob)
	rngScore := sendRecv(t, conn, reader, multiplexer.Request{
		DS:     "zset",
		UUID:   uuid,
		Method: "ZRANGEBYSCORE",
		Args:   []json.RawMessage{json.RawMessage(`"users_zset"`), json.RawMessage(`90.0`), json.RawMessage(`200.0`)},
	})
	if !rngScore.OK || len(rngScore.Data) != 4 {
		t.Fatalf("ZRANGEBYSCORE failed: data=%v, error=%s", rngScore.Data, rngScore.Error)
	}
	var rAlice, rBob TestUser
	_ = json.Unmarshal(rngScore.Data[0], &rAlice)
	_ = json.Unmarshal(rngScore.Data[2], &rBob)
	if rAlice.User != "Alice" || rBob.User != "Bob" {
		t.Errorf("expected Alice and Bob, got %s and %s", rAlice.User, rBob.User)
	}

	_ = srv.Stop()
	<-errChan
}

func TestApp_E2E_CrashRecoveryAndPersistence(t *testing.T) {
	walFile := "test_e2e_durability.wal"
	snapFile := "test_e2e_durability.rdb"
	defer os.Remove(walFile)
	defer os.Remove(snapFile)

	w, _ := wal.NewWal(walFile)
	snap := snapshot.NewSnapshot(snapFile)

	// Phase 1: Start server, create instances, write data, save snapshot
	srv1, errChan1 := startServerHelper(t, w, &snap, 5)

	addr := srv1.Addr().String()
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	reader1 := bufio.NewReader(conn1)

	// Create sub-instance
	res := sendRecv(t, conn1, reader1, multiplexer.Request{DS: "kv", Method: "CREATE"})
	uuid := res.UUID

	user1, _ := json.Marshal(TestUser{User: "David", Country: "DE"})
	_ = sendRecv(t, conn1, reader1, multiplexer.Request{
		DS:     "kv",
		UUID:   uuid,
		Method: "SET",
		Args:   []json.RawMessage{json.RawMessage(`"user:10"`), json.RawMessage(user1)},
	})

	// Truncate connection, shut down server 1 after snapshotting
	_ = conn1.Close()
	_ = srv1.Stop()
	<-errChan1

	// Retrieve persistence manager to trigger manual snapshot
	// Wait, we can trigger snapshot via new server save or pm, but to make sure WAL is flushed and snapshot saved,
	// let's reopen the server and perform SaveSnapshot via PersistenceManager directly.
	w2, _ := wal.NewWal(walFile)
	pm := persistence.NewPersistenceManager(w2, &snap)
	mux := multiplexer.NewMultiplexer(pm, 5)
	pm.RegisterEngine(mux)
	pm.RegisterFallbackEngine(mux)

	if err := pm.Restore(nil); err != nil {
		t.Fatalf("failed to restore: %v", err)
	}

	// Verify David is recovered in phase 2 from WAL
	kv, ok := mux.GetKV(uuid)
	if !ok {
		t.Fatalf("failed to restore KV instance")
	}
	val, _ := kv.Get("user:10")
	var uDavid TestUser
	_ = json.Unmarshal(val, &uDavid)
	if uDavid.User != "David" {
		t.Fatalf("expected David, got %s", uDavid.User)
	}

	// Write more data in WAL to ensure snapshot covers it
	user2, _ := json.Marshal(TestUser{User: "Elena", Country: "ES"})
	_ = kv.Set("user:20", user2, storage.NewPayloadMetadata(time.Now(), nil))

	// Save snapshot (this will write both David and Elena to snapshot, and truncate WAL)
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	_ = w2.CloseWal()

	// Verify WAL is truncated to size 0
	fi, _ := os.Stat(walFile)
	if fi != nil && fi.Size() != 0 {
		t.Errorf("expected truncated WAL size to be 0, got %d", fi.Size())
	}

	// Reopen server 3 from snapshot with new WAL
	w3, _ := wal.NewWal(walFile)
	defer w3.CloseWal()
	srv3, errChan3 := startServerHelper(t, w3, &snap, 5)
	defer srv3.Stop()

	addr3 := srv3.Addr().String()
	conn3, err := net.Dial("tcp", addr3)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn3.Close()
	reader3 := bufio.NewReader(conn3)

	// Fetch both David and Elena
	get1 := sendRecv(t, conn3, reader3, multiplexer.Request{
		DS:     "kv",
		UUID:   uuid,
		Method: "GET",
		Args:   []json.RawMessage{json.RawMessage(`"user:10"`)},
	})
	var rDavid TestUser
	_ = json.Unmarshal(get1.Data[0], &rDavid)

	get2 := sendRecv(t, conn3, reader3, multiplexer.Request{
		DS:     "kv",
		UUID:   uuid,
		Method: "GET",
		Args:   []json.RawMessage{json.RawMessage(`"user:20"`)},
	})
	var rElena TestUser
	_ = json.Unmarshal(get2.Data[0], &rElena)

	if rDavid.User != "David" || rElena.User != "Elena" {
		t.Errorf("expected David and Elena, got %s and %s", rDavid.User, rElena.User)
	}

	_ = srv3.Stop()
	<-errChan3
}

func sendRecvCompareSlice(t *testing.T, conn net.Conn, reader *bufio.Reader, req multiplexer.Request, expected []string) {
	resp := sendRecv(t, conn, reader, req)
	if !resp.OK {
		t.Fatalf("request failed: %s", resp.Error)
	}
	got := make([]string, len(resp.Data))
	for i, d := range resp.Data {
		var s string
		_ = json.Unmarshal(d, &s)
		got[i] = s
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}
