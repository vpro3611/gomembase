package tests

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

func TestTransaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "transaction_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.wal")
	w, _ := wal.NewWal(walPath)
	snap := snapshot.NewSnapshot(filepath.Join(tempDir, "test.snap"))
	pm := persistence.NewPersistenceManager(w, &snap)

	mux := multiplexer.NewMultiplexer(pm, 10)
	
	uuid, _ := mux.CreateInstance("kv")

	// Pre-fill
	req1 := multiplexer.Request{DS: "kv", UUID: uuid, Method: "SET", Args: []json.RawMessage{json.RawMessage(`"key1"`), json.RawMessage(`"val1"`)}}
	mux.Execute(req1)

	// MULTI
	txBuilder := multiplexer.NewTxBuilder(mux)

	// SET new key
	txBuilder.Queue(multiplexer.Request{DS: "kv", UUID: uuid, Method: "SET", Args: []json.RawMessage{json.RawMessage(`"key2"`), json.RawMessage(`"val2"`)}})

	// SET existing key
	txBuilder.Queue(multiplexer.Request{DS: "kv", UUID: uuid, Method: "SET", Args: []json.RawMessage{json.RawMessage(`"key1"`), json.RawMessage(`"val1_updated"`)}})

	// EXEC
	resp, err := txBuilder.Exec("tx-1")
	if err != nil {
		t.Fatalf("Tx exec failed: %v", err)
	}

	if len(resp) != 2 {
		t.Fatalf("Expected 2 responses, got %d", len(resp))
	}

	eng, _ := mux.GetKV(uuid)
	val, err := eng.Get("key1")
	if string(val) != `"val1_updated"` {
		t.Fatalf("Expected val1_updated, got %s", val)
	}

	val2, err := eng.Get("key2")
	if string(val2) != `"val2"` {
		t.Fatalf("Expected val2, got %s", val2)
	}
}

func TestTransactionRollback(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "transaction_test_rb")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.wal")
	w, _ := wal.NewWal(walPath)
	snap := snapshot.NewSnapshot(filepath.Join(tempDir, "test.snap"))
	pm := persistence.NewPersistenceManager(w, &snap)

	mux := multiplexer.NewMultiplexer(pm, 10)
	
	uuid, _ := mux.CreateInstance("kv")

	// Pre-fill
	req1 := multiplexer.Request{DS: "kv", UUID: uuid, Method: "SET", Args: []json.RawMessage{json.RawMessage(`"key1"`), json.RawMessage(`"val1"`)}}
	mux.Execute(req1)

	// MULTI
	txBuilder := multiplexer.NewTxBuilder(mux)

	// SET new key
	txBuilder.Queue(multiplexer.Request{DS: "kv", UUID: uuid, Method: "SET", Args: []json.RawMessage{json.RawMessage(`"key2"`), json.RawMessage(`"val2"`)}})

	// SET existing key
	txBuilder.Queue(multiplexer.Request{DS: "kv", UUID: uuid, Method: "SET", Args: []json.RawMessage{json.RawMessage(`"key1"`), json.RawMessage(`"val1_updated"`)}})

	// Intentional error (missing args)
	txBuilder.Queue(multiplexer.Request{DS: "kv", UUID: uuid, Method: "SET", Args: []json.RawMessage{}})

	// EXEC
	_, err = txBuilder.Exec("tx-2")
	if err == nil {
		t.Fatalf("Expected tx to fail")
	}

	eng, _ := mux.GetKV(uuid)
	
	// Should be rolled back
	val, _ := eng.Get("key1")
	if string(val) != `"val1"` {
		t.Fatalf("Expected key1 to be rolled back to val1, got %s", val)
	}

	exists := eng.Exists("key2")
	if exists {
		t.Fatalf("Expected key2 to be rolled back (deleted), but it exists")
	}
}

func TestTransactionCrossEngine(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "transaction_test_cross")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.wal")
	w, _ := wal.NewWal(walPath)
	snap := snapshot.NewSnapshot(filepath.Join(tempDir, "test.snap"))
	pm := persistence.NewPersistenceManager(w, &snap)

	mux := multiplexer.NewMultiplexer(pm, 10)
	
	kvUUID, _ := mux.CreateInstance("kv")
	listUUID, _ := mux.CreateInstance("list")
	setUUID, _ := mux.CreateInstance("set")
	zsetUUID, _ := mux.CreateInstance("zset")

	// MULTI
	txBuilder := multiplexer.NewTxBuilder(mux)

	// 1. KV SET
	txBuilder.Queue(multiplexer.Request{DS: "kv", UUID: kvUUID, Method: "SET", Args: []json.RawMessage{json.RawMessage(`"username"`), json.RawMessage(`"Alice"`)}})
	
	// 2. List LPUSH
	txBuilder.Queue(multiplexer.Request{DS: "list", UUID: listUUID, Method: "LPUSH", Args: []json.RawMessage{json.RawMessage(`"tasks"`), json.RawMessage(`"task1"`)}})

	// 3. Set SADD
	txBuilder.Queue(multiplexer.Request{DS: "set", UUID: setUUID, Method: "SADD", Args: []json.RawMessage{json.RawMessage(`"tags"`), json.RawMessage(`"urgent"`)}})

	// 4. ZSet ZADD
	txBuilder.Queue(multiplexer.Request{DS: "zset", UUID: zsetUUID, Method: "ZADD", Args: []json.RawMessage{json.RawMessage(`"scores"`), json.RawMessage(`100`), json.RawMessage(`"Alice"`)}})

	// EXEC
	resp, err := txBuilder.Exec("tx-cross-1")
	if err != nil {
		t.Fatalf("Cross-engine tx failed: %v", err)
	}

	if len(resp) != 4 {
		t.Fatalf("Expected 4 responses, got %d", len(resp))
	}
	
	for i, r := range resp {
		if !r.OK {
			t.Fatalf("Response %d failed: %s", i, r.Error)
		}
	}

	// Verify KV
	kv, _ := mux.GetKV(kvUUID)
	if val, _ := kv.Get("username"); string(val) != `"Alice"` {
		t.Fatalf("KV failed, got %s", val)
	}

	// Verify List
	ls, _ := mux.GetList(listUUID)
	if val, _ := ls.Len("tasks"); val != 1 {
		t.Fatalf("List failed, len %d", val)
	}

	// Verify Set
	set, _ := mux.GetSet(setUUID)
	if ok, _ := set.SIsMember("tags", []byte(`"urgent"`)); !ok {
		t.Fatalf("Set failed")
	}

	// Verify ZSet
	zs, _ := mux.GetZSet(zsetUUID)
	if score, ok, _ := zs.ZScore("scores", []byte(`"Alice"`)); !ok || score != 100 {
		t.Fatalf("ZSet failed, score %f, ok %v", score, ok)
	}
}
