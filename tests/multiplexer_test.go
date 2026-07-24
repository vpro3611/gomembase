package tests

import (
	"os"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

func TestMultiplexer_Lifecycle(t *testing.T) {
	mockWal := &MockWal{}
	mux := multiplexer.NewMultiplexer(mockWal, 3)

	// Create sub-instances and check maximum limits
	u1, err := mux.CreateInstance("kv")
	if err != nil {
		t.Fatalf("failed to create kv: %v", err)
	}
	u2, err := mux.CreateInstance("list")
	if err != nil {
		t.Fatalf("failed to create list: %v", err)
	}
	u3, err := mux.CreateInstance("set")
	if err != nil {
		t.Fatalf("failed to create set: %v", err)
	}
	_, err = mux.CreateInstance("zset")
	if err != multiplexer.ErrInstanceLimitReached {
		t.Errorf("expected limit reached error, got %v", err)
	}

	if mux.TotalInstances() != 3 {
		t.Errorf("expected 3 instances, got %d", mux.TotalInstances())
	}

	// Deletion check
	err = mux.DeleteInstance("list", u2)
	if err != nil {
		t.Fatalf("failed to delete instance: %v", err)
	}
	if mux.TotalInstances() != 2 {
		t.Errorf("expected 2 instances, got %d", mux.TotalInstances())
	}

	// Delete non-existent should fail
	err = mux.DeleteInstance("list", u2)
	if err == nil {
		t.Error("expected error deleting non-existent instance, got nil")
	}

	// Create again
	u4, err := mux.CreateInstance("zset")
	if err != nil {
		t.Fatalf("expected successful creation, got %v", err)
	}

	// Verify get methods
	kv, ok := mux.GetKV(u1)
	if !ok || kv == nil {
		t.Errorf("expected KV instance for %s", u1)
	}
	set, ok := mux.GetSet(u3)
	if !ok || set == nil {
		t.Errorf("expected Set instance for %s", u3)
	}
	zset, ok := mux.GetZSet(u4)
	if !ok || zset == nil {
		t.Errorf("expected ZSet instance for %s", u4)
	}

	// Verify expirations cleanup runs on all active instances without panicking
	mux.CleanupAllExpired()
}

func TestMultiplexer_WALRecoveryIntegration(t *testing.T) {
	walFile := "test_mux.wal"
	defer os.Remove(walFile)

	w, err := wal.NewWal(walFile)
	if err != nil {
		t.Fatalf("failed to create physical WAL: %v", err)
	}
	pm := persistence.NewPersistenceManager(w, nil)
	mux := multiplexer.NewMultiplexer(pm, 5)
	pm.RegisterEngine(mux)
	pm.RegisterFallbackEngine(mux)

	// Create and write to KV instance
	u1, err := mux.CreateInstance("kv")
	if err != nil {
		t.Fatalf("failed to create instance: %v", err)
	}
	kv, ok := mux.GetKV(u1)
	if !ok {
		t.Fatalf("KV instance %s not found", u1)
	}
	err = kv.Set("k1", []byte("v1"), storage.NewPayloadMetadata(time.Now(), nil))
	if err != nil {
		t.Fatalf("failed to Set: %v", err)
	}

	// Close WAL
	_ = w.CloseWal()

	// Recover in a new Multiplexer instance
	w2, err := wal.NewWal(walFile)
	if err != nil {
		t.Fatalf("failed to reopen physical WAL: %v", err)
	}
	defer w2.CloseWal()
	pm2 := persistence.NewPersistenceManager(w2, nil)
	mux2 := multiplexer.NewMultiplexer(pm2, 5)
	pm2.RegisterEngine(mux2)
	pm2.RegisterFallbackEngine(mux2)

	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify KV instance and data restored
	kv2, ok := mux2.GetKV(u1)
	if !ok {
		t.Fatalf("expected KV instance %s to be restored", u1)
	}
	val, err := kv2.Get("k1")
	if err != nil {
		t.Fatalf("Get k1 failed: %v", err)
	}
	if string(val) != "v1" {
		t.Errorf("expected v1, got %s", string(val))
	}
}

func TestMultiplexer_SnapshotIntegration(t *testing.T) {
	snapFile := "test_mux.snap"
	defer os.Remove(snapFile)

	mockWal := &MockWal{}
	snap := snapshot.NewSnapshot(snapFile)
	pm := persistence.NewPersistenceManager(mockWal, &snap)
	mux := multiplexer.NewMultiplexer(pm, 5)
	pm.RegisterEngine(mux)
	pm.RegisterFallbackEngine(mux)

	// Set data on KV and Set sub-instances
	u1, _ := mux.CreateInstance("kv")
	kv, _ := mux.GetKV(u1)
	_ = kv.Set("k1", []byte("v1"), storage.NewPayloadMetadata(time.Now(), nil))

	u2, _ := mux.CreateInstance("set")
	set, _ := mux.GetSet(u2)
	_, _ = set.SAdd("s1", [][]byte{[]byte("member1")}, nil)

	// Save snapshot
	err := pm.SaveSnapshot()
	if err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Recover in a new Multiplexer instance
	pm2 := persistence.NewPersistenceManager(mockWal, &snap)
	mux2 := multiplexer.NewMultiplexer(pm2, 5)
	pm2.RegisterEngine(mux2)
	pm2.RegisterFallbackEngine(mux2)

	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify both instances and data restored
	kv2, ok := mux2.GetKV(u1)
	if !ok {
		t.Fatalf("expected KV instance to be restored")
	}
	val, _ := kv2.Get("k1")
	if string(val) != "v1" {
		t.Errorf("expected k1 = v1, got %s", string(val))
	}

	set2, ok := mux2.GetSet(u2)
	if !ok {
		t.Fatalf("expected Set instance to be restored")
	}
	isMem, _ := set2.SIsMember("s1", []byte("member1"))
	if !isMem {
		t.Error("expected member1 to be in set s1")
	}
}
