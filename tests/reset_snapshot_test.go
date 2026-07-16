package tests

import (
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"os"
	"testing"
	"time"
)

func TestStorage_ResetWithSnapshot(t *testing.T) {
	mockWal := &MockWal{}
	snapPath := "test_reset.rdb"
	snap := snapshot.NewSnapshot(snapPath)
	pm := persistence.NewPersistenceManager(mockWal, &snap)
	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)

	// Clean up if file exists
	_ = os.Remove(snapPath)
	defer os.Remove(snapPath)

	// 1. Set some data
	s.Set("key1", []byte("value1"), storage.NewPayloadMetadata(time.Now(), nil))

	// 2. Save snapshot
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Verify snapshot file exists
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		t.Fatal("Snapshot file should exist after SaveSnapshot")
	}

	// 3. Reset storage via PersistenceManager
	if err := pm.Reset(); err != nil {
		t.Fatalf("Reset with snapshot failed: %v", err)
	}

	// 4. Verify snapshot file is gone
	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Error("Snapshot file should have been deleted after Reset")
	}

	// 5. Verify memory and WAL are empty (basic reset check)
	if len(s.Data()) != 0 {
		t.Error("Data should be empty")
	}
	if len(mockWal.Writes()) != 0 {
		t.Error("WAL should be truncated")
	}
}

func TestStorage_ClearWithSnapshot(t *testing.T) {
	mockWal := &MockWal{}
	snapPath := "test_clear.rdb"
	snap := snapshot.NewSnapshot(snapPath)
	pm := persistence.NewPersistenceManager(mockWal, &snap)
	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)

	// Clean up if file exists
	_ = os.Remove(snapPath)
	defer os.Remove(snapPath)

	// 1. Set some data
	s.Set("key1", []byte("value1"), storage.NewPayloadMetadata(time.Now(), nil))

	// 2. Save snapshot
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// 3. Clear storage with snapshot truncation
	if err := pm.Clear(); err != nil {
		t.Fatalf("Clear with snapshot failed: %v", err)
	}

	// 4. Verify snapshot file is gone
	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Error("Snapshot file should have been deleted after Clear")
	}
}
