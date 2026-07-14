package tests

import (
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"os"
	"testing"
	"time"
)

func TestStorage_ResetWithSnapshot(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)
	snapPath := "test_reset.rdb"
	snap := snapshot.NewSnapshot(snapPath)

	// Clean up if file exists
	_ = os.Remove(snapPath)
	defer os.Remove(snapPath)

	// 1. Set some data
	s.Set("key1", []byte("value1"), storage.NewPayloadMetadata(time.Now(), nil))

	// 2. Save snapshot
	if err := s.SaveSnapshot(&snap); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Verify snapshot file exists
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		t.Fatal("Snapshot file should exist after SaveSnapshot")
	}

	// 3. Reset storage with snapshot truncation
	s.AddSnapshotter(&snap)
	if err := s.Reset(); err != nil {
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
	s := storage.NewStorage(mockWal)
	snapPath := "test_clear.rdb"
	snap := snapshot.NewSnapshot(snapPath)

	// Clean up if file exists
	_ = os.Remove(snapPath)
	defer os.Remove(snapPath)

	// 1. Set some data
	s.Set("key1", []byte("value1"), storage.NewPayloadMetadata(time.Now(), nil))

	// 2. Save snapshot
	if err := s.SaveSnapshot(&snap); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// 3. Clear storage with snapshot truncation
	s.AddSnapshotter(&snap)
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear with snapshot failed: %v", err)
	}

	// 4. Verify snapshot file is gone
	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Error("Snapshot file should have been deleted after Clear")
	}
}

func TestStorage_ResetWithRegisteredSnapshot(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)
	snapPath := "test_registered.rdb"
	snap := snapshot.NewSnapshot(snapPath)

	_ = os.Remove(snapPath)
	defer os.Remove(snapPath)

	s.AddSnapshotter(&snap)
	s.Set("key1", []byte("value1"), storage.NewPayloadMetadata(time.Now(), nil))

	if err := s.SaveSnapshot(&snap); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		t.Fatal("Snapshot file should exist after SaveSnapshot")
	}

	// Reset WITHOUT passing snap - it should use the registered one
	if err := s.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Error("Snapshot file should have been deleted via registered snapshotter")
	}
}
