package tests

import (
	"github.com/vpro3611/gomembase.git/core"
	"os"
	"testing"
	"time"
	"bytes"
)

func TestSnapshot_WalIntegration(t *testing.T) {
	walPath := "test_integration.wal"
	snapPath := "test_integration.snap"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	wal, _ := core.NewWal(walPath)
	storage := core.NewStorage(wal)
	snapshot := core.NewSnapshot(snapPath)

	// 1. Set some data
	storage.Set("k1", []byte("v1"), core.PayloadMetadata{CreatedAt: time.Now()})
	storage.Set("k2", []byte("v2"), core.PayloadMetadata{CreatedAt: time.Now()})

	// 2. Save snapshot (should truncate WAL)
	if err := storage.SaveSnapshot(snapshot); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Verify WAL is truncated
	fi, _ := os.Stat(walPath)
	if fi.Size() != 0 {
		t.Errorf("expected WAL to be truncated, got size %d", fi.Size())
	}

	// 3. Set more data in WAL
	storage.Set("k3", []byte("v3"), core.PayloadMetadata{CreatedAt: time.Now()})

	// 4. Simulate restart: New storage, load snapshot then WAL
	wal2, _ := core.NewWal(walPath)
	defer wal2.CloseWal()
	storage2 := core.NewStorage(wal2)

	if err := storage2.LoadFromSnapshot(snapshot); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}
	if err := storage2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// 5. Verify all data is present
	keys := []string{"k1", "k2", "k3"}
	for _, k := range keys {
		if !storage2.Exists(k) {
			t.Errorf("key %s missing after recovery", k)
		}
	}
	v1, _ := storage2.Get("k1")
	if !bytes.Equal(v1, []byte("v1")) {
		t.Errorf("k1 value mismatch: got %s", string(v1))
	}
	v3, _ := storage2.Get("k3")
	if !bytes.Equal(v3, []byte("v3")) {
		t.Errorf("k3 value mismatch: got %s", string(v3))
	}
}
