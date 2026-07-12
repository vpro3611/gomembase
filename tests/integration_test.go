package tests

import (
	"bytes"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
	"os"
	"testing"
	"time"
)

func TestSnapshot_WalIntegration(t *testing.T) {
	walPath := "test_integration.wal"
	snapPath := "test_integration.snap"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, _ := wal.NewWal(walPath)
	s := storage.NewStorage(w)
	snap := snapshot.NewSnapshot(snapPath)

	// 1. Set some data
	now := time.Now()
	s.Set("k1", []byte("v1"), storage.NewPayloadMetadata(now, nil))
	s.Set("k2", []byte("v2"), storage.NewPayloadMetadata(now, nil))

	// 2. Save snapshot (should truncate WAL)
	if err := s.SaveSnapshot(&snap); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Verify WAL is truncated
	fi, _ := os.Stat(walPath)
	if fi.Size() != 0 {
		t.Errorf("expected WAL to be truncated, got size %d", fi.Size())
	}

	// 3. Set more data in WAL
	s.Set("k3", []byte("v3"), storage.NewPayloadMetadata(now, nil))

	// 4. Simulate restart: New storage, load snapshot then WAL
	w2, _ := wal.NewWal(walPath)
	defer w2.CloseWal()
	s2 := storage.NewStorage(w2)

	if err := s2.LoadFromSnapshot(&snap); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}
	if err := s2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// 5. Verify all data is present
	keys := []string{"k1", "k2", "k3"}
	for _, k := range keys {
		if !s2.Exists(k) {
			t.Errorf("key %s missing after recovery", k)
		}
	}
	v1, _ := s2.Get("k1")
	if !bytes.Equal(v1, []byte("v1")) {
		t.Errorf("k1 value mismatch: got %s", string(v1))
	}
	v3, _ := s2.Get("k3")
	if !bytes.Equal(v3, []byte("v3")) {
		t.Errorf("k3 value mismatch: got %s", string(v3))
	}
}

func TestWal_BinaryData(t *testing.T) {
	walPath := "test_binary.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	s := storage.NewStorage(w)

	// Binary data with delimiters and newlines
	key := "binary_key"
	val := []byte("value|with|delimiters\nand\nnewlines")

	if err := s.Set(key, val, storage.NewPayloadMetadata(time.Now(), nil)); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Close and recover
	w.CloseWal()

	w2, _ := wal.NewWal(walPath)
	defer w2.CloseWal()
	s2 := storage.NewStorage(w2)

	if err := s2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	got, err := s2.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, val) {
		t.Errorf("got %q, want %q", string(got), string(val))
	}
}
