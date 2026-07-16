package tests

import (
	"bytes"
	"errors"
	"fmt"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
	"os"
	"sync"
	"testing"
	"time"
)

func TestStorage_BasicOperations(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	key := "test_key"
	val := []byte("test_value")

	// Test Set and Get
	err := s.SetWithTTL(key, val, 10*time.Second)
	if err != nil {
		t.Fatalf("SetWithTTL failed: %v", err)
	}

	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("got %s, want %s", string(got), string(val))
	}

	// Test Exists
	if !s.Exists(key) {
		t.Errorf("Exists returned false for existing key")
	}

	// Test Delete
	err = s.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if s.Exists(key) {
		t.Errorf("Exists returned true for deleted key")
	}

	_, err = s.Get(key)
	if !errors.Is(err, pkgerrors.KeyNotFoundError) {
		t.Errorf("expected KeyNotFoundError, got %v", err)
	}
}

func TestStorage_TTLAndExpiration(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	key := "ttl_key"
	val := []byte("ttl_value")

	// Test immediate expiration (or very short)
	s.SetWithTTL(key, val, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	// Note: Exists() or Get() will trigger a delete if the key is expired.
	// We check if it's gone.
	if s.Exists(key) {
		t.Errorf("key should have expired and been removed")
	}

	_, err := s.Get(key)
	// After Exists() is called and returns false (because it's expired),
	// the key is deleted, so Get() will return KeyNotFoundError.
	if !errors.Is(err, pkgerrors.KeyNotFoundError) && !errors.Is(err, pkgerrors.KeyExpiredError) {
		t.Errorf("expected KeyNotFoundError or KeyExpiredError, got %v", err)
	}

	// Test CleanupExpired
	s.SetWithTTL("k1", []byte("v1"), 10*time.Millisecond)
	s.SetWithTTL("k2", []byte("v2"), 100*time.Second)
	time.Sleep(20 * time.Millisecond)

	s.CleanupExpired()

	if s.Exists("k1") {
		t.Errorf("k1 should have been cleaned up")
	}
	if !s.Exists("k2") {
		t.Errorf("k2 should still exist")
	}
}

func TestStorage_Concurrency(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	const workers = 50
	const ops = 100
	var wg sync.WaitGroup

	wg.Add(workers * 2)

	// Concurrent Setters
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				s.SetWithTTL(key, []byte("val"), 1*time.Second)
			}
		}(i)
	}

	// Concurrent Getters/Deleters
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				s.Get(key)
				if j%2 == 0 {
					s.Delete(key)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestStorage_OverwriteAndHeapUpdate(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	key := "update_key"

	// Set with long TTL
	s.SetWithTTL(key, []byte("v1"), 10*time.Second)

	// Overwrite with short TTL
	s.SetWithTTL(key, []byte("v2"), 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)

	if s.Exists(key) {
		t.Errorf("key should have expired after overwrite with shorter TTL")
	}
}

func TestStorage_DeleteNonExistent(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	err := s.Delete("non_existent")
	if err != nil {
		t.Errorf("Delete on non-existent key should not return error, got %v", err)
	}
}

func TestStorage_WALRecovery(t *testing.T) {
	mockWal := &MockWal{}
	s1 := storage.NewStorage(mockWal)
	now := time.Now()
	// 1. Setup initial state
	s1.SetWithTTL("key1", []byte("val1"), 10*time.Second)
	s1.Set("key2", []byte("val2"), storage.NewPayloadMetadata(now, nil))
	s1.Delete("key1")
	s1.Set("key3", []byte("val3"), storage.NewPayloadMetadata(now, nil))

	// 2. Simulate new storage instance with the same WAL
	s2 := storage.NewStorage(mockWal)
	if err := loadStorage(mockWal, s2); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// 3. Verify state
	if s2.Exists("key1") {
		t.Errorf("key1 should not exist (was deleted)")
	}
	v2, err := s2.Get("key2")
	if err != nil || string(v2) != "val2" {
		t.Errorf("key2 recovery failed: got %s, err %v", string(v2), err)
	}
	v3, err := s2.Get("key3")
	if err != nil || string(v3) != "val3" {
		t.Errorf("key3 recovery failed: got %s, err %v", string(v3), err)
	}
}

func TestStorage_WALRecovery_EdgeCases(t *testing.T) {
	mockWal := &MockWal{}
	_ = storage.NewStorage(mockWal) // Just to ensure storage initialization if needed

	// 1. Malformed entries
	mockWal.WriteToWal("INVALID_LINE\n")
	mockWal.WriteToWal("kv|SET|only_two_parts\n")
	mockWal.WriteToWal("kv|SET|key|val|INVALID_DATE\n")

	// 2. Expired entry
	past := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
	mockWal.WriteToWal(fmt.Sprintf("kv|SET|expired|val|%s\n", past))

	// 3. Valid entry
	mockWal.WriteToWal("kv|SET|valid|val|PERSISTENT\n")

	s2 := storage.NewStorage(mockWal)
	if err := loadStorage(mockWal, s2); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if s2.Exists("expired") {
		t.Errorf("expired key should not have been loaded")
	}
	if !s2.Exists("valid") {
		t.Errorf("valid key should have been loaded")
	}
}

func TestStorage_RealWALRecovery(t *testing.T) {
	tempFile, err := os.CreateTemp("", "wal_test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	w, err := wal.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.CloseWal()

	s1 := newStorageWithWal(w)
	s1.Set("k1", []byte("v1"), storage.NewPayloadMetadata(time.Now(), nil))
	s1.SetWithTTL("k2", []byte("v2"), 1*time.Hour)
	s1.Delete("k1")

	// Create new storage with same WAL file
	w2, err := wal.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.CloseWal()

	s2 := newStorageWithWal(w2)
	if err := loadStorage(w2, s2); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if s2.Exists("k1") {
		t.Errorf("k1 should be deleted")
	}
	v2, err := s2.Get("k2")
	if err != nil || string(v2) != "v2" {
		t.Errorf("k2 recovery failed: %v", err)
	}
}

func TestWal_AppendAndSeekBehavior(t *testing.T) {
	tempFile, err := os.CreateTemp("", "append_test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// 1. Open with O_APPEND
	w1, err := wal.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	// 2. Write something
	_, _ = w1.WriteToWal("LINE1\n")
	_, _ = w1.WriteToWal("LINE2\n")

	// 3. Test Recovery (Seek(0,0) and Read)
	var lines []string
	err = w1.RecoverFromWal(func(line string) error {
		lines = append(lines, line)
		return nil
	})
	if err != nil {
		t.Fatalf("RecoverFromWal failed: %v", err)
	}

	if len(lines) != 2 || lines[0] != "LINE1" || lines[1] != "LINE2" {
		t.Errorf("Unexpected lines after recovery: %v", lines)
	}

	// 4. Write after recovery - should STILL append even if we Seeked
	_, _ = w1.WriteToWal("LINE3\n")

	w1.CloseWal()

	// 5. Re-open and check all 3 lines
	w2_2, _ := wal.NewWal(tempFile.Name())
	defer w2_2.CloseWal()
	var finalLines []string
	_ = w2_2.RecoverFromWal(func(line string) error {
		finalLines = append(finalLines, line)
		return nil
	})

	if len(finalLines) != 3 {
		t.Errorf("Expected 3 lines, got %d: %v", len(finalLines), finalLines)
	}
	if finalLines[2] != "LINE3" {
		t.Errorf("Line 3 should be LINE3, got %s", finalLines[2])
	}
}
