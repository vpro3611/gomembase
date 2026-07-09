package tests

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/vpro3611/gomembase.git/core"
	"sync"
	"testing"
	"time"
)

func TestStorage_BasicOperations(t *testing.T) {
	s := core.NewStorage()
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
	if !errors.Is(err, core.KeyNotFoundError) {
		t.Errorf("expected KeyNotFoundError, got %v", err)
	}
}

func TestStorage_TTLAndExpiration(t *testing.T) {
	s := core.NewStorage()
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
	if !errors.Is(err, core.KeyNotFoundError) && !errors.Is(err, core.KeyExpiredError) {
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
	s := core.NewStorage()
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
	s := core.NewStorage()
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
	s := core.NewStorage()
	err := s.Delete("non_existent")
	if err != nil {
		t.Errorf("Delete on non-existent key should not return error, got %v", err)
	}
}
