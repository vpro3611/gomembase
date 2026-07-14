package tests

import (
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"testing"
	"time"
)

func TestStorage_FullLifecycle_ResetPersistence(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)

	// 1. Set initial data
	s.Set("key1", []byte("val1"), storage.NewPayloadMetadata(time.Now(), nil))
	s.Set("counter", []byte("0"), storage.NewPayloadMetadata(time.Now(), nil))
	s.IncrementBy("counter", 10)
	s.SetWithTTL("temp", []byte("gone"), 1*time.Hour)

	if !s.Exists("key1") {
		t.Error("key1 should exist")
	}
	if !s.Exists("counter") {
		t.Error("counter should exist")
	}
	if !s.Exists("temp") {
		t.Error("temp should exist")
	}

	if t.Failed() {
		t.Fatalf("Keys should exist before proceeding")
	}

	// 2. Simulate recovery check - it should find keys
	s2 := storage.NewStorage(mockWal)
	if err := s2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !s2.Exists("key1") {
		t.Error("Recovered storage should have key1")
	}

	// 3. Reset original storage
	if err := s.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// 4. Verify WAL is truncated in MockWal (Reset calls TruncateWal)
	if len(mockWal.Writes()) != 0 {
		t.Error("WAL should be empty after Reset")
	}

	// 5. Try to recover into a new storage again - it should be empty now
	s3 := storage.NewStorage(mockWal)
	if err := s3.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if s3.Exists("key1") {
		t.Error("Recovered storage should NOT have key1 after Reset")
	}
}

func TestStorage_Reset_Clear_Expirations(t *testing.T) {
	s := storage.NewStorage(&MockWal{})

	// Add keys with expirations
	s.SetWithTTL("k1", []byte("v1"), 1*time.Hour)
	s.SetWithTTL("k2", []byte("v2"), 2*time.Hour)

	if s.Expirations().Len() != 2 {
		t.Fatalf("Expected 2 expirations, got %d", s.Expirations().Len())
	}

	// Clear
	s.Clear()
	if s.Expirations().Len() != 0 {
		t.Error("Expirations should be cleared by Clear()")
	}
	if len(s.ExpirationMap()) != 0 {
		t.Error("ExpirationMap should be cleared by Clear()")
	}

	// Add again
	s.SetWithTTL("k3", []byte("v3"), 1*time.Hour)
	if s.Expirations().Len() != 1 {
		t.Fatalf("Expected 1 expiration, got %d", s.Expirations().Len())
	}

	// Reset
	s.Reset()
	if s.Expirations().Len() != 0 {
		t.Error("Expirations should be cleared by Reset()")
	}
	if len(s.ExpirationMap()) != 0 {
		t.Error("ExpirationMap should be cleared by Reset()")
	}
}
