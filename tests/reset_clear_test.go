package tests

import (
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"testing"
	"time"
)

func TestStorage_Reset(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)

	// Setup initial state
	s.Set("key1", []byte("val1"), storage.NewPayloadMetadata(time.Now(), nil))
	s.SetWithTTL("key2", []byte("val2"), 1*time.Hour)

	if !s.Exists("key1") || !s.Exists("key2") {
		t.Fatal("Keys should exist before Reset")
	}
	if len(mockWal.Writes()) == 0 {
		t.Fatal("WAL should have entries before Reset")
	}

	// Perform Reset
	if err := s.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify memory state
	if s.Exists("key1") || s.Exists("key2") {
		t.Error("Keys should NOT exist after Reset")
	}
	if len(s.Data()) != 0 {
		t.Error("Storage data should be empty after Reset")
	}
	if s.Expirations().Len() != 0 {
		t.Error("Expirations heap should be empty after Reset")
	}
	if len(s.ExpirationMap()) != 0 {
		t.Error("Expiration map should be empty after Reset")
	}

	// Verify WAL state
	if len(mockWal.Writes()) != 0 {
		t.Error("WAL should be empty after Reset")
	}
}

func TestStorage_Clear(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)

	// Setup initial state
	s.Set("key1", []byte("val1"), storage.NewPayloadMetadata(time.Now(), nil))
	s.SetWithTTL("key2", []byte("val2"), 1*time.Hour)

	// Perform Clear
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// Verify memory state
	if s.Exists("key1") || s.Exists("key2") {
		t.Error("Keys should NOT exist after Clear")
	}
	if len(s.Data()) != 0 {
		t.Error("Storage data should be empty after Clear")
	}
	if s.Expirations().Len() != 0 {
		t.Error("Expirations heap should be empty after Clear")
	}

	// Verify WAL state
	if len(mockWal.Writes()) != 0 {
		t.Error("WAL should be empty after Clear")
	}
}

func TestStorage_ResetClear_Concurrency(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)

	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			s.Set("key", []byte("val"), storage.NewPayloadMetadata(time.Now(), nil))
			s.Get("key")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 10; i++ {
			s.Reset()
			s.Clear()
		}
		done <- true
	}()

	<-done
	<-done
	// If it doesn't panic/deadlock, it's a good sign for basic mutex usage
}
