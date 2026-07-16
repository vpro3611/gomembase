package tests

import (
	"bytes"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"testing"
	"time"
)

func TestStorage_Mget_Basic(t *testing.T) {
	s := storage.NewStorage(&MockWal{})

	s.Set("key1", []byte("val1"), storage.NewPayloadMetadata(time.Now(), nil))
	s.Set("key2", []byte("val2"), storage.NewPayloadMetadata(time.Now(), nil))

	keys := []string{"key1", "key2"}
	result, err := s.Mget(keys)
	if err != nil {
		t.Fatalf("Mget failed: %v", err)
	}

	if !bytes.Equal(result["key1"], []byte("val1")) {
		t.Errorf("key1: got %s, want val1", string(result["key1"]))
	}
	if !bytes.Equal(result["key2"], []byte("val2")) {
		t.Errorf("key2: got %s, want val2", string(result["key2"]))
	}
}

func TestStorage_Mget_Partial(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	s.Set("key1", []byte("val1"), storage.NewPayloadMetadata(time.Now(), nil))

	keys := []string{"key1", "nonexistent"}
	result, err := s.Mget(keys)
	if err != nil {
		t.Fatalf("Mget failed: %v", err)
	}

	if !bytes.Equal(result["key1"], []byte("val1")) {
		t.Errorf("key1: got %s, want val1", string(result["key1"]))
	}
	if val, ok := result["nonexistent"]; !ok || val != nil {
		t.Errorf("nonexistent: expected nil, got %v", val)
	}
}

func TestStorage_Mget_Expired(t *testing.T) {
	s := storage.NewStorage(&MockWal{})

	// Set key1 that expires quickly
	s.SetWithTTL("key1", []byte("val1"), 10*time.Millisecond)
	// Set key2 that doesn't expire
	s.Set("key2", []byte("val2"), storage.NewPayloadMetadata(time.Now(), nil))

	time.Sleep(20 * time.Millisecond)

	keys := []string{"key1", "key2"}
	result, err := s.Mget(keys)
	if err != nil {
		t.Fatalf("Mget failed: %v", err)
	}

	// key1 should be nil because it expired
	if result["key1"] != nil {
		t.Errorf("key1: expected nil (expired), got %s", string(result["key1"]))
	}
	// key2 should still be there
	if !bytes.Equal(result["key2"], []byte("val2")) {
		t.Errorf("key2: got %s, want val2", string(result["key2"]))
	}

	// Verify key1 is actually deleted from storage
	if s.Exists("key1") {
		t.Errorf("key1 should have been deleted from storage")
	}
}

func TestStorage_Mget_Mixed(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	s.Set("existing", []byte("val"), storage.NewPayloadMetadata(time.Now(), nil))
	s.SetWithTTL("expired", []byte("old"), 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	keys := []string{"existing", "expired", "missing"}
	result, err := s.Mget(keys)
	if err != nil {
		t.Fatalf("Mget failed: %v", err)
	}

	if !bytes.Equal(result["existing"], []byte("val")) {
		t.Errorf("expected val, got %v", result["existing"])
	}
	if result["expired"] != nil {
		t.Errorf("expected nil for expired, got %v", result["expired"])
	}
	if result["missing"] != nil {
		t.Errorf("expected nil for missing, got %v", result["missing"])
	}
}

func TestStorage_Mget_Empty(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	result, err := s.Mget([]string{})
	if err != nil {
		t.Fatalf("Mget failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %v", result)
	}
}
