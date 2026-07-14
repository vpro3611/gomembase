package tests

import (
	"bytes"
	"fmt"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"testing"
	"time"
)

func TestStorage_Mset(t *testing.T) {
	s := storage.NewStorage(&MockWal{})

	data := map[string]storage.Payload{
		"key1": storage.NewPayload([]byte("val1"), storage.NewPayloadMetadata(time.Now(), nil)),
		"key2": storage.NewPayload([]byte("val2"), storage.NewPayloadMetadata(time.Now(), nil)),
	}

	// Now Mset takes map[string]Payload
	err := s.Mset(data)
	if err != nil {
		t.Fatalf("Mset failed: %v", err)
	}

	val1, err := s.Get("key1")
	if err != nil {
		t.Errorf("Get key1 failed: %v", err)
	}
	if !bytes.Equal(val1, []byte("val1")) {
		t.Errorf("key1: got %s, want val1", string(val1))
	}

	val2, err := s.Get("key2")
	if err != nil {
		t.Errorf("Get key2 failed: %v", err)
	}
	if !bytes.Equal(val2, []byte("val2")) {
		t.Errorf("key2: got %s, want val2", string(val2))
	}
}

func TestStorage_MsetWithTTL(t *testing.T) {
	s := storage.NewStorage(&MockWal{})

	data := map[string]storage.Payload{
		"key1": storage.NewPayload([]byte("val1"), storage.NewPayloadMetadata(time.Now(), nil)),
		"key2": storage.NewPayload([]byte("val2"), storage.NewPayloadMetadata(time.Now(), nil)),
	}

	// Now MsetWithTTL takes map[string]Payload
	err := s.MsetWithTTL(data, 1*time.Hour)
	if err != nil {
		t.Fatalf("MsetWithTTL failed: %v", err)
	}

	val1, err := s.Get("key1")
	if err != nil {
		t.Errorf("Get key1 failed: %v", err)
	}
	if !bytes.Equal(val1, []byte("val1")) {
		t.Errorf("key1: got %s, want val1", string(val1))
	}
}

func TestStorage_Mset_Nil(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	var data map[string]storage.Payload = nil
	err := s.Mset(data)
	if err != nil {
		t.Errorf("Mset should not fail on nil map, just do nothing: %v", err)
	}
}

func TestStorage_Mset_LargeBatch(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	const count = 1000
	data := make(map[string]storage.Payload, count)
	for i := range count {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		data[key] = storage.NewPayload([]byte(val), storage.NewPayloadMetadata(time.Now(), nil))
	}

	err := s.Mset(data)
	if err != nil {
		t.Fatalf("Mset large batch failed: %v", err)
	}

	// Spot check
	got, _ := s.Get("key_500")
	if !bytes.Equal(got, []byte("val_500")) {
		t.Errorf("expected val_500, got %s", string(got))
	}
}

func TestStorage_Mset_Overwrites(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	s.Set("key1", []byte("old1"), storage.NewPayloadMetadata(time.Now(), nil))

	data := map[string]storage.Payload{
		"key1": storage.NewPayload([]byte("new1"), storage.NewPayloadMetadata(time.Now(), nil)),
		"key2": storage.NewPayload([]byte("new2"), storage.NewPayloadMetadata(time.Now(), nil)),
	}

	s.Mset(data)

	got, _ := s.Get("key1")
	if !bytes.Equal(got, []byte("new1")) {
		t.Errorf("expected new1, got %s", string(got))
	}
}
