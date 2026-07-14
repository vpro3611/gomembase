package tests

import (
	"bytes"
	"errors"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"testing"
	"time"
)

func TestStorage_Increment_Basic(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)

	key := "counter"
	s.Set(key, []byte("10"), storage.NewPayloadMetadata(time.Now(), nil))

	newVal, err := s.Increment(key)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if newVal != 11 {
		t.Errorf("expected 11, got %d", newVal)
	}

	// Verify in storage
	got, _ := s.Get(key)
	if !bytes.Equal(got, []byte("11")) {
		t.Errorf("storage value: expected 11, got %s", string(got))
	}

	// Verify WAL was written
	found := false
	writes := mockWal.Writes()
	t.Logf("WAL writes: %v", writes)
	for _, entry := range writes {
		if bytes.Contains([]byte(entry), []byte("SET|counter|")) && bytes.Contains([]byte(entry), []byte("MTE=")) { // "11" in base64 is "MTE="
			found = true
			break
		}
	}
	if !found {
		t.Error("WAL entry for incremented value not found")
	}
}

func TestStorage_Increment_Expired(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)

	key := "expired_counter"
	s.SetWithTTL(key, []byte("10"), 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)

	_, err := s.Increment(key)
	if !errors.Is(err, pkgerrors.KeyExpiredError) && !errors.Is(err, pkgerrors.KeyNotFoundError) {
		t.Errorf("expected KeyExpiredError or KeyNotFoundError, got %v", err)
	}

	// Verify it was deleted from storage
	if s.Exists(key) {
		t.Error("expired key should have been deleted")
	}

	// Verify WAL deletion (user asked if we should write to wal deletion)
	foundDelete := false
	for _, entry := range mockWal.Writes() {
		if entry == "DELETE|expired_counter\n" {
			foundDelete = true
			break
		}
	}
	if !foundDelete {
		t.Error("WAL entry for deletion of expired key not found")
	}
}

func TestStorage_ArithmeticOperations(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)
	key := "math"
	s.Set(key, []byte("100"), storage.NewPayloadMetadata(time.Now(), nil))

	// IncrementBy
	val, _ := s.IncrementBy(key, 50)
	if val != 150 {
		t.Errorf("IncrementBy: expected 150, got %d", val)
	}

	// Decrement
	val, _ = s.Decrement(key)
	if val != 149 {
		t.Errorf("Decrement: expected 149, got %d", val)
	}

	// DecrementBy
	val, _ = s.DecrementBy(key, 100)
	if val != 49 {
		t.Errorf("DecrementBy: expected 49, got %d", val)
	}

	// Non-integer value
	s.Set("nan", []byte("abc"), storage.NewPayloadMetadata(time.Now(), nil))
	_, err := s.Increment("nan")
	if !errors.Is(err, pkgerrors.KeyNotIntegerError) {
		t.Errorf("expected KeyNotIntegerError, got %v", err)
	}
}

func TestStorage_Increment_EdgeCases(t *testing.T) {
	s := storage.NewStorage(&MockWal{})

	// Max Int64
	s.Set("max", []byte("9223372036854775807"), storage.NewPayloadMetadata(time.Now(), nil))
	// Overflowing should technically just follow Go's behavior since we use int64 addition
	val, _ := s.Increment("max")
	if val != -9223372036854775808 {
		t.Errorf("expected overflow to min int64, got %d", val)
	}

	// Non-existent key
	_, err := s.Increment("missing")
	if !errors.Is(err, pkgerrors.KeyNotFoundError) {
		t.Errorf("expected KeyNotFoundError, got %v", err)
	}

	// Empty string value
	s.Set("empty", []byte(""), storage.NewPayloadMetadata(time.Now(), nil))
	_, err = s.Increment("empty")
	if !errors.Is(err, pkgerrors.KeyNotIntegerError) {
		t.Errorf("expected KeyNotIntegerError for empty string, got %v", err)
	}
}
