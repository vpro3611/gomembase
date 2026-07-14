package tests

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

func TestStorage_PatternCountAndFind(t *testing.T) {
	s := storage.NewStorage(&MockWal{})

	// Seed data
	data := map[string][]byte{
		"user:100:profile": []byte("alice"),
		"user:101:profile": []byte("bob"),
		"user:102:meta":    []byte("metadata"),
		"group:99:profile": []byte("admin_group"),
		"other:key":        []byte("other"),
	}

	for k, v := range data {
		err := s.Set(k, v, storage.NewPayloadMetadata(time.Now(), nil))
		if err != nil {
			t.Fatalf("failed to set %s: %v", k, err)
		}
	}

	// 1. Prefix Tests
	t.Run("Prefix", func(t *testing.T) {
		count := s.CountByPrefix("user:")
		if count != 3 {
			t.Errorf("CountByPrefix(\"user:\") = %d; want 3", count)
		}

		found := s.FindByPrefix("user:")
		if len(found) != 3 {
			t.Errorf("len(FindByPrefix(\"user:\")) = %d; want 3", len(found))
		}
		if !bytes.Equal(found["user:100:profile"], []byte("alice")) {
			t.Errorf("mismatch value for user:100:profile")
		}
	})

	// 2. Suffix Tests
	t.Run("Suffix", func(t *testing.T) {
		count := s.CountBySuffix(":profile")
		if count != 3 {
			t.Errorf("CountBySuffix(\":profile\") = %d; want 3", count)
		}

		found := s.FindBySuffix(":profile")
		if len(found) != 3 {
			t.Errorf("len(FindBySuffix(\":profile\")) = %d; want 3", len(found))
		}
		if !bytes.Equal(found["group:99:profile"], []byte("admin_group")) {
			t.Errorf("mismatch value for group:99:profile")
		}
	})

	// 3. Regex Tests
	t.Run("Regex", func(t *testing.T) {
		count, err := s.CountByRegex(`^user:\d+:profile$`)
		if err != nil {
			t.Fatalf("CountByRegex failed: %v", err)
		}
		if count != 2 {
			t.Errorf("CountByRegex = %d; want 2", count)
		}

		found, err := s.FindByRegex(`^user:\d+:profile$`)
		if err != nil {
			t.Fatalf("FindByRegex failed: %v", err)
		}
		if len(found) != 2 {
			t.Errorf("len(FindByRegex) = %d; want 2", len(found))
		}
	})

	// 4. Invalid Regex Test
	t.Run("InvalidRegex", func(t *testing.T) {
		_, err := s.CountByRegex(`[invalid`)
		if err == nil {
			t.Fatal("expected error for invalid regex, got nil")
		}
		var regexErr pkgerrors.RegexError
		if !errors.As(err, &regexErr) {
			t.Errorf("expected pkgerrors.RegexError, got %T: %v", err, err)
		}

		_, err = s.FindByRegex(`[invalid`)
		if err == nil {
			t.Fatal("expected error for invalid regex in FindByRegex, got nil")
		}
	})
}

func TestStorage_PatternDelete(t *testing.T) {
	s := storage.NewStorage(&MockWal{})

	// Seed data
	data := map[string][]byte{
		"user:100:profile": []byte("alice"),
		"user:101:profile": []byte("bob"),
		"user:102:meta":    []byte("metadata"),
		"group:99:profile": []byte("admin_group"),
		"other:key":        []byte("other"),
	}

	resetStorage := func() {
		_ = s.Reset()
		for k, v := range data {
			_ = s.Set(k, v, storage.NewPayloadMetadata(time.Now(), nil))
		}
	}

	// 1. DeleteByPrefix
	t.Run("DeleteByPrefix", func(t *testing.T) {
		resetStorage()
		deleted := s.DeleteByPrefix("user:")
		if deleted != 3 {
			t.Errorf("DeleteByPrefix deleted %d; want 3", deleted)
		}
		if s.Exists("user:100:profile") || s.Exists("user:102:meta") {
			t.Error("keys with prefix user: should be deleted")
		}
		if !s.Exists("group:99:profile") {
			t.Error("group:99:profile should not be deleted")
		}
	})

	// 2. DeleteBySuffix
	t.Run("DeleteBySuffix", func(t *testing.T) {
		resetStorage()
		deleted := s.DeleteBySuffix(":profile")
		if deleted != 3 {
			t.Errorf("DeleteBySuffix deleted %d; want 3", deleted)
		}
		if s.Exists("user:100:profile") || s.Exists("group:99:profile") {
			t.Error("keys with suffix :profile should be deleted")
		}
		if !s.Exists("user:102:meta") {
			t.Error("user:102:meta should not be deleted")
		}
	})

	// 3. DeleteByRegex
	t.Run("DeleteByRegex", func(t *testing.T) {
		resetStorage()
		deleted, err := s.DeleteByRegex(`^user:\d+:profile$`)
		if err != nil {
			t.Fatalf("DeleteByRegex failed: %v", err)
		}
		if deleted != 2 {
			t.Errorf("DeleteByRegex deleted %d; want 2", deleted)
		}
		if s.Exists("user:100:profile") || s.Exists("user:101:profile") {
			t.Error("matching regex keys should be deleted")
		}
		if !s.Exists("user:102:meta") || !s.Exists("group:99:profile") {
			t.Error("non-matching keys should not be deleted")
		}

		_, err = s.DeleteByRegex(`[invalid`)
		if err == nil {
			t.Fatal("expected error for invalid regex, got nil")
		}
	})
}

func TestStorage_PatternTTLBehavior(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)

	// Set one persistent key and one expired key
	_ = s.Set("user:active", []byte("active"), storage.NewPayloadMetadata(time.Now(), nil))
	
	past := time.Now().Add(-1 * time.Hour)
	_ = s.Set("user:expired", []byte("expired"), storage.NewPayloadMetadata(time.Now(), &past))

	// Verify that the expired key is not counted
	count := s.CountByPrefix("user:")
	if count != 1 {
		t.Errorf("CountByPrefix returned %d; want 1 (expired key should be ignored), raw keys count: %d", count, len(s.Data()))
	}

	// Verify that the expired key is not found
	found := s.FindByPrefix("user:")
	if len(found) != 1 {
		t.Errorf("FindByPrefix returned %d items; want 1", len(found))
	}
	if _, exists := found["user:expired"]; exists {
		t.Error("user:expired should not be returned by FindByPrefix")
	}

	// Verify that deleting ignores or doesn't count expired keys (or cleans them up)
	deleted := s.DeleteByPrefix("user:")
	if deleted != 1 {
		t.Errorf("DeleteByPrefix deleted %d; want 1 (expired key should not count as deleted)", deleted)
	}

	// Verify that "DELETE|user:expired\n" was written to the WAL
	var foundExpiredDelete bool
	for _, w := range mockWal.Writes() {
		if w == "DELETE|user:expired\n" {
			foundExpiredDelete = true
			break
		}
	}
	if !foundExpiredDelete {
		t.Error("expected DELETE|user:expired\\n to be logged to WAL, but it wasn't")
	}
}

func TestStorage_PatternWALRecovery(t *testing.T) {
	tempFile, err := os.CreateTemp("", "pattern_wal_test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	w, err := wal.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	s1 := storage.NewStorage(w)
	_ = s1.Set("user:1", []byte("val1"), storage.NewPayloadMetadata(time.Now(), nil))
	_ = s1.Set("user:2", []byte("val2"), storage.NewPayloadMetadata(time.Now(), nil))
	_ = s1.Set("other:1", []byte("val3"), storage.NewPayloadMetadata(time.Now(), nil))

	// Delete by pattern
	s1.DeleteByPrefix("user:")

	w.CloseWal()

	// Reopen WAL and check recovery
	w2, err := wal.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.CloseWal()

	s2 := storage.NewStorage(w2)
	if err := s2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// If WAL was written correctly, user:1 and user:2 should not exist.
	if s2.Exists("user:1") {
		t.Errorf("user:1 should be deleted in recovered storage")
	}
	if s2.Exists("user:2") {
		t.Errorf("user:2 should be deleted in recovered storage")
	}
	if !s2.Exists("other:1") {
		t.Errorf("other:1 should still exist in recovered storage")
	}
}
