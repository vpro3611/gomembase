package tests

import (
	"container/heap"
	"errors"
	"github.com/vpro3611/gomembase.git/core"
	"os"
	"testing"
	"time"
)

func TestPayload_IsExpired(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	tests := []struct {
		name      string
		expiresAt *time.Time
		want      bool
	}{
		{"NoExpiration", nil, false},
		{"FutureExpiration", &future, false},
		{"PastExpiration", &past, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := core.PayloadMetadata{ExpiresAt: tt.expiresAt}
			if got := pm.IsExpired(); got != tt.want {
				t.Errorf("IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpirationHeap_Manual(t *testing.T) {
	h := &core.ExpirationHeap{}
	heap.Init(h)

	t1 := time.Now().Add(10 * time.Minute)
	t2 := time.Now().Add(5 * time.Minute)
	t3 := time.Now().Add(15 * time.Minute)

	heap.Push(h, &core.ExpirationEntry{Key: "k1", ExpiresAt: t1})
	heap.Push(h, &core.ExpirationEntry{Key: "k2", ExpiresAt: t2})
	heap.Push(h, &core.ExpirationEntry{Key: "k3", ExpiresAt: t3})

	if h.Len() != 3 {
		t.Errorf("expected len 3, got %d", h.Len())
	}

	// Should pop k2 first (earliest)
	e := heap.Pop(h).(*core.ExpirationEntry)
	if e.Key != "k2" {
		t.Errorf("expected k2, got %s", e.Key)
	}

	// Should pop k1 next
	e = heap.Pop(h).(*core.ExpirationEntry)
	if e.Key != "k1" {
		t.Errorf("expected k1, got %s", e.Key)
	}

	// Should pop k3 last
	e = heap.Pop(h).(*core.ExpirationEntry)
	if e.Key != "k3" {
		t.Errorf("expected k3, got %s", e.Key)
	}
}

func TestStorage_LoadFromSnapshot_NotExists(t *testing.T) {
	mockWal := &MockWal{}
	s := core.NewStorage(mockWal)
	snap := core.NewSnapshot("non_existent_snapshot.snap")

	// Should not return error if file doesn't exist
	err := s.LoadFromSnapshot(snap)
	if err != nil {
		t.Errorf("expected nil error for missing snapshot, got %v", err)
	}
}

func TestStorage_Exists_TriggersDelete(t *testing.T) {
	mockWal := &MockWal{}
	s := core.NewStorage(mockWal)

	past := time.Now().Add(-time.Hour)
	err := s.Set("expired", []byte("val"), core.PayloadMetadata{ExpiresAt: &past})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Exists should return false and trigger delete
	if s.Exists("expired") {
		t.Error("Exists should return false for expired key")
	}

	// Verify it's really gone from storage map
	if _, ok := s.Storage["expired"]; ok {
		t.Error("key should have been deleted from storage")
	}
}

func TestWal_TruncateError(t *testing.T) {
	// Truncate non-existent file or a directory
	wal := &core.Wal{Path: "a_directory_that_is_not_a_file"}
	_ = os.Mkdir(wal.Path, 0755)
	defer os.Remove(wal.Path)

	err := wal.TruncateWal()
	if err == nil {
		t.Error("expected error when truncating a directory, got nil")
	}
	var walErr core.WalError
	if !errors.As(err, &walErr) {
		t.Errorf("expected WalError, got %T", err)
	}
}

func TestWal_TruncateSuccess(t *testing.T) {
	tempFile, err := os.CreateTemp("", "truncate_test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	wal, err := core.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.CloseWal()

	_, _ = wal.WriteToWal("something\n")

	err = wal.TruncateWal()
	if err != nil {
		t.Fatalf("TruncateWal failed: %v", err)
	}

	fi, err := os.Stat(tempFile.Name())
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if fi.Size() != 0 {
		t.Errorf("expected size 0, got %d", fi.Size())
	}
}

func TestSnapshot_SaveErrorPaths(t *testing.T) {
	// Use an invalid path to trigger failure (reserved name on Windows or invalid characters)
	s := core.NewSnapshot("??invalid??")
	err := s.Save(map[string]core.Payload{"k": {Value: []byte("v")}})
	if err == nil {
		t.Error("expected error when saving to invalid path, got nil")
	}

	var snapErr core.SnapshotError
	if !errors.As(err, &snapErr) {
		t.Errorf("expected SnapshotError, got %T", err)
	}
}

func TestErrors_FormattingAndUnwrap(t *testing.T) {
	baseErr := errors.New("base")

	t.Run("KeyError", func(t *testing.T) {
		ke := core.KeyError{Key: "k", Err: baseErr}
		expected := "key k: base"
		if ke.Error() != expected {
			t.Errorf("expected %s, got %s", expected, ke.Error())
		}
		if errors.Unwrap(ke) != baseErr {
			t.Error("failed to unwrap KeyError")
		}
	})

	t.Run("WalError", func(t *testing.T) {
		we := core.WalError{Path: "p", Err: baseErr}
		expected := "wal error (p) : base"
		if we.Error() != expected {
			t.Errorf("expected %s, got %s", expected, we.Error())
		}
		if errors.Unwrap(we) != baseErr {
			t.Error("failed to unwrap WalError")
		}
	})

	t.Run("SnapshotError", func(t *testing.T) {
		se := core.SnapshotError{Path: "p", Err: baseErr}
		expected := "snapshot error (p): base"
		if se.Error() != expected {
			t.Errorf("expected %s, got %s", expected, se.Error())
		}
		if errors.Unwrap(se) != baseErr {
			t.Error("failed to unwrap SnapshotError")
		}

		seEmpty := core.SnapshotError{Err: baseErr}
		expectedEmpty := "snapshot error: base"
		if seEmpty.Error() != expectedEmpty {
			t.Errorf("expected %s, got %s", expectedEmpty, seEmpty.Error())
		}
	})
}

func TestStorage_WalErrorPropagation(t *testing.T) {
	mockWal := &MockWal{FailWrite: true}
	s := core.NewStorage(mockWal)

	err := s.Set("k", []byte("v"), core.PayloadMetadata{})
	if err == nil {
		t.Error("expected error when WAL write fails, got nil")
	}

	err = s.Delete("k")
	if err == nil {
		t.Error("expected error when WAL delete fails, got nil")
	}
}

func TestStorage_CleanupExpired_Multiple(t *testing.T) {
	mockWal := &MockWal{}
	s := core.NewStorage(mockWal)

	past1 := time.Now().Add(-10 * time.Minute)
	past2 := time.Now().Add(-5 * time.Minute)
	future := time.Now().Add(10 * time.Minute)

	_ = s.Set("p1", []byte("v1"), core.PayloadMetadata{ExpiresAt: &past1})
	_ = s.Set("p2", []byte("v2"), core.PayloadMetadata{ExpiresAt: &past2})
	_ = s.Set("f", []byte("vf"), core.PayloadMetadata{ExpiresAt: &future})

	if s.Expirations.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", s.Expirations.Len())
	}

	s.CleanupExpired()

	if s.Expirations.Len() != 1 {
		t.Errorf("expected 1 entry left, got %d", s.Expirations.Len())
	}

	if s.Exists("p1") || s.Exists("p2") {
		t.Error("expired keys still exist")
	}
	if !s.Exists("f") {
		t.Error("future key was deleted")
	}
}

func TestStorage_SaveSnapshot_TruncateFail(t *testing.T) {
	mockWal := &MockWal{FailTruncate: true}
	s := core.NewStorage(mockWal)
	snapPath := "test_save_truncate_fail.snap"
	defer os.Remove(snapPath)
	snap := core.NewSnapshot(snapPath)

	err := s.SaveSnapshot(snap)
	if err == nil {
		t.Error("expected error when WAL truncate fails, got nil")
	}
	if err.Error() != "wal truncate failed" {
		t.Errorf("expected 'wal truncate failed', got %v", err)
	}
}
