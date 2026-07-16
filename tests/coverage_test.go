package tests

import (
	"container/heap"
	"errors"
	"io"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
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
			pm := storage.NewPayloadMetadata(now, tt.expiresAt)
			if got := pm.IsExpired(); got != tt.want {
				t.Errorf("IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpirationHeap_Manual(t *testing.T) {
	h := &storage.ExpirationHeap{}
	heap.Init(h)

	t1 := time.Now().Add(10 * time.Minute)
	t2 := time.Now().Add(5 * time.Minute)
	t3 := time.Now().Add(15 * time.Minute)

	heap.Push(h, storage.NewExpirationEntry("k1", t1))
	heap.Push(h, storage.NewExpirationEntry("k2", t2))
	heap.Push(h, storage.NewExpirationEntry("k3", t3))

	if h.Len() != 3 {
		t.Errorf("expected len 3, got %d", h.Len())
	}

	// Should pop k2 first (earliest)
	e := heap.Pop(h).(*storage.ExpirationEntry)
	if e.Key() != "k2" {
		t.Errorf("expected k2, got %s", e.Key())
	}

	// Should pop k1 next
	e = heap.Pop(h).(*storage.ExpirationEntry)
	if e.Key() != "k1" {
		t.Errorf("expected k1, got %s", e.Key())
	}

	// Should pop k3 last
	e = heap.Pop(h).(*storage.ExpirationEntry)
	if e.Key() != "k3" {
		t.Errorf("expected k3, got %s", e.Key())
	}
}

func TestStorage_LoadFromSnapshot_NotExists(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)
	snap := snapshot.NewSnapshot("non_existent_snapshot.snap")

	// Should not return error if file doesn't exist
	err := loadStorageWithSnapshot(mockWal, &snap, s)
	if err != nil {
		t.Errorf("expected nil error for missing snapshot, got %v", err)
	}
}

func TestStorage_Exists_TriggersDelete(t *testing.T) {
	mockWal := &MockWal{}
	s := storage.NewStorage(mockWal)

	past := time.Now().Add(-time.Hour)
	err := s.Set("expired", []byte("val"), storage.NewPayloadMetadata(time.Now(), &past))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Exists should return false and trigger delete
	if s.Exists("expired") {
		t.Error("Exists should return false for expired key")
	}

	// Verify it's really gone from storage map
	if _, ok := s.Data()["expired"]; ok {
		t.Error("key should have been deleted from storage")
	}
}

func TestWal_TruncateError(t *testing.T) {
	// Truncate non-existent file or a directory
	path := "a_directory_that_is_not_a_file"
	_ = os.Mkdir(path, 0755)
	defer os.Remove(path)

	w, err := wal.NewWal(path)
	if err != nil {
		return // Success if NewWal fails
	}
	defer w.CloseWal()

	err = w.TruncateWal()
	if err == nil {
		t.Error("expected error when truncating a directory, got nil")
	}
}

func TestWal_TruncateSuccess(t *testing.T) {
	tempFile, err := os.CreateTemp("", "truncate_test")
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

	_, _ = w.WriteToWal("something\n")

	err = w.TruncateWal()
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
	s := snapshot.NewSnapshot("??invalid??")
	err := s.Save(map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error { return nil },
	})
	if err == nil {
		t.Error("expected error when saving to invalid path, got nil")
	}

	var snapErr pkgerrors.SnapshotError
	if !errors.As(err, &snapErr) {
		t.Errorf("expected SnapshotError, got %T", err)
	}
}

func TestErrors_FormattingAndUnwrap(t *testing.T) {
	baseErr := errors.New("base")

	t.Run("KeyError", func(t *testing.T) {
		ke := pkgerrors.KeyError{Key: "k", Err: baseErr}
		expected := "key error: key \"k\": base"
		if ke.Error() != expected {
			t.Errorf("expected %s, got %s", expected, ke.Error())
		}
		if errors.Unwrap(ke) != baseErr {
			t.Error("failed to unwrap KeyError")
		}
		// Test extra rich fields
		keRich := pkgerrors.KeyError{Key: "k", Err: pkgerrors.KeyNotFoundError, Operation: "GET"}
		expectedRich := "key error: key \"k\": during GET: key was not found"
		if keRich.Error() != expectedRich {
			t.Errorf("expected %s, got %s", expectedRich, keRich.Error())
		}
		if keRich.ErrorCode() != pkgerrors.CodeKeyNotFound {
			t.Errorf("expected code %s, got %s", pkgerrors.CodeKeyNotFound, keRich.ErrorCode())
		}
		if keRich.HTTPStatus() != 404 {
			t.Errorf("expected HTTP status 404, got %d", keRich.HTTPStatus())
		}
	})

	t.Run("WalError", func(t *testing.T) {
		we := pkgerrors.WalError{Path: "p", Err: baseErr}
		expected := "wal error at path \"p\": base"
		if we.Error() != expected {
			t.Errorf("expected %s, got %s", expected, we.Error())
		}
		if errors.Unwrap(we) != baseErr {
			t.Error("failed to unwrap WalError")
		}
		if we.ErrorCode() != pkgerrors.CodeWalError {
			t.Errorf("expected code %s, got %s", pkgerrors.CodeWalError, we.ErrorCode())
		}
		if we.HTTPStatus() != 500 {
			t.Errorf("expected HTTP status 500, got %d", we.HTTPStatus())
		}
	})

	t.Run("SnapshotError", func(t *testing.T) {
		se := pkgerrors.SnapshotError{Path: "p", Err: baseErr}
		expected := "snapshot error at path \"p\": base"
		if se.Error() != expected {
			t.Errorf("expected %s, got %s", expected, se.Error())
		}
		if errors.Unwrap(se) != baseErr {
			t.Error("failed to unwrap SnapshotError")
		}
		if se.ErrorCode() != pkgerrors.CodeSnapshotError {
			t.Errorf("expected code %s, got %s", pkgerrors.CodeSnapshotError, se.ErrorCode())
		}
		if se.HTTPStatus() != 500 {
			t.Errorf("expected HTTP status 500, got %d", se.HTTPStatus())
		}

		seEmpty := pkgerrors.SnapshotError{Err: baseErr}
		expectedEmpty := "snapshot error: base"
		if seEmpty.Error() != expectedEmpty {
			t.Errorf("expected %s, got %s", expectedEmpty, seEmpty.Error())
		}
	})
}

func TestStorage_WalErrorPropagation(t *testing.T) {
	mockWal := &MockWal{}
	mockWal.SetFailWrite(true)
	s := storage.NewStorage(mockWal)

	err := s.Set("k", []byte("v"), storage.NewPayloadMetadata(time.Now(), nil))
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
	s := storage.NewStorage(mockWal)

	now := time.Now()
	past1 := now.Add(-10 * time.Minute)
	past2 := now.Add(-5 * time.Minute)
	future := now.Add(10 * time.Minute)

	_ = s.Set("p1", []byte("v1"), storage.NewPayloadMetadata(now, &past1))
	_ = s.Set("p2", []byte("v2"), storage.NewPayloadMetadata(now, &past2))
	_ = s.Set("f", []byte("vf"), storage.NewPayloadMetadata(now, &future))

	if s.Expirations().Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", s.Expirations().Len())
	}

	s.CleanupExpired()

	if s.Expirations().Len() != 1 {
		t.Errorf("expected 1 entry left, got %d", s.Expirations().Len())
	}

	if s.Exists("p1") || s.Exists("p2") {
		t.Error("expired keys still exist")
	}
	if !s.Exists("f") {
		t.Error("future key was deleted")
	}
}

func TestStorage_SaveSnapshot_TruncateFail(t *testing.T) {
	mockWal := &MockWal{}
	mockWal.SetFailTruncate(true)
	snapPath := "test_save_truncate_fail.snap"
	defer os.Remove(snapPath)
	snap := snapshot.NewSnapshot(snapPath)
	pm := persistence.NewPersistenceManager(mockWal, &snap)
	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)

	err := pm.SaveSnapshot()
	if err == nil {
		t.Error("expected error when WAL truncate fails, got nil")
	}
	if !errors.Is(err, ErrMockTruncateFailed) {
		t.Errorf("expected ErrMockTruncateFailed, got %v", err)
	}
}
