package tests

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"os"
	"testing"
	"time"
)

func TestSnapshot_SaveLoad(t *testing.T) {
	s := snapshot.NewSnapshot("test.snap")
	mockWal := &MockWal{}
	engine := storage.NewStorage(mockWal)

	now := time.Now().Truncate(time.Second)
	engine.Set("key1", []byte("value1"), storage.NewPayloadMetadata(now, nil))
	engine.SetWithTTL("key2", []byte("value2"), time.Hour)

	var buf bytes.Buffer
	sections := map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error {
			return engine.SerializeState(w)
		},
	}
	if err := s.SaveSnapshot(&buf, sections); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	loadedEngine := storage.NewStorage(mockWal)
	err := s.LoadSnapshot(&buf, func(engineID string, r io.Reader) error {
		if engineID == "kv" {
			return loadedEngine.DeserializeState(r)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if !loadedEngine.Exists("key1") || !loadedEngine.Exists("key2") {
		t.Errorf("keys missing in loaded storage")
	}

	v1, _ := loadedEngine.Get("key1")
	if !bytes.Equal(v1, []byte("value1")) {
		t.Errorf("got key1 %s, want value1", string(v1))
	}
}

func TestSnapshot_InvalidHeader(t *testing.T) {
	s := snapshot.NewSnapshot("test.snap")

	t.Run("InvalidMagic", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte("WRNG"))            // Wrong magic (4 bytes)
		binary.Write(buf, binary.LittleEndian, uint16(1)) // Valid version
		binary.Write(buf, binary.LittleEndian, uint64(0)) // Count 0
		err := s.LoadSnapshot(buf, func(engineID string, r io.Reader) error { return nil })
		if err == nil {
			t.Fatal("expected error for invalid magic, got nil")
		}
		if !errors.Is(err, pkgerrors.ErrInvalidSnapshotMagic) {
			t.Errorf("expected ErrInvalidSnapshotMagic, got %v", err)
		}
	})

	t.Run("InvalidVersion", func(t *testing.T) {
		var buf bytes.Buffer
		buf.Write(snapshot.FirstBytes[:])
		binary.Write(&buf, binary.LittleEndian, uint16(999)) // Wrong version
		binary.Write(&buf, binary.LittleEndian, uint64(0))   // Count 0

		err := s.LoadSnapshot(&buf, func(engineID string, r io.Reader) error { return nil })
		if err == nil {
			t.Fatal("expected error for invalid version, got nil")
		}
		if !errors.Is(err, pkgerrors.ErrInvalidSnapshotVersion) {
			t.Errorf("expected ErrInvalidSnapshotVersion, got %v", err)
		}
	})
}

func TestSnapshot_EmptyStorage(t *testing.T) {
	s := snapshot.NewSnapshot("empty.snap")
	sections := make(map[string]func(w io.Writer) error)

	var buf bytes.Buffer
	if err := s.SaveSnapshot(&buf, sections); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	err := s.LoadSnapshot(&buf, func(engineID string, r io.Reader) error { return nil })
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}
}

func TestSnapshot_LargeStorage(t *testing.T) {
	s := snapshot.NewSnapshot("large.snap")
	mockWal := &MockWal{}
	engine := storage.NewStorage(mockWal)
	count := 1000

	for i := range count {
		key := fmt.Sprintf("key_%d", i)
		engine.Set(key, []byte(fmt.Sprintf("value_%d", i)), storage.NewPayloadMetadata(time.Now(), nil))
	}

	var buf bytes.Buffer
	sections := map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error {
			return engine.SerializeState(w)
		},
	}
	if err := s.SaveSnapshot(&buf, sections); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	loadedEngine := storage.NewStorage(mockWal)
	err := s.LoadSnapshot(&buf, func(engineID string, r io.Reader) error {
		if engineID == "kv" {
			return loadedEngine.DeserializeState(r)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if len(loadedEngine.Data()) != count {
		t.Errorf("expected %d items, got %d", count, len(loadedEngine.Data()))
	}
}

func TestSnapshot_TruncatedFile(t *testing.T) {
	s := snapshot.NewSnapshot("truncated.snap")
	mockWal := &MockWal{}
	engine := storage.NewStorage(mockWal)
	engine.Set("key1", []byte("v1"), storage.NewPayloadMetadata(time.Now(), nil))

	var buf bytes.Buffer
	sections := map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error {
			return engine.SerializeState(w)
		},
	}
	_ = s.SaveSnapshot(&buf, sections)
	raw := buf.Bytes()

	// Test truncation at different points
	for i := range len(raw) - 1 {
		t.Run(fmt.Sprintf("TruncatedAt_%d", i), func(t *testing.T) {
			reader := bytes.NewReader(raw[:i])
			err := s.LoadSnapshot(reader, func(engineID string, r io.Reader) error {
				return nil
			})
			if err == nil {
				t.Error("expected error for truncated file, got nil")
			}
			if !errors.Is(err, pkgerrors.ErrSnapshotReadFailed) {
				t.Errorf("expected ErrSnapshotReadFailed, got %v", err)
			}
		})
	}
}

func TestSnapshot_FileAtomicSave(t *testing.T) {
	path := "test_atomic.snap"
	defer os.Remove(path)
	defer os.Remove(path + ".tmp")

	s := snapshot.NewSnapshot(path)
	mockWal := &MockWal{}
	engine := storage.NewStorage(mockWal)
	engine.Set("key1", []byte("value1"), storage.NewPayloadMetadata(time.Now().Truncate(time.Second), nil))

	sections := map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error {
			return engine.SerializeState(w)
		},
	}
	if err := s.Save(sections); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("snapshot file was not created")
	}

	// Verify content
	loadedEngine := storage.NewStorage(mockWal)
	err := s.Load(func(engineID string, r io.Reader) error {
		if engineID == "kv" {
			return loadedEngine.DeserializeState(r)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	v, err := loadedEngine.Get("key1")
	if err != nil || !bytes.Equal(v, []byte("value1")) {
		t.Errorf("loaded data mismatch")
	}
}

func TestSnapshot_Consistency(t *testing.T) {
	s := snapshot.NewSnapshot("consistent.snap")
	mockWal := &MockWal{}
	engine := storage.NewStorage(mockWal)
	engine.Set("k1", []byte("v1"), storage.NewPayloadMetadata(time.Now().Truncate(time.Second), nil))

	var buf1 bytes.Buffer
	sections := map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error {
			return engine.SerializeState(w)
		},
	}
	if err := s.SaveSnapshot(&buf1, sections); err != nil {
		t.Fatalf("first SaveSnapshot failed: %v", err)
	}

	// Load and re-save
	loadedEngine := storage.NewStorage(mockWal)
	reader1 := bytes.NewReader(buf1.Bytes())
	err := s.LoadSnapshot(reader1, func(engineID string, r io.Reader) error {
		if engineID == "kv" {
			return loadedEngine.DeserializeState(r)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	var buf2 bytes.Buffer
	sections2 := map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error {
			return loadedEngine.SerializeState(w)
		},
	}
	if err := s.SaveSnapshot(&buf2, sections2); err != nil {
		t.Fatalf("second SaveSnapshot failed: %v", err)
	}

	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Errorf("binary output is not consistent between saves")
	}
}

func TestSnapshot_ExtremeValues(t *testing.T) {
	s := snapshot.NewSnapshot("extreme.snap")
	largeVal := make([]byte, 10*1024*1024) // 10MB
	for i := range largeVal {
		largeVal[i] = 'A'
	}

	mockWal := &MockWal{}
	engine := storage.NewStorage(mockWal)
	engine.Set("large", largeVal, storage.NewPayloadMetadata(time.Now(), nil))
	engine.Set("", []byte(""), storage.NewPayloadMetadata(time.Now(), nil))

	var buf bytes.Buffer
	sections := map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error {
			return engine.SerializeState(w)
		},
	}
	if err := s.SaveSnapshot(&buf, sections); err != nil {
		t.Fatalf("SaveSnapshot failed with extreme values: %v", err)
	}

	loadedEngine := storage.NewStorage(mockWal)
	err := s.LoadSnapshot(&buf, func(engineID string, r io.Reader) error {
		if engineID == "kv" {
			return loadedEngine.DeserializeState(r)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("LoadSnapshot failed with extreme values: %v", err)
	}

	v1, _ := loadedEngine.Get("large")
	if !bytes.Equal(v1, largeVal) {
		t.Error("large value mismatch")
	}
	v2, _ := loadedEngine.Get("")
	if len(v2) != 0 {
		t.Error("empty key/value mismatch")
	}
}

func TestSnapshot_FilePermissions(t *testing.T) {
	path := "readonly.snap"
	f, err := os.Create(path)
	if err != nil {
		t.Skip("could not create file for permission test")
	}
	f.Close()

	err = os.Chmod(path, 0444) // Read-only
	if err != nil {
		t.Skip("could not chmod file for permission test")
	}
	defer os.Remove(path)

	s := snapshot.NewSnapshot(path)
	mockWal := &MockWal{}
	engine := storage.NewStorage(mockWal)
	engine.Set("k", []byte("v"), storage.NewPayloadMetadata(time.Now(), nil))

	sections := map[string]func(w io.Writer) error{
		"kv": func(w io.Writer) error {
			return engine.SerializeState(w)
		},
	}
	err = s.Save(sections)
	if err == nil {
		t.Error("expected error when saving to read-only path, got nil")
	}

	var snapErr pkgerrors.SnapshotError
	if !errors.As(err, &snapErr) {
		t.Errorf("expected pkgerrors.SnapshotError, got %T", err)
	}
}
