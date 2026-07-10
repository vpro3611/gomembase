package tests

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/vpro3611/gomembase.git/core"
	"os"
	"testing"
	"time"
)

func TestSnapshot_SaveLoad(t *testing.T) {
	s := core.NewSnapshot("test.snap")
	storage := map[string]core.Payload{
		"key1": {
			Value: []byte("value1"),
			Metadata: core.PayloadMetadata{
				CreatedAt: time.Now().Truncate(time.Second),
				ExpiresAt: nil,
			},
		},
		"key2": {
			Value: []byte("value2"),
			Metadata: core.PayloadMetadata{
				CreatedAt: time.Now().Truncate(time.Second),
				ExpiresAt: func() *time.Time { t := time.Now().Add(time.Hour).Truncate(time.Second); return &t }(),
			},
		},
	}

	var buf bytes.Buffer
	if err := s.SaveSnapshot(&buf, storage); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	loadedStorage, err := s.LoadSnapshot(&buf)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if len(loadedStorage) != len(storage) {
		t.Errorf("got %d keys, want %d", len(loadedStorage), len(storage))
	}

	for k, expected := range storage {
		got, ok := loadedStorage[k]
		if !ok {
			t.Errorf("key %s missing in loaded storage", k)
			continue
		}
		if !bytes.Equal(got.Value, expected.Value) {
			t.Errorf("key %s: got value %s, want %s", k, string(got.Value), string(expected.Value))
		}
		if !got.Metadata.CreatedAt.Equal(expected.Metadata.CreatedAt) {
			t.Errorf("key %s: got CreatedAt %v, want %v", k, got.Metadata.CreatedAt, expected.Metadata.CreatedAt)
		}
		if (got.Metadata.ExpiresAt == nil) != (expected.Metadata.ExpiresAt == nil) {
			t.Errorf("key %s: got ExpiresAt nil status %v, want %v", k, got.Metadata.ExpiresAt == nil, expected.Metadata.ExpiresAt == nil)
		} else if got.Metadata.ExpiresAt != nil && !got.Metadata.ExpiresAt.Equal(*expected.Metadata.ExpiresAt) {
			t.Errorf("key %s: got ExpiresAt %v, want %v", k, *got.Metadata.ExpiresAt, *expected.Metadata.ExpiresAt)
		}
	}
}

func TestSnapshot_InvalidHeader(t *testing.T) {
	s := core.NewSnapshot("test.snap")

	t.Run("InvalidMagic", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte("WRNG"))            // Wrong magic (4 bytes)
		binary.Write(buf, binary.LittleEndian, uint16(1)) // Valid version
		binary.Write(buf, binary.LittleEndian, uint64(0)) // Count 0
		_, err := s.LoadSnapshot(buf)
		if err == nil {
			t.Fatal("expected error for invalid magic, got nil")
		}
		if !errors.Is(err, core.ErrInvalidSnapshotMagic) {
			t.Errorf("expected ErrInvalidSnapshotMagic, got %v", err)
		}
	})

	t.Run("InvalidVersion", func(t *testing.T) {
		var buf bytes.Buffer
		buf.Write(core.FirstBytes[:])
		binary.Write(&buf, binary.LittleEndian, uint16(999)) // Wrong version
		binary.Write(&buf, binary.LittleEndian, uint64(0))   // Count 0

		_, err := s.LoadSnapshot(&buf)
		if err == nil {
			t.Fatal("expected error for invalid version, got nil")
		}
		if !errors.Is(err, core.ErrInvalidSnapshotVersion) {
			t.Errorf("expected ErrInvalidSnapshotVersion, got %v", err)
		}
	})
}

func TestSnapshot_EmptyStorage(t *testing.T) {
	s := core.NewSnapshot("empty.snap")
	storage := make(map[string]core.Payload)

	var buf bytes.Buffer
	if err := s.SaveSnapshot(&buf, storage); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	loaded, err := s.LoadSnapshot(&buf)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if len(loaded) != 0 {
		t.Errorf("expected empty storage, got %d items", len(loaded))
	}
}

func TestSnapshot_LargeStorage(t *testing.T) {
	s := core.NewSnapshot("large.snap")
	storage := make(map[string]core.Payload)
	count := 1000

	for i := range count {
		key := "key_" + string(rune(i))
		storage[key] = core.Payload{
			Value: []byte("value_" + string(rune(i))),
			Metadata: core.PayloadMetadata{
				CreatedAt: time.Now(),
			},
		}
	}

	var buf bytes.Buffer
	if err := s.SaveSnapshot(&buf, storage); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	loaded, err := s.LoadSnapshot(&buf)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if len(loaded) != count {
		t.Errorf("expected %d items, got %d", count, len(loaded))
	}
}

func TestSnapshot_TruncatedFile(t *testing.T) {
	s := core.NewSnapshot("truncated.snap")
	storage := map[string]core.Payload{
		"key1": {Value: []byte("v1"), Metadata: core.PayloadMetadata{CreatedAt: time.Now()}},
	}

	var buf bytes.Buffer
	_ = s.SaveSnapshot(&buf, storage)
	data := buf.Bytes()

	// Test truncation at different points
	for i := range len(data) - 1 {
		t.Run("TruncatedAt_"+string(rune(i)), func(t *testing.T) {
			reader := bytes.NewReader(data[:i])
			_, err := s.LoadSnapshot(reader)
			if err == nil {
				t.Error("expected error for truncated file, got nil")
			}
			if !errors.Is(err, core.ErrSnapshotReadFailed) {
				t.Errorf("expected ErrSnapshotReadFailed, got %v", err)
			}
		})
	}
}

func TestSnapshot_FileAtomicSave(t *testing.T) {
	path := "test_atomic.snap"
	defer os.Remove(path)
	defer os.Remove(path + ".tmp")

	s := core.NewSnapshot(path)
	storage := map[string]core.Payload{
		"key1": {Value: []byte("value1"), Metadata: core.PayloadMetadata{CreatedAt: time.Now().Truncate(time.Second)}},
	}

	if err := s.Save(storage); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("snapshot file was not created")
	}

	// Verify content
	loaded, err := s.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(loaded) != 1 || !bytes.Equal(loaded["key1"].Value, storage["key1"].Value) {
		t.Errorf("loaded data mismatch")
	}
}

func TestSnapshot_Consistency(t *testing.T) {
	s := core.NewSnapshot("consistent.snap")
	storage := map[string]core.Payload{
		"k1": {Value: []byte("v1"), Metadata: core.PayloadMetadata{CreatedAt: time.Now().Truncate(time.Second)}},
	}

	// First Save
	var buf1 bytes.Buffer
	if err := s.SaveSnapshot(&buf1, storage); err != nil {
		t.Fatalf("first SaveSnapshot failed: %v", err)
	}

	// Load
	reader1 := bytes.NewReader(buf1.Bytes())
	loaded1, err := s.LoadSnapshot(reader1)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	// Second Save (from loaded)
	var buf2 bytes.Buffer
	if err := s.SaveSnapshot(&buf2, loaded1); err != nil {
		t.Fatalf("second SaveSnapshot failed: %v", err)
	}

	// Maps are unordered, but with 1 element it SHOULD be consistent.
	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Errorf("binary output is not consistent between saves\nbuf1: %x\nbuf2: %x\nlen1: %d, len2: %d", buf1.Bytes(), buf2.Bytes(), len(buf1.Bytes()), len(buf2.Bytes()))
	}
}

func TestSnapshot_ExtremeValues(t *testing.T) {
	s := core.NewSnapshot("extreme.snap")
	largeVal := make([]byte, 10*1024*1024) // 10MB
	for i := range largeVal {
		largeVal[i] = 'A'
	}

	storage := map[string]core.Payload{
		"large": {Value: largeVal, Metadata: core.PayloadMetadata{CreatedAt: time.Now()}},
		"":      {Value: []byte(""), Metadata: core.PayloadMetadata{CreatedAt: time.Now()}}, // Empty key and value
	}

	var buf bytes.Buffer
	if err := s.SaveSnapshot(&buf, storage); err != nil {
		t.Fatalf("SaveSnapshot failed with extreme values: %v", err)
	}

	loaded, err := s.LoadSnapshot(&buf)
	if err != nil {
		t.Fatalf("LoadSnapshot failed with extreme values: %v", err)
	}

	if !bytes.Equal(loaded["large"].Value, largeVal) {
		t.Error("large value mismatch")
	}
	if v, ok := loaded[""]; !ok || len(v.Value) != 0 {
		t.Error("empty key/value mismatch")
	}
}

func TestSnapshot_CorruptPayload(t *testing.T) {
	s := core.NewSnapshot("corrupt.snap")
	storage := map[string]core.Payload{
		"k1": {Value: []byte("v1"), Metadata: core.PayloadMetadata{CreatedAt: time.Now()}},
	}

	var buf bytes.Buffer
	_ = s.SaveSnapshot(&buf, storage)
	data := buf.Bytes()

	// Corrupt the 'count' in header to be much larger than reality
	// Header: Magic(4) + Version(2) + Count(8)
	// Count starts at index 6
	corruptedCount := make([]byte, len(data))
	copy(corruptedCount, data)
	binary.LittleEndian.PutUint64(corruptedCount[6:14], 9999)

	t.Run("CountTooLarge", func(t *testing.T) {
		reader := bytes.NewReader(corruptedCount)
		_, err := s.LoadSnapshot(reader)
		if err == nil {
			t.Error("expected error for count > actual data, got nil")
		}
	})

	// Corrupt a string length to be huge (memory exhaustion test)
	// After header (14 bytes), first entry starts: KeyLen(4) + Key + ValLen(4) + Val + CreatedAt(8) + HasExpiry(1)
	corruptedLen := make([]byte, len(data))
	copy(corruptedLen, data)
	binary.LittleEndian.PutUint32(corruptedLen[14:18], 0xFFFFFFFF) // Max uint32 key length

	t.Run("InvalidStringLength", func(t *testing.T) {
		reader := bytes.NewReader(corruptedLen)
		_, err := s.LoadSnapshot(reader)
		if err == nil {
			t.Error("expected error for impossible string length, got nil")
		}
	})
}

func TestSnapshot_FilePermissions(t *testing.T) {
	// Creating a read-only file to test Save failure
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

	s := core.NewSnapshot(path)
	storage := map[string]core.Payload{"k": {Value: []byte("v")}}

	err = s.Save(storage)
	if err == nil {
		t.Error("expected error when saving to read-only path, got nil")
	}

	var snapErr core.SnapshotError
	if !errors.As(err, &snapErr) {
		t.Errorf("expected core.SnapshotError, got %T", err)
	}
}

func TestSnapshot_DeepConsistency(t *testing.T) {
	s := core.NewSnapshot("deep.snap")

	// Create storage with multiple items to test map iteration order effect (or lack thereof if we want determinism)
	storage := make(map[string]core.Payload)
	for i := range 10 {
		key := fmt.Sprintf("k%d", i)
		storage[key] = core.Payload{
			Value: []byte(fmt.Sprintf("v%d", i)),
			Metadata: core.PayloadMetadata{
				CreatedAt: time.Now().Truncate(time.Second),
			},
		}
	}

	// In Go, map iteration is random.
	// To have binary consistency, the SAVE implementation would need to sort.
	// Let's see if the current implementation is deterministic (it probably isn't, but we should know).

	var buf1 bytes.Buffer
	if err := s.SaveSnapshot(&buf1, storage); err != nil {
		t.Fatalf("first save failed: %v", err)
	}

	var buf2 bytes.Buffer
	if err := s.SaveSnapshot(&buf2, storage); err != nil {
		t.Fatalf("second save failed: %v", err)
	}

	// This might fail if the implementation doesn't sort keys.
	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Log("Note: Binary output is non-deterministic due to map iteration order. This is expected unless we sort keys during Save.")
	} else {
		t.Log("Binary output was deterministic in this run.")
	}

	// Logical consistency check
	reader := bytes.NewReader(buf1.Bytes())
	loaded, err := s.LoadSnapshot(reader)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	if len(loaded) != len(storage) {
		t.Errorf("got %d items, want %d", len(loaded), len(storage))
	}

	for k, v := range storage {
		lv, ok := loaded[k]
		if !ok || !bytes.Equal(lv.Value, v.Value) {
			t.Errorf("logical mismatch for key %s", k)
		}
	}
}
