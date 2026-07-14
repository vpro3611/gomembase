package tests

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

// TestStorage_Load_DuplicateTTL verifies that loading a WAL with multiple SETs
// for the same key having expiration does not leak heap entries or cause infinite loops in CleanupExpired.
func TestStorage_Load_DuplicateTTL(t *testing.T) {
	tempFile, err := os.CreateTemp("", "dup_wal_test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// 1. Create storage and write two SETs for the same key with different TTLs
	w, err := wal.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	s1 := storage.NewStorage(w)

	// Set short TTL first, then overwrite with slightly longer TTL
	err = s1.SetWithTTL("dup_key", []byte("val1"), 10*time.Second)
	if err != nil {
		t.Fatalf("failed to set dup_key: %v", err)
	}
	err = s1.SetWithTTL("dup_key", []byte("val2"), 20*time.Second)
	if err != nil {
		t.Fatalf("failed to overwrite dup_key: %v", err)
	}

	w.CloseWal()

	// Print WAL content
	walBytes, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to read WAL file: %v", err)
	}
	t.Logf("WAL content:\n%s", string(walBytes))

	// 2. Open new storage and load from the WAL
	w2, err := wal.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.CloseWal()

	s2 := storage.NewStorage(w2)
	if err := s2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	t.Logf("Heap length: %d", s2.Expirations().Len())
	for _, entry := range s2.Expirations() {
		t.Logf("Heap entry: key=%s, expiresAt=%v", entry.Key(), entry.ExpiresAt())
	}

	// 3. Wait for the first expiration time to pass (approx 10 seconds)
	// To avoid sleeping for 10 seconds in a test, let's look at how we can test it.
	// But first let's see if the heap size is indeed 2 (which indicates duplicate entries).
	if s2.Expirations().Len() > 1 {
		t.Logf("Detected duplicate heap entries! Heap size is: %d", s2.Expirations().Len())
	}

	// Instead of sleeping for 10 seconds in the test, we can mock the time or we can just verify the duplicate entry
	// is present in the heap, and that a delete operation or cleanup can trigger the issue.
	// Wait, we can manually trigger the cleanup by calling CleanupExpired after injecting a stale expired entry,
	// or we can just sleep a shorter time but manually adjust the expiresAt time if possible.
	// But actually, we can just sleep 10 seconds, or we can write a test that verifies the duplicate heap entries.
	// Let's first run the test to see if the heap size is indeed 2.
	if s2.Expirations().Len() != 1 {
		t.Errorf("heap has %d entries, expected exactly 1 active entry after loading", s2.Expirations().Len())
	}
}

func TestStorage_WAL_SpecialCharacters(t *testing.T) {
	tempFile, err := os.CreateTemp("", "special_char_wal_test")
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

	// Set key with pipe delimiter
	pipeKey := "key|with|pipes"
	err = s1.Set(pipeKey, []byte("pipeVal"), storage.NewPayloadMetadata(time.Now(), nil))
	if err != nil {
		t.Fatalf("failed to set key with pipes: %v", err)
	}

	// Set key with newline character
	newlineKey := "key\nwith\nnewlines"
	err = s1.Set(newlineKey, []byte("newlineVal"), storage.NewPayloadMetadata(time.Now(), nil))
	if err != nil {
		t.Fatalf("failed to set key with newlines: %v", err)
	}

	w.CloseWal()

	// Reopen and Load
	w2, err := wal.NewWal(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.CloseWal()

	s2 := storage.NewStorage(w2)
	if err := s2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Check if keys recovered correctly
	gotPipe, err := s2.Get(pipeKey)
	if err != nil {
		t.Errorf("failed to get pipeKey: %v", err)
	} else if string(gotPipe) != "pipeVal" {
		t.Errorf("got %s, want pipeVal", string(gotPipe))
	}

	gotNewline, err := s2.Get(newlineKey)
	if err != nil {
		t.Errorf("failed to get newlineKey: %v", err)
	} else if string(gotNewline) != "newlineVal" {
		t.Errorf("got %s, want newlineVal", string(gotNewline))
	}
}

func TestStorage_PatternConcurrency(t *testing.T) {
	s := storage.NewStorage(&MockWal{})
	const workers = 20
	const ops = 50
	
	var wg sync.WaitGroup
	wg.Add(workers * 4)

	// 1. Concurrent Setters
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("prefix:%d:suffix_%d", workerID, j)
				s.SetWithTTL(key, []byte("value"), 10*time.Second)
			}
		}(i)
	}

	// 2. Concurrent Prefix/Suffix/Regex Counters
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				_ = s.CountByPrefix(fmt.Sprintf("prefix:%d:", workerID))
				_ = s.CountBySuffix(fmt.Sprintf("suffix_%d", j))
				_, _ = s.CountByRegex(fmt.Sprintf(`^prefix:%d:suffix_\d+$`, workerID))
			}
		}(i)
	}

	// 3. Concurrent Finders
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				_ = s.FindByPrefix(fmt.Sprintf("prefix:%d:", workerID))
				_ = s.FindBySuffix(fmt.Sprintf("suffix_%d", j))
				_, _ = s.FindByRegex(fmt.Sprintf(`^prefix:%d:suffix_\d+$`, workerID))
			}
		}(i)
	}

	// 4. Concurrent Deleters
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				if j%2 == 0 {
					_ = s.DeleteByPrefix(fmt.Sprintf("prefix:%d:", workerID))
				} else {
					_ = s.DeleteBySuffix(fmt.Sprintf("suffix_%d", j))
				}
			}
		}(i)
	}

	wg.Wait()
}
