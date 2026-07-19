package tests

import (
	"os"
	"sync"
	"testing"

	"github.com/vpro3611/gomembase.git/pkg/wal"
)

func TestBufferedWal_WriteAndFlush(t *testing.T) {
	walPath := "test_buffered_wal.wal"
	defer os.Remove(walPath)

	// Initialize raw and buffered WALs
	physWal, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create physical WAL: %v", err)
	}

	bufferedW := wal.NewBufferedWal(physWal)
	defer bufferedW.CloseWal()

	// 1. Write to WAL
	n, err := bufferedW.WriteToWal("kv|SET|key1|val1|PERSISTENT\n")
	if err != nil {
		t.Fatalf("WriteToWal failed: %v", err)
	}
	if n <= 0 {
		t.Errorf("expected positive bytes written, got %d", n)
	}

	// Verify that nothing has been written to the physical file yet (due to buffering)
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("expected physical file size to be 0 before Sync, got %d", info.Size())
	}

	// Verify that the memory buffer holds the item
	contents := bufferedW.BufferContent()
	if len(contents) != 1 || contents[0] != "kv|SET|key1|val1|PERSISTENT\n" {
		t.Errorf("unexpected buffer contents: %v", contents)
	}

	// 2. Sync / Flush
	if err := bufferedW.SyncWal(); err != nil {
		t.Fatalf("SyncWal failed: %v", err)
	}

	// Verify buffer is empty
	if len(bufferedW.BufferContent()) != 0 {
		t.Error("expected buffer to be cleared after SyncWal")
	}

	// Verify physical file size is now greater than 0
	info, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Error("expected physical file size to be greater than 0 after Sync")
	}
}

func TestBufferedWal_RecoverFromWal(t *testing.T) {
	walPath := "test_buffered_wal_recover.wal"
	defer os.Remove(walPath)

	physWal, _ := wal.NewWal(walPath)
	bufferedW := wal.NewBufferedWal(physWal)
	defer bufferedW.CloseWal()

	_, _ = bufferedW.WriteToWal("kv|SET|k1|v1\n")
	_, _ = bufferedW.WriteToWal("kv|SET|k2|v2\n")

	// Call recover: should trigger an implicit SyncWal and recover all entries
	var lines []string
	err := bufferedW.RecoverFromWal(func(line string) error {
		lines = append(lines, line)
		return nil
	})
	if err != nil {
		t.Fatalf("RecoverFromWal failed: %v", err)
	}

	if len(lines) != 2 || lines[0] != "kv|SET|k1|v1" || lines[1] != "kv|SET|k2|v2" {
		t.Errorf("recovered unexpected lines: %v", lines)
	}
}

func TestBufferedWal_Truncate(t *testing.T) {
	walPath := "test_buffered_wal_trunc.wal"
	defer os.Remove(walPath)

	physWal, _ := wal.NewWal(walPath)
	bufferedW := wal.NewBufferedWal(physWal)
	defer bufferedW.CloseWal()

	_, _ = bufferedW.WriteToWal("kv|SET|k1|v1\n")

	// Truncate should clear buffer
	if err := bufferedW.TruncateWal(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	if len(bufferedW.BufferContent()) != 0 {
		t.Error("expected buffer to be empty after Truncate")
	}
}

func TestBufferedWal_PartialWriteFailure(t *testing.T) {
	mockPhys := &MockWal{failAfter: 2} // Allow only 2 successful WriteRaw calls, then fail

	bufferedW := wal.NewBufferedWal(mockPhys)

	// Write 5 entries
	_, _ = bufferedW.WriteToWal("line1\n")
	_, _ = bufferedW.WriteToWal("line2\n")
	_, _ = bufferedW.WriteToWal("line3\n")
	_, _ = bufferedW.WriteToWal("line4\n")
	_, _ = bufferedW.WriteToWal("line5\n")

	// Trigger flush. It should fail on line3 (the 3rd call)
	err := bufferedW.SyncWal()
	if err == nil {
		t.Error("expected error during partial write, got nil")
	}

	// Verify that exactly 2 lines were written to mockPhys and removed from the buffer.
	// The remaining 3 lines ("line3\n", "line4\n", "line5\n") should still be in BufferedWal.
	if len(mockPhys.Writes()) != 2 {
		t.Errorf("expected 2 physical writes, got %d", len(mockPhys.Writes()))
	}

	bufContents := bufferedW.BufferContent()
	if len(bufContents) != 3 || bufContents[0] != "line3\n" || bufContents[1] != "line4\n" || bufContents[2] != "line5\n" {
		t.Errorf("unexpected remaining buffer contents: %v", bufContents)
	}

	// Resolve the failure condition
	mockPhys.failAfter = 0 // Allow all writes

	// Trigger flush again. It should succeed and write the remaining 3 lines.
	err = bufferedW.SyncWal()
	if err != nil {
		t.Fatalf("expected successful SyncWal, got %v", err)
	}

	if len(bufferedW.BufferContent()) != 0 {
		t.Error("expected buffer to be completely empty after successful retry SyncWal")
	}

	if len(mockPhys.Writes()) != 5 {
		t.Errorf("expected all 5 lines written, got %d", len(mockPhys.Writes()))
	}
}

func TestBufferedWal_Concurrency(t *testing.T) {
	mockPhys := &MockWal{}
	bufferedW := wal.NewBufferedWal(mockPhys)

	const numWorkers = 10
	const numWrites = 100

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < numWrites; i++ {
				_, _ = bufferedW.WriteToWal("line\n")
			}
		}(w)
	}

	wg.Wait()

	// Verify that buffer contains exactly numWorkers * numWrites lines
	bufContents := bufferedW.BufferContent()
	expectedTotal := numWorkers * numWrites
	if len(bufContents) != expectedTotal {
		t.Errorf("expected %d entries in buffer, got %d", expectedTotal, len(bufContents))
	}
}
