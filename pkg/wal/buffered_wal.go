package wal

import (
	"sync"
)

// WalBuffer holds pending WAL log entries in memory.
// It is NOT thread-safe on its own — the caller (BufferedWal) is responsible for synchronization.
type WalBuffer struct {
	content []string
}

func NewWalBuffer() *WalBuffer {
	return &WalBuffer{
		content: make([]string, 0),
	}
}

func (wb *WalBuffer) SaveToBuffer(actionString string) {
	wb.content = append(wb.content, actionString)
}

// ExportToWal writes buffered logs to the physical destination using WriteRaw and SyncWal.
// It returns the number of successfully written log entries and any error encountered.
func (wb *WalBuffer) ExportToWal(destination WalInterface) (int, error) {
	if len(wb.content) == 0 {
		return 0, nil
	}

	for i, line := range wb.content {
		if _, err := destination.WriteRaw(line); err != nil {
			return i, err
		}
	}

	if err := destination.SyncWal(); err != nil {
		return len(wb.content), err
	}

	return len(wb.content), nil
}

func (wb *WalBuffer) ClearBuffer() {
	wb.content = wb.content[:0]
}

func (wb *WalBuffer) BufferContent() []string {
	res := make([]string, len(wb.content))
	copy(res, wb.content)
	return res
}

// BufferedWal implements WalInterface and wraps a physical WalInterface with a WalBuffer.
// It is the single synchronization point — all access to the buffer and physical WAL
// is serialized through bw.mutex.
type BufferedWal struct {
	physicalWal WalInterface
	buffer      *WalBuffer
	mutex       sync.Mutex
}

func NewBufferedWal(physicalWal WalInterface) *BufferedWal {
	return &BufferedWal{
		physicalWal: physicalWal,
		buffer:      NewWalBuffer(),
	}
}

func (bw *BufferedWal) WriteToWal(actionString string) (int, error) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()
	bw.buffer.SaveToBuffer(actionString)
	return len(actionString), nil
}

func (bw *BufferedWal) WriteRaw(actionString string) (int, error) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()
	bw.buffer.SaveToBuffer(actionString)
	return len(actionString), nil
}

func (bw *BufferedWal) RecoverFromWal(applyFunc func(line string) error) error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	// Flush buffer to physical WAL before recovering to keep file state synchronous
	written, err := bw.buffer.ExportToWal(bw.physicalWal)
	if written > 0 {
		bw.buffer.content = bw.buffer.content[written:]
	}
	if err != nil {
		return err
	}

	return bw.physicalWal.RecoverFromWal(applyFunc)
}

func (bw *BufferedWal) CloseWal() error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	// Flush remaining buffer content to physical WAL before closing
	written, err := bw.buffer.ExportToWal(bw.physicalWal)
	if written > 0 {
		bw.buffer.content = bw.buffer.content[written:]
	}
	if err != nil {
		_ = bw.physicalWal.CloseWal() // Try to close anyway to prevent resource leak
		return err
	}
	return bw.physicalWal.CloseWal()
}

func (bw *BufferedWal) SyncWal() error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	written, err := bw.buffer.ExportToWal(bw.physicalWal)
	if written > 0 {
		bw.buffer.content = bw.buffer.content[written:]
	}
	return err
}

func (bw *BufferedWal) TruncateWal() error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	bw.buffer.ClearBuffer()
	return bw.physicalWal.TruncateWal()
}

// Buffer exposes the internal buffer (non-thread-safe raw buffer, primarily for unit tests)
func (bw *BufferedWal) Buffer() *WalBuffer {
	return bw.buffer
}

// BufferContent returns a thread-safe snapshot of the buffer's current contents
func (bw *BufferedWal) BufferContent() []string {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()
	return bw.buffer.BufferContent()
}
