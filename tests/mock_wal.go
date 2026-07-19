package tests

import (
	"errors"
	"strings"
	"sync"

	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

var (
	ErrMockWriteFailed    = errors.New("wal write failed")
	ErrMockTruncateFailed = errors.New("wal truncate failed")
)

type MockWal struct {
	mutex        sync.RWMutex
	writes       []string
	failWrite    bool
	failTruncate bool
	failAfter    int // Fail write after this many calls (if non-zero)
	writeCalls   int // Count of write calls
}

func (m *MockWal) Writes() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	res := make([]string, len(m.writes))
	copy(res, m.writes)
	return res
}

func (m *MockWal) SetFailWrite(b bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.failWrite = b
}

func (m *MockWal) SetFailTruncate(b bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.failTruncate = b
}

func (m *MockWal) WriteToWal(actionString string) (int, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.failWrite {
		return 0, ErrMockWriteFailed
	}
	if m.failAfter > 0 && m.writeCalls >= m.failAfter {
		return 0, ErrMockWriteFailed
	}
	m.writeCalls++
	m.writes = append(m.writes, actionString)
	return len(actionString), nil
}

func (m *MockWal) WriteRaw(actionString string) (int, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.failWrite {
		return 0, ErrMockWriteFailed
	}
	if m.failAfter > 0 && m.writeCalls >= m.failAfter {
		return 0, ErrMockWriteFailed
	}
	m.writeCalls++
	m.writes = append(m.writes, actionString)
	return len(actionString), nil
}

func (m *MockWal) ReadFromWal(buffer []byte) (int, error) {
	return 0, nil
}

func (m *MockWal) RecoverFromWal(applyFunc func(line string) error) error {
	m.mutex.RLock()
	// Copy slice to avoid holding read lock during applyFunc execution (which might do writes)
	writesCopy := make([]string, len(m.writes))
	copy(writesCopy, m.writes)
	m.mutex.RUnlock()

	for _, line := range writesCopy {
		trimmed := line
		if len(line) > 0 && line[len(line)-1] == '\n' {
			trimmed = line[:len(line)-1]
		}
		if err := applyFunc(trimmed); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockWal) CloseWal() error {
	return nil
}

func (m *MockWal) SyncWal() error {
	return nil
}

func (m *MockWal) TruncateWal() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.failTruncate {
		return ErrMockTruncateFailed
	}
	m.writes = nil
	return nil
}

// Log implements persistence.WalLogger
func (m *MockWal) Log(engineID string, action string, args ...string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.failWrite {
		return ErrMockWriteFailed
	}
	var parts []string
	parts = append(parts, engineID, action)
	parts = append(parts, args...)
	line := strings.Join(parts, "|") + "\n"
	m.writes = append(m.writes, line)
	return nil
}

// Helper functions for refactored tests
func loadStorage(w wal.WalInterface, s *storage.Storage) error {
	pm := persistence.NewPersistenceManager(w, nil)
	pm.RegisterEngine(s)
	return pm.Restore(nil)
}

func loadStorageWithSnapshot(w wal.WalInterface, snap *snapshot.Snapshot, s *storage.Storage) error {
	pm := persistence.NewPersistenceManager(w, snap)
	pm.RegisterEngine(s)
	return pm.Restore(nil)
}

func saveStorageSnapshot(w wal.WalInterface, snap *snapshot.Snapshot, s *storage.Storage) error {
	pm := persistence.NewPersistenceManager(w, snap)
	pm.RegisterEngine(s)
	return pm.SaveSnapshot()
}

func newStorageWithWal(w wal.WalInterface) *storage.Storage {
	pm := persistence.NewPersistenceManager(w, nil)
	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)
	return s
}

func newStorageWithWalAndSnap(w wal.WalInterface, snap *snapshot.Snapshot) (*storage.Storage, *persistence.PersistenceManager) {
	pm := persistence.NewPersistenceManager(w, snap)
	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)
	return s, pm
}
