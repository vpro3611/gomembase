package tests

import (
	"errors"
	"strings"

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
	writes       []string
	failWrite    bool
	failTruncate bool
}

func (m *MockWal) Writes() []string {
	return m.writes
}

func (m *MockWal) SetFailWrite(b bool) {
	m.failWrite = b
}

func (m *MockWal) SetFailTruncate(b bool) {
	m.failTruncate = b
}

func (m *MockWal) WriteToWal(actionString string) (int, error) {
	if m.failWrite {
		return 0, ErrMockWriteFailed
	}
	m.writes = append(m.writes, actionString)
	return len(actionString), nil
}

func (m *MockWal) ReadFromWal(buffer []byte) (int, error) {
	return 0, nil
}

func (m *MockWal) RecoverFromWal(applyFunc func(line string) error) error {
	for _, line := range m.writes {
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
	if m.failTruncate {
		return ErrMockTruncateFailed
	}
	m.writes = nil
	return nil
}

// Log implements persistence.WalLogger
func (m *MockWal) Log(engineID string, action string, args ...string) error {
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
