package tests

import "errors"

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
		// Remove trailing newline if present, as bufio.Scanner does
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
