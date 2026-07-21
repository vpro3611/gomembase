package multiplexer

import (
	"sync"

	"github.com/vpro3611/gomembase.git/pkg/persistence"
)

type walEntry struct {
	engineID string
	action   string
	args     []string
}

type TxWalLogger struct {
	real   persistence.WalLogger
	buffer []walEntry
	active bool
	mu     sync.Mutex
}

func NewTxWalLogger(real persistence.WalLogger) *TxWalLogger {
	return &TxWalLogger{
		real:   real,
		buffer: make([]walEntry, 0),
		active: false,
	}
}

func (l *TxWalLogger) Log(engineID string, action string, args ...string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active {
		l.buffer = append(l.buffer, walEntry{
			engineID: engineID,
			action:   action,
			args:     append([]string(nil), args...), // copy
		})
		return nil
	}

	return l.real.Log(engineID, action, args...)
}

func (l *TxWalLogger) BeginTx() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active = true
	l.buffer = l.buffer[:0]
}

func (l *TxWalLogger) CommitTx(txID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.active {
		return nil
	}

	// Flush atomic block to real WAL
	_ = l.real.Log("tx", "TX_BEGIN", txID)
	for _, entry := range l.buffer {
		_ = l.real.Log(entry.engineID, entry.action, entry.args...)
	}
	_ = l.real.Log("tx", "TX_COMMIT", txID)

	l.buffer = l.buffer[:0]
	l.active = false
	return nil
}

func (l *TxWalLogger) RollbackTx() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buffer = l.buffer[:0]
	l.active = false
}
