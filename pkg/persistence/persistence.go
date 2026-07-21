package persistence

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

type Engine interface {
	EngineID() string
	ProcessWalEntry(action string, args []string) error
	SerializeState(w io.Writer) error
	DeserializeState(r io.Reader) error
	Reset() error
	Clear() error
}

type DelegatedEngine interface {
	ProcessDelegatedWalEntry(engineID string, action string, args []string) error
}

type WalLogger interface {
	Log(engineID string, action string, args ...string) error
}

type PersistenceManager struct {
	wal            wal.WalInterface
	snapshot       *snapshot.Snapshot
	engines        map[string]Engine
	fallbackEngine DelegatedEngine
	mutex          sync.Mutex
}

func NewPersistenceManager(w wal.WalInterface, s *snapshot.Snapshot) *PersistenceManager {
	return &PersistenceManager{
		wal:      w,
		snapshot: s,
		engines:  make(map[string]Engine),
	}
}

func (pm *PersistenceManager) RegisterEngine(e Engine) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.engines[e.EngineID()] = e
}

func (pm *PersistenceManager) RegisterFallbackEngine(e DelegatedEngine) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.fallbackEngine = e
}

func (pm *PersistenceManager) Log(engineID string, action string, args ...string) error {
	var parts []string
	parts = append(parts, engineID, action)
	parts = append(parts, args...)
	line := strings.Join(parts, "|") + "\n"
	_, err := pm.wal.WriteToWal(line)
	return err
}

func (pm *PersistenceManager) Restore(restoreOnly []string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// 1. Load from snapshot first
	if pm.snapshot != nil {
		err := pm.snapshot.Load(func(engineID string, r io.Reader) error {
			if len(restoreOnly) > 0 && !slices.Contains(restoreOnly, engineID) {
				return nil
			}
			engine, ok := pm.engines[engineID]
			if !ok {
				return fmt.Errorf("unregistered engine in snapshot: %s", engineID)
			}
			return engine.DeserializeState(r)
		})
		if err != nil && !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "no such file or directory") {
			return fmt.Errorf("failed to load snapshot: %w", err)
		}
	}

	// 2. Load from WAL later than snapshot (Two-pass for TX recovery)
	// Pass 1: Identify incomplete transactions
	incompleteTxs := make(map[string]bool)
	activeTxs := make(map[string]bool)

	_ = pm.wal.RecoverFromWal(func(line string) error {
		parts := strings.Split(line, "|")
		if len(parts) >= 3 && parts[0] == "tx" {
			txID := parts[2]
			if parts[1] == "TX_BEGIN" {
				activeTxs[txID] = true
			} else if parts[1] == "TX_COMMIT" {
				delete(activeTxs, txID)
			}
		}
		return nil
	})

	for txID := range activeTxs {
		incompleteTxs[txID] = true
	}

	// Pass 2: Replay valid entries
	var currentTxID string

	err := pm.wal.RecoverFromWal(func(line string) error {
		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			return nil
		}
		engineID := parts[0]
		action := parts[1]
		args := parts[2:]

		if engineID == "tx" && len(args) > 0 {
			if action == "TX_BEGIN" {
				currentTxID = args[0]
			} else if action == "TX_COMMIT" {
				currentTxID = ""
			}
			return nil
		}

		if currentTxID != "" && incompleteTxs[currentTxID] {
			return nil // Skip incomplete TX entries
		}

		if len(restoreOnly) > 0 && !slices.Contains(restoreOnly, engineID) {
			return nil
		}

		engine, ok := pm.engines[engineID]
		if !ok {
			if pm.fallbackEngine != nil {
				return pm.fallbackEngine.ProcessDelegatedWalEntry(engineID, action, args)
			}
			return fmt.Errorf("unregistered engine in WAL: %s", engineID)
		}

		return engine.ProcessWalEntry(action, args)
	})
	if err != nil {
		return fmt.Errorf("failed to recover from WAL: %w", err)
	}

	return nil
}

func (pm *PersistenceManager) SaveSnapshot() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	sections := make(map[string]func(w io.Writer) error)
	for id, engine := range pm.engines {
		eng := engine
		sections[id] = func(w io.Writer) error {
			return eng.SerializeState(w)
		}
	}

	if pm.snapshot == nil {
		return fmt.Errorf("snapshot not initialized")
	}

	if err := pm.snapshot.Save(sections); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	if err := pm.wal.TruncateWal(); err != nil {
		return fmt.Errorf("failed to truncate WAL after snapshot: %w", err)
	}

	return nil
}

func (pm *PersistenceManager) Reset() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	for _, engine := range pm.engines {
		if err := engine.Reset(); err != nil {
			return err
		}
	}

	if pm.snapshot != nil {
		if err := pm.snapshot.Delete(); err != nil {
			return err
		}
	}

	return pm.wal.TruncateWal()
}

func (pm *PersistenceManager) Clear() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	for _, engine := range pm.engines {
		if err := engine.Clear(); err != nil {
			return err
		}
	}

	if pm.snapshot != nil {
		if err := pm.snapshot.Delete(); err != nil {
			return err
		}
	}

	return pm.wal.TruncateWal()
}
