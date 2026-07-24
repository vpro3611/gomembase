package transaction

import (
	"fmt"
	"sync"
)

type State int

const (
	StatePending State = iota
	StateCommitted
	StateRolledBack
)

// Op represents a single transactional operation.
// PrepareAndDo must:
//   1. Capture pre-state needed for rollback
//   2. Execute the mutation
//   3. Return an undo closure that restores pre-state
type Op struct {
	PrepareAndDo func() (undo func() error, err error)
}

type Transaction struct {
	ops   []Op
	state State
	mu    sync.Mutex
}

func New() *Transaction {
	return &Transaction{
		ops:   make([]Op, 0),
		state: StatePending,
	}
}

func (tx *Transaction) Add(op Op) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != StatePending {
		return fmt.Errorf("transaction is not pending")
	}

	tx.ops = append(tx.ops, op)
	return nil
}

func (tx *Transaction) Exec() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != StatePending {
		return fmt.Errorf("transaction is not pending")
	}

	undos := make([]func() error, 0, len(tx.ops))

	for i, op := range tx.ops {
		undo, err := op.PrepareAndDo()
		if err != nil {
			// Rollback in reverse order
			for j := len(undos) - 1; j >= 0; j-- {
				_ = undos[j]() // Ignore rollback errors, attempt all
			}
			tx.state = StateRolledBack
			return fmt.Errorf("tx failed at op %d: %w", i, err)
		}
		undos = append(undos, undo)
	}

	tx.state = StateCommitted
	return nil
}

func (tx *Transaction) Discard() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state == StatePending {
		tx.state = StateRolledBack
		tx.ops = nil
	}
}

func (tx *Transaction) State() State {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state
}

func (tx *Transaction) Len() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.ops)
}
