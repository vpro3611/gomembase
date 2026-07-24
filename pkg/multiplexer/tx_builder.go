package multiplexer

import (
	"fmt"

	"github.com/vpro3611/gomembase.git/pkg/transaction"
)

type TxBuilder struct {
	mux       *Multiplexer
	requests  []Request
	tx        *transaction.Transaction
	responses []Response
}

func NewTxBuilder(mux *Multiplexer) *TxBuilder {
	return &TxBuilder{
		mux:       mux,
		requests:  make([]Request, 0),
		tx:        transaction.New(),
		responses: make([]Response, 0),
	}
}

func (tb *TxBuilder) Queue(req Request) {
	tb.requests = append(tb.requests, req)
}

func (tb *TxBuilder) Len() int {
	return len(tb.requests)
}

// Build ops converts queued requests into executable and revertible ops.
func (tb *TxBuilder) buildOps() error {
	for _, req := range tb.requests {
		var op transaction.Op
		switch req.DS {
		case "kv":
			op = tb.buildKVOp(req)
		case "list":
			op = tb.buildListOp(req)
		case "set":
			op = tb.buildSetOp(req)
		case "zset":
			op = tb.buildZSetOp(req)
		default:
			return fmt.Errorf("invalid data structure type in tx: %s", req.DS)
		}

		if err := tb.tx.Add(op); err != nil {
			return err
		}
	}
	return nil
}

func (tb *TxBuilder) buildKVOp(req Request) transaction.Op {
	return transaction.Op{
		PrepareAndDo: func() (func() error, error) {
			eng, ok := tb.mux.GetKV(req.UUID)
			if !ok {
				return nil, ErrInstanceNotFound
			}

			// Capture undo state
			key := ""
			if len(req.Args) > 0 {
				key, _ = unmarshalString(req.Args[0])
			}
			var undo func() error

			switch req.Method {
			case "SET", "SET_TTL", "DEL", "INCR", "INCR_BY", "DECR", "DECR_BY":
				oldPayload, exists := eng.SnapshotKey(key)
				undo = func() error {
					if exists {
						ttl := oldPayload.Metadata().ExpiresAt()
						if ttl != nil {
							duration := ttl.Sub(oldPayload.Metadata().CreatedAt())
							return eng.SetWithTTL(key, oldPayload.Value(), duration)
						} else {
							return eng.Set(key, oldPayload.Value(), oldPayload.Metadata())
						}
					} else {
						return eng.Delete(key)
					}
				}
			default:
				undo = func() error { return nil }
			}

			// Execute without TxLock (already held by TxExec)
			res := tb.mux.executeKV(req)
			tb.responses = append(tb.responses, res)
			if !res.OK {
				return undo, fmt.Errorf("kv op failed: %v", res.Error)
			}
			return undo, nil
		},
	}
}

func (tb *TxBuilder) buildListOp(req Request) transaction.Op {
	return transaction.Op{
		PrepareAndDo: func() (func() error, error) {
			eng, ok := tb.mux.GetList(req.UUID)
			if !ok {
				return nil, ErrInstanceNotFound
			}

			key := ""
			if len(req.Args) > 0 {
				key, _ = unmarshalString(req.Args[0])
			}
			
			oldList, exists := eng.SnapshotList(key)
			undo := func() error {
				if exists {
					_ = eng.Delete(key)
					return eng.RightPush(key, oldList, nil)
				} else {
					return eng.Delete(key)
				}
			}

			res := tb.mux.executeList(req)
			tb.responses = append(tb.responses, res)
			if !res.OK {
				return undo, fmt.Errorf("list op failed: %v", res.Error)
			}
			return undo, nil
		},
	}
}

func (tb *TxBuilder) buildSetOp(req Request) transaction.Op {
	return transaction.Op{
		PrepareAndDo: func() (func() error, error) {
			eng, ok := tb.mux.GetSet(req.UUID)
			if !ok {
				return nil, ErrInstanceNotFound
			}

			key := ""
			if len(req.Args) > 0 {
				key, _ = unmarshalString(req.Args[0])
			}
			
			oldMembers, exists := eng.SnapshotMembers(key)
			undo := func() error {
				if exists {
					_ = eng.Delete(key)
					_, err := eng.SAdd(key, oldMembers, nil)
					return err
				} else {
					return eng.Delete(key)
				}
			}

			res := tb.mux.executeSet(req)
			tb.responses = append(tb.responses, res)
			if !res.OK {
				return undo, fmt.Errorf("set op failed: %v", res.Error)
			}
			return undo, nil
		},
	}
}

func (tb *TxBuilder) buildZSetOp(req Request) transaction.Op {
	return transaction.Op{
		PrepareAndDo: func() (func() error, error) {
			eng, ok := tb.mux.GetZSet(req.UUID)
			if !ok {
				return nil, ErrInstanceNotFound
			}

			key := ""
			if len(req.Args) > 0 {
				key, _ = unmarshalString(req.Args[0])
			}
			
			oldMembers, exists := eng.SnapshotZSetAll(key)
			undo := func() error {
				if exists {
					_ = eng.Delete(key)
					_, err := eng.ZAdd(key, oldMembers, nil)
					return err
				} else {
					return eng.Delete(key)
				}
			}

			res := tb.mux.executeZSet(req)
			tb.responses = append(tb.responses, res)
			if !res.OK {
				return undo, fmt.Errorf("zset op failed: %v", res.Error)
			}
			return undo, nil
		},
	}
}

// Exec executes the transaction.
func (tb *TxBuilder) Exec(txID string) ([]Response, error) {
	// Build ops
	if err := tb.buildOps(); err != nil {
		return nil, err
	}

	// Lock the world
	tb.mux.TxLock()
	defer tb.mux.TxUnlock()

	// Start WAL buffering
	logger := tb.mux.TxWalLogger()
	logger.BeginTx()

	tb.responses = make([]Response, 0, len(tb.requests))

	// Execute transaction
	if err := tb.tx.Exec(); err != nil {
		logger.RollbackTx()
		return tb.responses, err
	}

	// Commit WAL
	if err := logger.CommitTx(txID); err != nil {
		return tb.responses, err
	}

	return tb.responses, nil
}
