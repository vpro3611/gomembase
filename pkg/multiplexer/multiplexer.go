package multiplexer

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/vpro3611/gomembase.git/pkg/list_storage"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/set_storage"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/zset_storage"
)

var (
	ErrInstanceLimitReached = errors.New("sub-instance limit reached")
	ErrInstanceNotFound     = errors.New("sub-instance not found")
	ErrInvalidDsType        = errors.New("invalid data structure type")
)

type Multiplexer struct {
	walLogger    persistence.WalLogger
	txWalLogger  *TxWalLogger
	kvEngines    map[string]*storage.Storage
	listEngines  map[string]*list_storage.ListStorage
	setEngines   map[string]*set_storage.SetStorage
	zsetEngines  map[string]*zset_storage.ZSetStorage
	maxInstances int
	mutex        sync.RWMutex
	txLock       sync.RWMutex
}

func NewMultiplexer(logger persistence.WalLogger, maxInstances int) *Multiplexer {
	txLogger := NewTxWalLogger(logger)
	return &Multiplexer{
		walLogger:    txLogger, // Use TxWalLogger as the base logger
		txWalLogger:  txLogger, // Keep a reference to control transactions
		kvEngines:    make(map[string]*storage.Storage),
		listEngines:  make(map[string]*list_storage.ListStorage),
		setEngines:   make(map[string]*set_storage.SetStorage),
		zsetEngines:  make(map[string]*zset_storage.ZSetStorage),
		maxInstances: maxInstances,
	}
}

// Generate a random RFC-4122 compliant UUID
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	// Set version 4 and variant 1
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (m *Multiplexer) TotalInstances() int {
	return len(m.kvEngines) + len(m.listEngines) + len(m.setEngines) + len(m.zsetEngines)
}


func (m *Multiplexer) GetInfo(targetUUID string) InfoResponse {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	resp := InfoResponse{
		OK: true,
		Server: ServerInfo{
			OS:               runtime.GOOS,
			GoVersion:        runtime.Version(),
			NumGoroutine:     runtime.NumGoroutine(),
			NumCPU:           runtime.NumCPU(),
			MemoryAllocBytes: memStats.Alloc,
			TotalInstances:   m.TotalInstances(),
		},
		Users: []UserInfo{
			{
				UserID:    "default_user",
				Instances: make([]InstanceInfo, 0),
			},
		},
	}

	for uuid, eng := range m.kvEngines {
		if targetUUID == "" || targetUUID == uuid {
			resp.Users[0].Instances = append(resp.Users[0].Instances, InstanceInfo{
				UUID:             uuid,
				DSType:           "kv",
				KeyCount:         eng.KeyCount(),
				MemoryUsageBytes: eng.MemoryUsageBytes(),
			})
		}
	}

	for uuid, eng := range m.listEngines {
		if targetUUID == "" || targetUUID == uuid {
			resp.Users[0].Instances = append(resp.Users[0].Instances, InstanceInfo{
				UUID:             uuid,
				DSType:           "list",
				KeyCount:         eng.KeyCount(),
				MemoryUsageBytes: eng.MemoryUsageBytes(),
			})
		}
	}

	for uuid, eng := range m.setEngines {
		if targetUUID == "" || targetUUID == uuid {
			resp.Users[0].Instances = append(resp.Users[0].Instances, InstanceInfo{
				UUID:             uuid,
				DSType:           "set",
				KeyCount:         eng.KeyCount(),
				MemoryUsageBytes: eng.MemoryUsageBytes(),
			})
		}
	}

	for uuid, eng := range m.zsetEngines {
		if targetUUID == "" || targetUUID == uuid {
			resp.Users[0].Instances = append(resp.Users[0].Instances, InstanceInfo{
				UUID:             uuid,
				DSType:           "zset",
				KeyCount:         eng.KeyCount(),
				MemoryUsageBytes: eng.MemoryUsageBytes(),
			})
		}
	}

	return resp
}

func (m *Multiplexer) ExceedsMaxInstances() bool {
	return m.TotalInstances() >= m.maxInstances
}

func (m *Multiplexer) CreateInstance(dsType string) (string, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.TotalInstances() >= m.maxInstances {
		return "", ErrInstanceLimitReached
	}

	uuid := generateUUID()
	if err := m.createInstanceWithUUIDNoLock(dsType, uuid); err != nil {
		return "", err
	}

	// Log creation to WAL
	_ = m.walLogger.Log(m.EngineID(), "CREATE_INSTANCE", dsType, uuid)

	return uuid, nil
}

func (m *Multiplexer) createInstanceWithUUIDNoLock(dsType string, uuid string) error {
	scopedLogger := NewScopedWalLogger(m.walLogger, uuid)
	switch dsType {
	case "kv":
		m.kvEngines[uuid] = storage.NewStorage(scopedLogger)
	case "list":
		m.listEngines[uuid] = list_storage.NewListStorage(scopedLogger)
	case "set":
		m.setEngines[uuid] = set_storage.NewSetStorage(scopedLogger)
	case "zset":
		m.zsetEngines[uuid] = zset_storage.NewZSetStorage(scopedLogger)
	default:
		return ErrInvalidDsType
	}
	return nil
}

func (m *Multiplexer) DeleteInstance(dsType string, uuid string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if err := m.deleteInstanceNoWalNoLock(dsType, uuid); err != nil {
		return err
	}

	// Log deletion to WAL
	_ = m.walLogger.Log(m.EngineID(), "DELETE_INSTANCE", dsType, uuid)
	return nil
}

func (m *Multiplexer) deleteInstanceNoWalNoLock(dsType string, uuid string) error {
	switch dsType {
	case "kv":
		if _, ok := m.kvEngines[uuid]; !ok {
			return ErrInstanceNotFound
		}
		delete(m.kvEngines, uuid)
	case "list":
		if _, ok := m.listEngines[uuid]; !ok {
			return ErrInstanceNotFound
		}
		delete(m.listEngines, uuid)
	case "set":
		if _, ok := m.setEngines[uuid]; !ok {
			return ErrInstanceNotFound
		}
		delete(m.setEngines, uuid)
	case "zset":
		if _, ok := m.zsetEngines[uuid]; !ok {
			return ErrInstanceNotFound
		}
		delete(m.zsetEngines, uuid)
	default:
		return ErrInvalidDsType
	}
	return nil
}

func (m *Multiplexer) GetKV(uuid string) (*storage.Storage, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	eng, ok := m.kvEngines[uuid]
	return eng, ok
}

func (m *Multiplexer) GetList(uuid string) (*list_storage.ListStorage, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	eng, ok := m.listEngines[uuid]
	return eng, ok
}

func (m *Multiplexer) GetSet(uuid string) (*set_storage.SetStorage, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	eng, ok := m.setEngines[uuid]
	return eng, ok
}

func (m *Multiplexer) GetZSet(uuid string) (*zset_storage.ZSetStorage, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	eng, ok := m.zsetEngines[uuid]
	return eng, ok
}

// Transaction locking
func (m *Multiplexer) TxLock() { m.txLock.Lock() }
func (m *Multiplexer) TxUnlock() { m.txLock.Unlock() }
func (m *Multiplexer) RLockTx() { m.txLock.RLock() }
func (m *Multiplexer) RUnlockTx() { m.txLock.RUnlock() }

// TxWalLogger returns the transaction WAL logger
func (m *Multiplexer) TxWalLogger() *TxWalLogger {
	return m.txWalLogger
}

func (m *Multiplexer) CleanupAllExpired() {
	m.mutex.RLock()
	// Copy maps to avoid holding read lock for a long time
	kvs := make([]*storage.Storage, 0, len(m.kvEngines))
	for _, e := range m.kvEngines {
		kvs = append(kvs, e)
	}
	lists := make([]*list_storage.ListStorage, 0, len(m.listEngines))
	for _, e := range m.listEngines {
		lists = append(lists, e)
	}
	sets := make([]*set_storage.SetStorage, 0, len(m.setEngines))
	for _, e := range m.setEngines {
		sets = append(sets, e)
	}
	zsets := make([]*zset_storage.ZSetStorage, 0, len(m.zsetEngines))
	for _, e := range m.zsetEngines {
		zsets = append(zsets, e)
	}
	m.mutex.RUnlock()

	for _, e := range kvs {
		e.CleanupExpired()
	}
	for _, e := range lists {
		e.CleanupExpired()
	}
	for _, e := range sets {
		e.CleanupExpired()
	}
	for _, e := range zsets {
		e.CleanupExpired()
	}
}

// EngineID implements persistence.Engine
func (m *Multiplexer) EngineID() string {
	return "mux"
}

// ProcessWalEntry implements persistence.Engine
func (m *Multiplexer) ProcessWalEntry(action string, args []string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch action {
	case "CREATE_INSTANCE":
		if len(args) < 2 {
			return fmt.Errorf("CREATE_INSTANCE requires dsType and uuid")
		}
		dsType, uuid := args[0], args[1]
		return m.createInstanceWithUUIDNoLock(dsType, uuid)
	case "DELETE_INSTANCE":
		if len(args) < 2 {
			return fmt.Errorf("DELETE_INSTANCE requires dsType and uuid")
		}
		dsType, uuid := args[0], args[1]
		return m.deleteInstanceNoWalNoLock(dsType, uuid)
	}
	return fmt.Errorf("unknown multiplexer action: %s", action)
}

// ProcessDelegatedWalEntry implements persistence.DelegatedEngine
func (m *Multiplexer) ProcessDelegatedWalEntry(engineID string, action string, args []string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(args) < 1 {
		return fmt.Errorf("delegated action %s missing uuid argument", action)
	}
	uuid := args[0]
	remainingArgs := args[1:]

	switch engineID {
	case "kv":
		eng, ok := m.kvEngines[uuid]
		if !ok {
			// Auto-create on WAL replay if we don't have it (fallback recovery)
			_ = m.createInstanceWithUUIDNoLock("kv", uuid)
			eng = m.kvEngines[uuid]
		}
		return eng.ProcessWalEntry(action, remainingArgs)
	case "list":
		eng, ok := m.listEngines[uuid]
		if !ok {
			_ = m.createInstanceWithUUIDNoLock("list", uuid)
			eng = m.listEngines[uuid]
		}
		return eng.ProcessWalEntry(action, remainingArgs)
	case "set":
		eng, ok := m.setEngines[uuid]
		if !ok {
			_ = m.createInstanceWithUUIDNoLock("set", uuid)
			eng = m.setEngines[uuid]
		}
		return eng.ProcessWalEntry(action, remainingArgs)
	case "zset":
		eng, ok := m.zsetEngines[uuid]
		if !ok {
			_ = m.createInstanceWithUUIDNoLock("zset", uuid)
			eng = m.zsetEngines[uuid]
		}
		return eng.ProcessWalEntry(action, remainingArgs)
	}
	return fmt.Errorf("unknown engine ID: %s", engineID)
}

// SerializeState implements persistence.Engine
func (m *Multiplexer) SerializeState(w io.Writer) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	total := uint64(len(m.kvEngines) + len(m.listEngines) + len(m.setEngines) + len(m.zsetEngines))
	if err := binary.Write(w, binary.LittleEndian, total); err != nil {
		return err
	}

	serializeEngine := func(dsType string, uuid string, engine persistence.Engine) error {
		if err := writeString(w, dsType); err != nil {
			return err
		}
		if err := writeString(w, uuid); err != nil {
			return err
		}
		var buf bytes.Buffer
		if err := engine.SerializeState(&buf); err != nil {
			return err
		}
		size := uint64(buf.Len())
		if err := binary.Write(w, binary.LittleEndian, size); err != nil {
			return err
		}
		_, err := w.Write(buf.Bytes())
		return err
	}

	for uuid, eng := range m.kvEngines {
		if err := serializeEngine("kv", uuid, eng); err != nil {
			return err
		}
	}
	for uuid, eng := range m.listEngines {
		if err := serializeEngine("list", uuid, eng); err != nil {
			return err
		}
	}
	for uuid, eng := range m.setEngines {
		if err := serializeEngine("set", uuid, eng); err != nil {
			return err
		}
	}
	for uuid, eng := range m.zsetEngines {
		if err := serializeEngine("zset", uuid, eng); err != nil {
			return err
		}
	}

	return nil
}

// DeserializeState implements persistence.Engine
func (m *Multiplexer) DeserializeState(r io.Reader) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.kvEngines = make(map[string]*storage.Storage)
	m.listEngines = make(map[string]*list_storage.ListStorage)
	m.setEngines = make(map[string]*set_storage.SetStorage)
	m.zsetEngines = make(map[string]*zset_storage.ZSetStorage)

	var total uint64
	if err := binary.Read(r, binary.LittleEndian, &total); err != nil {
		return err
	}

	for i := uint64(0); i < total; i++ {
		dsType, err := readString(r)
		if err != nil {
			return err
		}
		uuid, err := readString(r)
		if err != nil {
			return err
		}
		var size uint64
		if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
			return err
		}

		limitReader := io.LimitReader(r, int64(size))

		scopedLogger := NewScopedWalLogger(m.walLogger, uuid)
		switch dsType {
		case "kv":
			eng := storage.NewStorage(scopedLogger)
			if err := eng.DeserializeState(limitReader); err != nil {
				return err
			}
			m.kvEngines[uuid] = eng
		case "list":
			eng := list_storage.NewListStorage(scopedLogger)
			if err := eng.DeserializeState(limitReader); err != nil {
				return err
			}
			m.listEngines[uuid] = eng
		case "set":
			eng := set_storage.NewSetStorage(scopedLogger)
			if err := eng.DeserializeState(limitReader); err != nil {
				return err
			}
			m.setEngines[uuid] = eng
		case "zset":
			eng := zset_storage.NewZSetStorage(scopedLogger)
			if err := eng.DeserializeState(limitReader); err != nil {
				return err
			}
			m.zsetEngines[uuid] = eng
		default:
			return fmt.Errorf("unknown dsType in snapshot: %s", dsType)
		}

		remaining := limitReader.(*io.LimitedReader).N
		if remaining > 0 {
			if _, err := io.CopyN(io.Discard, r, remaining); err != nil {
				return err
			}
		}
	}

	return nil
}

// Reset implements persistence.Engine
func (m *Multiplexer) Reset() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, eng := range m.kvEngines {
		_ = eng.Reset()
	}
	for _, eng := range m.listEngines {
		_ = eng.Reset()
	}
	for _, eng := range m.setEngines {
		_ = eng.Reset()
	}
	for _, eng := range m.zsetEngines {
		_ = eng.Reset()
	}

	m.kvEngines = make(map[string]*storage.Storage)
	m.listEngines = make(map[string]*list_storage.ListStorage)
	m.setEngines = make(map[string]*set_storage.SetStorage)
	m.zsetEngines = make(map[string]*zset_storage.ZSetStorage)
	return nil
}

// Clear implements persistence.Engine
func (m *Multiplexer) Clear() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, eng := range m.kvEngines {
		_ = eng.Clear()
	}
	for _, eng := range m.listEngines {
		_ = eng.Clear()
	}
	for _, eng := range m.setEngines {
		_ = eng.Clear()
	}
	for _, eng := range m.zsetEngines {
		_ = eng.Clear()
	}

	m.kvEngines = make(map[string]*storage.Storage)
	m.listEngines = make(map[string]*list_storage.ListStorage)
	m.setEngines = make(map[string]*set_storage.SetStorage)
	m.zsetEngines = make(map[string]*zset_storage.ZSetStorage)
	return nil
}

// Helper functions for binary string serialization
func writeString(w io.Writer, s string) error {
	b := []byte(s)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readString(r io.Reader) (string, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}
