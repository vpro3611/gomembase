package tests

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/list_storage"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

// MockEngine is a simple implementation of persistence.Engine for testing coordination
type MockEngine struct {
	id          string
	data        map[string]string
	walLog      []string
	readPartial bool // if true, DeserializeState will read only part of its payload
}

func NewMockEngine(id string) *MockEngine {
	return &MockEngine{
		id:   id,
		data: make(map[string]string),
	}
}

func (m *MockEngine) EngineID() string {
	return m.id
}

func (m *MockEngine) ProcessWalEntry(action string, args []string) error {
	m.walLog = append(m.walLog, fmt.Sprintf("%s:%v", action, args))
	if action == "PUT" && len(args) == 2 {
		m.data[args[0]] = args[1]
	}
	return nil
}

func (m *MockEngine) SerializeState(w io.Writer) error {
	// Write number of entries
	if err := snapshot.WriteUintValue(w, uint64(len(m.data))); err != nil {
		return err
	}
	for k, v := range m.data {
		if err := snapshot.WriteBytes(w, []byte(k)); err != nil {
			return err
		}
		if err := snapshot.WriteBytes(w, []byte(v)); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockEngine) DeserializeState(r io.Reader) error {
	count, err := snapshot.ReadUintValue[uint64](r)
	if err != nil {
		return err
	}

	m.data = make(map[string]string)
	for i := uint64(0); i < count; i++ {
		kBytes, err := snapshot.ReadBytes(r)
		if err != nil {
			return err
		}
		vBytes, err := snapshot.ReadBytes(r)
		if err != nil {
			return err
		}

		m.data[string(kBytes)] = string(vBytes)

		// Simulate partial read: stop reading early if flag is set
		if m.readPartial && i == 0 && count > 1 {
			return nil
		}
	}
	return nil
}

func (m *MockEngine) Reset() error {
	m.data = make(map[string]string)
	m.walLog = nil
	return nil
}

func (m *MockEngine) Clear() error {
	clear(m.data)
	m.walLog = nil
	return nil
}

// TestPersistence_SelectiveRestore verifies that we can selectively restore only specific engines
func TestPersistence_SelectiveRestore(t *testing.T) {
	mockWal := &MockWal{}
	snapBuf := &bytes.Buffer{}
	snap := snapshot.NewSnapshot("test.rdb")

	pm := persistence.NewPersistenceManager(mockWal, &snap)
	kv := storage.NewStorage(pm)
	pm.RegisterEngine(kv)

	dummy := NewMockEngine("dummy")
	pm.RegisterEngine(dummy)

	// 1. Populate data
	kv.Set("kv_key", []byte("kv_val"), storage.NewPayloadMetadata(time.Now(), nil))
	dummy.data["dummy_key"] = "dummy_val"

	// 2. Save snapshot manually using buffer
	sections := map[string]func(w io.Writer) error{
		"kv":    func(w io.Writer) error { return kv.SerializeState(w) },
		"dummy": func(w io.Writer) error { return dummy.SerializeState(w) },
	}
	if err := snap.SaveSnapshot(snapBuf, sections); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// 3. Clear memory state
	kv.Clear()
	dummy.Clear()

	// 4. Restore only "kv"
	err := snap.LoadSnapshot(snapBuf, func(engineID string, r io.Reader) error {
		if engineID == "kv" {
			return kv.DeserializeState(r)
		}
		// Skip dummy
		return nil
	})
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	// 5. Assert KV was restored, but Dummy remains empty
	if !kv.Exists("kv_key") {
		t.Error("kv_key should have been restored")
	}
	if len(dummy.data) != 0 {
		t.Errorf("dummy engine should be empty, but contains: %v", dummy.data)
	}
}

// TestPersistence_UnregisteredEngine verifies error propagation when an unregistered engine is restored
func TestPersistence_UnregisteredEngine(t *testing.T) {
	mockWal := &MockWal{}
	snapBuf := &bytes.Buffer{}
	snap := snapshot.NewSnapshot("test.rdb")

	pm := persistence.NewPersistenceManager(mockWal, &snap)

	// Save snapshot with an engine ID "unregistered"
	sections := map[string]func(w io.Writer) error{
		"unregistered": func(w io.Writer) error {
			return snapshot.WriteUintValue(w, uint64(0))
		},
	}
	if err := snap.SaveSnapshot(snapBuf, sections); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Restore
	err := pm.Restore(nil) // Will load snapBuf internally, but we use actual files in Restore, so let's mock it via pm.Restore error paths
	// Let's verify WAL unregistered engine detection
	mockWal.WriteToWal("unregistered|SET|key\n")
	err = pm.Restore(nil)
	if err == nil {
		t.Fatal("expected error when replaying WAL with unregistered engine, got nil")
	}
	if !strings.Contains(err.Error(), "unregistered engine in WAL") {
		t.Errorf("expected unregistered engine error, got: %v", err)
	}
}

// TestPersistence_SkipSectionAlignment verifies that if an engine does a partial read (or skips a section),
// the snapshot reader correctly discards the remaining bytes and aligns to the next section.
func TestPersistence_SkipSectionAlignment(t *testing.T) {
	snap := snapshot.NewSnapshot("test.rdb")
	snapBuf := &bytes.Buffer{}

	// Create two mock engines
	e1 := NewMockEngine("e1")
	e1.readPartial = true // will read only 1 entry out of 2
	e1.data["k1"] = "v1"
	e1.data["k2"] = "v2"

	e2 := NewMockEngine("e2")
	e2.data["target"] = "success"

	// Save both sections
	sections := map[string]func(w io.Writer) error{
		"e1": func(w io.Writer) error { return e1.SerializeState(w) },
		"e2": func(w io.Writer) error { return e2.SerializeState(w) },
	}
	if err := snap.SaveSnapshot(snapBuf, sections); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Reset memory
	e1.Clear()
	e2.Clear()

	// Load snapshot
	err := snap.LoadSnapshot(snapBuf, func(engineID string, r io.Reader) error {
		if engineID == "e1" {
			return e1.DeserializeState(r)
		}
		if engineID == "e2" {
			return e2.DeserializeState(r)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	// e1 should only have 1 entry restored due to partial read simulation
	if len(e1.data) != 1 {
		t.Errorf("expected e1 to have 1 entry due to partial read, got: %v", e1.data)
	}

	// e2 should be FULLY and CORRECTLY restored because the parser aligned correctly after e1
	if e2.data["target"] != "success" {
		t.Errorf("e2 recovery failed or got misaligned. target value: %q", e2.data["target"])
	}
}

// TestPersistence_JointKVAndListRecovery verifies that KV storage and List storage can coexist
// and restore successfully from the same snapshot and WAL logs without colliding.
func TestPersistence_JointKVAndListRecovery(t *testing.T) {
	walPath := "test_joint_recovery.wal"
	snapPath := "test_joint_recovery.snap"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	// Initialize the files
	w, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	snap := snapshot.NewSnapshot(snapPath)

	// Create and Register engines
	pm := persistence.NewPersistenceManager(w, &snap)
	kv := storage.NewStorage(pm)
	pm.RegisterEngine(kv)

	listEng := list_storage.NewListStorage(pm)
	pm.RegisterEngine(listEng)

	// Phase 1 (before snapshot):
	err = kv.Set("kv_key1", []byte("val1"), storage.NewPayloadMetadata(time.Now(), nil))
	if err != nil {
		t.Fatalf("KV Set failed: %v", err)
	}
	_ = kv.Set("kv_key2", []byte("val2"), storage.NewPayloadMetadata(time.Now(), nil))
	_ = listEng.LeftPush("list_key1", [][]byte{[]byte("a"), []byte("b"), []byte("c")}, nil) // [c, b, a]

	// Save snapshot (truncates WAL)
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Verify WAL is truncated
	fi, _ := os.Stat(walPath)
	if fi.Size() != 0 {
		t.Errorf("expected WAL to be empty after snapshot, got size %d", fi.Size())
	}

	// Phase 2 (after snapshot, logged to WAL):
	_ = kv.Set("kv_key3", []byte("val3"), storage.NewPayloadMetadata(time.Now(), nil))
	_ = kv.Delete("kv_key1")
	_ = listEng.RightPush("list_key1", [][]byte{[]byte("d")}, nil) // [c, b, a, d]
	_, _ = listEng.LeftPop("list_key1")                            // pop "c" -> [b, a, d]
	_ = listEng.RightPush("list_key2", [][]byte{[]byte("x"), []byte("y")}, nil)

	// Close original WAL
	w.CloseWal()

	// Shutdown & Rebuild State
	w2, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.CloseWal()

	pm2 := persistence.NewPersistenceManager(w2, &snap)
	kv2 := storage.NewStorage(pm2)
	pm2.RegisterEngine(kv2)

	listEng2 := list_storage.NewListStorage(pm2)
	pm2.RegisterEngine(listEng2)

	// Restore both simultaneously
	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Joint restore failed: %v", err)
	}

	// Verify KV Engine recovery
	if kv2.Exists("kv_key1") {
		t.Error("kv_key1 should be deleted after restore")
	}
	v2, _ := kv2.Get("kv_key2")
	if string(v2) != "val2" {
		t.Errorf("expected kv_key2 -> val2, got: %s", string(v2))
	}
	v3, _ := kv2.Get("kv_key3")
	if string(v3) != "val3" {
		t.Errorf("expected kv_key3 -> val3, got: %s", string(v3))
	}

	// Verify List Engine recovery
	l1, _ := listEng2.Get("list_key1")
	if len(l1) != 3 || string(l1[0]) != "b" || string(l1[1]) != "a" || string(l1[2]) != "d" {
		t.Errorf("list_key1 recovery failed. expected [b, a, d], got: %v", l1)
	}

	l2, _ := listEng2.Get("list_key2")
	if len(l2) != 2 || string(l2[0]) != "x" || string(l2[1]) != "y" {
		t.Errorf("list_key2 recovery failed. expected [x, y], got: %v", l2)
	}
}
