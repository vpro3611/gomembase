package tests

import (
	"os"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/list_storage"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

func newListStorageWithWal(w wal.WalInterface) (*list_storage.ListStorage, *persistence.PersistenceManager) {
	pm := persistence.NewPersistenceManager(w, nil)
	ls := list_storage.NewListStorage(pm)
	pm.RegisterEngine(ls)
	return ls, pm
}

func newListStorageWithWalAndSnap(w wal.WalInterface, snap *snapshot.Snapshot) (*list_storage.ListStorage, *persistence.PersistenceManager) {
	pm := persistence.NewPersistenceManager(w, snap)
	ls := list_storage.NewListStorage(pm)
	pm.RegisterEngine(ls)
	return ls, pm
}

func TestListStorage_BasicOperations(t *testing.T) {
	mockWal := &MockWal{}
	ls, _ := newListStorageWithWal(mockWal)

	key := "mylist"
	// Push left: [a] then [b, a] then [c, b, a]
	err := ls.LeftPush(key, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, nil)
	if err != nil {
		t.Fatalf("LeftPush failed: %v", err)
	}

	length, _ := ls.Len(key)
	if length != 3 {
		t.Errorf("expected length 3, got %d", length)
	}

	// Range checks
	all, _ := ls.Get(key)
	if len(all) != 3 || string(all[0]) != "c" || string(all[1]) != "b" || string(all[2]) != "a" {
		t.Errorf("expected [c, b, a], got: %v", all)
	}

	// Negative index range: last 2 elements
	lastTwo, _ := ls.Range(key, -2, -1)
	if len(lastTwo) != 2 || string(lastTwo[0]) != "b" || string(lastTwo[1]) != "a" {
		t.Errorf("expected [b, a], got: %v", lastTwo)
	}

	// Index check
	val, _ := ls.Index(key, 1)
	if string(val) != "b" {
		t.Errorf("expected b at index 1, got %s", string(val))
	}

	// Pos check
	pos, _ := ls.Pos(key, []byte("a"))
	if pos != 2 {
		t.Errorf("expected pos of 'a' to be 2, got %d", pos)
	}

	// Pop Right: should remove "a"
	popped, _ := ls.RightPop(key)
	if string(popped) != "a" {
		t.Errorf("expected popped a, got %s", string(popped))
	}

	// Pop Left: should remove "c"
	popped, _ = ls.LeftPop(key)
	if string(popped) != "c" {
		t.Errorf("expected popped c, got %s", string(popped))
	}

	// Only "b" should remain
	length, _ = ls.Len(key)
	if length != 1 {
		t.Errorf("expected length 1, got %d", length)
	}
}

func TestListStorage_ReplaceInsertRemove(t *testing.T) {
	mockWal := &MockWal{}
	ls, _ := newListStorageWithWal(mockWal)

	key := "list2"
	_ = ls.RightPush(key, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, nil)

	// Replace index 1 with "x": [a, x, c]
	err := ls.ReplaceAtIndex(key, 1, []byte("x"))
	if err != nil {
		t.Fatalf("ReplaceAtIndex failed: %v", err)
	}
	all, _ := ls.Get(key)
	if string(all[1]) != "x" {
		t.Errorf("expected 'x' at index 1, got %s", string(all[1]))
	}

	// Insert "y" before "x": [a, y, x, c]
	newLen, _ := ls.Insert(key, []byte("x"), []byte("y"), true)
	if newLen != 4 {
		t.Errorf("expected len 4, got %d", newLen)
	}
	all, _ = ls.Get(key)
	if string(all[1]) != "y" || string(all[2]) != "x" {
		t.Errorf("expected [a, y, x, c], got %v", all)
	}

	// Insert "z" after "x": [a, y, x, z, c]
	_, _ = ls.Insert(key, []byte("x"), []byte("z"), false)

	// Remove "x"
	removed, _ := ls.Remove(key, 1, []byte("x"))
	if removed != 1 {
		t.Errorf("expected 1 item removed, got %d", removed)
	}

	all, _ = ls.Get(key)
	// should be [a, y, z, c]
	if len(all) != 4 || string(all[1]) != "y" || string(all[2]) != "z" {
		t.Errorf("expected [a, y, z, c], got %v", all)
	}
}

func TestListStorage_Move(t *testing.T) {
	mockWal := &MockWal{}
	ls, _ := newListStorageWithWal(mockWal)

	src := "src_list"
	dst := "dst_list"

	_ = ls.RightPush(src, [][]byte{[]byte("a"), []byte("b")}, nil)
	_ = ls.RightPush(dst, [][]byte{[]byte("c"), []byte("d")}, nil)

	// Move Left of src to Right of dst
	// popped "a" from src, src: [b], dst: [c, d, a]
	moved, err := ls.Move(src, dst, true, false)
	if err != nil {
		t.Fatalf("Move failed: %v", err)
	}
	if string(moved) != "a" {
		t.Errorf("expected moved element a, got %s", string(moved))
	}

	srcLen, _ := ls.Len(src)
	dstLen, _ := ls.Len(dst)
	if srcLen != 1 || dstLen != 3 {
		t.Errorf("lengths mismatch after move. src: %d, dst: %d", srcLen, dstLen)
	}

	dstItems, _ := ls.Get(dst)
	if string(dstItems[2]) != "a" {
		t.Errorf("expected 'a' at end of dst, got: %s", string(dstItems[2]))
	}
}

func TestListStorage_Expiration(t *testing.T) {
	mockWal := &MockWal{}
	ls, _ := newListStorageWithWal(mockWal)

	key := "explist"
	ttl := 10 * time.Millisecond
	expireTime := time.Now().Add(ttl)

	_ = ls.LeftPush(key, [][]byte{[]byte("val")}, &expireTime)

	// Wait for expiration
	time.Sleep(20 * time.Millisecond)

	length, _ := ls.Len(key)
	if length != 0 {
		t.Errorf("expected expired list length to be 0, got %d", length)
	}

	// Trigger manual cleanup and check heap/map emptiness
	ls.CleanupExpired()
	if len(ls.Data()) != 0 {
		t.Error("expired list should be purged from memory")
	}
}

func TestListStorage_WALRecovery(t *testing.T) {
	walPath := "test_list_wal_recovery.wal"
	defer os.Remove(walPath)

	w, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	ls, _ := newListStorageWithWal(w)

	key := "persistent_list"
	_ = ls.RightPush(key, [][]byte{[]byte("first"), []byte("second")}, nil)
	_ = ls.LeftPush(key, [][]byte{[]byte("head")}, nil) // [head, first, second]
	_ = ls.ReplaceAtIndex(key, 1, []byte("replaced"))   // [head, replaced, second]
	_ = ls.Delete(key)                                  // deleted

	_ = ls.RightPush(key, [][]byte{[]byte("new1"), []byte("new2")}, nil)

	w.CloseWal()

	// Recover into a new ListStorage engine
	w2, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.CloseWal()

	ls2, pm2 := newListStorageWithWal(w2)
	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	items, _ := ls2.Get(key)
	if len(items) != 2 || string(items[0]) != "new1" || string(items[1]) != "new2" {
		t.Errorf("recovery failed, expected [new1, new2], got: %v", items)
	}
}

func TestListStorage_SnapshotRecovery(t *testing.T) {
	walPath := "test_list_snap.wal"
	snapPath := "test_list_snap.snap"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, _ := wal.NewWal(walPath)
	snap := snapshot.NewSnapshot(snapPath)

	ls, pm := newListStorageWithWalAndSnap(w, &snap)

	key1 := "list_to_snap"
	key2 := "list_to_snap_expire"
	expireTime := time.Now().Add(1 * time.Hour)

	_ = ls.RightPush(key1, [][]byte{[]byte("a"), []byte("b")}, nil)
	_ = ls.RightPush(key2, [][]byte{[]byte("exp1")}, &expireTime)

	// Save snapshot (truncates WAL)
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	w.CloseWal()

	// Restart and recover
	w2, _ := wal.NewWal(walPath)
	defer w2.CloseWal()

	ls2, pm2 := newListStorageWithWalAndSnap(w2, &snap)
	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Check restored keys
	items1, _ := ls2.Get(key1)
	if len(items1) != 2 || string(items1[0]) != "a" || string(items1[1]) != "b" {
		t.Errorf("list1 restore failed, got: %v", items1)
	}

	items2, _ := ls2.Get(key2)
	if len(items2) != 1 || string(items2[0]) != "exp1" {
		t.Errorf("list2 restore failed, got: %v", items2)
	}

	// Verify expiration heap restored correctly
	if len(ls2.Expirations()) != 1 {
		t.Errorf("expected 1 expiration entry, got %d", len(ls2.Expirations()))
	}
}

func TestListStorage_PatternCountAndFind(t *testing.T) {
	mockWal := &MockWal{}
	ls, _ := newListStorageWithWal(mockWal)

	past := time.Now().Add(-10 * time.Minute)
	future := time.Now().Add(10 * time.Minute)

	_ = ls.RightPush("user:1", [][]byte{[]byte("val1")}, nil)
	_ = ls.RightPush("user:2", [][]byte{[]byte("val2")}, &future)
	_ = ls.RightPush("group:1", [][]byte{[]byte("gval1")}, nil)
	_ = ls.RightPush("user:expired", [][]byte{[]byte("exp")}, &past)

	// Test Counts (excluding expired key)
	cPrefix, err := ls.CountByPrefix("user:")
	if err != nil {
		t.Fatalf("CountByPrefix failed: %v", err)
	}
	if cPrefix != 2 {
		t.Errorf("expected CountByPrefix(\"user:\") to be 2, got %d", cPrefix)
	}

	cSuffix, err := ls.CountBySuffix(":1")
	if err != nil {
		t.Fatalf("CountBySuffix failed: %v", err)
	}
	if cSuffix != 2 {
		t.Errorf("expected CountBySuffix(\":1\") to be 2, got %d", cSuffix)
	}

	cRegex, err := ls.CountByRegex(`^user:\d+$`)
	if err != nil {
		t.Fatalf("CountByRegex failed: %v", err)
	}
	if cRegex != 2 {
		t.Errorf("expected CountByRegex(\"^user:\\d+$\") to be 2, got %d", cRegex)
	}

	// Test Find (excluding expired key)
	fPrefix, err := ls.FindByPrefix("user:")
	if err != nil {
		t.Fatalf("FindByPrefix failed: %v", err)
	}
	if len(fPrefix) != 2 || string(fPrefix["user:1"][0]) != "val1" || string(fPrefix["user:2"][0]) != "val2" {
		t.Errorf("expected FindByPrefix(\"user:\") to find user:1 and user:2, got %v", fPrefix)
	}

	fSuffix, err := ls.FindBySuffix(":1")
	if err != nil {
		t.Fatalf("FindBySuffix failed: %v", err)
	}
	if len(fSuffix) != 2 || string(fSuffix["user:1"][0]) != "val1" || string(fSuffix["group:1"][0]) != "gval1" {
		t.Errorf("expected FindBySuffix(\":1\") to find user:1 and group:1, got %v", fSuffix)
	}

	fRegex, err := ls.FindByRegex(`^group:\d+$`)
	if err != nil {
		t.Fatalf("FindByRegex failed: %v", err)
	}
	if len(fRegex) != 1 || string(fRegex["group:1"][0]) != "gval1" {
		t.Errorf("expected FindByRegex to find group:1, got %v", fRegex)
	}
}

func TestListStorage_PatternDelete(t *testing.T) {
	mockWal := &MockWal{}
	ls, _ := newListStorageWithWal(mockWal)

	past := time.Now().Add(-10 * time.Minute)

	_ = ls.RightPush("user:1", [][]byte{[]byte("val1")}, nil)
	_ = ls.RightPush("user:2", [][]byte{[]byte("val2")}, nil)
	_ = ls.RightPush("group:1", [][]byte{[]byte("gval1")}, nil)
	_ = ls.RightPush("user:expired", [][]byte{[]byte("exp")}, &past)

	// Delete by prefix (should delete user:1 and user:2, and purge user:expired)
	deleted, err := ls.DeleteByPrefix("user:")
	if err != nil {
		t.Fatalf("DeleteByPrefix failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 keys deleted, got %d", deleted)
	}

	// Verify they are really deleted
	len1, _ := ls.Len("user:1")
	len2, _ := ls.Len("user:2")
	len3, _ := ls.Len("user:expired")
	lenGroup, _ := ls.Len("group:1")
	if len1 != 0 || len2 != 0 || len3 != 0 || lenGroup != 1 {
		t.Errorf("unexpected key deletion state after DeleteByPrefix")
	}

	// Re-add and test Suffix delete
	_ = ls.RightPush("user:1", [][]byte{[]byte("val1")}, nil)
	deleted, err = ls.DeleteBySuffix(":1")
	if err != nil {
		t.Fatalf("DeleteBySuffix failed: %v", err)
	}
	if deleted != 2 { // user:1 and group:1
		t.Errorf("expected 2 keys deleted, got %d", deleted)
	}
}

func TestListStorage_PatternEdgeCases(t *testing.T) {
	mockWal := &MockWal{}
	ls, _ := newListStorageWithWal(mockWal)

	// Invalid regex pattern
	_, err := ls.CountByRegex("[")
	if err == nil {
		t.Error("expected error for invalid regex, got nil")
	}

	_, err = ls.FindByRegex("[")
	if err == nil {
		t.Error("expected error for invalid regex, got nil")
	}

	_, err = ls.DeleteByRegex("[")
	if err == nil {
		t.Error("expected error for invalid regex, got nil")
	}

	// Empty storage check
	c, _ := ls.CountByPrefix("test")
	if c != 0 {
		t.Errorf("expected count 0 on empty database, got %d", c)
	}

	f, _ := ls.FindByPrefix("test")
	if len(f) != 0 {
		t.Errorf("expected empty map on empty database, got %v", f)
	}

	d, _ := ls.DeleteByPrefix("test")
	if d != 0 {
		t.Errorf("expected deleted count 0 on empty database, got %d", d)
	}
}

