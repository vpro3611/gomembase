package tests

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/list_storage"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/set_storage"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

func newSetStorageWithWal(w wal.WalInterface) (*set_storage.SetStorage, *persistence.PersistenceManager) {
	pm := persistence.NewPersistenceManager(w, nil)
	ss := set_storage.NewSetStorage(pm)
	pm.RegisterEngine(ss)
	return ss, pm
}

func newSetStorageWithWalAndSnap(w wal.WalInterface, snap *snapshot.Snapshot) (*set_storage.SetStorage, *persistence.PersistenceManager) {
	pm := persistence.NewPersistenceManager(w, snap)
	ss := set_storage.NewSetStorage(pm)
	pm.RegisterEngine(ss)
	return ss, pm
}

func TestSetStorage_BasicOperations(t *testing.T) {
	mockWal := &MockWal{}
	ss, _ := newSetStorageWithWal(mockWal)

	key := "myset"
	// SAdd: add a, b, c. "a" is added twice, should only count once.
	added, err := ss.SAdd(key, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("a")}, nil)
	if err != nil {
		t.Fatalf("SAdd failed: %v", err)
	}
	if added != 3 {
		t.Errorf("expected 3 newly added elements, got %d", added)
	}

	card, _ := ss.SCard(key)
	if card != 3 {
		t.Errorf("expected cardinality 3, got %d", card)
	}

	// Membership checks
	isMem, _ := ss.SIsMember(key, []byte("b"))
	if !isMem {
		t.Error("expected 'b' to be a member")
	}

	mIsMem, _ := ss.SMIsMember(key, [][]byte{[]byte("a"), []byte("z")})
	if len(mIsMem) != 2 || !mIsMem[0] || mIsMem[1] {
		t.Errorf("unexpected SMIsMember results: %v", mIsMem)
	}

	// SMembers check
	members, _ := ss.SMembers(key)
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}

	// SRem: remove "b"
	removed, _ := ss.SRem(key, [][]byte{[]byte("b"), []byte("non_existent")})
	if removed != 1 {
		t.Errorf("expected 1 element removed, got %d", removed)
	}

	card, _ = ss.SCard(key)
	if card != 2 {
		t.Errorf("expected cardinality 2, got %d", card)
	}
}

func TestSetStorage_PopAndRandom(t *testing.T) {
	mockWal := &MockWal{}
	ss, _ := newSetStorageWithWal(mockWal)

	key := "randset"
	_, _ = ss.SAdd(key, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, nil)

	// SRandMember positive count: unique random elements
	rands, _ := ss.SRandMember(key, 2)
	if len(rands) != 2 || bytes.Equal(rands[0], rands[1]) {
		t.Errorf("expected 2 unique random elements, got: %s, %s", string(rands[0]), string(rands[1]))
	}

	// SRandMember negative count: allows duplicates
	randsDup, _ := ss.SRandMember(key, -5)
	if len(randsDup) != 5 {
		t.Errorf("expected 5 random elements, got %d", len(randsDup))
	}

	// SPop
	popped, err := ss.SPop(key, 2)
	if err != nil {
		t.Fatalf("SPop failed: %v", err)
	}
	if len(popped) != 2 {
		t.Errorf("expected 2 popped elements, got %d", len(popped))
	}

	card, _ := ss.SCard(key)
	if card != 1 {
		t.Errorf("expected 1 remaining element, got %d", card)
	}
}

func TestSetStorage_SetMath(t *testing.T) {
	mockWal := &MockWal{}
	ss, _ := newSetStorageWithWal(mockWal)

	set1 := "set1"
	set2 := "set2"
	set3 := "set3"

	_, _ = ss.SAdd(set1, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, nil)
	_, _ = ss.SAdd(set2, [][]byte{[]byte("b"), []byte("c"), []byte("d")}, nil)
	_, _ = ss.SAdd(set3, [][]byte{[]byte("c"), []byte("e")}, nil)

	// SInter: intersection of set1 and set2 should be [b, c]
	inter, _ := ss.SInter([]string{set1, set2})
	if len(inter) != 2 {
		t.Errorf("expected intersection card 2, got %d: %v", len(inter), inter)
	}

	// SInterCard with limit
	icard, _ := ss.SInterCard([]string{set1, set2}, 1)
	if icard != 1 {
		t.Errorf("expected intersection card with limit 1 to be 1, got %d", icard)
	}

	// SInterStore
	storedCard, _ := ss.SInterStore("inter_res", []string{set1, set2, set3})
	if storedCard != 1 {
		t.Errorf("expected stored intersection card to be 1, got %d", storedCard)
	}
	interStored, _ := ss.SMembers("inter_res")
	if len(interStored) != 1 || string(interStored[0]) != "c" {
		t.Errorf("expected SInterStore member 'c', got: %v", interStored)
	}

	// SUnion: union of set1 and set2 should be [a, b, c, d]
	union, _ := ss.SUnion([]string{set1, set2})
	if len(union) != 4 {
		t.Errorf("expected union size 4, got %d: %v", len(union), union)
	}

	// SDiff: set1 - set2 should be [a]
	diff, _ := ss.SDiff([]string{set1, set2})
	if len(diff) != 1 || string(diff[0]) != "a" {
		t.Errorf("expected diff to be [a], got: %v", diff)
	}
}

func TestSetStorage_PatternQueries(t *testing.T) {
	mockWal := &MockWal{}
	ss, _ := newSetStorageWithWal(mockWal)

	_, _ = ss.SAdd("user:1", [][]byte{[]byte("a")}, nil)
	_, _ = ss.SAdd("user:2", [][]byte{[]byte("b")}, nil)
	_, _ = ss.SAdd("group:1", [][]byte{[]byte("c")}, nil)

	// Count check
	cnt, _ := ss.CountByPrefix("user:")
	if cnt != 2 {
		t.Errorf("expected 2 keys matching prefix 'user:', got %d", cnt)
	}

	// Find check
	found, _ := ss.FindBySuffix(":1")
	if len(found) != 2 {
		t.Errorf("expected 2 keys matching suffix ':1', got %d", len(found))
	}

	// Delete check
	del, _ := ss.DeleteByRegex(`^user:\d+$`)
	if del != 2 {
		t.Errorf("expected 2 deleted keys, got %d", del)
	}

	card, _ := ss.SCard("user:1")
	if card != 0 {
		t.Error("user:1 should be deleted")
	}
}

func TestSetStorage_WALRecovery(t *testing.T) {
	walPath := "test_set_wal_recovery.wal"
	defer os.Remove(walPath)

	w, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	ss, _ := newSetStorageWithWal(w)

	key1 := "persistent_set"
	key2 := "popped_set"
	_, _ = ss.SAdd(key1, [][]byte{[]byte("a"), []byte("b")}, nil)
	_, _ = ss.SAdd(key2, [][]byte{[]byte("x"), []byte("y")}, nil)

	// Mutations to log to WAL
	_, _ = ss.SAdd(key1, [][]byte{[]byte("c")}, nil)
	_, _ = ss.SRem(key1, [][]byte{[]byte("a")})
	_, _ = ss.SPop(key2, 1) // Pop one random member. Pop logs exact popped value to WAL

	w.CloseWal()

	// Reopen
	w2, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.CloseWal()

	ss2, pm2 := newSetStorageWithWal(w2)
	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Check key1 state: should have "b" and "c"
	members, _ := ss2.SMembers(key1)
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %v", members)
	}

	// Check key2 size: should be exactly 1
	card, _ := ss2.SCard(key2)
	if card != 1 {
		t.Errorf("expected key2 size 1, got %d", card)
	}
}

func TestSetStorage_SnapshotRecovery(t *testing.T) {
	walPath := "test_set_snap.wal"
	snapPath := "test_set_snap.snap"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, _ := wal.NewWal(walPath)
	snap := snapshot.NewSnapshot(snapPath)

	ss, pm := newSetStorageWithWalAndSnap(w, &snap)

	key := "snapset"
	_, _ = ss.SAdd(key, [][]byte{[]byte("member1"), []byte("member2")}, nil)

	// Save snapshot
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	w.CloseWal()

	// Restart
	w2, _ := wal.NewWal(walPath)
	defer w2.CloseWal()

	ss2, pm2 := newSetStorageWithWalAndSnap(w2, &snap)
	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	card, _ := ss2.SCard(key)
	if card != 2 {
		t.Errorf("expected restored cardinality 2, got %d", card)
	}
}

func TestPersistence_KVListAndSetJointRecovery(t *testing.T) {
	walPath := "test_kv_list_set_joint.wal"
	snapPath := "test_kv_list_set_joint.snap"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, _ := wal.NewWal(walPath)
	snap := snapshot.NewSnapshot(snapPath)

	// Setup all three engines registered to same PersistenceManager
	pm := persistence.NewPersistenceManager(w, &snap)
	kv := storage.NewStorage(pm)
	listEng := list_storage.NewListStorage(pm)
	setEng := set_storage.NewSetStorage(pm)

	pm.RegisterEngine(kv)
	pm.RegisterEngine(listEng)
	pm.RegisterEngine(setEng)

	// 1. Write basic state
	_ = kv.Set("kv_key", []byte("kv_val"), storage.NewPayloadMetadata(time.Now(), nil))
	_ = listEng.LeftPush("list_key", [][]byte{[]byte("list_val")}, nil)
	_, _ = setEng.SAdd("set_key", [][]byte{[]byte("set_val")}, nil)

	// 2. Save snapshot
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// 3. Write additional state post-snapshot (to WAL)
	_ = kv.Set("kv_key2", []byte("kv_val2"), storage.NewPayloadMetadata(time.Now(), nil))
	_ = listEng.LeftPush("list_key", [][]byte{[]byte("list_val2")}, nil)
	_, _ = setEng.SAdd("set_key", [][]byte{[]byte("set_val2")}, nil)

	w.CloseWal()

	// 4. Shutdown & restore under new instances
	w2, _ := wal.NewWal(walPath)
	defer w2.CloseWal()

	pm2 := persistence.NewPersistenceManager(w2, &snap)
	kv2 := storage.NewStorage(pm2)
	listEng2 := list_storage.NewListStorage(pm2)
	setEng2 := set_storage.NewSetStorage(pm2)

	pm2.RegisterEngine(kv2)
	pm2.RegisterEngine(listEng2)
	pm2.RegisterEngine(setEng2)

	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Joint restore failed: %v", err)
	}

	// 5. Verify all three engines recovered correctly
	v, _ := kv2.Get("kv_key2")
	if string(v) != "kv_val2" {
		t.Errorf("expected kv_val2, got: %s", string(v))
	}

	l, _ := listEng2.Get("list_key")
	if len(l) != 2 || string(l[0]) != "list_val2" {
		t.Errorf("list recovery failed, got: %v", l)
	}

	s, _ := setEng2.SMembers("set_key")
	if len(s) != 2 {
		t.Errorf("set recovery failed, got: %v", s)
	}
}
