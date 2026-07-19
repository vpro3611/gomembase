package tests

import (
	"os"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/list_storage"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/set_storage"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
	"github.com/vpro3611/gomembase.git/pkg/zset_storage"
)

func newZSetStorageWithWal(w wal.WalInterface) (*zset_storage.ZSetStorage, *persistence.PersistenceManager) {
	pm := persistence.NewPersistenceManager(w, nil)
	zs := zset_storage.NewZSetStorage(pm)
	pm.RegisterEngine(zs)
	return zs, pm
}

func newZSetStorageWithWalAndSnap(w wal.WalInterface, snap *snapshot.Snapshot) (*zset_storage.ZSetStorage, *persistence.PersistenceManager) {
	pm := persistence.NewPersistenceManager(w, snap)
	zs := zset_storage.NewZSetStorage(pm)
	pm.RegisterEngine(zs)
	return zs, pm
}

func TestZSetStorage_BasicOperations(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	key := "myzset"

	// 1. ZAdd - new additions
	added, err := zs.ZAdd(key, []zset_storage.ZSetMember{
		{Member: []byte("a"), Score: 10.5},
		{Member: []byte("b"), Score: 20.0},
		{Member: []byte("c"), Score: 15.0},
	}, nil)
	if err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}
	if added != 3 {
		t.Errorf("expected 3 elements added, got %d", added)
	}

	card, _ := zs.ZCard(key)
	if card != 3 {
		t.Errorf("expected cardinality 3, got %d", card)
	}

	// ZScore verification
	score, ok, _ := zs.ZScore(key, []byte("c"))
	if !ok || score != 15.0 {
		t.Errorf("expected score 15.0 for 'c', got %f, ok: %t", score, ok)
	}

	// 2. ZAdd - score update
	added, _ = zs.ZAdd(key, []zset_storage.ZSetMember{
		{Member: []byte("c"), Score: 25.5}, // score changed
		{Member: []byte("d"), Score: 5.0},  // new member
	}, nil)
	if added != 1 {
		t.Errorf("expected 1 element added, got %d", added)
	}

	score, _, _ = zs.ZScore(key, []byte("c"))
	if score != 25.5 {
		t.Errorf("expected updated score 25.5 for 'c', got %f", score)
	}

	// 3. Ranks (0-indexed ascending)
	// sorted order by score: d(5), a(10.5), b(20), c(25.5)
	rank, _, _ := zs.ZRank(key, []byte("a"))
	if rank != 1 { // 'a' is rank 1
		t.Errorf("expected rank 1 for 'a', got %d", rank)
	}

	revRank, _, _ := zs.ZRevRank(key, []byte("d"))
	if revRank != 3 { // 'd' is reverse rank 3 (highest score is rank 0)
		t.Errorf("expected revRank 3 for 'd', got %d", revRank)
	}

	// 4. ZCount
	cnt, _ := zs.ZCount(key, 10.0, 25.0)
	if cnt != 2 { // 'a'(10.5) and 'b'(20.0)
		t.Errorf("expected count 2 in range [10, 25], got %d", cnt)
	}

	// 5. ZRem
	rem, _ := zs.ZRem(key, [][]byte{[]byte("a"), []byte("non_existent")})
	if rem != 1 {
		t.Errorf("expected 1 element removed, got %d", rem)
	}

	card, _ = zs.ZCard(key)
	if card != 3 {
		t.Errorf("expected size 3 after ZRem, got %d", card)
	}
}

func TestZSetStorage_IncrByAndPops(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	key := "zset_pop"
	_, _ = zs.ZAdd(key, []zset_storage.ZSetMember{
		{Member: []byte("a"), Score: 10},
		{Member: []byte("b"), Score: 20},
		{Member: []byte("c"), Score: 30},
	}, nil)

	// ZIncrBy
	newScore, err := zs.ZIncrBy(key, 15.5, []byte("a"))
	if err != nil {
		t.Fatalf("ZIncrBy failed: %v", err)
	}
	if newScore != 25.5 {
		t.Errorf("expected score 25.5 for 'a', got %f", newScore)
	}

	// ZPopMax (returns highest: c(30), a(25.5))
	popped, err := zs.ZPopMax(key, 2)
	if err != nil {
		t.Fatalf("ZPopMax failed: %v", err)
	}
	if len(popped) != 2 || string(popped[0].Member) != "c" || string(popped[1].Member) != "a" {
		t.Errorf("unexpected ZPopMax result: %v", popped)
	}

	// ZPopMin (returns remaining: b(20))
	poppedMin, _ := zs.ZPopMin(key, 1)
	if len(poppedMin) != 1 || string(poppedMin[0].Member) != "b" {
		t.Errorf("unexpected ZPopMin result: %v", poppedMin)
	}

	card, _ := zs.ZCard(key)
	if card != 0 {
		t.Errorf("expected zset to be empty, size is %d", card)
	}
}

func TestZSetStorage_Ranges(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	key := "zset_range"
	_, _ = zs.ZAdd(key, []zset_storage.ZSetMember{
		{Member: []byte("a"), Score: 1.0},
		{Member: []byte("b"), Score: 2.0},
		{Member: []byte("c"), Score: 3.0},
		{Member: []byte("d"), Score: 4.0},
	}, nil)

	// ZRange by index (a, b, c)
	rng, _ := zs.ZRange(key, 0, 2)
	if len(rng) != 3 || string(rng[0].Member) != "a" || string(rng[2].Member) != "c" {
		t.Errorf("unexpected ZRange result: %v", rng)
	}

	// ZRevRange by index (d, c)
	revRng, _ := zs.ZRevRange(key, 0, 1)
	if len(revRng) != 2 || string(revRng[0].Member) != "d" || string(revRng[1].Member) != "c" {
		t.Errorf("unexpected ZRevRange result: %v", revRng)
	}

	// ZRangeByScore
	scoreRng, _ := zs.ZRangeByScore(key, 1.5, 3.5, 0, -1)
	if len(scoreRng) != 2 || string(scoreRng[0].Member) != "b" || string(scoreRng[1].Member) != "c" {
		t.Errorf("unexpected ZRangeByScore result: %v", scoreRng)
	}

	// ZRevRangeByScore
	revScoreRng, _ := zs.ZRevRangeByScore(key, 3.5, 1.5, 0, -1)
	if len(revScoreRng) != 2 || string(revScoreRng[0].Member) != "c" || string(revScoreRng[1].Member) != "b" {
		t.Errorf("unexpected ZRevRangeByScore result: %v", revScoreRng)
	}
}

func TestZSetStorage_PatternQueries(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	_, _ = zs.ZAdd("user:1", []zset_storage.ZSetMember{{Member: []byte("a"), Score: 1.0}}, nil)
	_, _ = zs.ZAdd("user:2", []zset_storage.ZSetMember{{Member: []byte("b"), Score: 2.0}}, nil)
	_, _ = zs.ZAdd("group:1", []zset_storage.ZSetMember{{Member: []byte("c"), Score: 3.0}}, nil)

	// Count check
	cnt, _ := zs.CountByPrefix("user:")
	if cnt != 2 {
		t.Errorf("expected 2 keys, got %d", cnt)
	}

	// Find check
	found, _ := zs.FindBySuffix(":1")
	if len(found) != 2 {
		t.Errorf("expected 2 keys, got %d", len(found))
	}

	// Delete check
	del, _ := zs.DeleteByRegex(`^user:\d+$`)
	if del != 2 {
		t.Errorf("expected 2 deleted keys, got %d", del)
	}

	card, _ := zs.ZCard("user:1")
	if card != 0 {
		t.Error("user:1 should be deleted")
	}
}

func TestZSetStorage_RecoveryAndSnapshot(t *testing.T) {
	walPath := "test_zset_recovery.wal"
	snapPath := "test_zset_recovery.snap"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, _ := wal.NewWal(walPath)
	snap := snapshot.NewSnapshot(snapPath)

	zs, pm := newZSetStorageWithWalAndSnap(w, &snap)

	key := "zset_snap"
	_, _ = zs.ZAdd(key, []zset_storage.ZSetMember{
		{Member: []byte("m1"), Score: 5.5},
		{Member: []byte("m2"), Score: 10.0},
	}, nil)

	// Save snapshot
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Write post-snapshot mutations to WAL
	_, _ = zs.ZAdd(key, []zset_storage.ZSetMember{{Member: []byte("m3"), Score: 15.0}}, nil)
	_, _ = zs.ZRem(key, [][]byte{[]byte("m1")})

	w.CloseWal()

	// Restart
	w2, _ := wal.NewWal(walPath)
	defer w2.CloseWal()

	zs2, pm2 := newZSetStorageWithWalAndSnap(w2, &snap)
	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verification of recovered state: key should hold m2(10.0) and m3(15.0)
	card, _ := zs2.ZCard(key)
	if card != 2 {
		t.Errorf("expected cardinality 2, got %d", card)
	}

	score, ok, _ := zs2.ZScore(key, []byte("m1"))
	if ok {
		t.Errorf("expected 'm1' to be deleted, found score %f", score)
	}

	score, ok, _ = zs2.ZScore(key, []byte("m3"))
	if !ok || score != 15.0 {
		t.Errorf("expected 'm3' to exist with score 15.0, got %f, ok: %t", score, ok)
	}
}

func TestPersistence_KVListSetAndZSetJointRecovery(t *testing.T) {
	walPath := "test_joint_all.wal"
	snapPath := "test_joint_all.snap"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, _ := wal.NewWal(walPath)
	snap := snapshot.NewSnapshot(snapPath)

	pm := persistence.NewPersistenceManager(w, &snap)
	kv := storage.NewStorage(pm)
	listEng := list_storage.NewListStorage(pm)
	setEng := set_storage.NewSetStorage(pm)
	zsetEng := zset_storage.NewZSetStorage(pm)

	pm.RegisterEngine(kv)
	pm.RegisterEngine(listEng)
	pm.RegisterEngine(setEng)
	pm.RegisterEngine(zsetEng)

	// 1. Initial writes
	_ = kv.Set("kvKey", []byte("kvVal"), storage.NewPayloadMetadata(time.Now(), nil))
	_ = listEng.LeftPush("listKey", [][]byte{[]byte("listVal")}, nil)
	_, _ = setEng.SAdd("setKey", [][]byte{[]byte("setVal")}, nil)
	_, _ = zsetEng.ZAdd("zsetKey", []zset_storage.ZSetMember{{Member: []byte("zsetVal"), Score: 1.0}}, nil)

	// 2. Save snapshot
	if err := pm.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// 3. Subsequent WAL updates
	_ = kv.Set("kvKey2", []byte("kvVal2"), storage.NewPayloadMetadata(time.Now(), nil))
	_ = listEng.LeftPush("listKey", [][]byte{[]byte("listVal2")}, nil)
	_, _ = setEng.SAdd("setKey", [][]byte{[]byte("setVal2")}, nil)
	_, _ = zsetEng.ZAdd("zsetKey", []zset_storage.ZSetMember{{Member: []byte("zsetVal2"), Score: 2.0}}, nil)

	w.CloseWal()

	// 4. Reopen and Recover
	w2, _ := wal.NewWal(walPath)
	defer w2.CloseWal()

	pm2 := persistence.NewPersistenceManager(w2, &snap)
	kv2 := storage.NewStorage(pm2)
	listEng2 := list_storage.NewListStorage(pm2)
	setEng2 := set_storage.NewSetStorage(pm2)
	zsetEng2 := zset_storage.NewZSetStorage(pm2)

	pm2.RegisterEngine(kv2)
	pm2.RegisterEngine(listEng2)
	pm2.RegisterEngine(setEng2)
	pm2.RegisterEngine(zsetEng2)

	if err := pm2.Restore(nil); err != nil {
		t.Fatalf("Joint restore failed: %v", err)
	}

	// 5. Verify correctness across all four engines
	v, _ := kv2.Get("kvKey2")
	if string(v) != "kvVal2" {
		t.Errorf("KV failed: expected kvVal2, got %s", string(v))
	}

	l, _ := listEng2.Get("listKey")
	if len(l) != 2 || string(l[0]) != "listVal2" {
		t.Errorf("List failed: expected length 2 and first elem listVal2, got %v", l)
	}

	s, _ := setEng2.SMembers("setKey")
	if len(s) != 2 {
		t.Errorf("Set failed: expected size 2, got %v", s)
	}

	zsCard, _ := zsetEng2.ZCard("zsetKey")
	if zsCard != 2 {
		t.Errorf("ZSet failed: expected cardinality 2, got %d", zsCard)
	}
}

func TestZSetStorage_ExpirationsAndHeap(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	past := time.Now().Add(-10 * time.Minute)
	future := time.Now().Add(10 * time.Minute)

	_, _ = zs.ZAdd("exp1", []zset_storage.ZSetMember{{Member: []byte("m1"), Score: 1.0}}, &past)
	_, _ = zs.ZAdd("exp2", []zset_storage.ZSetMember{{Member: []byte("m2"), Score: 2.0}}, &future)

	// Heap check
	if zs.Expirations().Len() != 2 {
		t.Errorf("expected heap length 2, got %d", zs.Expirations().Len())
	}

	// 1. ZScore on expired key should lazily delete it
	_, ok, _ := zs.ZScore("exp1", []byte("m1"))
	if ok {
		t.Error("expected expired key score lookup to return false")
	}

	// Verify key deleted from data map
	if _, exists := zs.Data()["exp1"]; exists {
		t.Error("expected key exp1 to be deleted from data")
	}

	// 2. ZCard on expired key should lazily delete it
	_, _ = zs.ZAdd("exp3", []zset_storage.ZSetMember{{Member: []byte("m3"), Score: 3.0}}, &past)
	card, _ := zs.ZCard("exp3")
	if card != 0 {
		t.Errorf("expected 0 cardinality for expired key, got %d", card)
	}

	// 3. ZRank on expired key
	_, _ = zs.ZAdd("exp4", []zset_storage.ZSetMember{{Member: []byte("m4"), Score: 4.0}}, &past)
	_, ok, _ = zs.ZRank("exp4", []byte("m4"))
	if ok {
		t.Error("expected ZRank to return false for expired key")
	}

	// 4. ZRevRank on expired key
	_, _ = zs.ZAdd("exp4_rev", []zset_storage.ZSetMember{{Member: []byte("m4"), Score: 4.0}}, &past)
	_, ok, _ = zs.ZRevRank("exp4_rev", []byte("m4"))
	if ok {
		t.Error("expected ZRevRank to return false for expired key")
	}

	// 5. ZCount on expired key
	_, _ = zs.ZAdd("exp4_cnt", []zset_storage.ZSetMember{{Member: []byte("m4"), Score: 4.0}}, &past)
	cnt, _ := zs.ZCount("exp4_cnt", 1.0, 5.0)
	if cnt != 0 {
		t.Errorf("expected ZCount 0 for expired key, got %d", cnt)
	}

	// 6. CleanupExpired
	_, _ = zs.ZAdd("exp5", []zset_storage.ZSetMember{{Member: []byte("m5"), Score: 5.0}}, &past)
	zs.CleanupExpired()
	if _, exists := zs.Data()["exp5"]; exists {
		t.Error("expected key exp5 to be deleted by CleanupExpired")
	}
}

func TestZSetStorage_ResetAndClear(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	_, _ = zs.ZAdd("k1", []zset_storage.ZSetMember{{Member: []byte("m1"), Score: 1.0}}, nil)

	_ = zs.Clear()
	if len(zs.Data()) != 0 {
		t.Error("expected data to be empty after Clear")
	}

	_, _ = zs.ZAdd("k2", []zset_storage.ZSetMember{{Member: []byte("m2"), Score: 2.0}}, nil)
	_ = zs.Reset()
	if len(zs.Data()) != 0 {
		t.Error("expected data to be empty after Reset")
	}
}

func TestZSetStorage_AllPatternQueries(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	_, _ = zs.ZAdd("prefix:1:suffix", []zset_storage.ZSetMember{{Member: []byte("a"), Score: 1.0}}, nil)
	_, _ = zs.ZAdd("prefix:2:suffix", []zset_storage.ZSetMember{{Member: []byte("b"), Score: 2.0}}, nil)
	_, _ = zs.ZAdd("other:3", []zset_storage.ZSetMember{{Member: []byte("c"), Score: 3.0}}, nil)

	// CountBySuffix
	cntSuffix, _ := zs.CountBySuffix("suffix")
	if cntSuffix != 2 {
		t.Errorf("expected count 2 for suffix, got %d", cntSuffix)
	}

	// CountByRegex
	cntRegex, _ := zs.CountByRegex(`^prefix:\d+:suffix$`)
	if cntRegex != 2 {
		t.Errorf("expected count 2 for regex, got %d", cntRegex)
	}

	// FindByPrefix
	foundPrefix, _ := zs.FindByPrefix("prefix:")
	if len(foundPrefix) != 2 {
		t.Errorf("expected 2 keys found by prefix, got %d", len(foundPrefix))
	}

	// FindByRegex
	foundRegex, _ := zs.FindByRegex(`^prefix:\d+:suffix$`)
	if len(foundRegex) != 2 {
		t.Errorf("expected 2 keys found by regex, got %d", len(foundRegex))
	}

	// DeleteByPrefix
	delPrefix, _ := zs.DeleteByPrefix("prefix:")
	if delPrefix != 2 {
		t.Errorf("expected 2 keys deleted by prefix, got %d", delPrefix)
	}

	// DeleteBySuffix
	_, _ = zs.ZAdd("test:suffix", []zset_storage.ZSetMember{{Member: []byte("a"), Score: 1.0}}, nil)
	delSuffix, _ := zs.DeleteBySuffix("suffix")
	if delSuffix != 1 {
		t.Errorf("expected 1 key deleted by suffix, got %d", delSuffix)
	}
}

func TestZSetStorage_ProcessWalEntryErrors(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	// Test malformed actions (should return nil and not panic)
	err := zs.ProcessWalEntry("ZADD", []string{}) // Empty args
	if err != nil {
		t.Errorf("expected nil error for empty ZADD args, got %v", err)
	}

	err = zs.ProcessWalEntry("ZREM", []string{}) // Empty args
	if err != nil {
		t.Errorf("expected nil error for empty ZREM args, got %v", err)
	}

	err = zs.ProcessWalEntry("DELETE", []string{}) // Empty args
	if err != nil {
		t.Errorf("expected nil error for empty DELETE args, got %v", err)
	}

	err = zs.ProcessWalEntry("ZPOPMAX", []string{}) // Empty args
	if err != nil {
		t.Errorf("expected nil error for empty ZPOPMAX args, got %v", err)
	}

	err = zs.ProcessWalEntry("ZINCRBY", []string{}) // Empty args
	if err != nil {
		t.Errorf("expected nil error for empty ZINCRBY args, got %v", err)
	}
}

func TestZSetStorage_RangeEdgeCases(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)
	key := "range_edge"

	// Query range on non-existent key
	rng, _ := zs.ZRange(key, 0, 10)
	if len(rng) != 0 {
		t.Errorf("expected empty range for non-existent key, got %v", rng)
	}

	// ZRevRange on non-existent key
	revRngNonExistent, _ := zs.ZRevRange(key, 0, 10)
	if len(revRngNonExistent) != 0 {
		t.Errorf("expected empty rev range for non-existent key, got %v", revRngNonExistent)
	}

	// Add entries
	_, _ = zs.ZAdd(key, []zset_storage.ZSetMember{
		{Member: []byte("a"), Score: 1.0},
		{Member: []byte("b"), Score: 2.0},
	}, nil)

	// Out of bounds / negative indexes
	rng, _ = zs.ZRange(key, -10, 10)
	if len(rng) != 2 {
		t.Errorf("expected 2 elements for out-of-bound indexes, got %d", len(rng))
	}

	rng, _ = zs.ZRange(key, 5, 2)
	if len(rng) != 0 {
		t.Errorf("expected empty range when start > stop, got %v", rng)
	}

	// RevRange negative indexes
	revRng, _ := zs.ZRevRange(key, -10, 10)
	if len(revRng) != 2 {
		t.Errorf("expected 2 elements for out-of-bound indexes in RevRange, got %d", len(revRng))
	}

	revRng, _ = zs.ZRevRange(key, 5, 2)
	if len(revRng) != 0 {
		t.Errorf("expected empty rev range when start > stop, got %v", revRng)
	}

	// ZRangeByScore offset out of bounds
	scoreRng, _ := zs.ZRangeByScore(key, 1.0, 2.0, 5, 2)
	if len(scoreRng) != 0 {
		t.Errorf("expected empty score range for offset out-of-bounds, got %v", scoreRng)
	}

	// ZRevRangeByScore offset out of bounds
	revScoreRng, _ := zs.ZRevRangeByScore(key, 2.0, 1.0, 5, 2)
	if len(revScoreRng) != 0 {
		t.Errorf("expected empty rev score range for offset out-of-bounds, got %v", revScoreRng)
	}
}

func TestZSetStorage_DirectDelete(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	_, _ = zs.ZAdd("delkey", []zset_storage.ZSetMember{{Member: []byte("m"), Score: 1.0}}, nil)
	err := zs.Delete("delkey")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	card, _ := zs.ZCard("delkey")
	if card != 0 {
		t.Errorf("expected 0 cardinality after delete, got %d", card)
	}

	// Verify expiration map accessor
	if zs.ExpirationMap() == nil {
		t.Error("expected ExpirationMap() to not be nil")
	}
}

func TestZSetExpirationEntry_Helpers(t *testing.T) {
	now := time.Now()
	entry := zset_storage.NewZSetExpirationEntry("key", now)
	if entry.Key() != "key" {
		t.Errorf("expected Key() 'key', got %s", entry.Key())
	}
	if !entry.ExpiresAt().Equal(now) {
		t.Errorf("expected ExpiresAt() %v, got %v", now, entry.ExpiresAt())
	}
	entry.SetIndex(5)
	if entry.Index() != 5 {
		t.Errorf("expected Index() 5, got %d", entry.Index())
	}
}

func TestBufferedWal_WriteRawAndBuffer(t *testing.T) {
	mockPhys := &MockWal{}
	bufferedW := wal.NewBufferedWal(mockPhys)
	defer bufferedW.CloseWal()

	_, _ = bufferedW.WriteRaw("rawLine\n")
	if bufferedW.Buffer() == nil {
		t.Error("expected Buffer() to not be nil")
	}
	contents := bufferedW.BufferContent()
	if len(contents) != 1 || contents[0] != "rawLine\n" {
		t.Errorf("expected rawLine in buffer, got %v", contents)
	}
}

func TestZSetStorage_PatternExpiredKeys(t *testing.T) {
	mockWal := &MockWal{}
	zs, _ := newZSetStorageWithWal(mockWal)

	past := time.Now().Add(-10 * time.Minute)
	_, _ = zs.ZAdd("prefix:expired", []zset_storage.ZSetMember{{Member: []byte("a"), Score: 1.0}}, &past)
	_, _ = zs.ZAdd("prefix:alive", []zset_storage.ZSetMember{{Member: []byte("b"), Score: 2.0}}, nil)

	// This should trigger deleteExpiredKeys on prefix:expired
	found, _ := zs.FindByPrefix("prefix:")
	if len(found) != 1 {
		t.Errorf("expected 1 key, got %d", len(found))
	}
	if _, ok := found["prefix:alive"]; !ok {
		t.Errorf("expected only prefix:alive, got %v", found)
	}

	// Verify prefix:expired is deleted
	if _, exists := zs.Data()["prefix:expired"]; exists {
		t.Error("expected prefix:expired to be deleted")
	}
}

