package tests

import (
	"fmt"
	"os"
	"strconv"
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

// TestKV_Benchmark_1000Requests simulates 1000 KV SET and GET requests
func TestKV_Benchmark_1000Requests(t *testing.T) {
	walPath := "kv_bench.wal"
	snapPath := "kv_bench.rdb"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			_ = bw.SyncWal()
		}
	}()
	defer ticker.Stop()

	snap := snapshot.NewSnapshot(snapPath)
	pm := persistence.NewPersistenceManager(bw, &snap)
	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)

	const count = 1000

	// Benchmark SET
	startSet := time.Now()
	for i := 0; i < count; i++ {
		key := "key:" + strconv.Itoa(i)
		val := []byte("value:" + strconv.Itoa(i))
		err := s.Set(key, val, storage.NewPayloadMetadata(time.Now(), nil))
		if err != nil {
			t.Fatalf("SET failed: %v", err)
		}
	}
	elapsedSet := time.Since(startSet)

	// Benchmark GET & correctness check
	startGet := time.Now()
	for i := 0; i < count; i++ {
		key := "key:" + strconv.Itoa(i)
		val, err := s.Get(key)
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		expected := "value:" + strconv.Itoa(i)
		if string(val) != expected {
			t.Errorf("correctness check failed: expected %s, got %s", expected, string(val))
		}
	}
	elapsedGet := time.Since(startGet)

	fmt.Printf("\n--- KV Benchmark (1000 requests) ---\n")
	fmt.Printf("SET: Total Time = %v, Avg/Op = %v\n", elapsedSet, elapsedSet/count)
	fmt.Printf("GET: Total Time = %v, Avg/Op = %v\n", elapsedGet, elapsedGet/count)
}

// TestList_Benchmark_1000Requests simulates 1000 List LeftPush and RightPop requests
func TestList_Benchmark_1000Requests(t *testing.T) {
	walPath := "list_bench.wal"
	snapPath := "list_bench.rdb"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			_ = bw.SyncWal()
		}
	}()
	defer ticker.Stop()

	snap := snapshot.NewSnapshot(snapPath)
	pm := persistence.NewPersistenceManager(bw, &snap)
	ls := list_storage.NewListStorage(pm)
	pm.RegisterEngine(ls)

	const count = 1000
	key := "benchlist"

	// Benchmark LeftPush
	startPush := time.Now()
	for i := 0; i < count; i++ {
		item := []byte("item:" + strconv.Itoa(i))
		err := ls.LeftPush(key, [][]byte{item}, nil)
		if err != nil {
			t.Fatalf("LeftPush failed: %v", err)
		}
	}
	elapsedPush := time.Since(startPush)

	// Check correct length
	length, _ := ls.Len(key)
	if length != count {
		t.Errorf("expected list length %d, got %d", count, length)
	}

	// Benchmark RightPop & correctness check
	startPop := time.Now()
	for i := 0; i < count; i++ {
		val, err := ls.RightPop(key)
		if err != nil {
			t.Fatalf("RightPop failed: %v", err)
		}
		expected := "item:" + strconv.Itoa(i)
		if string(val) != expected {
			t.Errorf("correctness check failed: expected %s, got %s", expected, string(val))
		}
	}
	elapsedPop := time.Since(startPop)

	fmt.Printf("\n--- List Benchmark (1000 requests) ---\n")
	fmt.Printf("PUSH: Total Time = %v, Avg/Op = %v\n", elapsedPush, elapsedPush/count)
	fmt.Printf("POP:  Total Time = %v, Avg/Op = %v\n", elapsedPop, elapsedPop/count)
}

// TestSet_Benchmark_1000Requests simulates 1000 Set SAdd and SIsMember requests
func TestSet_Benchmark_1000Requests(t *testing.T) {
	walPath := "set_bench.wal"
	snapPath := "set_bench.rdb"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			_ = bw.SyncWal()
		}
	}()
	defer ticker.Stop()

	snap := snapshot.NewSnapshot(snapPath)
	pm := persistence.NewPersistenceManager(bw, &snap)
	ss := set_storage.NewSetStorage(pm)
	pm.RegisterEngine(ss)

	const count = 1000
	key := "benchset"

	// Benchmark SAdd
	startAdd := time.Now()
	for i := 0; i < count; i++ {
		member := []byte("member:" + strconv.Itoa(i))
		_, err := ss.SAdd(key, [][]byte{member}, nil)
		if err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
	}
	elapsedAdd := time.Since(startAdd)

	// Check correct card
	card, _ := ss.SCard(key)
	if card != count {
		t.Errorf("expected set size %d, got %d", count, card)
	}

	// Benchmark SIsMember & correctness check
	startIsMem := time.Now()
	for i := 0; i < count; i++ {
		member := []byte("member:" + strconv.Itoa(i))
		isMem, err := ss.SIsMember(key, member)
		if err != nil {
			t.Fatalf("SIsMember failed: %v", err)
		}
		if !isMem {
			t.Errorf("correctness check failed: expected member:%d to exist", i)
		}
	}
	elapsedIsMem := time.Since(startIsMem)

	fmt.Printf("\n--- Set Benchmark (1000 requests) ---\n")
	fmt.Printf("SADD:      Total Time = %v, Avg/Op = %v\n", elapsedAdd, elapsedAdd/count)
	fmt.Printf("SISMEMBER: Total Time = %v, Avg/Op = %v\n", elapsedIsMem, elapsedIsMem/count)
}

// TestZSet_Benchmark_1000Requests simulates 1000 Sorted Set ZAdd and ZScore requests
func TestZSet_Benchmark_1000Requests(t *testing.T) {
	walPath := "zset_bench.wal"
	snapPath := "zset_bench.rdb"
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	w, err := wal.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			_ = bw.SyncWal()
		}
	}()
	defer ticker.Stop()

	snap := snapshot.NewSnapshot(snapPath)
	pm := persistence.NewPersistenceManager(bw, &snap)
	zs := zset_storage.NewZSetStorage(pm)
	pm.RegisterEngine(zs)

	const count = 1000
	key := "benchzset"

	// Benchmark ZAdd
	startAdd := time.Now()
	for i := 0; i < count; i++ {
		member := []byte("member:" + strconv.Itoa(i))
		score := float64(i)
		_, err := zs.ZAdd(key, []zset_storage.ZSetMember{{Member: member, Score: score}}, nil)
		if err != nil {
			t.Fatalf("ZAdd failed: %v", err)
		}
	}
	elapsedAdd := time.Since(startAdd)

	// Check correct card
	card, _ := zs.ZCard(key)
	if card != count {
		t.Errorf("expected sorted set size %d, got %d", count, card)
	}

	// Benchmark ZScore & correctness check
	startScore := time.Now()
	for i := 0; i < count; i++ {
		member := []byte("member:" + strconv.Itoa(i))
		score, ok, err := zs.ZScore(key, member)
		if err != nil {
			t.Fatalf("ZScore failed: %v", err)
		}
		if !ok || score != float64(i) {
			t.Errorf("correctness check failed: expected member:%d to exist with score %f, got %f", i, float64(i), score)
		}
	}
	elapsedScore := time.Since(startScore)

	fmt.Printf("\n--- ZSet Benchmark (1000 requests) ---\n")
	fmt.Printf("ZADD:   Total Time = %v, Avg/Op = %v\n", elapsedAdd, elapsedAdd/count)
	fmt.Printf("ZSCORE: Total Time = %v, Avg/Op = %v\n", elapsedScore, elapsedScore/count)
}

// Standard Go Benchmarks for detailed profiling

func BenchmarkKV_Set(b *testing.B) {
	walPath := "kv_bench_std.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	pm := persistence.NewPersistenceManager(bw, nil)
	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set("key", []byte("val"), storage.NewPayloadMetadata(time.Now(), nil))
	}
}

func BenchmarkKV_Get(b *testing.B) {
	walPath := "kv_bench_std.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	pm := persistence.NewPersistenceManager(bw, nil)
	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)
	_ = s.Set("key", []byte("val"), storage.NewPayloadMetadata(time.Now(), nil))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get("key")
	}
}

func BenchmarkList_Push(b *testing.B) {
	walPath := "list_bench_std.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	pm := persistence.NewPersistenceManager(bw, nil)
	ls := list_storage.NewListStorage(pm)
	pm.RegisterEngine(ls)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ls.LeftPush("key", [][]byte{[]byte("val")}, nil)
	}
}

func BenchmarkList_Pop(b *testing.B) {
	walPath := "list_bench_std.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	pm := persistence.NewPersistenceManager(bw, nil)
	ls := list_storage.NewListStorage(pm)
	pm.RegisterEngine(ls)

	// Pre-populate list
	for i := 0; i < b.N; i++ {
		_ = ls.LeftPush("key", [][]byte{[]byte("val")}, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ls.RightPop("key")
	}
}

func BenchmarkSet_Add(b *testing.B) {
	walPath := "set_bench_std.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	pm := persistence.NewPersistenceManager(bw, nil)
	ss := set_storage.NewSetStorage(pm)
	pm.RegisterEngine(ss)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ss.SAdd("key", [][]byte{[]byte("val" + strconv.Itoa(i))}, nil)
	}
}

func BenchmarkSet_IsMember(b *testing.B) {
	walPath := "set_bench_std.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	pm := persistence.NewPersistenceManager(bw, nil)
	ss := set_storage.NewSetStorage(pm)
	pm.RegisterEngine(ss)
	_, _ = ss.SAdd("key", [][]byte{[]byte("val")}, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ss.SIsMember("key", []byte("val"))
	}
}

func BenchmarkZSet_Add(b *testing.B) {
	walPath := "zset_bench_std.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	pm := persistence.NewPersistenceManager(bw, nil)
	zs := zset_storage.NewZSetStorage(pm)
	pm.RegisterEngine(zs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = zs.ZAdd("key", []zset_storage.ZSetMember{{Member: []byte("val" + strconv.Itoa(i)), Score: float64(i)}}, nil)
	}
}

func BenchmarkZSet_Range(b *testing.B) {
	walPath := "zset_bench_std.wal"
	defer os.Remove(walPath)

	w, _ := wal.NewWal(walPath)
	bw := wal.NewBufferedWal(w)
	defer bw.CloseWal()
	pm := persistence.NewPersistenceManager(bw, nil)
	zs := zset_storage.NewZSetStorage(pm)
	pm.RegisterEngine(zs)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		_, _ = zs.ZAdd("key", []zset_storage.ZSetMember{{Member: []byte("val" + strconv.Itoa(i)), Score: float64(i)}}, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = zs.ZRange("key", 100, 200)
	}
}
