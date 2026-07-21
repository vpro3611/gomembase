package zset_storage

import (
	"container/heap"
	"encoding/base64"
	"io"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
)

type ZSetMember struct {
	Member []byte
	Score  float64
}

type ZSetValue struct {
	dict      map[string]float64
	zsl       *zskiplist
	createdAt time.Time
	expiresAt *time.Time
}

type ZSetStorageInterface interface {
	ZAdd(key string, members []ZSetMember, expiresAt *time.Time) (int64, error)
	ZRem(key string, members [][]byte) (int64, error)
	ZIncrBy(key string, increment float64, member []byte) (float64, error)
	ZPopMax(key string, count int) ([]ZSetMember, error)
	ZPopMin(key string, count int) ([]ZSetMember, error)
	ZScore(key string, member []byte) (float64, bool, error)
	ZCard(key string) (int64, error)
	ZRank(key string, member []byte) (int64, bool, error)
	ZRevRank(key string, member []byte) (int64, bool, error)
	ZCount(key string, min float64, max float64) (int64, error)

	ZRange(key string, start int, stop int) ([]ZSetMember, error)
	ZRevRange(key string, start int, stop int) ([]ZSetMember, error)
	ZRangeByScore(key string, min float64, max float64, offset int, count int) ([]ZSetMember, error)
	ZRevRangeByScore(key string, max float64, min float64, offset int, count int) ([]ZSetMember, error)

	Delete(key string) error
	CleanupExpired()

	SnapshotZSetMember(key string, member []byte) (float64, bool)
	SnapshotZSetAll(key string) ([]ZSetMember, bool)

	DeleteByPrefix(prefix string) (int64, error)
	DeleteBySuffix(suffix string) (int64, error)
	DeleteByRegex(regex string) (int64, error)
	FindByPrefix(prefix string) (map[string][]ZSetMember, error)
	FindBySuffix(suffix string) (map[string][]ZSetMember, error)
	FindByRegex(regex string) (map[string][]ZSetMember, error)
	CountByPrefix(prefix string) (int64, error)
	CountBySuffix(suffix string) (int64, error)
	CountByRegex(regex string) (int64, error)
}

type ZSetStorage struct {
	walLogger     persistence.WalLogger
	data          map[string]ZSetValue
	expirations   ZSetExpirationHeap
	expirationMap map[string]*ZSetExpirationEntry
	mutex         sync.RWMutex
}

func NewZSetStorage(logger persistence.WalLogger) *ZSetStorage {
	return &ZSetStorage{
		walLogger:     logger,
		data:          make(map[string]ZSetValue),
		expirations:   make(ZSetExpirationHeap, 0),
		expirationMap: make(map[string]*ZSetExpirationEntry),
	}
}

func (zs *ZSetStorage) Data() map[string]ZSetValue {
	return zs.data
}

func (zs *ZSetStorage) Expirations() ZSetExpirationHeap {
	return zs.expirations
}

func (zs *ZSetStorage) ExpirationMap() map[string]*ZSetExpirationEntry {
	return zs.expirationMap
}

// SnapshotZSetMember returns the score of a member for undo purposes.
func (zs *ZSetStorage) SnapshotZSetMember(key string, member []byte) (float64, bool) {
	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	val, exists := zs.data[key]
	if !exists || (val.expiresAt != nil && val.expiresAt.Before(time.Now())) {
		return 0, false
	}

	score, ok := val.dict[string(member)]
	return score, ok
}

// SnapshotZSetAll returns all members with scores for undo (e.g. ZPOPMAX/MIN rollback).
func (zs *ZSetStorage) SnapshotZSetAll(key string) ([]ZSetMember, bool) {
	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	val, exists := zs.data[key]
	if !exists || (val.expiresAt != nil && val.expiresAt.Before(time.Now())) {
		return nil, false
	}

	members := make([]ZSetMember, 0, len(val.dict))
	// ZSet data is ordered by score in zsl, so we can iterate zsl
	curr := val.zsl.header.level[0].forward
	for curr != nil {
		members = append(members, ZSetMember{Member: []byte(curr.ele), Score: curr.score})
		curr = curr.level[0].forward
	}
	return members, true
}

// EngineID implements persistence.Engine
func (zs *ZSetStorage) EngineID() string {
	return "zset"
}

// ProcessWalEntry implements persistence.Engine
func (zs *ZSetStorage) ProcessWalEntry(action string, args []string) error {
	switch action {
	case "ZADD":
		if len(args) < 2 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		expiresStr := args[1]
		var expiresAt *time.Time
		if expiresStr != "PERSISTENT" {
			t, err := time.Parse(time.RFC3339, expiresStr)
			if err == nil {
				expiresAt = &t
			}
		}

		var members []ZSetMember
		for i := 2; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			score, err := strconv.ParseFloat(args[i], 64)
			if err != nil {
				continue
			}
			member, err := base64.StdEncoding.DecodeString(args[i+1])
			if err == nil {
				members = append(members, ZSetMember{Member: member, Score: score})
			}
		}
		zs.zAddNoLockAndNoWal(key, members, expiresAt)

	case "ZREM":
		if len(args) < 1 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		var members [][]byte
		for _, rawMember := range args[1:] {
			member, err := base64.StdEncoding.DecodeString(rawMember)
			if err == nil {
				members = append(members, member)
			}
		}
		zs.zRemNoLockAndNoWal(key, members)

	case "ZPOPMAX", "ZPOPMIN":
		if len(args) < 1 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		var members [][]byte
		for i := 1; i < len(args); i += 2 {
			member, err := base64.StdEncoding.DecodeString(args[i])
			if err == nil {
				members = append(members, member)
			}
		}
		zs.zRemNoLockAndNoWal(key, members)

	case "ZINCRBY":
		if len(args) < 3 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		increment, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return nil
		}
		member, err := base64.StdEncoding.DecodeString(args[2])
		if err == nil {
			zs.zIncrByNoLockAndNoWal(key, increment, member)
		}

	case "DELETE":
		if len(args) < 1 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		zs.deleteNoLock(key)
	}
	return nil
}

// SerializeState implements persistence.Engine
func (zs *ZSetStorage) SerializeState(w io.Writer) error {
	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	if err := snapshot.WriteUintValue(w, uint64(len(zs.data))); err != nil {
		return err
	}

	for key, val := range zs.data {
		if err := snapshot.WriteBytes(w, []byte(key)); err != nil {
			return err
		}

		if err := snapshot.WriteUintValue(w, val.zsl.length); err != nil {
			return err
		}

		// Traverse and serialize nodes from skiplist
		curr := val.zsl.header.level[0].forward
		for curr != nil {
			if err := snapshot.WriteBytes(w, []byte(curr.ele)); err != nil {
				return err
			}
			bits := math.Float64bits(curr.score)
			if err := snapshot.WriteUintValue(w, bits); err != nil {
				return err
			}
			curr = curr.level[0].forward
		}

		if err := snapshot.WriteInt64Value(w, val.createdAt.UnixNano()); err != nil {
			return err
		}

		if val.expiresAt == nil {
			if err := snapshot.WriteUintValue[uint8](w, uint8(0)); err != nil {
				return err
			}
		} else {
			if err := snapshot.WriteUintValue[uint8](w, uint8(1)); err != nil {
				return err
			}
			if err := snapshot.WriteInt64Value(w, val.expiresAt.UnixNano()); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeserializeState implements persistence.Engine
func (zs *ZSetStorage) DeserializeState(r io.Reader) error {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	count, err := snapshot.ReadUintValue[uint64](r)
	if err != nil {
		return err
	}

	zs.clearNoLock()

	for i := uint64(0); i < count; i++ {
		keyBytes, err := snapshot.ReadBytes(r)
		if err != nil {
			return err
		}
		key := string(keyBytes)

		setSize, err := snapshot.ReadUintValue[uint64](r)
		if err != nil {
			return err
		}

		dict := make(map[string]float64)
		zsl := newZskiplist()

		for j := uint64(0); j < setSize; j++ {
			memberBytes, err := snapshot.ReadBytes(r)
			if err != nil {
				return err
			}
			member := string(memberBytes)

			bits, err := snapshot.ReadUintValue[uint64](r)
			if err != nil {
				return err
			}
			score := math.Float64frombits(bits)

			dict[member] = score
			zsl.insert(score, member)
		}

		createdAtNano, err := snapshot.ReadInt64Value(r)
		if err != nil {
			return err
		}

		hasExpiry, err := snapshot.ReadUintValue[uint8](r)
		if err != nil {
			return err
		}

		var expires *time.Time
		if hasExpiry == 1 {
			expiresNano, err := snapshot.ReadInt64Value(r)
			if err != nil {
				return err
			}
			t := time.Unix(0, expiresNano)
			expires = &t
		}

		zs.data[key] = ZSetValue{
			dict:      dict,
			zsl:       zsl,
			createdAt: time.Unix(0, createdAtNano),
			expiresAt: expires,
		}

		if expires != nil {
			entry := &ZSetExpirationEntry{
				key:       key,
				expiresAt: *expires,
			}
			heap.Push(&zs.expirations, entry)
			zs.expirationMap[key] = entry
		}
	}
	return nil
}

// Reset implements persistence.Engine
func (zs *ZSetStorage) Reset() error {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	zs.data = make(map[string]ZSetValue)
	zs.expirations = make(ZSetExpirationHeap, 0)
	zs.expirationMap = make(map[string]*ZSetExpirationEntry)
	return nil
}

// Clear implements persistence.Engine
func (zs *ZSetStorage) Clear() error {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	zs.clearNoLock()
	return nil
}

func (zs *ZSetStorage) clearNoLock() {
	clear(zs.data)
	zs.expirations = make(ZSetExpirationHeap, 0)
	clear(zs.expirationMap)
}

func (zs *ZSetStorage) ZAdd(key string, members []ZSetMember, expiresAt *time.Time) (int64, error) {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	// Check expiration first
	if val, exists := zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.deleteNoLock(key)
		_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
	}

	added := zs.zAddNoLockAndNoWal(key, members, expiresAt)

	expiresStr := "PERSISTENT"
	if expiresAt != nil {
		expiresStr = expiresAt.Format(time.RFC3339)
	} else if val, exists := zs.data[key]; exists && val.expiresAt != nil {
		expiresStr = val.expiresAt.Format(time.RFC3339)
	}

	var args []string
	args = append(args, url.PathEscape(key), expiresStr)
	for _, m := range members {
		scoreStr := strconv.FormatFloat(m.Score, 'f', -1, 64)
		args = append(args, scoreStr, base64.StdEncoding.EncodeToString(m.Member))
	}

	if err := zs.walLogger.Log(zs.EngineID(), "ZADD", args...); err != nil {
		return 0, err
	}

	return added, nil
}

func (zs *ZSetStorage) zAddNoLockAndNoWal(key string, members []ZSetMember, expiresAt *time.Time) int64 {
	val, exists := zs.data[key]
	if !exists {
		val = ZSetValue{
			dict:      make(map[string]float64),
			zsl:       newZskiplist(),
			createdAt: time.Now(),
			expiresAt: expiresAt,
		}
	} else {
		if expiresAt != nil {
			val.expiresAt = expiresAt
		}
	}

	var added int64
	for _, m := range members {
		memberStr := string(m.Member)
		oldScore, found := val.dict[memberStr]
		if found {
			if oldScore != m.Score {
				val.zsl.delete(oldScore, memberStr)
				val.zsl.insert(m.Score, memberStr)
				val.dict[memberStr] = m.Score
			}
		} else {
			val.zsl.insert(m.Score, memberStr)
			val.dict[memberStr] = m.Score
			added++
		}
	}

	zs.data[key] = val

	// Update expiration heap
	if expiresAt != nil {
		if oldEntry, ok := zs.expirationMap[key]; ok {
			heap.Remove(&zs.expirations, oldEntry.index)
		}
		entry := &ZSetExpirationEntry{
			key:       key,
			expiresAt: *expiresAt,
		}
		heap.Push(&zs.expirations, entry)
		zs.expirationMap[key] = entry
	}

	return added
}

func (zs *ZSetStorage) ZRem(key string, members [][]byte) (int64, error) {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	// Check expiration first
	if val, exists := zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.deleteNoLock(key)
		_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
	}

	_, exists := zs.data[key]
	if !exists {
		return 0, nil
	}

	removed := zs.zRemNoLockAndNoWal(key, members)

	if removed > 0 {
		var args []string
		args = append(args, url.PathEscape(key))
		for _, m := range members {
			args = append(args, base64.StdEncoding.EncodeToString(m))
		}
		if err := zs.walLogger.Log(zs.EngineID(), "ZREM", args...); err != nil {
			return 0, err
		}
	}

	return removed, nil
}

func (zs *ZSetStorage) zRemNoLockAndNoWal(key string, members [][]byte) int64 {
	val := zs.data[key]
	var removed int64
	for _, m := range members {
		memberStr := string(m)
		oldScore, found := val.dict[memberStr]
		if found {
			val.zsl.delete(oldScore, memberStr)
			delete(val.dict, memberStr)
			removed++
		}
	}

	if len(val.dict) == 0 {
		zs.deleteNoLock(key)
	} else {
		zs.data[key] = val
	}

	return removed
}

func (zs *ZSetStorage) ZIncrBy(key string, increment float64, member []byte) (float64, error) {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	// Check expiration first
	if val, exists := zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.deleteNoLock(key)
		_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
	}

	newScore := zs.zIncrByNoLockAndNoWal(key, increment, member)

	incStr := strconv.FormatFloat(increment, 'f', -1, 64)
	if err := zs.walLogger.Log(zs.EngineID(), "ZINCRBY", url.PathEscape(key), incStr, base64.StdEncoding.EncodeToString(member)); err != nil {
		return 0, err
	}

	return newScore, nil
}

func (zs *ZSetStorage) zIncrByNoLockAndNoWal(key string, increment float64, member []byte) float64 {
	val, exists := zs.data[key]
	if !exists {
		val = ZSetValue{
			dict:      make(map[string]float64),
			zsl:       newZskiplist(),
			createdAt: time.Now(),
		}
	}

	memberStr := string(member)
	oldScore, found := val.dict[memberStr]
	newScore := increment
	if found {
		newScore = oldScore + increment
		val.zsl.delete(oldScore, memberStr)
	}
	val.zsl.insert(newScore, memberStr)
	val.dict[memberStr] = newScore

	zs.data[key] = val
	return newScore
}

func (zs *ZSetStorage) ZPopMax(key string, count int) ([]ZSetMember, error) {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	// Check expiration first
	if val, exists := zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.deleteNoLock(key)
		_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
	}

	val, exists := zs.data[key]
	if !exists || val.zsl.length == 0 {
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "ZPOPMAX"}
	}

	if count <= 0 {
		return []ZSetMember{}, nil
	}

	limit := count
	if uint64(limit) > val.zsl.length {
		limit = int(val.zsl.length)
	}

	popped := make([]ZSetMember, 0, limit)
	curr := val.zsl.tail
	for i := 0; i < limit && curr != nil; i++ {
		popped = append(popped, ZSetMember{Member: []byte(curr.ele), Score: curr.score})
		curr = curr.backward
	}

	// Remove from data structures
	for _, p := range popped {
		val.zsl.delete(p.Score, string(p.Member))
		delete(val.dict, string(p.Member))
	}

	if len(val.dict) == 0 {
		zs.deleteNoLock(key)
	} else {
		zs.data[key] = val
	}

	// Log ZPOPMAX with exact popped elements for deterministic recovery
	var args []string
	args = append(args, url.PathEscape(key))
	for _, p := range popped {
		scoreStr := strconv.FormatFloat(p.Score, 'f', -1, 64)
		args = append(args, base64.StdEncoding.EncodeToString(p.Member), scoreStr)
	}
	if err := zs.walLogger.Log(zs.EngineID(), "ZPOPMAX", args...); err != nil {
		return nil, err
	}

	return popped, nil
}

func (zs *ZSetStorage) ZPopMin(key string, count int) ([]ZSetMember, error) {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	// Check expiration first
	if val, exists := zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.deleteNoLock(key)
		_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
	}

	val, exists := zs.data[key]
	if !exists || val.zsl.length == 0 {
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "ZPOPMIN"}
	}

	if count <= 0 {
		return []ZSetMember{}, nil
	}

	limit := count
	if uint64(limit) > val.zsl.length {
		limit = int(val.zsl.length)
	}

	popped := make([]ZSetMember, 0, limit)
	curr := val.zsl.header.level[0].forward
	for i := 0; i < limit && curr != nil; i++ {
		popped = append(popped, ZSetMember{Member: []byte(curr.ele), Score: curr.score})
		curr = curr.level[0].forward
	}

	// Remove from data structures
	for _, p := range popped {
		val.zsl.delete(p.Score, string(p.Member))
		delete(val.dict, string(p.Member))
	}

	if len(val.dict) == 0 {
		zs.deleteNoLock(key)
	} else {
		zs.data[key] = val
	}

	// Log ZPOPMIN with exact popped elements for deterministic recovery
	var args []string
	args = append(args, url.PathEscape(key))
	for _, p := range popped {
		scoreStr := strconv.FormatFloat(p.Score, 'f', -1, 64)
		args = append(args, base64.StdEncoding.EncodeToString(p.Member), scoreStr)
	}
	if err := zs.walLogger.Log(zs.EngineID(), "ZPOPMIN", args...); err != nil {
		return nil, err
	}

	return popped, nil
}

func (zs *ZSetStorage) ZScore(key string, member []byte) (float64, bool, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return 0, false, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return 0, false, nil
	}
	defer zs.mutex.RUnlock()

	score, ok := val.dict[string(member)]
	return score, ok, nil
}

func (zs *ZSetStorage) ZCard(key string) (int64, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return 0, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return 0, nil
	}
	defer zs.mutex.RUnlock()

	return int64(val.zsl.length), nil
}

func (zs *ZSetStorage) ZRank(key string, member []byte) (int64, bool, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return 0, false, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return 0, false, nil
	}
	defer zs.mutex.RUnlock()

	memberStr := string(member)
	score, ok := val.dict[memberStr]
	if !ok {
		return 0, false, nil
	}

	rank := val.zsl.getRank(score, memberStr)
	if rank == 0 {
		return 0, false, nil
	}
	return int64(rank - 1), true, nil
}

func (zs *ZSetStorage) ZRevRank(key string, member []byte) (int64, bool, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return 0, false, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return 0, false, nil
	}
	defer zs.mutex.RUnlock()

	memberStr := string(member)
	score, ok := val.dict[memberStr]
	if !ok {
		return 0, false, nil
	}

	rank := val.zsl.getRank(score, memberStr)
	if rank == 0 {
		return 0, false, nil
	}
	revRank := val.zsl.length - rank
	return int64(revRank), true, nil
}

func (zs *ZSetStorage) ZCount(key string, min float64, max float64) (int64, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return 0, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return 0, nil
	}
	defer zs.mutex.RUnlock()

	firstRank := val.zsl.firstInRangeRank(min)
	if firstRank == 0 {
		return 0, nil
	}
	lastRank := val.zsl.lastInRangeRank(max)
	if lastRank == 0 || firstRank > lastRank {
		return 0, nil
	}

	return int64(lastRank - firstRank + 1), nil
}

func (zs *zskiplist) firstInRangeRank(min float64) uint64 {
	var rank uint64
	x := zs.header
	for i := zs.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.score < min {
			rank += x.level[i].span
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x != nil && x.score >= min {
		return rank + 1
	}
	return 0
}

func (zs *zskiplist) lastInRangeRank(max float64) uint64 {
	var rank uint64
	x := zs.header
	for i := zs.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.score <= max {
			rank += x.level[i].span
			x = x.level[i].forward
		}
	}
	if x != zs.header {
		return rank
	}
	return 0
}

func (zs *ZSetStorage) ZRange(key string, start int, stop int) ([]ZSetMember, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return []ZSetMember{}, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return []ZSetMember{}, nil
	}
	defer zs.mutex.RUnlock()

	length := int(val.zsl.length)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || length == 0 {
		return []ZSetMember{}, nil
	}

	startRank := uint64(start + 1)
	node := val.zsl.getElementByRank(startRank)
	if node == nil {
		return []ZSetMember{}, nil
	}

	res := make([]ZSetMember, 0, stop-start+1)
	for i := start; i <= stop && node != nil; i++ {
		res = append(res, ZSetMember{Member: []byte(node.ele), Score: node.score})
		node = node.level[0].forward
	}

	return res, nil
}

func (zs *ZSetStorage) ZRevRange(key string, start int, stop int) ([]ZSetMember, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return []ZSetMember{}, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return []ZSetMember{}, nil
	}
	defer zs.mutex.RUnlock()

	length := int(val.zsl.length)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || length == 0 {
		return []ZSetMember{}, nil
	}

	// ZRevRange rank start translates to zsl.length - start rank index
	startRank := uint64(length - start)
	node := val.zsl.getElementByRank(startRank)
	if node == nil {
		return []ZSetMember{}, nil
	}

	res := make([]ZSetMember, 0, stop-start+1)
	for i := start; i <= stop && node != nil; i++ {
		res = append(res, ZSetMember{Member: []byte(node.ele), Score: node.score})
		node = node.backward
	}

	return res, nil
}

func (zs *ZSetStorage) ZRangeByScore(key string, min float64, max float64, offset int, count int) ([]ZSetMember, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return []ZSetMember{}, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return []ZSetMember{}, nil
	}
	defer zs.mutex.RUnlock()

	// Find the first node in range
	node := val.zsl.header
	for i := val.zsl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && node.level[i].forward.score < min {
			node = node.level[i].forward
		}
	}
	node = node.level[0].forward

	if node == nil || node.score > max {
		return []ZSetMember{}, nil
	}

	// Skip offset entries
	for offset > 0 && node != nil {
		node = node.level[0].forward
		offset--
	}

	res := make([]ZSetMember, 0)
	for node != nil && node.score <= max {
		res = append(res, ZSetMember{Member: []byte(node.ele), Score: node.score})
		if count > 0 && len(res) == count {
			break
		}
		node = node.level[0].forward
	}

	return res, nil
}

func (zs *ZSetStorage) ZRevRangeByScore(key string, max float64, min float64, offset int, count int) ([]ZSetMember, error) {
	zs.mutex.RLock()
	val, exists := zs.data[key]
	if !exists {
		zs.mutex.RUnlock()
		return []ZSetMember{}, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		zs.mutex.RUnlock()
		zs.mutex.Lock()
		if val, exists = zs.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(key)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key))
		}
		zs.mutex.Unlock()
		return []ZSetMember{}, nil
	}
	defer zs.mutex.RUnlock()

	// Find the last node in range (highest score <= max)
	node := val.zsl.header
	for i := val.zsl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && node.level[i].forward.score <= max {
			node = node.level[i].forward
		}
	}

	if node == val.zsl.header || node.score < min {
		return []ZSetMember{}, nil
	}

	// Skip offset entries
	for offset > 0 && node != nil && node != val.zsl.header {
		node = node.backward
		offset--
	}

	res := make([]ZSetMember, 0)
	for node != nil && node != val.zsl.header && node.score >= min {
		res = append(res, ZSetMember{Member: []byte(node.ele), Score: node.score})
		if count > 0 && len(res) == count {
			break
		}
		node = node.backward
	}

	return res, nil
}

func (zs *ZSetStorage) Delete(key string) error {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	if err := zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(key)); err != nil {
		return err
	}

	zs.deleteNoLock(key)
	return nil
}

func (zs *ZSetStorage) deleteNoLock(key string) {
	delete(zs.data, key)
	if entry, ok := zs.expirationMap[key]; ok {
		heap.Remove(&zs.expirations, entry.index)
		delete(zs.expirationMap, key)
	}
}

func (zs *ZSetStorage) CleanupExpired() {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	now := time.Now()
	for zs.expirations.Len() > 0 {
		entry := zs.expirations[0]
		if entry.expiresAt.After(now) {
			break
		}
		zs.deleteNoLock(entry.key)
		_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(entry.key))
	}
}

func (zs *ZSetStorage) deleteExpiredKeys(keys []string) {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	for _, k := range keys {
		if val, exists := zs.data[k]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			zs.deleteNoLock(k)
			_ = zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(k))
		}
	}
}

func (zs *ZSetStorage) getZSetBytesNoLock(zsl *zskiplist) []ZSetMember {
	res := make([]ZSetMember, 0, zsl.length)
	curr := zsl.header.level[0].forward
	for curr != nil {
		res = append(res, ZSetMember{Member: []byte(curr.ele), Score: curr.score})
		curr = curr.level[0].forward
	}
	return res
}

func (zs *ZSetStorage) CountByPrefix(prefix string) (int64, error) {
	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasPrefix(k, prefix) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		zs.mutex.RUnlock()
		zs.deleteExpiredKeys(expiredKeys)
		zs.mutex.RLock()
	}

	return count, nil
}

func (zs *ZSetStorage) CountBySuffix(suffix string) (int64, error) {
	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasSuffix(k, suffix) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		zs.mutex.RUnlock()
		zs.deleteExpiredKeys(expiredKeys)
		zs.mutex.RLock()
	}

	return count, nil
}

func (zs *ZSetStorage) CountByRegex(regex string) (int64, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return 0, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if re.MatchString(k) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		zs.mutex.RUnlock()
		zs.deleteExpiredKeys(expiredKeys)
		zs.mutex.RLock()
	}

	return count, nil
}

func (zs *ZSetStorage) FindByPrefix(prefix string) (map[string][]ZSetMember, error) {
	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	var result = make(map[string][]ZSetMember)
	var expiredKeys []string

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasPrefix(k, prefix) {
			result[k] = zs.getZSetBytesNoLock(v.zsl)
		}
	}

	if len(expiredKeys) > 0 {
		zs.mutex.RUnlock()
		zs.deleteExpiredKeys(expiredKeys)
		zs.mutex.RLock()
	}

	return result, nil
}

func (zs *ZSetStorage) FindBySuffix(suffix string) (map[string][]ZSetMember, error) {
	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	var result = make(map[string][]ZSetMember)
	var expiredKeys []string

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasSuffix(k, suffix) {
			result[k] = zs.getZSetBytesNoLock(v.zsl)
		}
	}

	if len(expiredKeys) > 0 {
		zs.mutex.RUnlock()
		zs.deleteExpiredKeys(expiredKeys)
		zs.mutex.RLock()
	}

	return result, nil
}

func (zs *ZSetStorage) FindByRegex(regex string) (map[string][]ZSetMember, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	zs.mutex.RLock()
	defer zs.mutex.RUnlock()

	var result = make(map[string][]ZSetMember)
	var expiredKeys []string

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if re.MatchString(k) {
			result[k] = zs.getZSetBytesNoLock(v.zsl)
		}
	}

	if len(expiredKeys) > 0 {
		zs.mutex.RUnlock()
		zs.deleteExpiredKeys(expiredKeys)
		zs.mutex.RLock()
	}

	return result, nil
}

func (zs *ZSetStorage) DeleteByPrefix(prefix string) (int64, error) {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	return zs.deleteByPrefixNoLock(prefix)
}

func (zs *ZSetStorage) deleteByPrefixNoLock(prefix string) (int64, error) {
	var count int64

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			if err := zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				zs.deleteNoLock(k)
			}
			continue
		}
		if strings.HasPrefix(k, prefix) {
			if err := zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				return count, err
			}
			zs.deleteNoLock(k)
			count++
		}
	}
	return count, nil
}

func (zs *ZSetStorage) DeleteBySuffix(suffix string) (int64, error) {
	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	return zs.deleteBySuffixNoLock(suffix)
}

func (zs *ZSetStorage) deleteBySuffixNoLock(suffix string) (int64, error) {
	var count int64

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			if err := zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				zs.deleteNoLock(k)
			}
			continue
		}
		if strings.HasSuffix(k, suffix) {
			if err := zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				return count, err
			}
			zs.deleteNoLock(k)
			count++
		}
	}
	return count, nil
}

func (zs *ZSetStorage) DeleteByRegex(regex string) (int64, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return 0, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	zs.mutex.Lock()
	defer zs.mutex.Unlock()

	return zs.deleteByRegexNoLock(re)
}

func (zs *ZSetStorage) deleteByRegexNoLock(re *regexp.Regexp) (int64, error) {
	var count int64

	for k, v := range zs.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			if err := zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				zs.deleteNoLock(k)
			}
			continue
		}
		if re.MatchString(k) {
			if err := zs.walLogger.Log(zs.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				return count, err
			}
			zs.deleteNoLock(k)
			count++
		}
	}
	return count, nil
}
