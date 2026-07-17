package set_storage

import (
	"container/heap"
	"encoding/base64"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"io"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Value struct {
	members   map[string]struct{}
	createdAt time.Time
	expiresAt *time.Time
}

func NewValue(members map[string]struct{}, createdAt time.Time, expiresAt *time.Time) Value {
	return Value{members: members, createdAt: createdAt, expiresAt: expiresAt}
}

type SetStorageInterface interface {
	SAdd(key string, members [][]byte, expiresAt *time.Time) (int64, error)
	SRem(key string, members [][]byte) (int64, error)
	SPop(key string, count int) ([][]byte, error)
	SMove(srcKey string, dstKey string, member []byte) (bool, error)
	SIsMember(key string, member []byte) (bool, error)
	SMIsMember(key string, members [][]byte) ([]bool, error)
	SMembers(key string) ([][]byte, error)
	SCard(key string) (int64, error)
	SRandMember(key string, count int) ([][]byte, error)

	SInter(keys []string) ([][]byte, error)
	SInterCard(keys []string, limit int) (int64, error)
	SInterStore(dstKey string, keys []string) (int64, error)
	SUnion(keys []string) ([][]byte, error)
	SUnionStore(dstKey string, keys []string) (int64, error)
	SDiff(keys []string) ([][]byte, error)
	SDiffStore(dstKey string, keys []string) (int64, error)

	Delete(key string) error
	CleanupExpired()

	DeleteByPrefix(prefix string) (int64, error)
	DeleteBySuffix(suffix string) (int64, error)
	DeleteByRegex(regex string) (int64, error)
	FindByPrefix(prefix string) (map[string][][]byte, error)
	FindBySuffix(suffix string) (map[string][][]byte, error)
	FindByRegex(regex string) (map[string][][]byte, error)
	CountByPrefix(prefix string) (int64, error)
	CountBySuffix(suffix string) (int64, error)
	CountByRegex(regex string) (int64, error)
}

type SetStorage struct {
	walLogger     persistence.WalLogger
	data          map[string]Value
	expirations   SetExpirationHeap
	expirationMap map[string]*SetExpirationEntry
	mutex         sync.RWMutex
}

func NewSetStorage(logger persistence.WalLogger) *SetStorage {
	return &SetStorage{
		walLogger:     logger,
		data:          make(map[string]Value),
		expirations:   make(SetExpirationHeap, 0),
		expirationMap: make(map[string]*SetExpirationEntry),
	}
}

func (ss *SetStorage) Data() map[string]Value {
	return ss.data
}

func (ss *SetStorage) Expirations() SetExpirationHeap {
	return ss.expirations
}

func (ss *SetStorage) ExpirationMap() map[string]*SetExpirationEntry {
	return ss.expirationMap
}

// EngineID implements persistence.Engine
func (ss *SetStorage) EngineID() string {
	return "set"
}

// ProcessWalEntry implements persistence.Engine
func (ss *SetStorage) ProcessWalEntry(action string, args []string) error {
	switch action {
	case "SADD":
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

		var members [][]byte
		for _, rawMember := range args[2:] {
			member, err := base64.StdEncoding.DecodeString(rawMember)
			if err == nil {
				members = append(members, member)
			}
		}
		ss.sAddNoLockAndNoWal(key, members, expiresAt)

	case "SREM":
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
		ss.sRemNoLockAndNoWal(key, members)

	case "SPOP":
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
		ss.sRemNoLockAndNoWal(key, members)

	case "SMOVE":
		if len(args) < 3 {
			return nil
		}
		srcKey, _ := url.PathUnescape(args[0])
		dstKey, _ := url.PathUnescape(args[1])
		member, err := base64.StdEncoding.DecodeString(args[2])
		if err == nil {
			ss.sMoveNoLockAndNoWal(srcKey, dstKey, member)
		}

	case "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE":
		if len(args) < 1 {
			return nil
		}
		dstKey, _ := url.PathUnescape(args[0])
		var keys []string
		for _, rawKey := range args[1:] {
			key, _ := url.PathUnescape(rawKey)
			keys = append(keys, key)
		}
		switch action {
		case "SINTERSTORE":
			ss.sInterStoreNoLockAndNoWal(dstKey, keys)
		case "SUNIONSTORE":
			ss.sUnionStoreNoLockAndNoWal(dstKey, keys)
		case "SDIFFSTORE":
			ss.sDiffStoreNoLockAndNoWal(dstKey, keys)
		}

	case "DELETE":
		if len(args) < 1 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		ss.deleteNoLock(key)
	}
	return nil
}

// SerializeState implements persistence.Engine
func (ss *SetStorage) SerializeState(w io.Writer) error {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	if err := snapshot.WriteUintValue(w, uint64(len(ss.data))); err != nil {
		return err
	}

	for key, val := range ss.data {
		if err := snapshot.WriteBytes(w, []byte(key)); err != nil {
			return err
		}

		if err := snapshot.WriteUintValue(w, uint64(len(val.members))); err != nil {
			return err
		}

		for member := range val.members {
			if err := snapshot.WriteBytes(w, []byte(member)); err != nil {
				return err
			}
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
func (ss *SetStorage) DeserializeState(r io.Reader) error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	count, err := snapshot.ReadUintValue[uint64](r)
	if err != nil {
		return err
	}

	ss.clearNoLock()

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

		members := make(map[string]struct{})
		for j := uint64(0); j < setSize; j++ {
			memberBytes, err := snapshot.ReadBytes(r)
			if err != nil {
				return err
			}
			members[string(memberBytes)] = struct{}{}
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

		ss.data[key] = Value{
			members:   members,
			createdAt: time.Unix(0, createdAtNano),
			expiresAt: expires,
		}

		if expires != nil {
			entry := &SetExpirationEntry{
				key:       key,
				expiresAt: *expires,
			}
			heap.Push(&ss.expirations, entry)
			ss.expirationMap[key] = entry
		}
	}
	return nil
}

// Reset implements persistence.Engine
func (ss *SetStorage) Reset() error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.data = make(map[string]Value)
	ss.expirations = make(SetExpirationHeap, 0)
	ss.expirationMap = make(map[string]*SetExpirationEntry)
	return nil
}

// Clear implements persistence.Engine
func (ss *SetStorage) Clear() error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.clearNoLock()
	return nil
}

func (ss *SetStorage) clearNoLock() {
	clear(ss.data)
	ss.expirations = make(SetExpirationHeap, 0)
	clear(ss.expirationMap)
}

func (ss *SetStorage) SAdd(key string, members [][]byte, expiresAt *time.Time) (int64, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	// Check expiration first
	if val, exists := ss.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.deleteNoLock(key)
		_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key))
	}

	added := ss.sAddNoLockAndNoWal(key, members, expiresAt)

	expiresStr := "PERSISTENT"
	if expiresAt != nil {
		expiresStr = expiresAt.Format(time.RFC3339)
	} else if val, exists := ss.data[key]; exists && val.expiresAt != nil {
		expiresStr = val.expiresAt.Format(time.RFC3339)
	}

	var args []string
	args = append(args, url.PathEscape(key), expiresStr)
	for _, m := range members {
		args = append(args, base64.StdEncoding.EncodeToString(m))
	}

	if err := ss.walLogger.Log(ss.EngineID(), "SADD", args...); err != nil {
		return 0, err
	}

	return added, nil
}

func (ss *SetStorage) sAddNoLockAndNoWal(key string, members [][]byte, expiresAt *time.Time) int64 {
	val, exists := ss.data[key]
	var mSet map[string]struct{}
	if !exists {
		mSet = make(map[string]struct{})
		val = Value{
			members:   mSet,
			createdAt: time.Now(),
			expiresAt: expiresAt,
		}
	} else {
		mSet = val.members
		if expiresAt != nil {
			val.expiresAt = expiresAt
		}
	}

	var added int64
	for _, m := range members {
		s := string(m)
		if _, ok := mSet[s]; !ok {
			mSet[s] = struct{}{}
			added++
		}
	}

	ss.data[key] = val

	// Update expiration heap
	if expiresAt != nil {
		if oldEntry, ok := ss.expirationMap[key]; ok {
			heap.Remove(&ss.expirations, oldEntry.index)
		}
		entry := &SetExpirationEntry{
			key:       key,
			expiresAt: *expiresAt,
		}
		heap.Push(&ss.expirations, entry)
		ss.expirationMap[key] = entry
	}

	return added
}

func (ss *SetStorage) SRem(key string, members [][]byte) (int64, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	// Check expiration first
	if val, exists := ss.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.deleteNoLock(key)
		_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key))
	}

	_, exists := ss.data[key]
	if !exists {
		return 0, nil
	}

	removed := ss.sRemNoLockAndNoWal(key, members)

	if removed > 0 {
		var args []string
		args = append(args, url.PathEscape(key))
		for _, m := range members {
			args = append(args, base64.StdEncoding.EncodeToString(m))
		}
		if err := ss.walLogger.Log(ss.EngineID(), "SREM", args...); err != nil {
			return 0, err
		}
	}

	return removed, nil
}

func (ss *SetStorage) sRemNoLockAndNoWal(key string, members [][]byte) int64 {
	val := ss.data[key]
	var removed int64
	for _, m := range members {
		s := string(m)
		if _, ok := val.members[s]; ok {
			delete(val.members, s)
			removed++
		}
	}

	if len(val.members) == 0 {
		ss.deleteNoLock(key)
	} else {
		ss.data[key] = val
	}

	return removed
}

func (ss *SetStorage) SPop(key string, count int) ([][]byte, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	// Check expiration first
	if val, exists := ss.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.deleteNoLock(key)
		_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key))
	}

	val, exists := ss.data[key]
	if !exists || len(val.members) == 0 {
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "SPOP"}
	}

	if count <= 0 {
		return [][]byte{}, nil
	}

	limit := count
	if limit > len(val.members) {
		limit = len(val.members)
	}

	popped := make([][]byte, 0, limit)
	for m := range val.members {
		popped = append(popped, []byte(m))
		delete(val.members, m)
		if len(popped) == limit {
			break
		}
	}

	if len(val.members) == 0 {
		ss.deleteNoLock(key)
	}

	// Log SPOP with exact popped elements for deterministic recovery
	var args []string
	args = append(args, url.PathEscape(key))
	for _, p := range popped {
		args = append(args, base64.StdEncoding.EncodeToString(p))
	}
	if err := ss.walLogger.Log(ss.EngineID(), "SPOP", args...); err != nil {
		return nil, err
	}

	return popped, nil
}

func (ss *SetStorage) SMove(srcKey string, dstKey string, member []byte) (bool, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	// Check expiration on srcKey
	if val, exists := ss.data[srcKey]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.deleteNoLock(srcKey)
		_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(srcKey))
	}

	srcVal, exists := ss.data[srcKey]
	if !exists {
		return false, nil
	}
	memberStr := string(member)
	if _, ok := srcVal.members[memberStr]; !ok {
		return false, nil
	}

	// Check expiration on dstKey
	if val, exists := ss.data[dstKey]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.deleteNoLock(dstKey)
		_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(dstKey))
	}

	ss.sMoveNoLockAndNoWal(srcKey, dstKey, member)

	encodedMember := base64.StdEncoding.EncodeToString(member)
	if err := ss.walLogger.Log(ss.EngineID(), "SMOVE", url.PathEscape(srcKey), url.PathEscape(dstKey), encodedMember); err != nil {
		return false, err
	}

	return true, nil
}

func (ss *SetStorage) sMoveNoLockAndNoWal(srcKey string, dstKey string, member []byte) {
	ss.sRemNoLockAndNoWal(srcKey, [][]byte{member})
	ss.sAddNoLockAndNoWal(dstKey, [][]byte{member}, nil)
}

func (ss *SetStorage) SIsMember(key string, member []byte) (bool, error) {
	ss.mutex.RLock()
	val, exists := ss.data[key]
	if !exists {
		ss.mutex.RUnlock()
		return false, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.mutex.RUnlock()
		ss.mutex.Lock()
		if val, exists = ss.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(key)
			_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key))
		}
		ss.mutex.Unlock()
		return false, nil
	}
	defer ss.mutex.RUnlock()

	_, ok := val.members[string(member)]
	return ok, nil
}

func (ss *SetStorage) SMIsMember(key string, members [][]byte) ([]bool, error) {
	ss.mutex.RLock()
	val, exists := ss.data[key]
	if !exists {
		ss.mutex.RUnlock()
		return make([]bool, len(members)), nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.mutex.RUnlock()
		ss.mutex.Lock()
		if val, exists = ss.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(key)
			_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key))
		}
		ss.mutex.Unlock()
		return make([]bool, len(members)), nil
	}
	defer ss.mutex.RUnlock()

	res := make([]bool, len(members))
	for i, m := range members {
		_, res[i] = val.members[string(m)]
	}
	return res, nil
}

func (ss *SetStorage) SMembers(key string) ([][]byte, error) {
	ss.mutex.RLock()
	val, exists := ss.data[key]
	if !exists {
		ss.mutex.RUnlock()
		return [][]byte{}, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.mutex.RUnlock()
		ss.mutex.Lock()
		if val, exists = ss.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(key)
			_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key))
		}
		ss.mutex.Unlock()
		return [][]byte{}, nil
	}
	defer ss.mutex.RUnlock()

	res := make([][]byte, 0, len(val.members))
	for m := range val.members {
		res = append(res, []byte(m))
	}
	return res, nil
}

func (ss *SetStorage) SCard(key string) (int64, error) {
	ss.mutex.RLock()
	val, exists := ss.data[key]
	if !exists {
		ss.mutex.RUnlock()
		return 0, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.mutex.RUnlock()
		ss.mutex.Lock()
		if val, exists = ss.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(key)
			_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key))
		}
		ss.mutex.Unlock()
		return 0, nil
	}
	defer ss.mutex.RUnlock()

	return int64(len(val.members)), nil
}

func (ss *SetStorage) SRandMember(key string, count int) ([][]byte, error) {
	ss.mutex.RLock()
	val, exists := ss.data[key]
	if !exists {
		ss.mutex.RUnlock()
		return [][]byte{}, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ss.mutex.RUnlock()
		ss.mutex.Lock()
		if val, exists = ss.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(key)
			_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key))
		}
		ss.mutex.Unlock()
		return [][]byte{}, nil
	}
	defer ss.mutex.RUnlock()

	length := len(val.members)
	if length == 0 || count == 0 {
		return [][]byte{}, nil
	}

	if count >= 0 {
		limit := count
		if limit > length {
			limit = length
		}
		res := make([][]byte, 0, limit)
		for k := range val.members {
			res = append(res, []byte(k))
			if len(res) == limit {
				break
			}
		}
		return res, nil
	} else {
		limit := -count
		allKeys := make([]string, 0, length)
		for k := range val.members {
			allKeys = append(allKeys, k)
		}
		res := make([][]byte, 0, limit)
		for i := 0; i < limit; i++ {
			idx := int((time.Now().UnixNano() + int64(i)) % int64(length))
			if idx < 0 {
				idx = -idx
			}
			res = append(res, []byte(allKeys[idx]))
		}
		return res, nil
	}
}

func (ss *SetStorage) SInter(keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	var sets []map[string]struct{}
	var expiredKeys []string
	for _, k := range keys {
		val, exists := ss.data[k]
		if !exists {
			return [][]byte{}, nil
		}
		if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		sets = append(sets, val.members)
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()
		return [][]byte{}, nil
	}

	// Find the smallest set to iterate on
	smallestIdx := 0
	for i := 1; i < len(sets); i++ {
		if len(sets[i]) < len(sets[smallestIdx]) {
			smallestIdx = i
		}
	}

	res := make([][]byte, 0)
	for item := range sets[smallestIdx] {
		intersect := true
		for idx, set := range sets {
			if idx == smallestIdx {
				continue
			}
			if _, ok := set[item]; !ok {
				intersect = false
				break
			}
		}
		if intersect {
			res = append(res, []byte(item))
		}
	}

	return res, nil
}

func (ss *SetStorage) SInterCard(keys []string, limit int) (int64, error) {
	res, err := ss.SInter(keys)
	if err != nil {
		return 0, err
	}
	card := int64(len(res))
	if limit > 0 && card > int64(limit) {
		return int64(limit), nil
	}
	return card, nil
}

func (ss *SetStorage) SInterStore(dstKey string, keys []string) (int64, error) {
	res, err := ss.SInter(keys)
	if err != nil {
		return 0, err
	}

	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.sInterStoreNoLockAndNoWal(dstKey, keys)

	// Log SINTERSTORE to WAL
	var args []string
	args = append(args, url.PathEscape(dstKey))
	for _, k := range keys {
		args = append(args, url.PathEscape(k))
	}
	if err := ss.walLogger.Log(ss.EngineID(), "SINTERSTORE", args...); err != nil {
		return 0, err
	}

	return int64(len(res)), nil
}

func (ss *SetStorage) sInterStoreNoLockAndNoWal(dstKey string, keys []string) {
	ss.deleteNoLock(dstKey)

	if len(keys) == 0 {
		return
	}

	var sets []map[string]struct{}
	for _, k := range keys {
		val, exists := ss.data[k]
		if exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(k)
			continue
		}
		if !exists {
			return
		}
		sets = append(sets, val.members)
	}

	smallestIdx := 0
	for i := 1; i < len(sets); i++ {
		if len(sets[i]) < len(sets[smallestIdx]) {
			smallestIdx = i
		}
	}

	mSet := make(map[string]struct{})
	for item := range sets[smallestIdx] {
		intersect := true
		for idx, set := range sets {
			if idx == smallestIdx {
				continue
			}
			if _, ok := set[item]; !ok {
				intersect = false
				break
			}
		}
		if intersect {
			mSet[item] = struct{}{}
		}
	}

	if len(mSet) > 0 {
		ss.data[dstKey] = Value{
			members:   mSet,
			createdAt: time.Now(),
		}
	}
}

func (ss *SetStorage) SUnion(keys []string) ([][]byte, error) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	tempSet := make(map[string]struct{})
	var expiredKeys []string
	for _, k := range keys {
		val, exists := ss.data[k]
		if !exists {
			continue
		}
		if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		for item := range val.members {
			tempSet[item] = struct{}{}
		}
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()
	}

	res := make([][]byte, 0, len(tempSet))
	for item := range tempSet {
		res = append(res, []byte(item))
	}
	return res, nil
}

func (ss *SetStorage) SUnionStore(dstKey string, keys []string) (int64, error) {
	res, err := ss.SUnion(keys)
	if err != nil {
		return 0, err
	}

	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.sUnionStoreNoLockAndNoWal(dstKey, keys)

	var args []string
	args = append(args, url.PathEscape(dstKey))
	for _, k := range keys {
		args = append(args, url.PathEscape(k))
	}
	if err := ss.walLogger.Log(ss.EngineID(), "SUNIONSTORE", args...); err != nil {
		return 0, err
	}

	return int64(len(res)), nil
}

func (ss *SetStorage) sUnionStoreNoLockAndNoWal(dstKey string, keys []string) {
	ss.deleteNoLock(dstKey)

	mSet := make(map[string]struct{})
	for _, k := range keys {
		val, exists := ss.data[k]
		if exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(k)
			continue
		}
		if !exists {
			continue
		}
		for item := range val.members {
			mSet[item] = struct{}{}
		}
	}

	if len(mSet) > 0 {
		ss.data[dstKey] = Value{
			members:   mSet,
			createdAt: time.Now(),
		}
	}
}

func (ss *SetStorage) SDiff(keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	firstKey := keys[0]
	firstVal, exists := ss.data[firstKey]
	var expiredKeys []string

	if exists && firstVal.expiresAt != nil && firstVal.expiresAt.Before(time.Now()) {
		expiredKeys = append(expiredKeys, firstKey)
	}

	for i := 1; i < len(keys); i++ {
		k := keys[i]
		val, exists := ss.data[k]
		if exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()

		if firstVal.expiresAt != nil && firstVal.expiresAt.Before(time.Now()) {
			return [][]byte{}, nil
		}
	}

	firstVal, exists = ss.data[firstKey]
	if !exists || (firstVal.expiresAt != nil && firstVal.expiresAt.Before(time.Now())) {
		return [][]byte{}, nil
	}

	tempSet := make(map[string]struct{})
	for m := range firstVal.members {
		tempSet[m] = struct{}{}
	}

	for i := 1; i < len(keys); i++ {
		k := keys[i]
		val, exists := ss.data[k]
		if !exists || (val.expiresAt != nil && val.expiresAt.Before(time.Now())) {
			continue
		}
		for item := range val.members {
			delete(tempSet, item)
		}
	}

	res := make([][]byte, 0, len(tempSet))
	for item := range tempSet {
		res = append(res, []byte(item))
	}
	return res, nil
}

func (ss *SetStorage) SDiffStore(dstKey string, keys []string) (int64, error) {
	res, err := ss.SDiff(keys)
	if err != nil {
		return 0, err
	}

	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.sDiffStoreNoLockAndNoWal(dstKey, keys)

	var args []string
	args = append(args, url.PathEscape(dstKey))
	for _, k := range keys {
		args = append(args, url.PathEscape(k))
	}
	if err := ss.walLogger.Log(ss.EngineID(), "SDIFFSTORE", args...); err != nil {
		return 0, err
	}

	return int64(len(res)), nil
}

func (ss *SetStorage) sDiffStoreNoLockAndNoWal(dstKey string, keys []string) {
	ss.deleteNoLock(dstKey)

	if len(keys) == 0 {
		return
	}

	firstKey := keys[0]
	firstVal, exists := ss.data[firstKey]
	if exists && firstVal.expiresAt != nil && firstVal.expiresAt.Before(time.Now()) {
		ss.deleteNoLock(firstKey)
		return
	}
	if !exists {
		return
	}

	mSet := make(map[string]struct{})
	for m := range firstVal.members {
		mSet[m] = struct{}{}
	}

	for i := 1; i < len(keys); i++ {
		k := keys[i]
		val, exists := ss.data[k]
		if exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(k)
			continue
		}
		if !exists {
			continue
		}
		for item := range val.members {
			delete(mSet, item)
		}
	}

	if len(mSet) > 0 {
		ss.data[dstKey] = Value{
			members:   mSet,
			createdAt: time.Now(),
		}
	}
}

func (ss *SetStorage) Delete(key string) error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	if err := ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(key)); err != nil {
		return err
	}

	ss.deleteNoLock(key)
	return nil
}

func (ss *SetStorage) deleteNoLock(key string) {
	delete(ss.data, key)
	if entry, ok := ss.expirationMap[key]; ok {
		heap.Remove(&ss.expirations, entry.index)
		delete(ss.expirationMap, key)
	}
}

func (ss *SetStorage) CleanupExpired() {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	now := time.Now()
	for ss.expirations.Len() > 0 {
		entry := ss.expirations[0]
		if entry.expiresAt.After(now) {
			break
		}
		ss.deleteNoLock(entry.key)
		_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(entry.key))
	}
}

func (ss *SetStorage) deleteExpiredKeys(keys []string) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	for _, k := range keys {
		if val, exists := ss.data[k]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ss.deleteNoLock(k)
			_ = ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(k))
		}
	}
}

func (ss *SetStorage) getSetBytesNoLock(m map[string]struct{}) [][]byte {
	res := make([][]byte, 0, len(m))
	for item := range m {
		res = append(res, []byte(item))
	}
	return res
}

func (ss *SetStorage) CountByPrefix(prefix string) (int64, error) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasPrefix(k, prefix) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()
	}

	return count, nil
}

func (ss *SetStorage) CountBySuffix(suffix string) (int64, error) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasSuffix(k, suffix) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()
	}

	return count, nil
}

func (ss *SetStorage) CountByRegex(regex string) (int64, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return 0, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if re.MatchString(k) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()
	}

	return count, nil
}

func (ss *SetStorage) FindByPrefix(prefix string) (map[string][][]byte, error) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	var result = make(map[string][][]byte)
	var expiredKeys []string

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasPrefix(k, prefix) {
			result[k] = ss.getSetBytesNoLock(v.members)
		}
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()
	}

	return result, nil
}

func (ss *SetStorage) FindBySuffix(suffix string) (map[string][][]byte, error) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	var result = make(map[string][][]byte)
	var expiredKeys []string

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasSuffix(k, suffix) {
			result[k] = ss.getSetBytesNoLock(v.members)
		}
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()
	}

	return result, nil
}

func (ss *SetStorage) FindByRegex(regex string) (map[string][][]byte, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	var result = make(map[string][][]byte)
	var expiredKeys []string

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if re.MatchString(k) {
			result[k] = ss.getSetBytesNoLock(v.members)
		}
	}

	if len(expiredKeys) > 0 {
		ss.mutex.RUnlock()
		ss.deleteExpiredKeys(expiredKeys)
		ss.mutex.RLock()
	}

	return result, nil
}

func (ss *SetStorage) DeleteByPrefix(prefix string) (int64, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	return ss.deleteByPrefixNoLock(prefix)
}

func (ss *SetStorage) deleteByPrefixNoLock(prefix string) (int64, error) {
	var count int64

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			if err := ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				ss.deleteNoLock(k)
			}
			continue
		}
		if strings.HasPrefix(k, prefix) {
			if err := ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				return count, err
			}
			ss.deleteNoLock(k)
			count++
		}
	}
	return count, nil
}

func (ss *SetStorage) DeleteBySuffix(suffix string) (int64, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	return ss.deleteBySuffixNoLock(suffix)
}

func (ss *SetStorage) deleteBySuffixNoLock(suffix string) (int64, error) {
	var count int64

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			if err := ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				ss.deleteNoLock(k)
			}
			continue
		}
		if strings.HasSuffix(k, suffix) {
			if err := ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				return count, err
			}
			ss.deleteNoLock(k)
			count++
		}
	}
	return count, nil
}

func (ss *SetStorage) DeleteByRegex(regex string) (int64, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return 0, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	return ss.deleteByRegexNoLock(re)
}

func (ss *SetStorage) deleteByRegexNoLock(re *regexp.Regexp) (int64, error) {
	var count int64

	for k, v := range ss.data {
		if v.expiresAt != nil && v.expiresAt.Before(time.Now()) {
			if err := ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				ss.deleteNoLock(k)
			}
			continue
		}
		if re.MatchString(k) {
			if err := ss.walLogger.Log(ss.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				return count, err
			}
			ss.deleteNoLock(k)
			count++
		}
	}
	return count, nil
}
