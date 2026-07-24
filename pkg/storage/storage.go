package storage

import (
	"container/heap"
	"encoding/base64"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StorageInterface interface {
	Set(key string, value []byte, metadata PayloadMetadata) error
	Mset(data map[string]Payload) error
	SetWithTTL(key string, value []byte, ttl time.Duration) error
	MsetWithTTL(data map[string]Payload, ttl time.Duration) error
	Get(key string) ([]byte, error)
	SnapshotKey(key string) (Payload, bool)
	Mget(keys []string) (map[string][]byte, error)
	Delete(key string) error
	Exists(key string) bool
	Increment(key string) (int64, error)
	IncrementBy(key string, amount int64) (int64, error)
	Decrement(key string) (int64, error)
	DecrementBy(key string, amount int64) (int64, error)
	Clear() error    // preserves the map allocation but resets the map to empty. good if expected to re-fill the map to a similar size, avoids allocation and preserves buckets.
	Reset() error    // creates a new empty map, does not preserve the map allocation and capacity. the opposite of Clear().
	CleanupExpired() // Cleanup expired entries
	Load() error     // Load from WAL
	DeleteByPrefix(prefix string) int64
	DeleteByRegex(regex string) (int64, error)
	DeleteBySuffix(suffix string) int64
	FindByPrefix(prefix string) map[string][]byte
	FindByRegex(regex string) (map[string][]byte, error)
	FindBySuffix(suffix string) map[string][]byte
	CountByPrefix(prefix string) int64
	CountByRegex(regex string) (int64, error)
	CountBySuffix(suffix string) int64
	KeyCount() int
	MemoryUsageBytes() int64
}

type Storage struct {
	walLogger     persistence.WalLogger
	data          map[string]Payload
	expirations   ExpirationHeap
	expirationMap map[string]*ExpirationEntry
	mutex         sync.RWMutex
}

// SnapshotKey returns the full payload (value + metadata) for undo purposes.
// Returns (payload, true) if key exists and is not expired, (zero, false) otherwise.
func (s *Storage) SnapshotKey(key string) (Payload, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	p, ok := s.data[key]
	if !ok || p.metadata.IsExpired() {
		return Payload{}, false
	}
	return p, true
}

func (s *Storage) KeyCount() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.data)
}

func (s *Storage) MemoryUsageBytes() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	var mem int64 = 0
	for k, v := range s.data {
		mem += int64(16 + len(k)) // string header + length
		mem += 40                 // approx map overhead per entry
		mem += int64(24 + cap(v.value)) // slice header + capacity
	}
	return mem
}

func (s *Storage) CountBySuffix(suffix string) int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasSuffix(k, suffix) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		s.mutex.RUnlock()
		s.deleteExpiredKeys(expiredKeys)
		s.mutex.RLock()
	}

	return count
}

func (s *Storage) CountByRegex(regex string) (int64, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return 0, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if re.MatchString(k) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		s.mutex.RUnlock()
		s.deleteExpiredKeys(expiredKeys)
		s.mutex.RLock()
	}

	return count, nil
}

func (s *Storage) CountByPrefix(prefix string) int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var count int64
	var expiredKeys []string

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasPrefix(k, prefix) {
			count++
		}
	}

	if len(expiredKeys) > 0 {
		s.mutex.RUnlock()
		s.deleteExpiredKeys(expiredKeys)
		s.mutex.RLock()
	}

	return count
}

func (s *Storage) FindBySuffix(suffix string) map[string][]byte {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var result = make(map[string][]byte)
	var expiredKeys []string

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasSuffix(k, suffix) {
			result[k] = v.value
		}
	}

	if len(expiredKeys) > 0 {
		s.mutex.RUnlock()
		s.deleteExpiredKeys(expiredKeys)
		s.mutex.RLock()
	}

	return result
}

func (s *Storage) FindByRegex(regex string) (map[string][]byte, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var result = make(map[string][]byte)
	var expiredKeys []string

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if re.MatchString(k) {
			result[k] = v.value
		}
	}

	if len(expiredKeys) > 0 {
		s.mutex.RUnlock()
		s.deleteExpiredKeys(expiredKeys)
		s.mutex.RLock()
	}

	return result, nil
}

func (s *Storage) FindByPrefix(prefix string) map[string][]byte {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var result = make(map[string][]byte)
	var expiredKeys []string

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			expiredKeys = append(expiredKeys, k)
			continue
		}
		if strings.HasPrefix(k, prefix) {
			result[k] = v.value
		}
	}

	if len(expiredKeys) > 0 {
		s.mutex.RUnlock()
		s.deleteExpiredKeys(expiredKeys)
		s.mutex.RLock()
	}

	return result
}

func (s *Storage) DeleteBySuffix(suffix string) int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.deleteBySuffixNoLock(suffix)
}

func (s *Storage) deleteBySuffixNoLock(suffix string) int64 {
	var count int64

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				s.deleteNoLock(k)
			}
			continue
		}
		if strings.HasSuffix(k, suffix) {
			if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				break
			}
			s.deleteNoLock(k)
			count++
		}
	}
	return count
}

func (s *Storage) DeleteByRegex(regex string) (int64, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return 0, pkgerrors.RegexError{RegexExpr: regex, Err: err}
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.deleteByRegexNoLock(re)
}

func (s *Storage) deleteByRegexNoLock(re *regexp.Regexp) (int64, error) {
	var count int64

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				s.deleteNoLock(k)
			}
			continue
		}
		if re.MatchString(k) {
			if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				return count, err
			}
			s.deleteNoLock(k)
			count++
		}
	}
	return count, nil
}

func (s *Storage) DeleteByPrefix(prefix string) int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.deleteByPrefixNoLock(prefix)
}

func (s *Storage) deleteByPrefixNoLock(prefix string) int64 {
	var count int64

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(k)); err == nil {
				s.deleteNoLock(k)
			}
			continue
		}
		if strings.HasPrefix(k, prefix) {
			if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(k)); err != nil {
				break
			}
			s.deleteNoLock(k)
			count++
		}
	}
	return count
}

func (s *Storage) Reset() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.resetNoLock()
}

func (s *Storage) resetNoLock() error {
	s.data = make(map[string]Payload)
	s.expirations = make(ExpirationHeap, 0)
	s.expirationMap = make(map[string]*ExpirationEntry)
	return nil
}

func (s *Storage) Clear() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.clearNoLock()
}

func (s *Storage) clearNoLock() error {
	clear(s.data)
	s.expirations = make(ExpirationHeap, 0)
	clear(s.expirationMap)
	return nil
}

func (s *Storage) DecrementBy(key string, amount int64) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.incrementByNoLock(key, -amount)
}

func (s *Storage) Decrement(key string) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.incrementByNoLock(key, -1)
}

func (s *Storage) IncrementBy(key string, amount int64) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.incrementByNoLock(key, amount)
}

func (s *Storage) Increment(key string) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.incrementByNoLock(key, 1)
}

func (s *Storage) incrementByNoLock(key string, amount int64) (int64, error) {
	op := "INCREMENT"
	if amount < 0 {
		op = "DECREMENT"
	}

	payload, exists := s.data[key]
	if !exists {
		return 0, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: op}
	}

	if payload.metadata.IsExpired() {
		if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(key)); err == nil {
			s.deleteNoLock(key)
		}
		return 0, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyExpiredError, Operation: op}
	}

	intVal, err := strconv.ParseInt(string(payload.value), 10, 64)
	if err != nil {
		return 0, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotIntegerError, Value: string(payload.value), Operation: op}
	}

	newVal := intVal + amount
	newValueBytes := []byte(strconv.FormatInt(newVal, 10))

	// Update in-memory
	s.data[key] = Payload{
		value:    newValueBytes,
		metadata: payload.metadata,
	}

	// Update WAL
	expiresStr := "PERSISTENT"
	if payload.metadata.expiresAt != nil {
		expiresStr = payload.metadata.expiresAt.Format(time.RFC3339)
	}
	encodedValue := base64.StdEncoding.EncodeToString(newValueBytes)
	if err := s.walLogger.Log(s.EngineID(), "SET", url.PathEscape(key), encodedValue, expiresStr); err != nil {
		return 0, err
	}

	return newVal, nil
}

func (s *Storage) Mget(keys []string) (map[string][]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	result := make(map[string][]byte, len(keys))

	var expiredKeys []string

	for _, key := range keys {
		if payload, ok := s.data[key]; ok {
			if payload.metadata.IsExpired() {
				result[key] = nil
				expiredKeys = append(expiredKeys, key)
			} else {
				result[key] = payload.value
			}
		} else {
			result[key] = nil
		}
	}

	if len(expiredKeys) > 0 {
		s.mutex.RUnlock()
		s.deleteExpiredKeys(expiredKeys)
		s.mutex.RLock()
	}

	return result, nil
}

func NewStorage(logger persistence.WalLogger) *Storage {
	return &Storage{
		walLogger:     logger,
		data:          make(map[string]Payload),
		expirations:   make(ExpirationHeap, 0),
		expirationMap: make(map[string]*ExpirationEntry),
	}
}

func (s *Storage) Data() map[string]Payload {
	return s.data
}

func (s *Storage) Expirations() ExpirationHeap {
	return s.expirations
}

func (s *Storage) ExpirationMap() map[string]*ExpirationEntry {
	return s.expirationMap
}

func (s *Storage) Set(key string, value []byte, metadata PayloadMetadata) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.setNoLock(key, value, metadata)
}

func (s *Storage) Mset(data map[string]Payload) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.msetNoLock(data)
}

func (s *Storage) msetNoLock(data map[string]Payload) error {
	for k, v := range data {
		if err := s.setNoLock(k, v.value, v.metadata); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) setNoLock(key string, value []byte, metadata PayloadMetadata) error {
	s.data[key] = Payload{
		value:    value,
		metadata: metadata,
	}

	// Remove old expiration if exists
	if oldEntry, ok := s.expirationMap[key]; ok {
		heap.Remove(&s.expirations, oldEntry.index)
		delete(s.expirationMap, key)
	}

	// Add new expiration if set
	if metadata.expiresAt != nil {
		entry := &ExpirationEntry{
			key:       key,
			expiresAt: *metadata.expiresAt,
		}
		heap.Push(&s.expirations, entry)
		s.expirationMap[key] = entry
	}
	expiresStr := "PERSISTENT"
	if metadata.expiresAt != nil {
		expiresStr = metadata.expiresAt.Format(time.RFC3339)
	}
	encodedValue := base64.StdEncoding.EncodeToString(value)
	if err := s.walLogger.Log(s.EngineID(), "SET", url.PathEscape(key), encodedValue, expiresStr); err != nil {
		return err
	}
	return nil
}

func (s *Storage) SetWithTTL(key string, value []byte, ttl time.Duration) error {
	now := time.Now()
	expiresAt := now.Add(ttl)
	metadata := NewPayloadMetadata(now, &expiresAt)
	return s.Set(key, value, metadata)
}

func (s *Storage) MsetWithTTL(data map[string]Payload, ttl time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.msetWithTTLNoLock(data, ttl)
}

func (s *Storage) msetWithTTLNoLock(data map[string]Payload, ttl time.Duration) error {
	now := time.Now()
	expiresAt := now.Add(ttl)
	metadata := NewPayloadMetadata(now, &expiresAt)

	for k, v := range data {
		if err := s.setNoLock(k, v.value, metadata); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) Get(key string) ([]byte, error) {
	s.mutex.RLock()
	payload, exists := s.data[key]
	if !exists {
		s.mutex.RUnlock()
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "GET"}
	}

	if payload.metadata.IsExpired() {
		s.mutex.RUnlock()
		_ = s.Delete(key)
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyExpiredError, Operation: "GET"}
	}
	defer s.mutex.RUnlock()
	return payload.value, nil
}

func (s *Storage) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(key)); err != nil {
		return err
	}

	s.deleteNoLock(key)
	return nil
}

func (s *Storage) deleteNoLock(key string) {
	delete(s.data, key)
	if entry, ok := s.expirationMap[key]; ok {
		heap.Remove(&s.expirations, entry.index)
		delete(s.expirationMap, key)
	}
}

func (s *Storage) Exists(key string) bool {
	s.mutex.RLock()
	payload, exists := s.data[key]
	if !exists {
		s.mutex.RUnlock()
		return false
	}

	if payload.metadata.IsExpired() {
		s.mutex.RUnlock()
		_ = s.Delete(key)
		return false
	}
	s.mutex.RUnlock()
	return true
}

func (s *Storage) CleanupExpired() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.cleanupExpiredNoLock()
}

func (s *Storage) cleanupExpiredNoLock() {
	now := time.Now()
	for s.expirations.Len() > 0 {
		entry := s.expirations[0]
		if entry.expiresAt.After(now) {
			break
		}
		s.deleteNoLock(entry.key)
	}
}

func (s *Storage) deleteExpiredKeys(keys []string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, key := range keys {
		if payload, ok := s.data[key]; ok && payload.metadata.IsExpired() {
			if err := s.walLogger.Log(s.EngineID(), "DELETE", url.PathEscape(key)); err == nil {
				s.deleteNoLock(key)
			}
		}
	}
}

// EngineID implements persistence.Engine
func (s *Storage) EngineID() string {
	return "kv"
}

// ProcessWalEntry implements persistence.Engine
func (s *Storage) ProcessWalEntry(action string, args []string) error {
	switch action {
	case "DELETE":
		if len(args) < 1 {
			return nil
		}
		key, err := url.PathUnescape(args[0])
		if err != nil {
			key = args[0]
		}
		s.deleteNoLock(key)
	case "SET":
		if len(args) < 3 {
			return nil
		}
		key, err := url.PathUnescape(args[0])
		if err != nil {
			key = args[0]
		}
		value, err := base64.StdEncoding.DecodeString(args[1])
		if err != nil {
			value = []byte(args[1])
		}
		expiresStr := args[2]

		var expiresAt *time.Time
		if expiresStr != "PERSISTENT" {
			t, err := time.Parse(time.RFC3339, expiresStr)
			if err != nil {
				return nil
			}
			if t.Before(time.Now()) {
				return nil
			}
			expiresAt = &t
		}

		s.data[key] = Payload{
			value: value,
			metadata: PayloadMetadata{
				createdAt: time.Now(),
				expiresAt: expiresAt,
			},
		}

		if oldEntry, ok := s.expirationMap[key]; ok {
			heap.Remove(&s.expirations, oldEntry.index)
			delete(s.expirationMap, key)
		}

		if expiresAt != nil {
			entry := &ExpirationEntry{key: key, expiresAt: *expiresAt}
			heap.Push(&s.expirations, entry)
			s.expirationMap[key] = entry
		}
	}
	return nil
}

// SerializeState implements persistence.Engine
func (s *Storage) SerializeState(w io.Writer) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if err := snapshot.WriteUintValue(w, uint64(len(s.data))); err != nil {
		return err
	}

	for key, payload := range s.data {
		if err := snapshot.WriteBytes(w, []byte(key)); err != nil {
			return err
		}

		if err := snapshot.WriteBytes(w, payload.value); err != nil {
			return err
		}

		if err := snapshot.WriteInt64Value(w, payload.metadata.createdAt.UnixNano()); err != nil {
			return err
		}

		if payload.metadata.expiresAt == nil {
			if err := snapshot.WriteUintValue[uint8](w, uint8(0)); err != nil {
				return err
			}
		} else {
			if err := snapshot.WriteUintValue[uint8](w, uint8(1)); err != nil {
				return err
			}
			if err := snapshot.WriteInt64Value(w, payload.metadata.expiresAt.UnixNano()); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeserializeState implements persistence.Engine
func (s *Storage) DeserializeState(r io.Reader) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	count, err := snapshot.ReadUintValue[uint64](r)
	if err != nil {
		return err
	}

	s.clearNoLock()

	for i := uint64(0); i < count; i++ {
		keyBytes, err := snapshot.ReadBytes(r)
		if err != nil {
			return err
		}
		key := string(keyBytes)

		valueBytes, err := snapshot.ReadBytes(r)
		if err != nil {
			return err
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

		s.data[key] = Payload{
			value: valueBytes,
			metadata: PayloadMetadata{
				createdAt: time.Unix(0, createdAtNano),
				expiresAt: expires,
			},
		}

		if expires != nil {
			entry := &ExpirationEntry{
				key:       key,
				expiresAt: *expires,
			}
			heap.Push(&s.expirations, entry)
			s.expirationMap[key] = entry
		}
	}
	return nil
}
