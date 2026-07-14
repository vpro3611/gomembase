package storage

import (
	"container/heap"
	"encoding/base64"
	"errors"
	"fmt"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/wal"
	"net/url"
	"os"
	"regexp"
	"slices"
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
}

type Storage struct {
	wal           wal.WalInterface
	data          map[string]Payload
	expirations   ExpirationHeap
	expirationMap map[string]*ExpirationEntry
	snapshots     []Snapshotter
	mutex         sync.RWMutex
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
		s.mutex.Lock()
		for _, key := range expiredKeys {
			if payload, ok := s.data[key]; ok && payload.metadata.IsExpired() {
				if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(key) + "\n"); err == nil {
					s.deleteNoLock(key)
				}
			}
		}
		s.mutex.Unlock()
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
		s.mutex.Lock()
		for _, key := range expiredKeys {
			if payload, ok := s.data[key]; ok && payload.metadata.IsExpired() {
				if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(key) + "\n"); err == nil {
					s.deleteNoLock(key)
				}
			}
		}
		s.mutex.Unlock()
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
		s.mutex.Lock()
		for _, key := range expiredKeys {
			if payload, ok := s.data[key]; ok && payload.metadata.IsExpired() {
				if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(key) + "\n"); err == nil {
					s.deleteNoLock(key)
				}
			}
		}
		s.mutex.Unlock()
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
		s.mutex.Lock()
		for _, key := range expiredKeys {
			if payload, ok := s.data[key]; ok && payload.metadata.IsExpired() {
				if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(key) + "\n"); err == nil {
					s.deleteNoLock(key)
				}
			}
		}
		s.mutex.Unlock()
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
		s.mutex.Lock()
		for _, key := range expiredKeys {
			if payload, ok := s.data[key]; ok && payload.metadata.IsExpired() {
				if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(key) + "\n"); err == nil {
					s.deleteNoLock(key)
				}
			}
		}
		s.mutex.Unlock()
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
		s.mutex.Lock()
		for _, key := range expiredKeys {
			if payload, ok := s.data[key]; ok && payload.metadata.IsExpired() {
				if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(key) + "\n"); err == nil {
					s.deleteNoLock(key)
				}
			}
		}
		s.mutex.Unlock()
		s.mutex.RLock()
	}

	return result
}

func (s *Storage) DeleteBySuffix(suffix string) int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var count int64

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(k) + "\n"); err == nil {
				s.deleteNoLock(k)
			}
			continue
		}
		if strings.HasSuffix(k, suffix) {
			if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(k) + "\n"); err != nil {
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

	var count int64

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(k) + "\n"); err == nil {
				s.deleteNoLock(k)
			}
			continue
		}
		if re.MatchString(k) {
			if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(k) + "\n"); err != nil {
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

	var count int64

	for k, v := range s.data {
		if v.metadata.IsExpired() {
			if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(k) + "\n"); err == nil {
				s.deleteNoLock(k)
			}
			continue
		}
		if strings.HasPrefix(k, prefix) {
			if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(k) + "\n"); err != nil {
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

	s.data = make(map[string]Payload)
	s.expirations = make(ExpirationHeap, 0)
	s.expirationMap = make(map[string]*ExpirationEntry)

	for _, snap := range s.snapshots {
		if err := snap.Delete(); err != nil {
			return err
		}
	}

	return s.wal.TruncateWal()
}

func (s *Storage) Clear() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	clear(s.data)
	s.expirations = make(ExpirationHeap, 0)
	clear(s.expirationMap)

	for _, snap := range s.snapshots {
		if err := snap.Delete(); err != nil {
			return err
		}
	}

	return s.wal.TruncateWal()
}

func (s *Storage) DecrementBy(key string, amount int64) (int64, error) {
	return s.incrementBy(key, -amount)
}

func (s *Storage) Decrement(key string) (int64, error) {
	return s.incrementBy(key, -1)
}

func (s *Storage) IncrementBy(key string, amount int64) (int64, error) {
	return s.incrementBy(key, amount)
}

func (s *Storage) Increment(key string) (int64, error) {
	return s.incrementBy(key, 1)
}

func (s *Storage) incrementBy(key string, amount int64) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	payload, exists := s.data[key]
	if !exists {
		return 0, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError}
	}

	if payload.metadata.IsExpired() {
		if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(key) + "\n"); err == nil {
			s.deleteNoLock(key)
		}
		return 0, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyExpiredError}
	}

	intVal, err := strconv.ParseInt(string(payload.value), 10, 64)
	if err != nil {
		return 0, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotIntegerError}
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
	walEntry := fmt.Sprintf("SET|%s|%s|%s\n", url.PathEscape(key), encodedValue, expiresStr)
	if _, err := s.wal.WriteToWal(walEntry); err != nil {
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
		s.mutex.Lock()
		for _, key := range expiredKeys {
			if payload, ok := s.data[key]; ok && payload.metadata.IsExpired() {
				if _, err := s.wal.WriteToWal("DELETE|" + key + "\n"); err == nil {
					s.deleteNoLock(key)
				}
			}
		}
		s.mutex.Unlock()
		s.mutex.RLock()
	}

	return result, nil
}

func NewStorage(w wal.WalInterface) *Storage {
	return &Storage{
		wal:           w,
		data:          make(map[string]Payload),
		expirations:   make(ExpirationHeap, 0),
		expirationMap: make(map[string]*ExpirationEntry),
		snapshots:     make([]Snapshotter, 0),
	}
}

func (s *Storage) Wal() wal.WalInterface {
	return s.wal
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

func (s *Storage) AddSnapshotter(snap Snapshotter) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.snapshots = append(s.snapshots, snap)
}

func (s *Storage) Snapshots() []Snapshotter {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return slices.Clone(s.snapshots)
}

func (s *Storage) Set(key string, value []byte, metadata PayloadMetadata) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.setNoLock(key, value, metadata)
}

func (s *Storage) Mset(data map[string]Payload) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

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
	walEntry := fmt.Sprintf("SET|%s|%s|%s\n", url.PathEscape(key), encodedValue, expiresStr)
	if _, err := s.wal.WriteToWal(walEntry); err != nil {
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
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError}
	}

	if payload.metadata.IsExpired() {
		s.mutex.RUnlock()
		_ = s.Delete(key)
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyExpiredError}
	}
	defer s.mutex.RUnlock()
	return payload.value, nil
}

func (s *Storage) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, err := s.wal.WriteToWal("DELETE|" + url.PathEscape(key) + "\n"); err != nil {
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

	now := time.Now()
	for s.expirations.Len() > 0 {
		entry := s.expirations[0]
		if entry.expiresAt.After(now) {
			break
		}
		s.deleteNoLock(entry.key)
	}
}

// Snapshotter interface to avoid circular dependency
type Snapshotter interface {
	Load() (map[string]Payload, error)
	Save(data map[string]Payload) error
	Delete() error
}

func (s *Storage) LoadFromSnapshot(snap Snapshotter) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := snap.Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "no such file or directory") {
			return nil
		}
		return err
	}

	for k, v := range data {
		if v.metadata.IsExpired() {
			continue
		}
		s.data[k] = v
		if oldEntry, ok := s.expirationMap[k]; ok {
			heap.Remove(&s.expirations, oldEntry.index)
			delete(s.expirationMap, k)
		}
		if v.metadata.expiresAt != nil {
			entry := &ExpirationEntry{
				key:       k,
				expiresAt: *v.metadata.expiresAt,
			}
			heap.Push(&s.expirations, entry)
			s.expirationMap[k] = entry
		}
	}
	return nil
}

func (s *Storage) SaveSnapshot(snap Snapshotter) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := snap.Save(s.data); err != nil {
		return err
	}

	return s.wal.TruncateWal()
}

func (s *Storage) Load() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.wal.RecoverFromWal(func(line string) error {
		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			return nil
		}

		action := parts[0]
		escapedKey := parts[1]
		key, err := url.PathUnescape(escapedKey)
		if err != nil {
			key = escapedKey
		}

		switch action {
		case "DELETE":
			s.deleteNoLock(key)
		case "SET":
			if len(parts) < 4 {
				return nil
			}
			value, err := base64.StdEncoding.DecodeString(parts[2])
			if err != nil {
				// Fallback to raw string if base64 decoding fails (for backward compatibility)
				// or just log/skip if it's strictly enforced.
				// For now, let's assume it should be decoded.
				value = []byte(parts[2])
			}
			expiresStr := parts[3]

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
	})
}
