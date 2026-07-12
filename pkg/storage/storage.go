package storage

import (
	"container/heap"
	"encoding/base64"
	"errors"
	"fmt"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/wal"
	"os"
	"strings"
	"sync"
	"time"
)

type StorageInterface interface {
	Set(key string, value []byte, metadata PayloadMetadata) error
	SetWithTTL(key string, value []byte, ttl time.Duration) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Exists(key string) bool
	CleanupExpired() // Cleanup expired entries
	Load() error     // Load from WAL
}

type Storage struct {
	wal           wal.WalInterface
	data          map[string]Payload
	expirations   ExpirationHeap
	expirationMap map[string]*ExpirationEntry
	mutex         sync.RWMutex
}

func NewStorage(w wal.WalInterface) *Storage {
	return &Storage{
		wal:           w,
		data:          make(map[string]Payload),
		expirations:   make(ExpirationHeap, 0),
		expirationMap: make(map[string]*ExpirationEntry),
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

func (s *Storage) Set(key string, value []byte, metadata PayloadMetadata) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

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
	walEntry := fmt.Sprintf("SET|%s|%s|%s\n", key, encodedValue, expiresStr)
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

	if _, err := s.wal.WriteToWal("DELETE|" + key + "\n"); err != nil {
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
		key := parts[1]

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

			if expiresAt != nil {
				entry := &ExpirationEntry{key: key, expiresAt: *expiresAt}
				heap.Push(&s.expirations, entry)
				s.expirationMap[key] = entry
			}
		}
		return nil
	})
}
