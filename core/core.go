package core

import (
	"container/heap"
	"errors"
	"fmt"
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
	Wal           WalInterface
	Storage       map[string]Payload
	Expirations   ExpirationHeap
	ExpirationMap map[string]*ExpirationEntry
	mutex         sync.RWMutex
}

func NewStorage(wal WalInterface) *Storage {
	return &Storage{
		Wal:           wal,
		Storage:       make(map[string]Payload),
		Expirations:   make(ExpirationHeap, 0),
		ExpirationMap: make(map[string]*ExpirationEntry),
	}
}

func (s *Storage) Set(key string, value []byte, metadata PayloadMetadata) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.Storage[key] = Payload{
		Value:    value,
		Metadata: metadata,
	}

	// Remove old expiration if exists
	if oldEntry, ok := s.ExpirationMap[key]; ok {
		heap.Remove(&s.Expirations, oldEntry.Index)
		delete(s.ExpirationMap, key)
	}

	// Add new expiration if set
	if metadata.ExpiresAt != nil {
		entry := &ExpirationEntry{
			Key:       key,
			ExpiresAt: *metadata.ExpiresAt,
		}
		heap.Push(&s.Expirations, entry)
		s.ExpirationMap[key] = entry
	}
	expiresStr := "PERSISTENT"
	if metadata.ExpiresAt != nil {
		expiresStr = metadata.ExpiresAt.Format(time.RFC3339)
	}
	walEntry := fmt.Sprintf("SET|%s|%s|%s\n", key, string(value), expiresStr)
	if _, err := s.Wal.WriteToWal(walEntry); err != nil {
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
	payload, exists := s.Storage[key]
	if !exists {
		s.mutex.RUnlock()
		return nil, KeyError{Key: key, Err: KeyNotFoundError}
	}

	if payload.Metadata.IsExpired() {
		s.mutex.RUnlock()
		_ = s.Delete(key)
		return nil, KeyError{Key: key, Err: KeyExpiredError}
	}
	defer s.mutex.RUnlock()
	return payload.Value, nil
}

func (s *Storage) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, err := s.Wal.WriteToWal("DELETE|" + key + "\n"); err != nil {
		return err
	}

	s.deleteNoLock(key)
	return nil
}

func (s *Storage) deleteNoLock(key string) {
	delete(s.Storage, key)
	if entry, ok := s.ExpirationMap[key]; ok {
		heap.Remove(&s.Expirations, entry.Index)
		delete(s.ExpirationMap, key)
	}
}

func (s *Storage) Exists(key string) bool {
	s.mutex.RLock()
	payload, exists := s.Storage[key]
	if !exists {
		s.mutex.RUnlock()
		return false
	}

	if payload.Metadata.IsExpired() {
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
	for s.Expirations.Len() > 0 {
		entry := s.Expirations[0]
		if entry.ExpiresAt.After(now) {
			break
		}
		s.deleteNoLock(entry.Key)
	}
}

func (s *Storage) LoadFromSnapshot(snap Snapshot) error {
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
		if v.Metadata.IsExpired() {
			continue
		}
		s.Storage[k] = v
		if v.Metadata.ExpiresAt != nil {
			entry := &ExpirationEntry{
				Key:       k,
				ExpiresAt: *v.Metadata.ExpiresAt,
			}
			heap.Push(&s.Expirations, entry)
			s.ExpirationMap[k] = entry
		}
	}
	return nil
}

func (s *Storage) SaveSnapshot(snap Snapshot) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := snap.Save(s.Storage); err != nil {
		return err
	}

	return s.Wal.TruncateWal()
}

func (s *Storage) Load() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.Wal.RecoverFromWal(func(line string) error {
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
			value := []byte(parts[2])
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

			s.Storage[key] = Payload{
				Value: value,
				Metadata: PayloadMetadata{
					CreatedAt: time.Now(),
					ExpiresAt: expiresAt,
				},
			}

			if expiresAt != nil {
				entry := &ExpirationEntry{Key: key, ExpiresAt: *expiresAt}
				heap.Push(&s.Expirations, entry)
				s.ExpirationMap[key] = entry
			}
		}
		return nil
	})
}
