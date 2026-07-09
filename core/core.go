package core

import (
	"container/heap"
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
}

type Storage struct {
	Storage       map[string]Payload
	Expirations   ExpirationHeap
	ExpirationMap map[string]*ExpirationEntry
	mutex         sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
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
		s.Delete(key)
		return nil, KeyError{Key: key, Err: KeyExpiredError}
	}
	defer s.mutex.RUnlock()
	return payload.Value, nil
}

func (s *Storage) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
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
		s.Delete(key)
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
