package storage

import (
	"time"
)

// ExpirationEntry represents an item in the expiration heap
type ExpirationEntry struct {
	key       string
	expiresAt time.Time
	index     int // Index in the heap - needed by container/heap
}

func (e *ExpirationEntry) Key() string {
	return e.key
}

func (e *ExpirationEntry) ExpiresAt() time.Time {
	return e.expiresAt
}

func (e *ExpirationEntry) Index() int {
	return e.index
}

func NewExpirationEntry(key string, expiresAt time.Time) *ExpirationEntry {
	return &ExpirationEntry{key: key, expiresAt: expiresAt}
}

func (e *ExpirationEntry) SetIndex(i int) {
	e.index = i
}

// ExpirationHeap implements heap.Interface
type ExpirationHeap []*ExpirationEntry

func (eh ExpirationHeap) Len() int           { return len(eh) }
func (eh ExpirationHeap) Less(i, j int) bool { return eh[i].expiresAt.Before(eh[j].expiresAt) }
func (eh ExpirationHeap) Swap(i, j int) {
	eh[i], eh[j] = eh[j], eh[i]
	eh[i].index = i
	eh[j].index = j
}
func (eh *ExpirationHeap) Push(x interface{}) {
	entry := x.(*ExpirationEntry)
	entry.index = len(*eh)
	*eh = append(*eh, entry)
}
func (eh *ExpirationHeap) Pop() interface{} {
	old := *eh
	n := len(old)
	entry := old[n-1]
	entry.index = -1
	*eh = old[0 : n-1]
	return entry
}
