package set_storage

import (
	"time"
)

// SetExpirationEntry represents an item in the expiration heap
type SetExpirationEntry struct {
	key       string
	expiresAt time.Time
	index     int // Index in the heap - needed by container/heap
}

func (e *SetExpirationEntry) Key() string {
	return e.key
}

func (e *SetExpirationEntry) ExpiresAt() time.Time {
	return e.expiresAt
}

func (e *SetExpirationEntry) Index() int {
	return e.index
}

func NewSetExpirationEntry(key string, expiresAt time.Time) *SetExpirationEntry {
	return &SetExpirationEntry{key: key, expiresAt: expiresAt}
}

func (e *SetExpirationEntry) SetIndex(i int) {
	e.index = i
}

// SetExpirationHeap implements heap.Interface
type SetExpirationHeap []*SetExpirationEntry

func (eh SetExpirationHeap) Len() int           { return len(eh) }
func (eh SetExpirationHeap) Less(i, j int) bool { return eh[i].expiresAt.Before(eh[j].expiresAt) }
func (eh SetExpirationHeap) Swap(i, j int) {
	eh[i], eh[j] = eh[j], eh[i]
	eh[i].index = i
	eh[j].index = j
}
func (eh *SetExpirationHeap) Push(x interface{}) {
	entry := x.(*SetExpirationEntry)
	entry.index = len(*eh)
	*eh = append(*eh, entry)
}
func (eh *SetExpirationHeap) Pop() interface{} {
	old := *eh
	n := len(old)
	entry := old[n-1]
	entry.index = -1
	*eh = old[0 : n-1]
	return entry
}
