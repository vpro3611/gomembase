package list_storage

import (
	"time"
)

// ListExpirationEntry represents an item in the expiration heap
type ListExpirationEntry struct {
	key       string
	expiresAt time.Time
	index     int // Index in the heap - needed by container/heap
}

func (e *ListExpirationEntry) Key() string {
	return e.key
}

func (e *ListExpirationEntry) ExpiresAt() time.Time {
	return e.expiresAt
}

func (e *ListExpirationEntry) Index() int {
	return e.index
}

func NewListExpirationEntry(key string, expiresAt time.Time) *ListExpirationEntry {
	return &ListExpirationEntry{key: key, expiresAt: expiresAt}
}

func (e *ListExpirationEntry) SetIndex(i int) {
	e.index = i
}

// ListExpirationHeap implements heap.Interface
type ListExpirationHeap []*ListExpirationEntry

func (eh ListExpirationHeap) Len() int           { return len(eh) }
func (eh ListExpirationHeap) Less(i, j int) bool { return eh[i].expiresAt.Before(eh[j].expiresAt) }
func (eh ListExpirationHeap) Swap(i, j int) {
	eh[i], eh[j] = eh[j], eh[i]
	eh[i].index = i
	eh[j].index = j
}
func (eh *ListExpirationHeap) Push(x interface{}) {
	entry := x.(*ListExpirationEntry)
	entry.index = len(*eh)
	*eh = append(*eh, entry)
}
func (eh *ListExpirationHeap) Pop() interface{} {
	old := *eh
	n := len(old)
	entry := old[n-1]
	entry.index = -1
	*eh = old[0 : n-1]
	return entry
}
