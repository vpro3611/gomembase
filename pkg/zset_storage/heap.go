package zset_storage

import (
	"time"
)

// ZSetExpirationEntry represents an item in the expiration heap
type ZSetExpirationEntry struct {
	key       string
	expiresAt time.Time
	index     int // Index in the heap - needed by container/heap
}

func (e *ZSetExpirationEntry) Key() string {
	return e.key
}

func (e *ZSetExpirationEntry) ExpiresAt() time.Time {
	return e.expiresAt
}

func (e *ZSetExpirationEntry) Index() int {
	return e.index
}

func NewZSetExpirationEntry(key string, expiresAt time.Time) *ZSetExpirationEntry {
	return &ZSetExpirationEntry{key: key, expiresAt: expiresAt}
}

func (e *ZSetExpirationEntry) SetIndex(i int) {
	e.index = i
}

// ZSetExpirationHeap implements heap.Interface
type ZSetExpirationHeap []*ZSetExpirationEntry

func (eh ZSetExpirationHeap) Len() int           { return len(eh) }
func (eh ZSetExpirationHeap) Less(i, j int) bool { return eh[i].expiresAt.Before(eh[j].expiresAt) }
func (eh ZSetExpirationHeap) Swap(i, j int) {
	eh[i], eh[j] = eh[j], eh[i]
	eh[i].index = i
	eh[j].index = j
}
func (eh *ZSetExpirationHeap) Push(x interface{}) {
	entry := x.(*ZSetExpirationEntry)
	entry.index = len(*eh)
	*eh = append(*eh, entry)
}
func (eh *ZSetExpirationHeap) Pop() interface{} {
	old := *eh
	n := len(old)
	entry := old[n-1]
	entry.index = -1
	*eh = old[0 : n-1]
	return entry
}
