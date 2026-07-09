package core

// Add to core.go or create a new file: heap.go

import (
	"time"
)

// ExpirationEntry represents an item in the expiration heap
type ExpirationEntry struct {
	Key       string
	ExpiresAt time.Time
	Index     int // Index in the heap - needed by container/heap
}

// ExpirationHeap implements heap.Interface
type ExpirationHeap []*ExpirationEntry

func (eh ExpirationHeap) Len() int           { return len(eh) }
func (eh ExpirationHeap) Less(i, j int) bool { return eh[i].ExpiresAt.Before(eh[j].ExpiresAt) }
func (eh ExpirationHeap) Swap(i, j int) {
	eh[i], eh[j] = eh[j], eh[i]
	eh[i].Index = i
	eh[j].Index = j
}
func (eh *ExpirationHeap) Push(x interface{}) {
	entry := x.(*ExpirationEntry)
	entry.Index = len(*eh)
	*eh = append(*eh, entry)
}
func (eh *ExpirationHeap) Pop() interface{} {
	old := *eh
	n := len(old)
	entry := old[n-1]
	entry.Index = -1
	*eh = old[0 : n-1]
	return entry
}
