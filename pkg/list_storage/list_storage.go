package list_storage

import (
	"bytes"
	"container/heap"
	"container/list"
	"encoding/base64"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"io"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type Value struct {
	value     *list.List
	createdAt time.Time
	expiresAt *time.Time
}

func NewValue(value *list.List, createdAt time.Time, expiresAt *time.Time) Value {
	return Value{value: value, createdAt: createdAt, expiresAt: expiresAt}
}

type ListStorageInterface interface {
	LeftPush(key string, values [][]byte, expiresAt *time.Time) error
	RightPush(key string, values [][]byte, expiresAt *time.Time) error
	LeftPop(key string) ([]byte, error)
	RightPop(key string) ([]byte, error)
	Move(keyFrom string, keyTo string, srcLeft, dstLeft bool) ([]byte, error)
	Len(key string) (int64, error)
	Range(key string, startIndex int, stopIndex int) ([][]byte, error)
	Get(key string) ([][]byte, error)
	Index(key string, index int) ([]byte, error)
	ReplaceAtIndex(key string, index int, value []byte) error
	Insert(key string, pivot []byte, value []byte, before bool) (int64, error)
	Remove(key string, count int, exactElement []byte) (int64, error)
	Pos(key string, value []byte) (int64, error)
	Delete(key string) error
	CleanupExpired()
}

type ListStorage struct {
	walLogger     persistence.WalLogger
	data          map[string]Value
	expirations   ListExpirationHeap
	expirationMap map[string]*ListExpirationEntry
	mutex         sync.RWMutex
}

func NewListStorage(logger persistence.WalLogger) *ListStorage {
	return &ListStorage{
		walLogger:     logger,
		data:          make(map[string]Value),
		expirations:   make(ListExpirationHeap, 0),
		expirationMap: make(map[string]*ListExpirationEntry),
	}
}

func (ls *ListStorage) Data() map[string]Value {
	return ls.data
}

func (ls *ListStorage) Expirations() ListExpirationHeap {
	return ls.expirations
}

func (ls *ListStorage) ExpirationMap() map[string]*ListExpirationEntry {
	return ls.expirationMap
}

// EngineID implements persistence.Engine
func (ls *ListStorage) EngineID() string {
	return "list"
}

// ProcessWalEntry implements persistence.Engine
func (ls *ListStorage) ProcessWalEntry(action string, args []string) error {
	switch action {
	case "LPUSH", "RPUSH":
		if len(args) < 2 {
			return nil
		}
		key, err := url.PathUnescape(args[0])
		if err != nil {
			key = args[0]
		}
		expiresStr := args[1]
		var expiresAt *time.Time
		if expiresStr != "PERSISTENT" {
			t, err := time.Parse(time.RFC3339, expiresStr)
			if err == nil {
				expiresAt = &t
			}
		}

		var values [][]byte
		for _, rawVal := range args[2:] {
			val, err := base64.StdEncoding.DecodeString(rawVal)
			if err != nil {
				val = []byte(rawVal)
			}
			values = append(values, val)
		}

		ls.pushNoLockAndNoWal(key, values, expiresAt, action == "LPUSH")

	case "LPOP", "RPOP":
		if len(args) < 1 {
			return nil
		}
		key, err := url.PathUnescape(args[0])
		if err != nil {
			key = args[0]
		}
		ls.popNoLockAndNoWal(key, action == "LPOP")

	case "MOVE":
		if len(args) < 4 {
			return nil
		}
		keyFrom, _ := url.PathUnescape(args[0])
		keyTo, _ := url.PathUnescape(args[1])
		srcLeft := args[2] == "true"
		dstLeft := args[3] == "true"
		ls.moveNoLockAndNoWal(keyFrom, keyTo, srcLeft, dstLeft)

	case "REPLACE":
		if len(args) < 3 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		index, err := strconv.Atoi(args[1])
		if err != nil {
			return nil
		}
		value, err := base64.StdEncoding.DecodeString(args[2])
		if err != nil {
			value = []byte(args[2])
		}
		ls.replaceNoLockAndNoWal(key, index, value)

	case "INSERT":
		if len(args) < 4 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		pivot, _ := base64.StdEncoding.DecodeString(args[1])
		value, _ := base64.StdEncoding.DecodeString(args[2])
		before := args[3] == "true"
		ls.insertNoLockAndNoWal(key, pivot, value, before)

	case "REMOVE":
		if len(args) < 3 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		count, _ := strconv.Atoi(args[1])
		value, _ := base64.StdEncoding.DecodeString(args[2])
		ls.removeNoLockAndNoWal(key, count, value)

	case "DELETE":
		if len(args) < 1 {
			return nil
		}
		key, _ := url.PathUnescape(args[0])
		ls.deleteNoLock(key)
	}
	return nil
}

// SerializeState implements persistence.Engine
func (ls *ListStorage) SerializeState(w io.Writer) error {
	ls.mutex.RLock()
	defer ls.mutex.RUnlock()

	if err := snapshot.WriteUintValue(w, uint64(len(ls.data))); err != nil {
		return err
	}

	for key, val := range ls.data {
		if err := snapshot.WriteBytes(w, []byte(key)); err != nil {
			return err
		}

		if err := snapshot.WriteUintValue(w, uint64(val.value.Len())); err != nil {
			return err
		}

		for elem := val.value.Front(); elem != nil; elem = elem.Next() {
			elemVal := elem.Value.([]byte)
			if err := snapshot.WriteBytes(w, elemVal); err != nil {
				return err
			}
		}

		if err := snapshot.WriteInt64Value(w, val.createdAt.UnixNano()); err != nil {
			return err
		}

		if val.expiresAt == nil {
			if err := snapshot.WriteUintValue[uint8](w, uint8(0)); err != nil {
				return err
			}
		} else {
			if err := snapshot.WriteUintValue[uint8](w, uint8(1)); err != nil {
				return err
			}
			if err := snapshot.WriteInt64Value(w, val.expiresAt.UnixNano()); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeserializeState implements persistence.Engine
func (ls *ListStorage) DeserializeState(r io.Reader) error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	count, err := snapshot.ReadUintValue[uint64](r)
	if err != nil {
		return err
	}

	ls.clearNoLock()

	for i := uint64(0); i < count; i++ {
		keyBytes, err := snapshot.ReadBytes(r)
		if err != nil {
			return err
		}
		key := string(keyBytes)

		listLen, err := snapshot.ReadUintValue[uint64](r)
		if err != nil {
			return err
		}

		lst := list.New()
		for j := uint64(0); j < listLen; j++ {
			elemVal, err := snapshot.ReadBytes(r)
			if err != nil {
				return err
			}
			lst.PushBack(elemVal)
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

		ls.data[key] = Value{
			value:     lst,
			createdAt: time.Unix(0, createdAtNano),
			expiresAt: expires,
		}

		if expires != nil {
			entry := &ListExpirationEntry{
				key:       key,
				expiresAt: *expires,
			}
			heap.Push(&ls.expirations, entry)
			ls.expirationMap[key] = entry
		}
	}
	return nil
}

// Reset implements persistence.Engine
func (ls *ListStorage) Reset() error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	ls.data = make(map[string]Value)
	ls.expirations = make(ListExpirationHeap, 0)
	ls.expirationMap = make(map[string]*ListExpirationEntry)
	return nil
}

// Clear implements persistence.Engine
func (ls *ListStorage) Clear() error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	ls.clearNoLock()
	return nil
}

func (ls *ListStorage) clearNoLock() {
	clear(ls.data)
	ls.expirations = make(ListExpirationHeap, 0)
	clear(ls.expirationMap)
}

func (ls *ListStorage) LeftPush(key string, values [][]byte, expiresAt *time.Time) error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	return ls.pushNoLock(key, values, expiresAt, true)
}

func (ls *ListStorage) RightPush(key string, values [][]byte, expiresAt *time.Time) error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	return ls.pushNoLock(key, values, expiresAt, false)
}

func (ls *ListStorage) pushNoLock(key string, values [][]byte, expiresAt *time.Time, toLeft bool) error {
	ls.pushNoLockAndNoWal(key, values, expiresAt, toLeft)

	// Log to WAL
	action := "RPUSH"
	if toLeft {
		action = "LPUSH"
	}

	expiresStr := "PERSISTENT"
	if expiresAt != nil {
		expiresStr = expiresAt.Format(time.RFC3339)
	}

	var args []string
	args = append(args, url.PathEscape(key), expiresStr)
	for _, val := range values {
		args = append(args, base64.StdEncoding.EncodeToString(val))
	}

	return ls.walLogger.Log(ls.EngineID(), action, args...)
}

func (ls *ListStorage) pushNoLockAndNoWal(key string, values [][]byte, expiresAt *time.Time, toLeft bool) {
	// Check expiration first
	if val, exists := ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.deleteNoLock(key)
	}

	val, exists := ls.data[key]
	var lst *list.List
	if !exists {
		lst = list.New()
		val = Value{
			value:     lst,
			createdAt: time.Now(),
			expiresAt: expiresAt,
		}
	} else {
		lst = val.value
		if expiresAt != nil {
			val.expiresAt = expiresAt
		}
	}

	// Insert elements
	for _, v := range values {
		if toLeft {
			lst.PushFront(v)
		} else {
			lst.PushBack(v)
		}
	}

	ls.data[key] = val

	// Update expiration heap
	if expiresAt != nil {
		if oldEntry, ok := ls.expirationMap[key]; ok {
			heap.Remove(&ls.expirations, oldEntry.index)
		}
		entry := &ListExpirationEntry{
			key:       key,
			expiresAt: *expiresAt,
		}
		heap.Push(&ls.expirations, entry)
		ls.expirationMap[key] = entry
	}
}

func (ls *ListStorage) LeftPop(key string) ([]byte, error) {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	return ls.popNoLock(key, true)
}

func (ls *ListStorage) RightPop(key string) ([]byte, error) {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	return ls.popNoLock(key, false)
}

func (ls *ListStorage) popNoLock(key string, fromLeft bool) ([]byte, error) {
	// Check expiration first
	if val, exists := ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.deleteNoLock(key)
		_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key))
	}

	val, exists := ls.data[key]
	if !exists || val.value.Len() == 0 {
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "POP"}
	}

	res := ls.popNoLockAndNoWal(key, fromLeft)

	action := "RPOP"
	if fromLeft {
		action = "LPOP"
	}
	if err := ls.walLogger.Log(ls.EngineID(), action, url.PathEscape(key)); err != nil {
		return nil, err
	}

	return res, nil
}

func (ls *ListStorage) popNoLockAndNoWal(key string, fromLeft bool) []byte {
	val := ls.data[key]
	var elem *list.Element
	if fromLeft {
		elem = val.value.Front()
	} else {
		elem = val.value.Back()
	}

	res := elem.Value.([]byte)
	val.value.Remove(elem)

	if val.value.Len() == 0 {
		ls.deleteNoLock(key)
	}
	return res
}

func (ls *ListStorage) Move(keyFrom string, keyTo string, srcLeft, dstLeft bool) ([]byte, error) {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	// Check expiration on keyFrom
	if val, exists := ls.data[keyFrom]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.deleteNoLock(keyFrom)
		_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(keyFrom))
	}

	valFrom, exists := ls.data[keyFrom]
	if !exists || valFrom.value.Len() == 0 {
		return nil, pkgerrors.KeyError{Key: keyFrom, Err: pkgerrors.KeyNotFoundError, Operation: "MOVE"}
	}

	// Check expiration on keyTo
	if val, exists := ls.data[keyTo]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.deleteNoLock(keyTo)
		_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(keyTo))
	}

	popped := ls.moveNoLockAndNoWal(keyFrom, keyTo, srcLeft, dstLeft)

	srcLeftStr := "false"
	if srcLeft {
		srcLeftStr = "true"
	}
	dstLeftStr := "false"
	if dstLeft {
		dstLeftStr = "true"
	}

	if err := ls.walLogger.Log(ls.EngineID(), "MOVE", url.PathEscape(keyFrom), url.PathEscape(keyTo), srcLeftStr, dstLeftStr); err != nil {
		return nil, err
	}

	return popped, nil
}

func (ls *ListStorage) moveNoLockAndNoWal(keyFrom string, keyTo string, srcLeft, dstLeft bool) []byte {
	popped := ls.popNoLockAndNoWal(keyFrom, srcLeft)

	valTo, exists := ls.data[keyTo]
	var lstTo *list.List
	if !exists {
		lstTo = list.New()
		valTo = Value{
			value:     lstTo,
			createdAt: time.Now(),
		}
	} else {
		lstTo = valTo.value
	}

	if dstLeft {
		lstTo.PushFront(popped)
	} else {
		lstTo.PushBack(popped)
	}

	ls.data[keyTo] = valTo
	return popped
}

func (ls *ListStorage) Len(key string) (int64, error) {
	// Read lock wrapper with expiration check
	ls.mutex.RLock()
	val, exists := ls.data[key]
	if !exists {
		ls.mutex.RUnlock()
		return 0, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.mutex.RUnlock()
		ls.mutex.Lock()
		if val, exists = ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ls.deleteNoLock(key)
			_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key))
		}
		ls.mutex.Unlock()
		return 0, nil
	}
	defer ls.mutex.RUnlock()

	return int64(val.value.Len()), nil
}

func (ls *ListStorage) Range(key string, startIndex int, stopIndex int) ([][]byte, error) {
	ls.mutex.RLock()
	val, exists := ls.data[key]
	if !exists {
		ls.mutex.RUnlock()
		return [][]byte{}, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.mutex.RUnlock()
		ls.mutex.Lock()
		if val, exists = ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ls.deleteNoLock(key)
			_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key))
		}
		ls.mutex.Unlock()
		return [][]byte{}, nil
	}
	defer ls.mutex.RUnlock()

	length := val.value.Len()
	if length == 0 {
		return [][]byte{}, nil
	}

	// Resolve negative indices
	if startIndex < 0 {
		startIndex = length + startIndex
	}
	if stopIndex < 0 {
		stopIndex = length + stopIndex
	}

	// Bounds checks
	if startIndex < 0 {
		startIndex = 0
	}
	if stopIndex >= length {
		stopIndex = length - 1
	}
	if startIndex > stopIndex {
		return [][]byte{}, nil
	}

	res := make([][]byte, 0, stopIndex-startIndex+1)
	idx := 0
	for elem := val.value.Front(); elem != nil; elem = elem.Next() {
		if idx >= startIndex && idx <= stopIndex {
			res = append(res, elem.Value.([]byte))
		}
		if idx > stopIndex {
			break
		}
		idx++
	}
	return res, nil
}

func (ls *ListStorage) Get(key string) ([][]byte, error) {
	return ls.Range(key, 0, -1)
}

func (ls *ListStorage) Index(key string, index int) ([]byte, error) {
	ls.mutex.RLock()
	val, exists := ls.data[key]
	if !exists {
		ls.mutex.RUnlock()
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "INDEX"}
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.mutex.RUnlock()
		ls.mutex.Lock()
		if val, exists = ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ls.deleteNoLock(key)
			_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key))
		}
		ls.mutex.Unlock()
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "INDEX"}
	}
	defer ls.mutex.RUnlock()

	length := val.value.Len()
	if index < 0 {
		index = length + index
	}

	if index < 0 || index >= length {
		return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "INDEX"}
	}

	idx := 0
	for elem := val.value.Front(); elem != nil; elem = elem.Next() {
		if idx == index {
			return elem.Value.([]byte), nil
		}
		idx++
	}
	return nil, pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "INDEX"}
}

func (ls *ListStorage) ReplaceAtIndex(key string, index int, value []byte) error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	// Check expiration first
	if val, exists := ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.deleteNoLock(key)
		_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key))
	}

	val, exists := ls.data[key]
	if !exists {
		return pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "REPLACE"}
	}

	length := val.value.Len()
	resIndex := index
	if resIndex < 0 {
		resIndex = length + resIndex
	}

	if resIndex < 0 || resIndex >= length {
		return pkgerrors.KeyError{Key: key, Err: pkgerrors.KeyNotFoundError, Operation: "REPLACE"}
	}

	ls.replaceNoLockAndNoWal(key, resIndex, value)

	encodedVal := base64.StdEncoding.EncodeToString(value)
	return ls.walLogger.Log(ls.EngineID(), "REPLACE", url.PathEscape(key), strconv.Itoa(index), encodedVal)
}

func (ls *ListStorage) replaceNoLockAndNoWal(key string, index int, value []byte) {
	val := ls.data[key]
	idx := 0
	for elem := val.value.Front(); elem != nil; elem = elem.Next() {
		if idx == index {
			elem.Value = value
			break
		}
		idx++
	}
}

func (ls *ListStorage) Insert(key string, pivot []byte, value []byte, before bool) (int64, error) {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	// Check expiration first
	if val, exists := ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.deleteNoLock(key)
		_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key))
	}

	_, exists := ls.data[key]
	if !exists {
		return 0, nil
	}

	newLen := ls.insertNoLockAndNoWal(key, pivot, value, before)
	if newLen < 0 {
		return -1, nil
	}

	beforeStr := "false"
	if before {
		beforeStr = "true"
	}
	encodedPivot := base64.StdEncoding.EncodeToString(pivot)
	encodedValue := base64.StdEncoding.EncodeToString(value)

	if err := ls.walLogger.Log(ls.EngineID(), "INSERT", url.PathEscape(key), encodedPivot, encodedValue, beforeStr); err != nil {
		return 0, err
	}

	return newLen, nil
}

func (ls *ListStorage) insertNoLockAndNoWal(key string, pivot []byte, value []byte, before bool) int64 {
	val := ls.data[key]
	var pivotElem *list.Element
	for elem := val.value.Front(); elem != nil; elem = elem.Next() {
		if bytes.Equal(elem.Value.([]byte), pivot) {
			pivotElem = elem
			break
		}
	}

	if pivotElem == nil {
		return -1
	}

	if before {
		val.value.InsertBefore(value, pivotElem)
	} else {
		val.value.InsertAfter(value, pivotElem)
	}

	return int64(val.value.Len())
}

func (ls *ListStorage) Remove(key string, count int, exactElement []byte) (int64, error) {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	// Check expiration first
	if val, exists := ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.deleteNoLock(key)
		_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key))
	}

	_, exists := ls.data[key]
	if !exists {
		return 0, nil
	}

	removed := ls.removeNoLockAndNoWal(key, count, exactElement)
	if removed > 0 {
		encodedElem := base64.StdEncoding.EncodeToString(exactElement)
		if err := ls.walLogger.Log(ls.EngineID(), "REMOVE", url.PathEscape(key), strconv.Itoa(count), encodedElem); err != nil {
			return 0, err
		}
	}
	return removed, nil
}

func (ls *ListStorage) removeNoLockAndNoWal(key string, count int, exactElement []byte) int64 {
	val := ls.data[key]
	var removed int64

	if count > 0 {
		// Remove `count` occurrences from Left to Right
		elem := val.value.Front()
		for elem != nil && removed < int64(count) {
			next := elem.Next()
			if bytes.Equal(elem.Value.([]byte), exactElement) {
				val.value.Remove(elem)
				removed++
			}
			elem = next
		}
	} else if count < 0 {
		// Remove `-count` occurrences from Right to Left
		elem := val.value.Back()
		limit := -count
		for elem != nil && removed < int64(limit) {
			prev := elem.Prev()
			if bytes.Equal(elem.Value.([]byte), exactElement) {
				val.value.Remove(elem)
				removed++
			}
			elem = prev
		}
	} else {
		// Remove all occurrences
		elem := val.value.Front()
		for elem != nil {
			next := elem.Next()
			if bytes.Equal(elem.Value.([]byte), exactElement) {
				val.value.Remove(elem)
				removed++
			}
			elem = next
		}
	}

	if val.value.Len() == 0 {
		ls.deleteNoLock(key)
	}

	return removed
}

func (ls *ListStorage) Pos(key string, value []byte) (int64, error) {
	ls.mutex.RLock()
	val, exists := ls.data[key]
	if !exists {
		ls.mutex.RUnlock()
		return -1, nil
	}

	if val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
		ls.mutex.RUnlock()
		ls.mutex.Lock()
		if val, exists = ls.data[key]; exists && val.expiresAt != nil && val.expiresAt.Before(time.Now()) {
			ls.deleteNoLock(key)
			_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key))
		}
		ls.mutex.Unlock()
		return -1, nil
	}
	defer ls.mutex.RUnlock()

	var idx int64
	for elem := val.value.Front(); elem != nil; elem = elem.Next() {
		if bytes.Equal(elem.Value.([]byte), value) {
			return idx, nil
		}
		idx++
	}
	return -1, nil
}

func (ls *ListStorage) Delete(key string) error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	if err := ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(key)); err != nil {
		return err
	}

	ls.deleteNoLock(key)
	return nil
}

func (ls *ListStorage) deleteNoLock(key string) {
	delete(ls.data, key)
	if entry, ok := ls.expirationMap[key]; ok {
		heap.Remove(&ls.expirations, entry.index)
		delete(ls.expirationMap, key)
	}
}

func (ls *ListStorage) CleanupExpired() {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	now := time.Now()
	for ls.expirations.Len() > 0 {
		entry := ls.expirations[0]
		if entry.expiresAt.After(now) {
			break
		}
		ls.deleteNoLock(entry.key)
		_ = ls.walLogger.Log(ls.EngineID(), "DELETE", url.PathEscape(entry.key))
	}
}
