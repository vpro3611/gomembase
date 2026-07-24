package multiplexer

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/zset_storage"
)

// Helpers to safely parse JSON arguments
func unmarshalString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return string(raw), nil
	}
	return s, nil
}

func unmarshalFloat64(raw json.RawMessage) (float64, error) {
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		var s string
		if errS := json.Unmarshal(raw, &s); errS == nil {
			return strconv.ParseFloat(s, 64)
		}
		return 0, err
	}
	return f, nil
}

func unmarshalInt(raw json.RawMessage) (int, error) {
	var i int
	if err := json.Unmarshal(raw, &i); err != nil {
		var s string
		if errS := json.Unmarshal(raw, &s); errS == nil {
			return strconv.Atoi(s)
		}
		return 0, err
	}
	return i, nil
}

func unmarshalInt64(raw json.RawMessage) (int64, error) {
	var i int64
	if err := json.Unmarshal(raw, &i); err != nil {
		var s string
		if errS := json.Unmarshal(raw, &s); errS == nil {
			return strconv.ParseInt(s, 10, 64)
		}
		return 0, err
	}
	return i, nil
}

func (m *Multiplexer) Execute(req Request) Response {
	m.mutex.Lock()
	total := m.TotalInstances()
	m.mutex.Unlock()

	if req.Method == "CREATE" {
		uuid, err := m.CreateInstance(req.DS)
		if err != nil {
			return Fail(err)
		}
		return WithUUID(uuid)
	}

	if req.Method == "DELETE_INSTANCE" {
		err := m.DeleteInstance(req.DS, req.UUID)
		if err != nil {
			return Fail(err)
		}
		return OK()
	}

	if req.Method == "TOTAL_INSTANCES" {
		return WithInteger(int64(total))
	}

	// Acquire read lock for normal execution to prevent transactions from running concurrently
	m.RLockTx()
	defer m.RUnlockTx()

	switch req.DS {
	case "kv":
		return m.executeKV(req)
	case "list":
		return m.executeList(req)
	case "set":
		return m.executeSet(req)
	case "zset":
		return m.executeZSet(req)
	}

	return FailMsg("unknown data structure type")
}

func (m *Multiplexer) executeKV(req Request) Response {
	eng, ok := m.GetKV(req.UUID)
	if !ok {
		return Fail(ErrInstanceNotFound)
	}

	switch req.Method {
	case "SET":
		if len(req.Args) < 2 {
			return FailMsg("SET requires key and value")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		err = eng.Set(key, []byte(req.Args[1]), storage.NewPayloadMetadata(time.Now(), nil))
		if err != nil {
			return Fail(err)
		}
		return OK()

	case "SET_TTL":
		if len(req.Args) < 3 {
			return FailMsg("SET_TTL requires key, value, and ttl_ms")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		ttlMs, err := unmarshalInt64(req.Args[2])
		if err != nil {
			return FailMsg("invalid ttl_ms parameter")
		}
		err = eng.SetWithTTL(key, []byte(req.Args[1]), time.Duration(ttlMs)*time.Millisecond)
		if err != nil {
			return Fail(err)
		}
		return OK()

	case "GET":
		if len(req.Args) < 1 {
			return FailMsg("GET requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		val, err := eng.Get(key)
		if err != nil {
			return Fail(err)
		}
		return WithValue(json.RawMessage(val))

	case "DEL":
		if len(req.Args) < 1 {
			return FailMsg("DEL requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		err = eng.Delete(key)
		if err != nil {
			return Fail(err)
		}
		return OK()

	case "EXISTS":
		if len(req.Args) < 1 {
			return FailMsg("EXISTS requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		exists := eng.Exists(key)
		return WithBoolean(exists)

	case "INCR":
		if len(req.Args) < 1 {
			return FailMsg("INCR requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		newVal, err := eng.Increment(key)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(newVal)

	case "DECR":
		if len(req.Args) < 1 {
			return FailMsg("DECR requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		newVal, err := eng.Decrement(key)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(newVal)

	case "INCRBY":
		if len(req.Args) < 2 {
			return FailMsg("INCRBY requires key and amount")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		amt, err := unmarshalInt64(req.Args[1])
		if err != nil {
			return FailMsg("invalid amount parameter")
		}
		newVal, err := eng.IncrementBy(key, amt)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(newVal)

	case "DECRBY":
		if len(req.Args) < 2 {
			return FailMsg("DECRBY requires key and amount")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		amt, err := unmarshalInt64(req.Args[1])
		if err != nil {
			return FailMsg("invalid amount parameter")
		}
		newVal, err := eng.DecrementBy(key, amt)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(newVal)

	case "MGET":
		if len(req.Args) < 1 {
			return FailMsg("MGET requires at least one key")
		}
		keys := make([]string, len(req.Args))
		for i, arg := range req.Args {
			k, err := unmarshalString(arg)
			if err != nil {
				return FailMsg("failed to parse key")
			}
			keys[i] = k
		}
		resMap, err := eng.Mget(keys)
		if err != nil {
			return Fail(err)
		}
		data := make(map[string]json.RawMessage, len(keys))
		for _, k := range keys {
			if v, ok := resMap[k]; ok {
				data[k] = json.RawMessage(v)
			} else {
				data[k] = nil
			}
		}
		return WithKeyVals(data)

	case "MSET":
		if len(req.Args)%2 != 0 {
			return FailMsg("MSET requires key-value pairs")
		}
		payloadMap := make(map[string]storage.Payload)
		now := time.Now()
		for i := 0; i < len(req.Args); i += 2 {
			k, err := unmarshalString(req.Args[i])
			if err != nil {
				return FailMsg("failed to parse key")
			}
			metadata := storage.NewPayloadMetadata(now, nil)
			payloadMap[k] = storage.NewPayload([]byte(req.Args[i+1]), metadata)
		}
		err := eng.Mset(payloadMap)
		if err != nil {
			return Fail(err)
		}
		return OK()

	case "MSET_TTL":
		// Args: [key1, val1, key2, val2, ..., ttl_ms]
		if len(req.Args) < 3 || len(req.Args)%2 == 0 {
			return FailMsg("MSET_TTL requires key-value pairs followed by ttl_ms")
		}
		ttlMs, err := unmarshalInt64(req.Args[len(req.Args)-1])
		if err != nil {
			return FailMsg("invalid ttl_ms parameter")
		}
		ttl := time.Duration(ttlMs) * time.Millisecond
		now := time.Now()
		expAt := now.Add(ttl)
		payloadMap := make(map[string]storage.Payload)
		pairs := req.Args[:len(req.Args)-1]
		for i := 0; i < len(pairs); i += 2 {
			k, err := unmarshalString(pairs[i])
			if err != nil {
				return FailMsg("failed to parse key")
			}
			metadata := storage.NewPayloadMetadata(now, &expAt)
			payloadMap[k] = storage.NewPayload([]byte(pairs[i+1]), metadata)
		}
		if err := eng.MsetWithTTL(payloadMap, ttl); err != nil {
			return Fail(err)
		}
		return OK()

	case "COUNT_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		return WithInteger(eng.CountByPrefix(pref))
	case "COUNT_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		return WithInteger(eng.CountBySuffix(suf))
	case "COUNT_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		c, err := eng.CountByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)

	case "FIND_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		m := eng.FindByPrefix(pref)
		data := make(map[string]json.RawMessage, len(m))
		for k, v := range m {
			data[k] = json.RawMessage(v)
		}
		return WithKeyVals(data)
	case "FIND_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		m := eng.FindBySuffix(suf)
		data := make(map[string]json.RawMessage, len(m))
		for k, v := range m {
			data[k] = json.RawMessage(v)
		}
		return WithKeyVals(data)
	case "FIND_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		m, err := eng.FindByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		data := make(map[string]json.RawMessage, len(m))
		for k, v := range m {
			data[k] = json.RawMessage(v)
		}
		return WithKeyVals(data)

	case "DEL_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		return WithInteger(eng.DeleteByPrefix(pref))
	case "DEL_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		return WithInteger(eng.DeleteBySuffix(suf))
	case "DEL_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		c, err := eng.DeleteByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	}

	return FailMsg("unknown method")
}

func (m *Multiplexer) executeList(req Request) Response {
	eng, ok := m.GetList(req.UUID)
	if !ok {
		return Fail(ErrInstanceNotFound)
	}

	switch req.Method {
	case "LPUSH":
		if len(req.Args) < 2 {
			return FailMsg("LPUSH requires key and at least one value")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		vals := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			vals[i] = []byte(arg)
		}
		err = eng.LeftPush(key, vals, nil)
		if err != nil {
			return Fail(err)
		}
		return OK()

	case "RPUSH":
		if len(req.Args) < 2 {
			return FailMsg("RPUSH requires key and at least one value")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		vals := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			vals[i] = []byte(arg)
		}
		err = eng.RightPush(key, vals, nil)
		if err != nil {
			return Fail(err)
		}
		return OK()

	case "LPOP":
		if len(req.Args) < 1 {
			return FailMsg("LPOP requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		val, err := eng.LeftPop(key)
		if err != nil {
			return Fail(err)
		}
		return WithValue(json.RawMessage(val))

	case "RPOP":
		if len(req.Args) < 1 {
			return FailMsg("RPOP requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		val, err := eng.RightPop(key)
		if err != nil {
			return Fail(err)
		}
		return WithValue(json.RawMessage(val))

	case "LLEN":
		if len(req.Args) < 1 {
			return FailMsg("LLEN requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		length, err := eng.Len(key)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(length)

	case "LRANGE":
		if len(req.Args) < 3 {
			return FailMsg("LRANGE requires key, start, and stop")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		start, err := unmarshalInt(req.Args[1])
		if err != nil {
			return FailMsg("invalid start index")
		}
		stop, err := unmarshalInt(req.Args[2])
		if err != nil {
			return FailMsg("invalid stop index")
		}
		listBytes, err := eng.Range(key, start, stop)
		if err != nil {
			return Fail(err)
		}
		data := make([]json.RawMessage, len(listBytes))
		for i, v := range listBytes {
			data[i] = json.RawMessage(v)
		}
		return WithItems(data)

	case "LINDEX":
		if len(req.Args) < 2 {
			return FailMsg("LINDEX requires key and index")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		idx, err := unmarshalInt(req.Args[1])
		if err != nil {
			return FailMsg("invalid index")
		}
		val, err := eng.Index(key, idx)
		if err != nil {
			return Fail(err)
		}
		return WithValue(json.RawMessage(val))

	case "LSET":
		if len(req.Args) < 3 {
			return FailMsg("LSET requires key, index, and value")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		idx, err := unmarshalInt(req.Args[1])
		if err != nil {
			return FailMsg("invalid index")
		}
		err = eng.ReplaceAtIndex(key, idx, []byte(req.Args[2]))
		if err != nil {
			return Fail(err)
		}
		return OK()

	case "LINSERT":
		if len(req.Args) < 4 {
			return FailMsg("LINSERT requires key, BEFORE/AFTER, pivot, and value")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		pos, err := unmarshalString(req.Args[1])
		if err != nil {
			return FailMsg("failed to parse position")
		}
		var before bool
		if pos == "BEFORE" {
			before = true
		} else if pos == "AFTER" {
			before = false
		} else {
			return FailMsg("invalid position (must be BEFORE or AFTER)")
		}
		newLen, err := eng.Insert(key, []byte(req.Args[2]), []byte(req.Args[3]), before)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(newLen)

	case "LREM":
		if len(req.Args) < 3 {
			return FailMsg("LREM requires key, count, and value")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		count, err := unmarshalInt(req.Args[1])
		if err != nil {
			return FailMsg("invalid count")
		}
		removed, err := eng.Remove(key, count, []byte(req.Args[2]))
		if err != nil {
			return Fail(err)
		}
		return WithInteger(removed)

	case "LPOS":
		if len(req.Args) < 2 {
			return FailMsg("LPOS requires key and value")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		pos, err := eng.Pos(key, []byte(req.Args[1]))
		if err != nil {
			return Fail(err)
		}
		return WithInteger(pos)

	case "RPOPLPUSH":
		if len(req.Args) < 2 {
			return FailMsg("RPOPLPUSH requires keyFrom and keyTo")
		}
		keyFrom, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse keyFrom")
		}
		keyTo, err := unmarshalString(req.Args[1])
		if err != nil {
			return FailMsg("failed to parse keyTo")
		}
		val, err := eng.Move(keyFrom, keyTo, false, true)
		if err != nil {
			return Fail(err)
		}
		return WithValue(json.RawMessage(val))

	case "LGET":
		if len(req.Args) < 1 {
			return FailMsg("LGET requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		listBytes, err := eng.Get(key)
		if err != nil {
			return Fail(err)
		}
		listData := make([]json.RawMessage, len(listBytes))
		for i, v := range listBytes {
			listData[i] = json.RawMessage(v)
		}
		return WithItems(listData)

	case "DEL":
		if len(req.Args) < 1 {
			return FailMsg("DEL requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		err = eng.Delete(key)
		if err != nil {
			return Fail(err)
		}
		return OK()

	case "COUNT_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		c, err := eng.CountByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "COUNT_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		c, err := eng.CountBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "COUNT_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		c, err := eng.CountByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)

	case "FIND_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		listFindMap, err := eng.FindByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		entries := make([]Entry, 0, len(listFindMap))
		for k, vals := range listFindMap {
			items := make([]json.RawMessage, len(vals))
			for i, v := range vals {
				items[i] = json.RawMessage(v)
			}
			entries = append(entries, Entry{Key: k, Items: items})
		}
		return WithEntries(entries)
	case "FIND_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		listFindMap, err := eng.FindBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		entries := make([]Entry, 0, len(listFindMap))
		for k, vals := range listFindMap {
			items := make([]json.RawMessage, len(vals))
			for i, v := range vals {
				items[i] = json.RawMessage(v)
			}
			entries = append(entries, Entry{Key: k, Items: items})
		}
		return WithEntries(entries)
	case "FIND_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		listFindMap, err := eng.FindByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		entries := make([]Entry, 0, len(listFindMap))
		for k, vals := range listFindMap {
			items := make([]json.RawMessage, len(vals))
			for i, v := range vals {
				items[i] = json.RawMessage(v)
			}
			entries = append(entries, Entry{Key: k, Items: items})
		}
		return WithEntries(entries)

	case "DEL_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		c, err := eng.DeleteByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "DEL_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		c, err := eng.DeleteBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "DEL_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		c, err := eng.DeleteByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	}

	return FailMsg("unknown method")
}

func (m *Multiplexer) executeSet(req Request) Response {
	eng, ok := m.GetSet(req.UUID)
	if !ok {
		return Fail(ErrInstanceNotFound)
	}

	switch req.Method {
	case "SADD":
		if len(req.Args) < 2 {
			return FailMsg("SADD requires key and at least one member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		mems := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			mems[i] = []byte(arg)
		}
		added, err := eng.SAdd(key, mems, nil)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(added)

	case "SREM":
		if len(req.Args) < 2 {
			return FailMsg("SREM requires key and at least one member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		mems := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			mems[i] = []byte(arg)
		}
		removed, err := eng.SRem(key, mems)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(removed)

	case "SPOP":
		if len(req.Args) < 1 {
			return FailMsg("SPOP requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		count := 1
		if len(req.Args) >= 2 {
			var err error
			count, err = unmarshalInt(req.Args[1])
			if err != nil {
				return FailMsg("invalid count")
			}
		}
		popped, err := eng.SPop(key, count)
		if err != nil {
			return Fail(err)
		}
		data := make([]json.RawMessage, len(popped))
		for i, v := range popped {
			data[i] = json.RawMessage(v)
		}
		return WithItems(data)

	case "SMOVE":
		if len(req.Args) < 3 {
			return FailMsg("SMOVE requires source, destination, and member")
		}
		srcKey, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse source key")
		}
		dstKey, err := unmarshalString(req.Args[1])
		if err != nil {
			return FailMsg("failed to parse destination key")
		}
		moved, err := eng.SMove(srcKey, dstKey, []byte(req.Args[2]))
		if err != nil {
			return Fail(err)
		}
		return WithBoolean(moved)

	case "SISMEMBER":
		if len(req.Args) < 2 {
			return FailMsg("SISMEMBER requires key and member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		isMem, err := eng.SIsMember(key, []byte(req.Args[1]))
		if err != nil {
			return Fail(err)
		}
		return WithBoolean(isMem)

	case "SMISMEMBER":
		if len(req.Args) < 2 {
			return FailMsg("SMISMEMBER requires key and at least one member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		mems := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			mems[i] = []byte(arg)
		}
		isMems, err := eng.SMIsMember(key, mems)
		if err != nil {
			return Fail(err)
		}
		return WithFlags(isMems)

	case "SMEMBERS":
		if len(req.Args) < 1 {
			return FailMsg("SMEMBERS requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		mems, err := eng.SMembers(key)
		if err != nil {
			return Fail(err)
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return WithItems(data)

	case "SCARD":
		if len(req.Args) < 1 {
			return FailMsg("SCARD requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		card, err := eng.SCard(key)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(card)

	case "SRANDMEMBER":
		if len(req.Args) < 1 {
			return FailMsg("SRANDMEMBER requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		count := 1
		if len(req.Args) >= 2 {
			var err error
			count, err = unmarshalInt(req.Args[1])
			if err != nil {
				return FailMsg("invalid count")
			}
		}
		mems, err := eng.SRandMember(key, count)
		if err != nil {
			return Fail(err)
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return WithItems(data)

	case "SINTER":
		if len(req.Args) < 1 {
			return FailMsg("SINTER requires at least one key")
		}
		keys := make([]string, len(req.Args))
		for i, arg := range req.Args {
			k, err := unmarshalString(arg)
			if err != nil {
				return FailMsg("failed to parse key")
			}
			keys[i] = k
		}
		mems, err := eng.SInter(keys)
		if err != nil {
			return Fail(err)
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return WithItems(data)

	case "SINTERSTORE":
		if len(req.Args) < 2 {
			return FailMsg("SINTERSTORE requires destination and at least one source key")
		}
		dstKey, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse destination key")
		}
		keys := make([]string, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			k, err := unmarshalString(arg)
			if err != nil {
				return FailMsg("failed to parse key")
			}
			keys[i] = k
		}
		stored, err := eng.SInterStore(dstKey, keys)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(stored)

	case "SUNION":
		if len(req.Args) < 1 {
			return FailMsg("SUNION requires at least one key")
		}
		keys := make([]string, len(req.Args))
		for i, arg := range req.Args {
			k, err := unmarshalString(arg)
			if err != nil {
				return FailMsg("failed to parse key")
			}
			keys[i] = k
		}
		mems, err := eng.SUnion(keys)
		if err != nil {
			return Fail(err)
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return WithItems(data)

	case "SUNIONSTORE":
		if len(req.Args) < 2 {
			return FailMsg("SUNIONSTORE requires destination and at least one source key")
		}
		dstKey, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse destination key")
		}
		keys := make([]string, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			k, err := unmarshalString(arg)
			if err != nil {
				return FailMsg("failed to parse key")
			}
			keys[i] = k
		}
		stored, err := eng.SUnionStore(dstKey, keys)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(stored)

	case "SDIFF":
		if len(req.Args) < 1 {
			return FailMsg("SDIFF requires at least one key")
		}
		keys := make([]string, len(req.Args))
		for i, arg := range req.Args {
			k, err := unmarshalString(arg)
			if err != nil {
				return FailMsg("failed to parse key")
			}
			keys[i] = k
		}
		mems, err := eng.SDiff(keys)
		if err != nil {
			return Fail(err)
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return WithItems(data)

	case "SDIFFSTORE":
		if len(req.Args) < 2 {
			return FailMsg("SDIFFSTORE requires destination and at least one source key")
		}
		dstKey, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse destination key")
		}
		keys := make([]string, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			k, err := unmarshalString(arg)
			if err != nil {
				return FailMsg("failed to parse key")
			}
			keys[i] = k
		}
		stored, err := eng.SDiffStore(dstKey, keys)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(stored)

	case "SINTERCARD":
		if len(req.Args) < 1 {
			return FailMsg("SINTERCARD requires at least one key")
		}
		limit := 0
		keys := make([]string, len(req.Args))
		// Optional last arg is limit if all but last parse as keys; we always parse all as keys
		// Convention: args are [key1, key2, ..., limit] where limit is optional integer
		lastIdx := len(req.Args) - 1
		if l, err := unmarshalInt(req.Args[lastIdx]); err == nil {
			// Check if second-to-last is also a valid int (unlikely for a key name)
			// Use heuristic: if only one arg, treat as key; otherwise last is limit
			if len(req.Args) > 1 {
				limit = l
				for i, arg := range req.Args[:lastIdx] {
					k, err := unmarshalString(arg)
					if err != nil {
						return FailMsg("failed to parse key")
					}
					keys[i] = k
				}
				keys = keys[:lastIdx]
			} else {
				k, err := unmarshalString(req.Args[0])
				if err != nil {
					return FailMsg("failed to parse key")
				}
				keys[0] = k
			}
		} else {
			for i, arg := range req.Args {
				k, err := unmarshalString(arg)
				if err != nil {
					return FailMsg("failed to parse key")
				}
				keys[i] = k
			}
		}
		card, err := eng.SInterCard(keys, limit)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(card)

	case "COUNT_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		c, err := eng.CountByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "COUNT_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		c, err := eng.CountBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "COUNT_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		c, err := eng.CountByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)

	case "FIND_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		resMap, err := eng.FindByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		entries := make([]Entry, 0, len(resMap))
		for k, vals := range resMap {
			items := make([]json.RawMessage, len(vals))
			for i, v := range vals {
				items[i] = json.RawMessage(v)
			}
			entries = append(entries, Entry{Key: k, Items: items})
		}
		return WithEntries(entries)
	case "FIND_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		resMap, err := eng.FindBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		entries := make([]Entry, 0, len(resMap))
		for k, vals := range resMap {
			items := make([]json.RawMessage, len(vals))
			for i, v := range vals {
				items[i] = json.RawMessage(v)
			}
			entries = append(entries, Entry{Key: k, Items: items})
		}
		return WithEntries(entries)
	case "FIND_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		resMap, err := eng.FindByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		entries := make([]Entry, 0, len(resMap))
		for k, vals := range resMap {
			items := make([]json.RawMessage, len(vals))
			for i, v := range vals {
				items[i] = json.RawMessage(v)
			}
			entries = append(entries, Entry{Key: k, Items: items})
		}
		return WithEntries(entries)

	case "DEL_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		c, err := eng.DeleteByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "DEL_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		c, err := eng.DeleteBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "DEL_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		c, err := eng.DeleteByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)

	case "DEL":
		if len(req.Args) < 1 {
			return FailMsg("DEL requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		err = eng.Delete(key)
		if err != nil {
			return Fail(err)
		}
		return OK()
	}

	return FailMsg("unknown method")
}

func (m *Multiplexer) executeZSet(req Request) Response {
	eng, ok := m.GetZSet(req.UUID)
	if !ok {
		return Fail(ErrInstanceNotFound)
	}

	switch req.Method {
	case "ZADD":
		if len(req.Args) < 3 || (len(req.Args)-1)%2 != 0 {
			return FailMsg("ZADD requires key and score-member pairs")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		mems := make([]zset_storage.ZSetMember, 0, (len(req.Args)-1)/2)
		for i := 1; i < len(req.Args); i += 2 {
			score, err := unmarshalFloat64(req.Args[i])
			if err != nil {
				return FailMsg("invalid score value")
			}
			mems = append(mems, zset_storage.ZSetMember{
				Score:  score,
				Member: []byte(req.Args[i+1]),
			})
		}
		added, err := eng.ZAdd(key, mems, nil)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(added)

	case "ZREM":
		if len(req.Args) < 2 {
			return FailMsg("ZREM requires key and at least one member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		mems := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			mems[i] = []byte(arg)
		}
		removed, err := eng.ZRem(key, mems)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(removed)

	case "ZINCRBY":
		if len(req.Args) < 3 {
			return FailMsg("ZINCRBY requires key, increment, and member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		inc, err := unmarshalFloat64(req.Args[1])
		if err != nil {
			return FailMsg("invalid increment value")
		}
		newScore, err := eng.ZIncrBy(key, inc, []byte(req.Args[2]))
		if err != nil {
			return Fail(err)
		}
		return WithFloat(newScore)

	case "ZPOPMAX":
		if len(req.Args) < 1 {
			return FailMsg("ZPOPMAX requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		count := 1
		if len(req.Args) >= 2 {
			var err error
			count, err = unmarshalInt(req.Args[1])
			if err != nil {
				return FailMsg("invalid count")
			}
		}
		popped, err := eng.ZPopMax(key, count)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredMember, 0, len(popped))
		for _, m := range popped {
			data = append(data, ScoredMember{Member: json.RawMessage(m.Member), Score: m.Score})
		}
		return WithScored(data)

	case "ZPOPMIN":
		if len(req.Args) < 1 {
			return FailMsg("ZPOPMIN requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		count := 1
		if len(req.Args) >= 2 {
			var err error
			count, err = unmarshalInt(req.Args[1])
			if err != nil {
				return FailMsg("invalid count")
			}
		}
		popped, err := eng.ZPopMin(key, count)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredMember, 0, len(popped))
		for _, m := range popped {
			data = append(data, ScoredMember{Member: json.RawMessage(m.Member), Score: m.Score})
		}
		return WithScored(data)

	case "ZSCORE":
		if len(req.Args) < 2 {
			return FailMsg("ZSCORE requires key and member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		score, found, err := eng.ZScore(key, []byte(req.Args[1]))
		if err != nil {
			return Fail(err)
		}
		if !found {
			return OK()
		}
		return WithFloat(score)

	case "ZCARD":
		if len(req.Args) < 1 {
			return FailMsg("ZCARD requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		card, err := eng.ZCard(key)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(card)

	case "ZRANK":
		if len(req.Args) < 2 {
			return FailMsg("ZRANK requires key and member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		rank, found, err := eng.ZRank(key, []byte(req.Args[1]))
		if err != nil {
			return Fail(err)
		}
		if !found {
			return OK()
		}
		return WithInteger(rank)

	case "ZREVRANK":
		if len(req.Args) < 2 {
			return FailMsg("ZREVRANK requires key and member")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		rank, found, err := eng.ZRevRank(key, []byte(req.Args[1]))
		if err != nil {
			return Fail(err)
		}
		if !found {
			return OK()
		}
		return WithInteger(rank)

	case "ZCOUNT":
		if len(req.Args) < 3 {
			return FailMsg("ZCOUNT requires key, min, and max")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		min, err := unmarshalFloat64(req.Args[1])
		if err != nil {
			return FailMsg("invalid min score")
		}
		max, err := unmarshalFloat64(req.Args[2])
		if err != nil {
			return FailMsg("invalid max score")
		}
		cnt, err := eng.ZCount(key, min, max)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(cnt)

	case "ZRANGE":
		if len(req.Args) < 3 {
			return FailMsg("ZRANGE requires key, start, and stop")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		start, err := unmarshalInt(req.Args[1])
		if err != nil {
			return FailMsg("invalid start rank")
		}
		stop, err := unmarshalInt(req.Args[2])
		if err != nil {
			return FailMsg("invalid stop rank")
		}
		res, err := eng.ZRange(key, start, stop)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredMember, 0, len(res))
		for _, m := range res {
			data = append(data, ScoredMember{Member: json.RawMessage(m.Member), Score: m.Score})
		}
		return WithScored(data)

	case "ZREVRANGE":
		if len(req.Args) < 3 {
			return FailMsg("ZREVRANGE requires key, start, and stop")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		start, err := unmarshalInt(req.Args[1])
		if err != nil {
			return FailMsg("invalid start rank")
		}
		stop, err := unmarshalInt(req.Args[2])
		if err != nil {
			return FailMsg("invalid stop rank")
		}
		res, err := eng.ZRevRange(key, start, stop)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredMember, 0, len(res))
		for _, m := range res {
			data = append(data, ScoredMember{Member: json.RawMessage(m.Member), Score: m.Score})
		}
		return WithScored(data)

	case "ZRANGEBYSCORE":
		if len(req.Args) < 3 {
			return FailMsg("ZRANGEBYSCORE requires key, min, and max")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		min, err := unmarshalFloat64(req.Args[1])
		if err != nil {
			return FailMsg("invalid min score")
		}
		max, err := unmarshalFloat64(req.Args[2])
		if err != nil {
			return FailMsg("invalid max score")
		}
		offset := 0
		count := -1
		if len(req.Args) >= 5 {
			var err error
			offset, err = unmarshalInt(req.Args[3])
			if err != nil {
				return FailMsg("invalid offset")
			}
			count, err = unmarshalInt(req.Args[4])
			if err != nil {
				return FailMsg("invalid count")
			}
		}
		res, err := eng.ZRangeByScore(key, min, max, offset, count)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredMember, 0, len(res))
		for _, m := range res {
			data = append(data, ScoredMember{Member: json.RawMessage(m.Member), Score: m.Score})
		}
		return WithScored(data)

	case "ZREVRANGEBYSCORE":
		if len(req.Args) < 3 {
			return FailMsg("ZREVRANGEBYSCORE requires key, max, and min")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		max, err := unmarshalFloat64(req.Args[1])
		if err != nil {
			return FailMsg("invalid max score")
		}
		min, err := unmarshalFloat64(req.Args[2])
		if err != nil {
			return FailMsg("invalid min score")
		}
		offset := 0
		count := -1
		if len(req.Args) >= 5 {
			var err error
			offset, err = unmarshalInt(req.Args[3])
			if err != nil {
				return FailMsg("invalid offset")
			}
			count, err = unmarshalInt(req.Args[4])
			if err != nil {
				return FailMsg("invalid count")
			}
		}
		res, err := eng.ZRevRangeByScore(key, max, min, offset, count)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredMember, 0, len(res))
		for _, m := range res {
			data = append(data, ScoredMember{Member: json.RawMessage(m.Member), Score: m.Score})
		}
		return WithScored(data)

	case "COUNT_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		c, err := eng.CountByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "COUNT_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		c, err := eng.CountBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "COUNT_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("COUNT_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		c, err := eng.CountByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)

	case "FIND_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		resMap, err := eng.FindByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredEntry, 0, len(resMap))
		for k, members := range resMap {
			items := make([]ScoredMember, 0, len(members))
			for _, member := range members {
				items = append(items, ScoredMember{Member: json.RawMessage(member.Member), Score: member.Score})
			}
			data = append(data, ScoredEntry{Key: k, Items: items})
		}
		return WithGrouped(data)
	case "FIND_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		resMap, err := eng.FindBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredEntry, 0, len(resMap))
		for k, members := range resMap {
			items := make([]ScoredMember, 0, len(members))
			for _, member := range members {
				items = append(items, ScoredMember{Member: json.RawMessage(member.Member), Score: member.Score})
			}
			data = append(data, ScoredEntry{Key: k, Items: items})
		}
		return WithGrouped(data)
	case "FIND_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("FIND_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		resMap, err := eng.FindByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		data := make([]ScoredEntry, 0, len(resMap))
		for k, members := range resMap {
			items := make([]ScoredMember, 0, len(members))
			for _, member := range members {
				items = append(items, ScoredMember{Member: json.RawMessage(member.Member), Score: member.Score})
			}
			data = append(data, ScoredEntry{Key: k, Items: items})
		}
		return WithGrouped(data)

	case "DEL_PREFIX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_PREFIX requires prefix")
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse prefix")
		}
		c, err := eng.DeleteByPrefix(pref)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "DEL_SUFFIX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_SUFFIX requires suffix")
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse suffix")
		}
		c, err := eng.DeleteBySuffix(suf)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)
	case "DEL_REGEX":
		if len(req.Args) < 1 {
			return FailMsg("DEL_REGEX requires regex")
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse regex")
		}
		c, err := eng.DeleteByRegex(rx)
		if err != nil {
			return Fail(err)
		}
		return WithInteger(c)

	case "DEL":
		if len(req.Args) < 1 {
			return FailMsg("DEL requires key")
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return FailMsg("failed to parse key")
		}
		err = eng.Delete(key)
		if err != nil {
			return Fail(err)
		}
		return OK()
	}

	return FailMsg("unknown method")
}
