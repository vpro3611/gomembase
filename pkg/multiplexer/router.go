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

func marshalString(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

func marshalInt64(v int64) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func marshalBool(v bool) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func marshalFloat64(v float64) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func (m *Multiplexer) Execute(req Request) Response {
	m.mutex.Lock()
	total := m.TotalInstances()
	m.mutex.Unlock()

	if req.Method == "CREATE" {
		uuid, err := m.CreateInstance(req.DS)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, UUID: uuid}
	}

	if req.Method == "DELETE_INSTANCE" {
		err := m.DeleteInstance(req.DS, req.UUID)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}
	}

	if req.Method == "TOTAL_INSTANCES" {
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(int64(total))}}
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

	return Response{OK: false, Error: "unknown data structure type"}
}

func (m *Multiplexer) executeKV(req Request) Response {
	eng, ok := m.GetKV(req.UUID)
	if !ok {
		return Response{OK: false, Error: ErrInstanceNotFound.Error()}
	}

	switch req.Method {
	case "SET":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "SET requires key and value"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		err = eng.Set(key, []byte(req.Args[1]), storage.NewPayloadMetadata(time.Now(), nil))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}

	case "SET_TTL":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "SET_TTL requires key, value, and ttl_ms"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		ttlMs, err := unmarshalInt64(req.Args[2])
		if err != nil {
			return Response{OK: false, Error: "invalid ttl_ms parameter"}
		}
		err = eng.SetWithTTL(key, []byte(req.Args[1]), time.Duration(ttlMs)*time.Millisecond)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}

	case "GET":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "GET requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		val, err := eng.Get(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{json.RawMessage(val)}}

	case "DEL":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "DEL requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		err = eng.Delete(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}

	case "EXISTS":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "EXISTS requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		exists := eng.Exists(key)
		return Response{OK: true, Data: []json.RawMessage{marshalBool(exists)}}

	case "INCR":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "INCR requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		newVal, err := eng.Increment(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(newVal)}}

	case "DECR":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "DECR requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		newVal, err := eng.Decrement(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(newVal)}}

	case "INCRBY":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "INCRBY requires key and amount"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		amt, err := unmarshalInt64(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid amount parameter"}
		}
		newVal, err := eng.IncrementBy(key, amt)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(newVal)}}

	case "DECRBY":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "DECRBY requires key and amount"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		amt, err := unmarshalInt64(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid amount parameter"}
		}
		newVal, err := eng.DecrementBy(key, amt)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(newVal)}}

	case "MGET":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "MGET requires at least one key"}
		}
		keys := make([]string, len(req.Args))
		for i, arg := range req.Args {
			k, err := unmarshalString(arg)
			if err != nil {
				return Response{OK: false, Error: "failed to parse key"}
			}
			keys[i] = k
		}
		resMap, err := eng.Mget(keys)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, 0, len(keys)*2)
		for _, k := range keys {
			data = append(data, marshalString(k))
			if v, ok := resMap[k]; ok {
				data = append(data, json.RawMessage(v))
			} else {
				data = append(data, nil)
			}
		}
		return Response{OK: true, Data: data}

	case "MSET":
		if len(req.Args)%2 != 0 {
			return Response{OK: false, Error: "MSET requires key-value pairs"}
		}
		payloadMap := make(map[string]storage.Payload)
		now := time.Now()
		for i := 0; i < len(req.Args); i += 2 {
			k, err := unmarshalString(req.Args[i])
			if err != nil {
				return Response{OK: false, Error: "failed to parse key"}
			}
			metadata := storage.NewPayloadMetadata(now, nil)
			payloadMap[k] = storage.NewPayload([]byte(req.Args[i+1]), metadata)
		}
		err := eng.Mset(payloadMap)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}

	case "COUNT_PREFIX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "COUNT_PREFIX requires prefix"}
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse prefix"}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(eng.CountByPrefix(pref))}}
	case "COUNT_SUFFIX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "COUNT_SUFFIX requires suffix"}
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse suffix"}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(eng.CountBySuffix(suf))}}
	case "COUNT_REGEX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "COUNT_REGEX requires regex"}
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse regex"}
		}
		c, err := eng.CountByRegex(rx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(c)}}

	case "FIND_PREFIX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "FIND_PREFIX requires prefix"}
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse prefix"}
		}
		m := eng.FindByPrefix(pref)
		data := make([]json.RawMessage, 0, len(m)*2)
		for k, v := range m {
			data = append(data, marshalString(k), json.RawMessage(v))
		}
		return Response{OK: true, Data: data}
	case "FIND_SUFFIX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "FIND_SUFFIX requires suffix"}
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse suffix"}
		}
		m := eng.FindBySuffix(suf)
		data := make([]json.RawMessage, 0, len(m)*2)
		for k, v := range m {
			data = append(data, marshalString(k), json.RawMessage(v))
		}
		return Response{OK: true, Data: data}
	case "FIND_REGEX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "FIND_REGEX requires regex"}
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse regex"}
		}
		m, err := eng.FindByRegex(rx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, 0, len(m)*2)
		for k, v := range m {
			data = append(data, marshalString(k), json.RawMessage(v))
		}
		return Response{OK: true, Data: data}

	case "DEL_PREFIX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "DEL_PREFIX requires prefix"}
		}
		pref, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse prefix"}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(eng.DeleteByPrefix(pref))}}
	case "DEL_SUFFIX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "DEL_SUFFIX requires suffix"}
		}
		suf, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse suffix"}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(eng.DeleteBySuffix(suf))}}
	case "DEL_REGEX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "DEL_REGEX requires regex"}
		}
		rx, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse regex"}
		}
		c, err := eng.DeleteByRegex(rx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(c)}}
	}

	return Response{OK: false, Error: "unknown method"}
}

func (m *Multiplexer) executeList(req Request) Response {
	eng, ok := m.GetList(req.UUID)
	if !ok {
		return Response{OK: false, Error: ErrInstanceNotFound.Error()}
	}

	switch req.Method {
	case "LPUSH":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "LPUSH requires key and at least one value"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		vals := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			vals[i] = []byte(arg)
		}
		err = eng.LeftPush(key, vals, nil)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}

	case "RPUSH":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "RPUSH requires key and at least one value"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		vals := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			vals[i] = []byte(arg)
		}
		err = eng.RightPush(key, vals, nil)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}

	case "LPOP":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "LPOP requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		val, err := eng.LeftPop(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{json.RawMessage(val)}}

	case "RPOP":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "RPOP requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		val, err := eng.RightPop(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{json.RawMessage(val)}}

	case "LLEN":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "LLEN requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		length, err := eng.Len(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(length)}}

	case "LRANGE":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "LRANGE requires key, start, and stop"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		start, err := unmarshalInt(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid start index"}
		}
		stop, err := unmarshalInt(req.Args[2])
		if err != nil {
			return Response{OK: false, Error: "invalid stop index"}
		}
		listBytes, err := eng.Range(key, start, stop)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, len(listBytes))
		for i, v := range listBytes {
			data[i] = json.RawMessage(v)
		}
		return Response{OK: true, Data: data}

	case "LINDEX":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "LINDEX requires key and index"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		idx, err := unmarshalInt(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid index"}
		}
		val, err := eng.Index(key, idx)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{json.RawMessage(val)}}

	case "LSET":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "LSET requires key, index, and value"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		idx, err := unmarshalInt(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid index"}
		}
		err = eng.ReplaceAtIndex(key, idx, []byte(req.Args[2]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}

	case "LINSERT":
		if len(req.Args) < 4 {
			return Response{OK: false, Error: "LINSERT requires key, BEFORE/AFTER, pivot, and value"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		pos, err := unmarshalString(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "failed to parse position"}
		}
		var before bool
		if pos == "BEFORE" {
			before = true
		} else if pos == "AFTER" {
			before = false
		} else {
			return Response{OK: false, Error: "invalid position (must be BEFORE or AFTER)"}
		}
		newLen, err := eng.Insert(key, []byte(req.Args[2]), []byte(req.Args[3]), before)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(newLen)}}

	case "LREM":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "LREM requires key, count, and value"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		count, err := unmarshalInt(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid count"}
		}
		removed, err := eng.Remove(key, count, []byte(req.Args[2]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(removed)}}

	case "LPOS":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "LPOS requires key and value"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		pos, err := eng.Pos(key, []byte(req.Args[1]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(pos)}}

	case "RPOPLPUSH":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "RPOPLPUSH requires keyFrom and keyTo"}
		}
		keyFrom, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse keyFrom"}
		}
		keyTo, err := unmarshalString(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "failed to parse keyTo"}
		}
		val, err := eng.Move(keyFrom, keyTo, false, true)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{json.RawMessage(val)}}

	case "DEL":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "DEL requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		err = eng.Delete(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}
	}

	return Response{OK: false, Error: "unknown method"}
}

func (m *Multiplexer) executeSet(req Request) Response {
	eng, ok := m.GetSet(req.UUID)
	if !ok {
		return Response{OK: false, Error: ErrInstanceNotFound.Error()}
	}

	switch req.Method {
	case "SADD":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "SADD requires key and at least one member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		mems := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			mems[i] = []byte(arg)
		}
		added, err := eng.SAdd(key, mems, nil)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(added)}}

	case "SREM":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "SREM requires key and at least one member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		mems := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			mems[i] = []byte(arg)
		}
		removed, err := eng.SRem(key, mems)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(removed)}}

	case "SPOP":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "SPOP requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		count := 1
		if len(req.Args) >= 2 {
			var err error
			count, err = unmarshalInt(req.Args[1])
			if err != nil {
				return Response{OK: false, Error: "invalid count"}
			}
		}
		popped, err := eng.SPop(key, count)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, len(popped))
		for i, v := range popped {
			data[i] = json.RawMessage(v)
		}
		return Response{OK: true, Data: data}

	case "SMOVE":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "SMOVE requires source, destination, and member"}
		}
		srcKey, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse source key"}
		}
		dstKey, err := unmarshalString(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "failed to parse destination key"}
		}
		moved, err := eng.SMove(srcKey, dstKey, []byte(req.Args[2]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalBool(moved)}}

	case "SISMEMBER":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "SISMEMBER requires key and member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		isMem, err := eng.SIsMember(key, []byte(req.Args[1]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalBool(isMem)}}

	case "SMISMEMBER":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "SMISMEMBER requires key and at least one member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		mems := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			mems[i] = []byte(arg)
		}
		isMems, err := eng.SMIsMember(key, mems)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, len(isMems))
		for i, b := range isMems {
			data[i] = marshalBool(b)
		}
		return Response{OK: true, Data: data}

	case "SMEMBERS":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "SMEMBERS requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		mems, err := eng.SMembers(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return Response{OK: true, Data: data}

	case "SCARD":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "SCARD requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		card, err := eng.SCard(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(card)}}

	case "SRANDMEMBER":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "SRANDMEMBER requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		count := 1
		if len(req.Args) >= 2 {
			var err error
			count, err = unmarshalInt(req.Args[1])
			if err != nil {
				return Response{OK: false, Error: "invalid count"}
			}
		}
		mems, err := eng.SRandMember(key, count)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return Response{OK: true, Data: data}

	case "SINTER":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "SINTER requires at least one key"}
		}
		keys := make([]string, len(req.Args))
		for i, arg := range req.Args {
			k, err := unmarshalString(arg)
			if err != nil {
				return Response{OK: false, Error: "failed to parse key"}
			}
			keys[i] = k
		}
		mems, err := eng.SInter(keys)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return Response{OK: true, Data: data}

	case "SINTERSTORE":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "SINTERSTORE requires destination and at least one source key"}
		}
		dstKey, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse destination key"}
		}
		keys := make([]string, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			k, err := unmarshalString(arg)
			if err != nil {
				return Response{OK: false, Error: "failed to parse key"}
			}
			keys[i] = k
		}
		stored, err := eng.SInterStore(dstKey, keys)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(stored)}}

	case "SUNION":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "SUNION requires at least one key"}
		}
		keys := make([]string, len(req.Args))
		for i, arg := range req.Args {
			k, err := unmarshalString(arg)
			if err != nil {
				return Response{OK: false, Error: "failed to parse key"}
			}
			keys[i] = k
		}
		mems, err := eng.SUnion(keys)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return Response{OK: true, Data: data}

	case "SUNIONSTORE":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "SUNIONSTORE requires destination and at least one source key"}
		}
		dstKey, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse destination key"}
		}
		keys := make([]string, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			k, err := unmarshalString(arg)
			if err != nil {
				return Response{OK: false, Error: "failed to parse key"}
			}
			keys[i] = k
		}
		stored, err := eng.SUnionStore(dstKey, keys)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(stored)}}

	case "SDIFF":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "SDIFF requires at least one key"}
		}
		keys := make([]string, len(req.Args))
		for i, arg := range req.Args {
			k, err := unmarshalString(arg)
			if err != nil {
				return Response{OK: false, Error: "failed to parse key"}
			}
			keys[i] = k
		}
		mems, err := eng.SDiff(keys)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, len(mems))
		for i, v := range mems {
			data[i] = json.RawMessage(v)
		}
		return Response{OK: true, Data: data}

	case "SDIFFSTORE":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "SDIFFSTORE requires destination and at least one source key"}
		}
		dstKey, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse destination key"}
		}
		keys := make([]string, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			k, err := unmarshalString(arg)
			if err != nil {
				return Response{OK: false, Error: "failed to parse key"}
			}
			keys[i] = k
		}
		stored, err := eng.SDiffStore(dstKey, keys)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(stored)}}

	case "DEL":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "DEL requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		err = eng.Delete(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}
	}

	return Response{OK: false, Error: "unknown method"}
}

func (m *Multiplexer) executeZSet(req Request) Response {
	eng, ok := m.GetZSet(req.UUID)
	if !ok {
		return Response{OK: false, Error: ErrInstanceNotFound.Error()}
	}

	switch req.Method {
	case "ZADD":
		if len(req.Args) < 3 || (len(req.Args)-1)%2 != 0 {
			return Response{OK: false, Error: "ZADD requires key and score-member pairs"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		mems := make([]zset_storage.ZSetMember, 0, (len(req.Args)-1)/2)
		for i := 1; i < len(req.Args); i += 2 {
			score, err := unmarshalFloat64(req.Args[i])
			if err != nil {
				return Response{OK: false, Error: "invalid score value"}
			}
			mems = append(mems, zset_storage.ZSetMember{
				Score:  score,
				Member: []byte(req.Args[i+1]),
			})
		}
		added, err := eng.ZAdd(key, mems, nil)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(added)}}

	case "ZREM":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "ZREM requires key and at least one member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		mems := make([][]byte, len(req.Args)-1)
		for i, arg := range req.Args[1:] {
			mems[i] = []byte(arg)
		}
		removed, err := eng.ZRem(key, mems)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(removed)}}

	case "ZINCRBY":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "ZINCRBY requires key, increment, and member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		inc, err := unmarshalFloat64(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid increment value"}
		}
		newScore, err := eng.ZIncrBy(key, inc, []byte(req.Args[2]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalFloat64(newScore)}}

	case "ZPOPMAX":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "ZPOPMAX requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		count := 1
		if len(req.Args) >= 2 {
			var err error
			count, err = unmarshalInt(req.Args[1])
			if err != nil {
				return Response{OK: false, Error: "invalid count"}
			}
		}
		popped, err := eng.ZPopMax(key, count)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, 0, len(popped)*2)
		for _, m := range popped {
			data = append(data, json.RawMessage(m.Member), marshalFloat64(m.Score))
		}
		return Response{OK: true, Data: data}

	case "ZPOPMIN":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "ZPOPMIN requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		count := 1
		if len(req.Args) >= 2 {
			var err error
			count, err = unmarshalInt(req.Args[1])
			if err != nil {
				return Response{OK: false, Error: "invalid count"}
			}
		}
		popped, err := eng.ZPopMin(key, count)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, 0, len(popped)*2)
		for _, m := range popped {
			data = append(data, json.RawMessage(m.Member), marshalFloat64(m.Score))
		}
		return Response{OK: true, Data: data}

	case "ZSCORE":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "ZSCORE requires key and member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		score, found, err := eng.ZScore(key, []byte(req.Args[1]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		if !found {
			return Response{OK: true, Data: []json.RawMessage{}}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalFloat64(score)}}

	case "ZCARD":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "ZCARD requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		card, err := eng.ZCard(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(card)}}

	case "ZRANK":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "ZRANK requires key and member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		rank, found, err := eng.ZRank(key, []byte(req.Args[1]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		if !found {
			return Response{OK: true, Data: []json.RawMessage{}}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(rank)}}

	case "ZREVRANK":
		if len(req.Args) < 2 {
			return Response{OK: false, Error: "ZREVRANK requires key and member"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		rank, found, err := eng.ZRevRank(key, []byte(req.Args[1]))
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		if !found {
			return Response{OK: true, Data: []json.RawMessage{}}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(rank)}}

	case "ZCOUNT":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "ZCOUNT requires key, min, and max"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		min, err := unmarshalFloat64(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid min score"}
		}
		max, err := unmarshalFloat64(req.Args[2])
		if err != nil {
			return Response{OK: false, Error: "invalid max score"}
		}
		cnt, err := eng.ZCount(key, min, max)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: []json.RawMessage{marshalInt64(cnt)}}

	case "ZRANGE":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "ZRANGE requires key, start, and stop"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		start, err := unmarshalInt(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid start rank"}
		}
		stop, err := unmarshalInt(req.Args[2])
		if err != nil {
			return Response{OK: false, Error: "invalid stop rank"}
		}
		res, err := eng.ZRange(key, start, stop)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, 0, len(res)*2)
		for _, m := range res {
			data = append(data, json.RawMessage(m.Member), marshalFloat64(m.Score))
		}
		return Response{OK: true, Data: data}

	case "ZREVRANGE":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "ZREVRANGE requires key, start, and stop"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		start, err := unmarshalInt(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid start rank"}
		}
		stop, err := unmarshalInt(req.Args[2])
		if err != nil {
			return Response{OK: false, Error: "invalid stop rank"}
		}
		res, err := eng.ZRevRange(key, start, stop)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, 0, len(res)*2)
		for _, m := range res {
			data = append(data, json.RawMessage(m.Member), marshalFloat64(m.Score))
		}
		return Response{OK: true, Data: data}

	case "ZRANGEBYSCORE":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "ZRANGEBYSCORE requires key, min, and max"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		min, err := unmarshalFloat64(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid min score"}
		}
		max, err := unmarshalFloat64(req.Args[2])
		if err != nil {
			return Response{OK: false, Error: "invalid max score"}
		}
		offset := 0
		count := -1
		if len(req.Args) >= 5 {
			var err error
			offset, err = unmarshalInt(req.Args[3])
			if err != nil {
				return Response{OK: false, Error: "invalid offset"}
			}
			count, err = unmarshalInt(req.Args[4])
			if err != nil {
				return Response{OK: false, Error: "invalid count"}
			}
		}
		res, err := eng.ZRangeByScore(key, min, max, offset, count)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, 0, len(res)*2)
		for _, m := range res {
			data = append(data, json.RawMessage(m.Member), marshalFloat64(m.Score))
		}
		return Response{OK: true, Data: data}

	case "ZREVRANGEBYSCORE":
		if len(req.Args) < 3 {
			return Response{OK: false, Error: "ZREVRANGEBYSCORE requires key, max, and min"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		max, err := unmarshalFloat64(req.Args[1])
		if err != nil {
			return Response{OK: false, Error: "invalid max score"}
		}
		min, err := unmarshalFloat64(req.Args[2])
		if err != nil {
			return Response{OK: false, Error: "invalid min score"}
		}
		offset := 0
		count := -1
		if len(req.Args) >= 5 {
			var err error
			offset, err = unmarshalInt(req.Args[3])
			if err != nil {
				return Response{OK: false, Error: "invalid offset"}
			}
			count, err = unmarshalInt(req.Args[4])
			if err != nil {
				return Response{OK: false, Error: "invalid count"}
			}
		}
		res, err := eng.ZRevRangeByScore(key, max, min, offset, count)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		data := make([]json.RawMessage, 0, len(res)*2)
		for _, m := range res {
			data = append(data, json.RawMessage(m.Member), marshalFloat64(m.Score))
		}
		return Response{OK: true, Data: data}

	case "DEL":
		if len(req.Args) < 1 {
			return Response{OK: false, Error: "DEL requires key"}
		}
		key, err := unmarshalString(req.Args[0])
		if err != nil {
			return Response{OK: false, Error: "failed to parse key"}
		}
		err = eng.Delete(key)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true}
	}

	return Response{OK: false, Error: "unknown method"}
}
