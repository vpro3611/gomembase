# GObase JSON-over-TCP API Contract Reference

This document serves as a guide and map for the exact request argument formats (`Request.Args`) and corresponding response outputs for each data structure engine.

All requests and responses are sent as newline-delimited JSON payloads over TCP.

---

## 1. Key-Value (KV) Operations

| Method | `Request.Args` | Response Payload | Description |
| :--- | :--- | :--- | :--- |
| **`SET`** | `[key, value]` | `{ "ok": true }` | Sets key to value. Value can be any serializable JSON object/scalar. |
| **`SET_TTL`** | `[key, value, ttl_ms]` | `{ "ok": true }` | Sets key to value with expiration in milliseconds (integer). |
| **`GET`** | `[key]` | `{ "ok": true, "value": <raw-json> }` | Retrieves the value of the key (stored JSON raw bytes). |
| **`DEL`** | `[key]` | `{ "ok": true }` | Deletes the key from the storage engine. |
| **`EXISTS`** | `[key]` | `{ "ok": true, "boolean": <true/false> }` | Returns `true` or `false` JSON literal if the key exists. |
| **`INCR` / `DECR`** | `[key]` | `{ "ok": true, "integer": <new-val> }` | Increments/decrements integer value by 1. Returns new value. |
| **`INCRBY` / `DECRBY`** | `[key, amount]` | `{ "ok": true, "integer": <new-val> }` | Increments/decrements key by amount (integer). Returns new value. |
| **`MGET`** | `[key1, key2, ...]` | `{ "ok": true, "key_vals": { "key1": <raw-json-or-null>, ... } }` | Returns an object keyed by request key. |
| **`MSET`** | `[key1, val1, key2, val2, ...]` | `{ "ok": true }` | Performs multiple set operations atomically. |

---

## 2. List Operations

| Method | `Request.Args` | Response Payload | Description |
| :--- | :--- | :--- | :--- |
| **`LPUSH` / `RPUSH`** | `[key, value1, value2, ...]` | `{ "ok": true }` | Pushes one or more values onto the left/right of the list. |
| **`LPOP` / `RPOP`** | `[key]` | `{ "ok": true, "value": <raw-json> }` | Pops and returns the left-most / right-most item in the list. |
| **`LLEN`** | `[key]` | `{ "ok": true, "integer": <count> }` | Returns the total count of items in the list. |
| **`LRANGE`** | `[key, start_index, stop_index]` | `{ "ok": true, "items": [<val1>, <val2>, ...] }` | Returns a slice of elements in the 0-indexed range (inclusive). |
| **`LINDEX`** | `[key, index]` | `{ "ok": true, "value": <raw-json> }` | Returns the element at the specified 0-indexed position. |
| **`LSET`** | `[key, index, value]` | `{ "ok": true }` | Overwrites the element at index with new value. |
| **`LINSERT`** | `[key, "BEFORE" or "AFTER", pivot, value]` | `{ "ok": true, "integer": <new-len> }` | Inserts value relative to pivot element. Returns new length. |
| **`LREM`** | `[key, count, value]` | `{ "ok": true, "integer": <removed> }` | Removes count elements matching value. Returns count removed. |
| **`LPOS`** | `[key, value]` | `{ "ok": true, "integer": <index> }` | Returns the 0-indexed position of the first matching value. |
| **`RPOPLPUSH`** | `[keyFrom, keyTo]` | `{ "ok": true, "value": <raw-json> }` | Pops from tail of keyFrom and pushes to head of keyTo. Returns value. |

---

## 3. Set Operations

| Method | `Request.Args` | Response Payload | Description |
| :--- | :--- | :--- | :--- |
| **`SADD` / `SREM`** | `[key, member1, member2, ...]` | `{ "ok": true, "integer": <count> }` | Adds/removes unique members. Returns count of changes. |
| **`SPOP`** | `[key, count]` (count is optional) | `{ "ok": true, "items": [<mem1>, <mem2>, ...] }` | Pops one or more random members from the set. |
| **`SMOVE`** | `[srcKey, dstKey, member]` | `{ "ok": true, "boolean": <true/false> }` | Moves member from srcKey to dstKey. |
| **`SISMEMBER`** | `[key, member]` | `{ "ok": true, "boolean": <true/false> }` | Returns `true` if member is in set, else `false`. |
| **`SMISMEMBER`** | `[key, member1, member2, ...]` | `{ "ok": true, "flags": [<true>, <false>, ...] }` | Returns array of booleans indicating membership. |
| **`SMEMBERS`** | `[key]` | `{ "ok": true, "items": [<mem1>, <mem2>, ...] }` | Returns all members in the set. |
| **`SCARD`** | `[key]` | `{ "ok": true, "integer": <count> }` | Returns the total cardinality count of the set. |
| **`SRANDMEMBER`** | `[key, count]` (count is optional) | `{ "ok": true, "items": [<mem1>, <mem2>, ...] }` | Returns count random members without removing them. |
| **`SINTER` / `SUNION` / `SDIFF`** | `[key1, key2, ...]` | `{ "ok": true, "items": [<mem1>, <mem2>, ...] }` | Performs set intersection, union, or difference operations. |
| **`SINTERSTORE` / `SUNIONSTORE` / `SDIFFSTORE`** | `[dstKey, key1, key2, ...]` | `{ "ok": true, "integer": <new-size> }` | Stores set operation result in dstKey. Returns new set size. |

---

## 4. Sorted Set (ZSet) Operations

| Method | `Request.Args` | Response Payload | Description |
| :--- | :--- | :--- | :--- |
| **`ZADD`** | `[key, score1, member1, ...]` | `{ "ok": true, "integer": <added> }` | Adds members with scores. Returns count of added members. |
| **`ZREM`** | `[key, member1, member2, ...]` | `{ "ok": true, "integer": <removed> }` | Removes members from the sorted set. Returns count removed. |
| **`ZINCRBY`** | `[key, increment, member]` | `{ "ok": true, "float": <new-score> }` | Increments member's score by increment. Returns new score. |
| **`ZPOPMAX` / `ZPOPMIN`** | `[key, count]` (count is optional) | `{ "ok": true, "scored": [{"member": ..., "score": ...}, ...] }` | Pops count highest/lowest elements. |
| **`ZSCORE`** | `[key, member]` | `{ "ok": true, "float": <score> }` (float omitted if nil) | Returns score of member as a float. |
| **`ZCARD`** | `[key]` | `{ "ok": true, "integer": <count> }` | Returns total cardinality count of the sorted set. |
| **`ZRANK` / `ZREVRANK`** | `[key, member]` | `{ "ok": true, "integer": <rank> }` (integer omitted if nil) | Returns 0-indexed rank of member. |
| **`ZCOUNT`** | `[key, min_score, max_score]` | `{ "ok": true, "integer": <count> }` | Returns count of members with score in [min, max] range. |
| **`ZRANGE` / `ZREVRANGE`** | `[key, start, stop]` | `{ "ok": true, "scored": [{"member": ..., "score": ...}, ...] }` | Returns members within rank bounds. |
| **`ZRANGEBYSCORE`** / **`ZREVRANGEBYSCORE`** | `[key, min, max, offset, count]` | `{ "ok": true, "scored": [{"member": ..., "score": ...}, ...] }` | Returns members in score range with limit. |

---

## 5. Pattern-Matching Operations

Pattern-matching operations are supported on `kv`, `list`, `set`, and `zset`.

| Method | `Request.Args` | Response Payload | Description |
| :--- | :--- | :--- | :--- |
| **`FIND_PREFIX` / `FIND_SUFFIX` / `FIND_REGEX`** | `[pattern]` | `{ "ok": true, "entries": [...], "key_vals": {...}, "grouped": [...] }` | Returns matching keys and their values/members. |
| **`COUNT_PREFIX` / `COUNT_SUFFIX` / `COUNT_REGEX`** | `[pattern]` | `{ "ok": true, "integer": <count> }` | Total count of keys matching the pattern. |
| **`DEL_PREFIX` / `DEL_SUFFIX` / `DEL_REGEX`** | `[pattern]` | `{ "ok": true, "integer": <count> }` | Deletes all matching keys. Returns count of deleted keys. |

---

## 6. Transaction Operations

Transactions allow executing multiple commands atomically across any data structure or instance over a single TCP connection.

| Method | `Request.Args` | Response Payload | Description |
| :--- | :--- | :--- | :--- |
| **`MULTI`** | (empty) | `{ "ok": true }` | Starts a transaction block. Subsequent commands are queued. |
| **(Any Command)** | (varies) | `{ "ok": true, "queued": true }` | When in a transaction block, commands return `"queued": true` instead of executing immediately. |
| **`EXEC`** | (empty) | `{ "ok": true, "responses": [Response1, Response2, ...] }` | Executes queued commands atomically. |
| **`DISCARD`** | (empty) | `{ "ok": true }` | Discards all queued commands and exits the transaction block. |

---

## 7. Pub/Sub Operations

Pub/Sub is an ephemeral, real-time messaging system.

| Method | `Request.Args` | Response Payload | Description |
| :--- | :--- | :--- | :--- |
| **`SUBSCRIBE`** | `[channel1, ...]` | `{ "ok": true, "pubsub": {"action": "subscribe", "channel": "...", "count": <num>} }` | Subscribes the connection to exact channel names. Returns one response per channel. |
| **`UNSUBSCRIBE`** | `[channel1, ...]` | `{ "ok": true, "pubsub": {"action": "unsubscribe", "channel": "...", "count": <num>} }` | Unsubscribes from channels. |
| **`PSUBSCRIBE`** | `[pattern1, ...]` | `{ "ok": true, "pubsub": {"action": "psubscribe", "channel": "...", "count": <num>} }` | Subscribes to channels matching a glob pattern. |
| **`PUNSUBSCRIBE`** | `[pattern1, ...]` | `{ "ok": true, "pubsub": {"action": "punsubscribe", "channel": "...", "count": <num>} }` | Unsubscribes from pattern. |
| **`PUBLISH`** | `[channel, message]` | `{ "ok": true, "integer": <count> }` | Broadcasts the payload. Returns the count of recipients. |

### Pushed Messages

When a connection is subscribed to a channel, the server will asynchronously push JSON frames to it whenever a message is published.

**Format for exact channel subscriptions:**
```json
{"type": "message", "channel": "news.sports", "data": "goal scored!"}
```

**Format for pattern subscriptions:**
```json
{"type": "pmessage", "pattern": "news.*", "channel": "news.sports", "data": "goal scored!"}
```

---

## 8. System & Control Plane Operations

| Method | `Request.Args` | `Request.UUID` | Response Payload | Description |
| :--- | :--- | :--- | :--- | :--- |
| **`CREATE`** | (empty) | N/A | `{ "ok": true, "uuid": "<uuid_string>" }` | Creates a new sub-instance database for the requested `Request.DS` type. |
| **`DELETE_INSTANCE`** | (empty) | `[uuid_string]` | `{ "ok": true }` | Deletes the specified sub-instance and frees its memory. |
| **`INFO`** | (empty) | `[uuid_string]` (optional) | `{ "ok": true, "info": { "server": ..., "users": ... } }` | Returns server diagnostics in the standard response envelope. |
