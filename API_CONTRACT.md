# GObase JSON-over-TCP API Contract Reference

This document serves as a guide and map for the exact request argument formats (`Request.Args`) and corresponding response outputs (`Response.Data`) for each data structure engine.

All requests and responses are sent as newline-delimited JSON payloads over TCP.

---

## 1. Key-Value (KV) Operations

| Method | `Request.Args` | `Response.Data` | Description |
| :--- | :--- | :--- | :--- |
| **`SET`** | `[key, value]` | (empty) | Sets key to value. Value can be any serializable JSON object/scalar. |
| **`SET_TTL`** | `[key, value, ttl_ms]` | (empty) | Sets key to value with expiration in milliseconds (integer). |
| **`GET`** | `[key]` | `[value]` | Retrieves the value of the key (stored JSON raw bytes). |
| **`DEL`** | `[key]` | (empty) | Deletes the key from the storage engine. |
| **`EXISTS`** | `[key]` | `[boolean]` | Returns `true` or `false` JSON literal if the key exists. |
| **`INCR` / `DECR`** | `[key]` | `[integer]` | Increments/decrements integer value by 1. Returns new value. |
| **`INCRBY` / `DECRBY`** | `[key, amount]` | `[integer]` | Increments/decrements key by amount (integer). Returns new value. |
| **`MGET`** | `[key1, key2, ...]` | `[key1, val1, key2, val2, ...]` | Interleaved keys and values. Missing keys return JSON `null`. |
| **`MSET`** | `[key1, val1, key2, val2, ...]` | (empty) | Performs multiple set operations atomically. |

---

## 2. List Operations

| Method | `Request.Args` | `Response.Data` | Description |
| :--- | :--- | :--- | :--- |
| **`LPUSH` / `RPUSH`** | `[key, value1, value2, ...]` | (empty) | Pushes one or more values onto the left/right of the list. |
| **`LPOP` / `RPOP`** | `[key]` | `[value]` | Pops and returns the left-most / right-most item in the list. |
| **`LLEN`** | `[key]` | `[integer]` | Returns the total count of items in the list. |
| **`LRANGE`** | `[key, start_index, stop_index]` | `[value1, value2, ...]` | Returns a slice of elements in the 0-indexed range (inclusive). |
| **`LINDEX`** | `[key, index]` | `[value]` | Returns the element at the specified 0-indexed position. |
| **`LSET`** | `[key, index, value]` | (empty) | Overwrites the element at index with new value. |
| **`LINSERT`** | `[key, "BEFORE" or "AFTER", pivot, value]` | `[integer]` | Inserts value relative to pivot element. Returns new length. |
| **`LREM`** | `[key, count, value]` | `[integer]` | Removes count elements matching value. Returns count removed. |
| **`LPOS`** | `[key, value]` | `[integer]` | Returns the 0-indexed position of the first matching value. |
| **`RPOPLPUSH`** | `[keyFrom, keyTo]` | `[value]` | Pops from tail of keyFrom and pushes to head of keyTo. Returns value. |

---

## 3. Set Operations

| Method | `Request.Args` | `Response.Data` | Description |
| :--- | :--- | :--- | :--- |
| **`SADD` / `SREM`** | `[key, member1, member2, ...]` | `[integer]` | Adds/removes unique members. Returns count of changes. |
| **`SPOP`** | `[key, count]` (count is optional) | `[member1, member2, ...]` | Pops one or more random members from the set. |
| **`SMOVE`** | `[srcKey, dstKey, member]` | `[boolean]` | Moves member from srcKey to dstKey. |
| **`SISMEMBER`** | `[key, member]` | `[boolean]` | Returns `true` if member is in set, else `false`. |
| **`SMISMEMBER`** | `[key, member1, member2, ...]` | `[bool1, bool2, ...]` | Returns array of booleans indicating membership. |
| **`SMEMBERS`** | `[key]` | `[member1, member2, ...]` | Returns all members in the set. |
| **`SCARD`** | `[key]` | `[integer]` | Returns the total cardinality count of the set. |
| **`SRANDMEMBER`** | `[key, count]` (count is optional) | `[member1, member2, ...]` | Returns count random members without removing them. |
| **`SINTER` / `SUNION` / `SDIFF`** | `[key1, key2, ...]` | `[member1, member2, ...]` | Performs set intersection, union, or difference operations. |
| **`SINTERSTORE` / `SUNIONSTORE` / `SDIFFSTORE`** | `[dstKey, key1, key2, ...]` | `[integer]` | Stores set operation result in dstKey. Returns new set size. |

---

## 4. Sorted Set (ZSet) Operations

| Method | `Request.Args` | `Response.Data` | Description |
| :--- | :--- | :--- | :--- |
| **`ZADD`** | `[key, score1, member1, score2, member2, ...]` | `[integer]` | Adds members with scores. Returns count of added members. |
| **`ZREM`** | `[key, member1, member2, ...]` | `[integer]` | Removes members from the sorted set. Returns count removed. |
| **`ZINCRBY`** | `[key, increment, member]` | `[float]` | Increments member's score by increment (float). Returns new score. |
| **`ZPOPMAX` / `ZPOPMIN`** | `[key, count]` (count is optional) | `[member1, score1, member2, score2, ...]` | Pops count highest/lowest elements. Returns interleaved pairs. |
| **`ZSCORE`** | `[key, member]` | `[]` (if nil) or `[float]` | Returns score of member as a float. |
| **`ZCARD`** | `[key]` | `[integer]` | Returns total cardinality count of the sorted set. |
| **`ZRANK` / `ZREVRANK`** | `[key, member]` | `[]` (if nil) or `[integer]` | Returns 0-indexed rank of member (asc/desc order). |
| **`ZCOUNT`** | `[key, min_score, max_score]` | `[integer]` | Returns count of members with score in [min, max] range. |
| **`ZRANGE` / `ZREVRANGE`** | `[key, start_rank, stop_rank]` | `[member1, score1, member2, score2, ...]` | Returns members within rank bounds. Interleaved pairs. |
| **`ZRANGEBYSCORE`** | `[key, min_score, max_score, offset, count]` | `[member1, score1, member2, score2, ...]` | Returns members in score range with limit. Interleaved pairs. |
| **`ZREVRANGEBYSCORE`** | `[key, max_score, min_score, offset, count]` | `[member1, score1, member2, score2, ...]` | Returns members in score range (desc order). Interleaved pairs. |

---

## 5. Pattern-Matching Operations

Pattern-matching operations are supported on `kv`, `list`, `set`, and `zset`.

| Method | `Request.Args` | `Response.Data` | Description |
| :--- | :--- | :--- | :--- |
| **`FIND_PREFIX` / `FIND_SUFFIX` / `FIND_REGEX`** | `[pattern]` | `[key1, val1, key2, val2, ...]` | Flat interleaved pairs of matching keys and value contents. |
| **`COUNT_PREFIX` / `COUNT_SUFFIX` / `COUNT_REGEX`** | `[pattern]` | `[integer]` | Total count of keys matching the pattern. |
| **`DEL_PREFIX` / `DEL_SUFFIX` / `DEL_REGEX`** | `[pattern]` | `[integer]` | Deletes all matching keys. Returns count of deleted keys. |

---

## 6. Transaction Operations

Transactions allow executing multiple commands atomically across any data structure or instance over a single TCP connection.

| Method | `Request.Args` | `Response.Data` | Description |
| :--- | :--- | :--- | :--- |
| **`MULTI`** | (empty) | (empty) | Starts a transaction block. Subsequent commands are queued. |
| **(Any Command)** | (varies) | `["QUEUED"]` | When in a transaction block, commands return `"QUEUED"` instead of executing immediately. |
| **`EXEC`** | (empty) | `[Response1, Response2, ...]` | Executes all queued commands atomically. Returns an array of their respective JSON responses. |
| **`DISCARD`** | (empty) | (empty) | Discards all queued commands and exits the transaction block. |

---

## 7. Pub/Sub Operations

Pub/Sub is an ephemeral, real-time messaging system. Subscriptions live only as long as the TCP connection is active. When a connection issues `SUBSCRIBE` or `PSUBSCRIBE`, it enters **Subscriber Mode** and cannot issue any standard database commands (`SET`, `GET`, `MULTI`, etc.) until it unsubscribes from all channels.

| Method | `Request.Args` | `Response.Data` | Description |
| :--- | :--- | :--- | :--- |
| **`SUBSCRIBE`** | `[channel1, channel2, ...]` | `["subscribe", channel, total_subscriptions]` | Subscribes the connection to exact channel names. Returns one response per channel. |
| **`UNSUBSCRIBE`** | `[channel1, channel2, ...]` | `["unsubscribe", channel, total_subscriptions]` | Unsubscribes from channels. When total_subscriptions hits 0, exits Subscriber Mode. |
| **`PSUBSCRIBE`** | `[pattern1, pattern2, ...]` | `["psubscribe", pattern, total_subscriptions]` | Subscribes to channels matching a glob pattern (e.g. `news.*`). |
| **`PUNSUBSCRIBE`** | `[pattern1, pattern2, ...]` | `["punsubscribe", pattern, total_subscriptions]` | Unsubscribes from pattern. |
| **`PUBLISH`** | `[channel, message_payload]` | `[integer]` | Broadcasts the payload to all subscribers on the channel. Returns the count of recipients. |

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

These commands operate at the multiplexer or server level, outside the context of a specific data structure engine.

| Method | `Request.Args` | `Request.UUID` | `Response.Data` | Description |
| :--- | :--- | :--- | :--- | :--- |
| **`CREATE`** | (empty) | N/A | (empty, but sets `Response.UUID`) | Creates a new sub-instance database for the requested `Request.DS` type (e.g., `kv`, `list`). |
| **`DELETE_INSTANCE`** | (empty) | `[uuid_string]` | (empty) | Deletes the specified sub-instance and frees its memory. |
| **`INFO`** | (empty) | `[uuid_string]` (optional) | Custom JSON Object | Returns server diagnostics and memory usage tracking. If `uuid` is provided, filters memory stats to only that sub-instance. |

