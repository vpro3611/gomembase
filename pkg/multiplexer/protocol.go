package multiplexer

import "encoding/json"

type Request struct {
	DS     string            `json:"ds"`             // "kv", "list", "set", "zset"
	UUID   string            `json:"uuid,omitempty"` // Instance UUID
	Method string            `json:"method"`         // e.g. "SET", "CREATE", "DELETE_INSTANCE"
	Args   []json.RawMessage `json:"args,omitempty"` // Raw JSON arguments
}

type Response struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`

	UUID string `json:"uuid,omitempty"` // multiplexer.Create(), multiplexer.CreateInstance()

	Value     json.RawMessage            `json:"value,omitempty"`     // kv.Get(), list.LGET()
	Integer   *int64                     `json:"integer,omitempty"`   // kv.Incr(), kv.Decr(), list.LLEN(), zset.ZCARD(), multiplexer.TotalInstances()
	Float     *float64                   `json:"float,omitempty"`     // zset.ZSCORE()
	Boolean   *bool                      `json:"boolean,omitempty"`   // kv.Exists(), set.SISMEMBER()
	Items     []json.RawMessage          `json:"items,omitempty"`     // kv.Keys(), list.LRANGE(), set.SMEMBERS()
	Flags     []bool                     `json:"flags,omitempty"`     // set.SMISMEMBER()
	KeyVals   map[string]json.RawMessage `json:"key_vals,omitempty"`  // kv.MGET()
	Entries   []Entry                    `json:"entries,omitempty"`   // kv.All(), list.All(), set.All()
	Scored    []ScoredMember             `json:"scored,omitempty"`    // zset.ZRANGE(), zset.ZREVRANGE(), zset.ZRANGEBYSCORE()
	Grouped   []ScoredEntry              `json:"grouped,omitempty"`   // zset.All()
	Responses []Response                 `json:"responses,omitempty"` // server.Exec()
	Queued    *bool                      `json:"queued,omitempty"`    // server.MULTI()
	PubSub    *PubSubAck                 `json:"pubsub,omitempty"`    // server.SUBSCRIBE(), server.UNSUBSCRIBE(), server.PSUBSCRIBE(), server.PUNSUBSCRIBE()
	Info      *InfoPayload               `json:"info,omitempty"`      // server.INFO()

}

type Entry struct {
	Key   string            `json:"key"`
	Items []json.RawMessage `json:"items"`
}

type ScoredMember struct {
	Member json.RawMessage `json:"member"`
	Score  float64         `json:"score"`
}

type ScoredEntry struct {
	Key   string         `json:"key"`
	Items []ScoredMember `json:"items"`
}

type PubSubAck struct {
	Action  string `json:"action"`
	Channel string `json:"channel"`
	Count   int    `json:"count"`
}

type InfoPayload struct {
	Server ServerInfo `json:"server"`
	Users  []UserInfo `json:"users"`
}

func intPtr(v int64) *int64 {
	return &v
}

func floatPtr(v float64) *float64 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func OK() Response {
	return Response{OK: true}
}

func WithValue(v json.RawMessage) Response {
	return Response{OK: true, Value: v}
}

func WithInteger(v int64) Response {
	return Response{OK: true, Integer: intPtr(v)}
}

func WithFloat(v float64) Response {
	return Response{OK: true, Float: floatPtr(v)}
}

func WithBoolean(v bool) Response {
	return Response{OK: true, Boolean: boolPtr(v)}
}

func WithItems(v []json.RawMessage) Response {
	return Response{OK: true, Items: v}
}

func WithFlags(v []bool) Response {
	return Response{OK: true, Flags: v}
}

func WithKeyVals(v map[string]json.RawMessage) Response {
	return Response{OK: true, KeyVals: v}
}

func WithEntries(v []Entry) Response {
	return Response{OK: true, Entries: v}
}

func WithScored(v []ScoredMember) Response {
	return Response{OK: true, Scored: v}
}

func WithGrouped(v []ScoredEntry) Response {
	return Response{OK: true, Grouped: v}
}

func WithResponses(v []Response) Response {
	return Response{OK: true, Responses: v}
}

func WithQueued() Response {
	return Response{OK: true, Queued: boolPtr(true)}
}

func WithPubSub(v *PubSubAck) Response {
	return Response{OK: true, PubSub: v}
}

func WithInfo(v InfoPayload) Response {
	return Response{OK: true, Info: &v}
}

func WithUUID(uuid string) Response {
	return Response{OK: true, UUID: uuid}
}

func Fail(err error) Response {
	return Response{OK: false, Error: err.Error()}
}

func FailMsg(msg string) Response {
	return Response{OK: false, Error: msg}
}



type ServerInfo struct {
	OS               string `json:"os"`
	GoVersion        string `json:"go_version"`
	NumGoroutine     int    `json:"num_goroutine"`
	NumCPU           int    `json:"num_cpu"`
	MemoryAllocBytes uint64 `json:"memory_alloc_bytes"`
	TotalInstances   int    `json:"total_instances"`
}

type UserInfo struct {
	UserID    string         `json:"user_id"`
	Instances []InstanceInfo `json:"instances"`
}

type InstanceInfo struct {
	UUID             string `json:"uuid"`
	DSType           string `json:"ds_type"`
	KeyCount         int    `json:"key_count"`
	MemoryUsageBytes int64  `json:"memory_usage_bytes"`
}
