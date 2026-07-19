package multiplexer

import "encoding/json"

type Request struct {
	DS     string            `json:"ds"`             // "kv", "list", "set", "zset"
	UUID   string            `json:"uuid,omitempty"` // Instance UUID
	Method string            `json:"method"`         // e.g. "SET", "CREATE", "DELETE_INSTANCE"
	Args   []json.RawMessage `json:"args,omitempty"` // Raw JSON arguments
}

type Response struct {
	OK    bool              `json:"ok"`
	UUID  string            `json:"uuid,omitempty"`
	Data  []json.RawMessage `json:"data,omitempty"`
	Error string            `json:"error,omitempty"`
}
