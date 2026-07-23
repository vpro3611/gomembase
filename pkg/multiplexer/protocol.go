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

type InfoResponse struct {
	OK     bool         `json:"ok"`
	Error  string       `json:"error,omitempty"`
	Server ServerInfo   `json:"server,omitempty"`
	Users  []UserInfo   `json:"users,omitempty"`
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
