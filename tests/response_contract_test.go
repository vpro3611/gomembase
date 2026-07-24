package tests

import (
	"encoding/json"
	"testing"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
)

func int64Ptr(v int64) *int64 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func TestResponse_JSON_OmitsNilTypedFields(t *testing.T) {
	resp := multiplexer.Response{OK: true}

	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if payload["ok"] != true {
		t.Fatalf("expected ok=true, got %v", payload["ok"])
	}
	if _, exists := payload["responses"]; exists {
		t.Fatalf("responses should be omitted when nil: %v", payload)
	}
	if _, exists := payload["integer"]; exists {
		t.Fatalf("integer should be omitted when nil: %v", payload)
	}
	if _, exists := payload["queued"]; exists {
		t.Fatalf("queued should be omitted when nil: %v", payload)
	}
}

func TestResponse_JSON_ExecEmbedsTypedResponses(t *testing.T) {
	resp := multiplexer.Response{
		OK: true,
		Responses: []multiplexer.Response{
			{OK: true, Integer: int64Ptr(42)},
			{OK: true, Queued: boolPtr(true)},
		},
	}

	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if payload["ok"] != true {
		t.Fatalf("expected ok=true, got %v", payload["ok"])
	}

	responses, ok := payload["responses"].([]any)
	if !ok {
		t.Fatalf("expected responses array, got %T (%v)", payload["responses"], payload["responses"])
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 nested responses, got %d", len(responses))
	}

	first, ok := responses[0].(map[string]any)
	if !ok {
		t.Fatalf("expected first nested response object, got %T", responses[0])
	}
	if first["ok"] != true {
		t.Fatalf("expected first nested response ok=true, got %v", first["ok"])
	}
	if first["integer"] != float64(42) {
		t.Fatalf("expected first nested integer=42, got %v", first["integer"])
	}
	if _, exists := first["queued"]; exists {
		t.Fatalf("queued should be omitted from integer response: %v", first)
	}

	second, ok := responses[1].(map[string]any)
	if !ok {
		t.Fatalf("expected second nested response object, got %T", responses[1])
	}
	if second["ok"] != true {
		t.Fatalf("expected second nested response ok=true, got %v", second["ok"])
	}
	if second["queued"] != true {
		t.Fatalf("expected second nested queued=true, got %v", second["queued"])
	}
	if _, exists := second["integer"]; exists {
		t.Fatalf("integer should be omitted from queued response: %v", second)
	}
}

func TestResponse_JSON_InfoUsesEnvelope(t *testing.T) {
	resp := multiplexer.WithInfo(multiplexer.InfoPayload{
		Server: multiplexer.ServerInfo{
			OS:             "linux",
			GoVersion:      "go1.24.0",
			TotalInstances: 3,
		},
		Users: []multiplexer.UserInfo{
			{
				UserID: "default_user",
				Instances: []multiplexer.InstanceInfo{
					{UUID: "abc", DSType: "kv", KeyCount: 2},
				},
			},
		},
	})

	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if payload["ok"] != true {
		t.Fatalf("expected ok=true, got %v", payload["ok"])
	}
	if _, exists := payload["server"]; exists {
		t.Fatalf("server should not appear at the top level: %v", payload)
	}
	if _, exists := payload["users"]; exists {
		t.Fatalf("users should not appear at the top level: %v", payload)
	}

	info, ok := payload["info"].(map[string]any)
	if !ok {
		t.Fatalf("expected info object, got %T (%v)", payload["info"], payload["info"])
	}
	server, ok := info["server"].(map[string]any)
	if !ok {
		t.Fatalf("expected server object, got %T (%v)", info["server"], info["server"])
	}
	if server["os"] != "linux" {
		t.Fatalf("expected server.os=linux, got %v", server["os"])
	}
	if server["total_instances"] != float64(3) {
		t.Fatalf("expected server.total_instances=3, got %v", server["total_instances"])
	}
	users, ok := info["users"].([]any)
	if !ok || len(users) != 1 {
		t.Fatalf("expected one user entry, got %T (%v)", info["users"], info["users"])
	}
}
