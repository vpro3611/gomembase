package tests

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/server"
)

// startTestServer spins up a real TCP server bound to a random port.
// Returns the server and a connected client connection+reader.
func startTestServer(t *testing.T) (*server.Server, net.Conn, *bufio.Reader) {
	t.Helper()
	mux := multiplexer.NewMultiplexer(&MockWal{}, 50)
	srv := server.NewServer(mux, nil, "127.0.0.1:0")

	go func() { _ = srv.Start() }()
	time.Sleep(60 * time.Millisecond)

	if srv.Addr() == nil {
		t.Fatal("server failed to bind")
	}

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})

	return srv, conn, bufio.NewReader(conn)
}

// mustSend sends a request and returns the unmarshalled Response. Fatals on any error.
func mustSend(t *testing.T, conn net.Conn, r *bufio.Reader, req multiplexer.Request) multiplexer.Response {
	t.Helper()
	b, _ := json.Marshal(req)
	b = append(b, '\n')
	if _, err := conn.Write(b); err != nil {
		t.Fatalf("write error: %v", err)
	}
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	var resp multiplexer.Response
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatalf("unmarshal error: %v\nraw: %s", err, line)
	}
	return resp
}

// mustCreate creates a DS instance, fatals if it fails, returns the UUID.
func mustCreate(t *testing.T, conn net.Conn, r *bufio.Reader, ds string) string {
	t.Helper()
	resp := mustSend(t, conn, r, multiplexer.Request{Method: "CREATE", DS: ds})
	if !resp.OK {
		t.Fatalf("CREATE %s failed: %s", ds, resp.Error)
	}
	return resp.UUID
}

// ── KV: MSET_TTL ─────────────────────────────────────────────────────────────

func TestRouter_KV_MSET_TTL(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "kv")

	// Set two keys with a 500ms TTL
	resp := mustSend(t, conn, r, multiplexer.Request{
		Method: "MSET_TTL",
		DS:     "kv",
		UUID:   uuid,
		Args:   jArgs(`"k1"`, `"v1"`, `"k2"`, `"v2"`, `500`),
	})
	if !resp.OK {
		t.Fatalf("MSET_TTL failed: %s", resp.Error)
	}

	// Both keys must exist immediately
	for _, k := range []string{`"k1"`, `"k2"`} {
		resp := mustSend(t, conn, r, multiplexer.Request{Method: "GET", DS: "kv", UUID: uuid, Args: jArgs(k)})
		if !resp.OK {
			t.Errorf("GET %s failed immediately after MSET_TTL: %s", k, resp.Error)
		}
	}

	// After TTL expires, both must be gone
	time.Sleep(600 * time.Millisecond)
	for _, k := range []string{`"k1"`, `"k2"`} {
		resp := mustSend(t, conn, r, multiplexer.Request{Method: "GET", DS: "kv", UUID: uuid, Args: jArgs(k)})
		if resp.OK {
			t.Errorf("GET %s should have expired after MSET_TTL TTL", k)
		}
	}
}

func TestRouter_KV_MSET_TTL_BadArgs(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "kv")

	// Wrong arg count (even number = no TTL suffix)
	resp := mustSend(t, conn, r, multiplexer.Request{
		Method: "MSET_TTL", DS: "kv", UUID: uuid,
		Args: jArgs(`"k1"`, `"v1"`),
	})
	if resp.OK {
		t.Fatal("expected MSET_TTL to fail with even arg count (missing TTL)")
	}

	// Non-integer TTL
	resp = mustSend(t, conn, r, multiplexer.Request{
		Method: "MSET_TTL", DS: "kv", UUID: uuid,
		Args: jArgs(`"k1"`, `"v1"`, `"notanint"`),
	})
	if resp.OK {
		t.Fatal("expected MSET_TTL to fail with non-integer TTL")
	}
}

// ── LIST: LGET ───────────────────────────────────────────────────────────────

func TestRouter_List_LGET(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "list")

	// Push 3 values right
	mustSend(t, conn, r, multiplexer.Request{Method: "RPUSH", DS: "list", UUID: uuid, Args: jArgs(`"mylist"`, `"a"`, `"b"`, `"c"`)})

	resp := mustSend(t, conn, r, multiplexer.Request{Method: "LGET", DS: "list", UUID: uuid, Args: jArgs(`"mylist"`)})
	if !resp.OK {
		t.Fatalf("LGET failed: %s", resp.Error)
	}
	if len(resp.Items) != 3 {
		t.Fatalf("expected 3 elements from LGET, got %d", len(resp.Items))
	}

	// Verify order matches RPUSH order
	expected := []string{`"a"`, `"b"`, `"c"`}
	for i, raw := range resp.Items {
		if string(raw) != expected[i] {
			t.Errorf("LGET[%d]: expected %s, got %s", i, expected[i], string(raw))
		}
	}
}

func TestRouter_List_LGET_Empty(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "list")

	// LGET on a non-existent key: the engine returns an empty list (not an error)
	// Verify the response is OK and Data is empty.
	resp := mustSend(t, conn, r, multiplexer.Request{Method: "LGET", DS: "list", UUID: uuid, Args: jArgs(`"ghost"`)})
	if !resp.OK {
		t.Fatalf("LGET on non-existent key should succeed: %s", resp.Error)
	}
	if len(resp.Items) != 0 {
		t.Errorf("LGET on non-existent key: expected empty data, got %d elements", len(resp.Items))
	}
}

// ── LIST: Pattern Operations ─────────────────────────────────────────────────

func TestRouter_List_PatternOps(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "list")

	// Populate: user:1, user:2, order:1
	for _, key := range []string{`"user:1"`, `"user:2"`, `"order:1"`} {
		mustSend(t, conn, r, multiplexer.Request{Method: "RPUSH", DS: "list", UUID: uuid,
			Args: jArgs(key, `"val"`)})
	}

	// COUNT_PREFIX "user:"
	resp := mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_PREFIX", DS: "list", UUID: uuid, Args: jArgs(`"user:"`)})
	if !resp.OK {
		t.Fatalf("COUNT_PREFIX failed: %s", resp.Error)
	}
	assertInt64(t, "COUNT_PREFIX user:", resp, 2)

	// COUNT_SUFFIX ":1"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_SUFFIX", DS: "list", UUID: uuid, Args: jArgs(`":1"`)})
	if !resp.OK {
		t.Fatalf("COUNT_SUFFIX failed: %s", resp.Error)
	}
	assertInt64(t, "COUNT_SUFFIX :1", resp, 2)

	// COUNT_REGEX "user:.*"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_REGEX", DS: "list", UUID: uuid, Args: jArgs(`"user:.*"`)})
	if !resp.OK {
		t.Fatalf("COUNT_REGEX failed: %s", resp.Error)
	}
	assertInt64(t, "COUNT_REGEX user:.*", resp, 2)

	// FIND_PREFIX "user:" → returns 2 key/value pairs
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_PREFIX", DS: "list", UUID: uuid, Args: jArgs(`"user:"`)})
	if !resp.OK {
		t.Fatalf("FIND_PREFIX failed: %s", resp.Error)
	}
	if len(resp.Entries) != 2 {
		t.Errorf("FIND_PREFIX: expected 2 entries, got %d", len(resp.Entries))
	}

	// FIND_SUFFIX ":2"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_SUFFIX", DS: "list", UUID: uuid, Args: jArgs(`":2"`)})
	if !resp.OK {
		t.Fatalf("FIND_SUFFIX failed: %s", resp.Error)
	}
	if len(resp.Entries) != 1 {
		t.Errorf("FIND_SUFFIX: expected 1 entry, got %d", len(resp.Entries))
	}

	// FIND_REGEX "order:.*"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_REGEX", DS: "list", UUID: uuid, Args: jArgs(`"order:.*"`)})
	if !resp.OK {
		t.Fatalf("FIND_REGEX failed: %s", resp.Error)
	}
	if len(resp.Entries) != 1 {
		t.Errorf("FIND_REGEX: expected 1 entry, got %d", len(resp.Entries))
	}

	// DEL_PREFIX "user:" → removes 2
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_PREFIX", DS: "list", UUID: uuid, Args: jArgs(`"user:"`)})
	if !resp.OK {
		t.Fatalf("DEL_PREFIX failed: %s", resp.Error)
	}
	assertInt64(t, "DEL_PREFIX user:", resp, 2)

	// Now only order:1 remains — verify COUNT_PREFIX "user:" = 0
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_PREFIX", DS: "list", UUID: uuid, Args: jArgs(`"user:"`)})
	assertInt64(t, "COUNT_PREFIX after DEL_PREFIX", resp, 0)

	// Add more keys for suffix/regex deletion tests
	for _, key := range []string{`"item:a"`, `"item:b"`, `"item:c"`} {
		mustSend(t, conn, r, multiplexer.Request{Method: "RPUSH", DS: "list", UUID: uuid, Args: jArgs(key, `"x"`)})
	}

	// DEL_SUFFIX ":c"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_SUFFIX", DS: "list", UUID: uuid, Args: jArgs(`":c"`)})
	if !resp.OK {
		t.Fatalf("DEL_SUFFIX failed: %s", resp.Error)
	}
	assertInt64(t, "DEL_SUFFIX :c", resp, 1)

	// DEL_REGEX "item:.*"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_REGEX", DS: "list", UUID: uuid, Args: jArgs(`"item:.*"`)})
	if !resp.OK {
		t.Fatalf("DEL_REGEX failed: %s", resp.Error)
	}
	assertInt64(t, "DEL_REGEX item:.*", resp, 2)

	// Only order:1 should remain
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_REGEX", DS: "list", UUID: uuid, Args: jArgs(`".*"`)})
	assertInt64(t, "final total count", resp, 1)
}

// ── SET: SINTERCARD ──────────────────────────────────────────────────────────

func TestRouter_Set_SINTERCARD(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "set")

	// s1 = {a, b, c, d}  s2 = {b, c, d, e}  → intersection = {b, c, d}
	mustSend(t, conn, r, multiplexer.Request{Method: "SADD", DS: "set", UUID: uuid,
		Args: jArgs(`"s1"`, `"a"`, `"b"`, `"c"`, `"d"`)})
	mustSend(t, conn, r, multiplexer.Request{Method: "SADD", DS: "set", UUID: uuid,
		Args: jArgs(`"s2"`, `"b"`, `"c"`, `"d"`, `"e"`)})

	// Without limit — expect 3
	resp := mustSend(t, conn, r, multiplexer.Request{Method: "SINTERCARD", DS: "set", UUID: uuid,
		Args: jArgs(`"s1"`, `"s2"`)})
	if !resp.OK {
		t.Fatalf("SINTERCARD failed: %s", resp.Error)
	}
	assertInt64(t, "SINTERCARD no limit", resp, 3)

	// With limit=2 — expect 2
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "SINTERCARD", DS: "set", UUID: uuid,
		Args: jArgs(`"s1"`, `"s2"`, `2`)})
	if !resp.OK {
		t.Fatalf("SINTERCARD with limit failed: %s", resp.Error)
	}
	assertInt64(t, "SINTERCARD limit=2", resp, 2)

	// With limit=100 (larger than actual) — expect 3
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "SINTERCARD", DS: "set", UUID: uuid,
		Args: jArgs(`"s1"`, `"s2"`, `100`)})
	if !resp.OK {
		t.Fatalf("SINTERCARD with large limit failed: %s", resp.Error)
	}
	assertInt64(t, "SINTERCARD limit=100", resp, 3)

	// Disjoint sets → 0
	mustSend(t, conn, r, multiplexer.Request{Method: "SADD", DS: "set", UUID: uuid,
		Args: jArgs(`"s3"`, `"x"`, `"y"`, `"z"`)})
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "SINTERCARD", DS: "set", UUID: uuid,
		Args: jArgs(`"s1"`, `"s3"`)})
	if !resp.OK {
		t.Fatalf("SINTERCARD disjoint failed: %s", resp.Error)
	}
	assertInt64(t, "SINTERCARD disjoint", resp, 0)
}

// ── SET: Pattern Operations ──────────────────────────────────────────────────

func TestRouter_Set_PatternOps(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "set")

	// Populate: tag:go, tag:rust, tag:python, config:main
	for _, key := range []string{`"tag:go"`, `"tag:rust"`, `"tag:python"`, `"config:main"`} {
		mustSend(t, conn, r, multiplexer.Request{Method: "SADD", DS: "set", UUID: uuid,
			Args: jArgs(key, `"member1"`)})
	}

	// COUNT_PREFIX "tag:"
	resp := mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_PREFIX", DS: "set", UUID: uuid, Args: jArgs(`"tag:"`)})
	if !resp.OK {
		t.Fatalf("COUNT_PREFIX set failed: %s", resp.Error)
	}
	assertInt64(t, "SET COUNT_PREFIX tag:", resp, 3)

	// COUNT_SUFFIX ":go"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_SUFFIX", DS: "set", UUID: uuid, Args: jArgs(`":go"`)})
	if !resp.OK {
		t.Fatalf("COUNT_SUFFIX set failed: %s", resp.Error)
	}
	assertInt64(t, "SET COUNT_SUFFIX :go", resp, 1)

	// COUNT_REGEX "tag:.*"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_REGEX", DS: "set", UUID: uuid, Args: jArgs(`"tag:.*"`)})
	if !resp.OK {
		t.Fatalf("COUNT_REGEX set failed: %s", resp.Error)
	}
	assertInt64(t, "SET COUNT_REGEX tag:.*", resp, 3)

	// FIND_PREFIX "tag:" → 3 pairs
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_PREFIX", DS: "set", UUID: uuid, Args: jArgs(`"tag:"`)})
	if !resp.OK {
		t.Fatalf("FIND_PREFIX set failed: %s", resp.Error)
	}
	if len(resp.Entries) != 3 {
		t.Errorf("FIND_PREFIX set: expected 3 entries, got %d", len(resp.Entries))
	}

	// FIND_SUFFIX ":main"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_SUFFIX", DS: "set", UUID: uuid, Args: jArgs(`":main"`)})
	if !resp.OK {
		t.Fatalf("FIND_SUFFIX set failed: %s", resp.Error)
	}
	if len(resp.Entries) != 1 {
		t.Errorf("FIND_SUFFIX set: expected 1 entry, got %d", len(resp.Entries))
	}

	// FIND_REGEX "config:.*"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_REGEX", DS: "set", UUID: uuid, Args: jArgs(`"config:.*"`)})
	if !resp.OK {
		t.Fatalf("FIND_REGEX set failed: %s", resp.Error)
	}
	if len(resp.Entries) != 1 {
		t.Errorf("FIND_REGEX set: expected 1 entry, got %d", len(resp.Entries))
	}

	// DEL_PREFIX "tag:" → removes 3
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_PREFIX", DS: "set", UUID: uuid, Args: jArgs(`"tag:"`)})
	if !resp.OK {
		t.Fatalf("DEL_PREFIX set failed: %s", resp.Error)
	}
	assertInt64(t, "SET DEL_PREFIX tag:", resp, 3)

	// Only config:main left
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_REGEX", DS: "set", UUID: uuid, Args: jArgs(`".*"`)})
	assertInt64(t, "SET final count after DEL_PREFIX", resp, 1)

	// DEL_SUFFIX ":main"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_SUFFIX", DS: "set", UUID: uuid, Args: jArgs(`":main"`)})
	if !resp.OK {
		t.Fatalf("DEL_SUFFIX set failed: %s", resp.Error)
	}
	assertInt64(t, "SET DEL_SUFFIX :main", resp, 1)

	// Add keys for regex deletion test
	mustSend(t, conn, r, multiplexer.Request{Method: "SADD", DS: "set", UUID: uuid, Args: jArgs(`"zone:a"`, `"m"`)})
	mustSend(t, conn, r, multiplexer.Request{Method: "SADD", DS: "set", UUID: uuid, Args: jArgs(`"zone:b"`, `"m"`)})

	// DEL_REGEX "zone:.*"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_REGEX", DS: "set", UUID: uuid, Args: jArgs(`"zone:.*"`)})
	if !resp.OK {
		t.Fatalf("DEL_REGEX set failed: %s", resp.Error)
	}
	assertInt64(t, "SET DEL_REGEX zone:.*", resp, 2)

	// Verify everything is gone
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_REGEX", DS: "set", UUID: uuid, Args: jArgs(`".*"`)})
	assertInt64(t, "SET total after all deletes", resp, 0)
}

// ── ZSET: Pattern Operations ─────────────────────────────────────────────────

func TestRouter_ZSet_PatternOps(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "zset")

	// Populate: score:alice=10, score:bob=20, rank:alice=1, rank:bob=2
	for _, args := range [][]string{
		{`"score:alice"`, `10`, `"alice"`},
		{`"score:bob"`, `20`, `"bob"`},
		{`"rank:alice"`, `1`, `"alice"`},
		{`"rank:bob"`, `2`, `"bob"`},
	} {
		mustSend(t, conn, r, multiplexer.Request{Method: "ZADD", DS: "zset", UUID: uuid, Args: jArgs(args...)})
	}

	// COUNT_PREFIX "score:"
	resp := mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_PREFIX", DS: "zset", UUID: uuid, Args: jArgs(`"score:"`)})
	if !resp.OK {
		t.Fatalf("ZSET COUNT_PREFIX failed: %s", resp.Error)
	}
	assertInt64(t, "ZSET COUNT_PREFIX score:", resp, 2)

	// COUNT_SUFFIX ":alice"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_SUFFIX", DS: "zset", UUID: uuid, Args: jArgs(`":alice"`)})
	if !resp.OK {
		t.Fatalf("ZSET COUNT_SUFFIX failed: %s", resp.Error)
	}
	assertInt64(t, "ZSET COUNT_SUFFIX :alice", resp, 2)

	// COUNT_REGEX "rank:.*"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_REGEX", DS: "zset", UUID: uuid, Args: jArgs(`"rank:.*"`)})
	if !resp.OK {
		t.Fatalf("ZSET COUNT_REGEX failed: %s", resp.Error)
	}
	assertInt64(t, "ZSET COUNT_REGEX rank:.*", resp, 2)

	// FIND_PREFIX "score:" → 2 pairs
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_PREFIX", DS: "zset", UUID: uuid, Args: jArgs(`"score:"`)})
	if !resp.OK {
		t.Fatalf("ZSET FIND_PREFIX failed: %s", resp.Error)
	}
	if len(resp.Grouped) != 2 {
		t.Errorf("ZSET FIND_PREFIX: expected 2 entries, got %d", len(resp.Grouped))
	}

	// FIND_SUFFIX ":bob"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_SUFFIX", DS: "zset", UUID: uuid, Args: jArgs(`":bob"`)})
	if !resp.OK {
		t.Fatalf("ZSET FIND_SUFFIX failed: %s", resp.Error)
	}
	if len(resp.Grouped) != 2 {
		t.Errorf("ZSET FIND_SUFFIX: expected 2 entries, got %d", len(resp.Grouped))
	}

	// FIND_REGEX ".*:alice"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "FIND_REGEX", DS: "zset", UUID: uuid, Args: jArgs(`".*:alice"`)})
	if !resp.OK {
		t.Fatalf("ZSET FIND_REGEX failed: %s", resp.Error)
	}
	if len(resp.Grouped) != 2 {
		t.Errorf("ZSET FIND_REGEX: expected 2 entries, got %d", len(resp.Grouped))
	}

	// DEL_PREFIX "score:" → removes 2
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_PREFIX", DS: "zset", UUID: uuid, Args: jArgs(`"score:"`)})
	if !resp.OK {
		t.Fatalf("ZSET DEL_PREFIX failed: %s", resp.Error)
	}
	assertInt64(t, "ZSET DEL_PREFIX score:", resp, 2)

	// Only rank:* left
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_REGEX", DS: "zset", UUID: uuid, Args: jArgs(`".*"`)})
	assertInt64(t, "ZSET count after DEL_PREFIX", resp, 2)

	// DEL_SUFFIX ":alice"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_SUFFIX", DS: "zset", UUID: uuid, Args: jArgs(`":alice"`)})
	if !resp.OK {
		t.Fatalf("ZSET DEL_SUFFIX failed: %s", resp.Error)
	}
	assertInt64(t, "ZSET DEL_SUFFIX :alice", resp, 1)

	// DEL_REGEX "rank:.*"
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "DEL_REGEX", DS: "zset", UUID: uuid, Args: jArgs(`"rank:.*"`)})
	if !resp.OK {
		t.Fatalf("ZSET DEL_REGEX failed: %s", resp.Error)
	}
	assertInt64(t, "ZSET DEL_REGEX rank:.*", resp, 1)

	// Empty
	resp = mustSend(t, conn, r, multiplexer.Request{Method: "COUNT_REGEX", DS: "zset", UUID: uuid, Args: jArgs(`".*"`)})
	assertInt64(t, "ZSET total after all deletes", resp, 0)
}

func TestRouter_ZSetCoverage(t *testing.T) {
	_, conn, r := startTestServer(t)
	uuid := mustCreate(t, conn, r, "zset")

	mustSend(t, conn, r, multiplexer.Request{
		Method: "ZADD",
		DS:     "zset",
		UUID:   uuid,
		Args:   jArgs(`"scores"`, `10`, `"alice"`),
	})

	scoreResp := mustSend(t, conn, r, multiplexer.Request{
		Method: "ZSCORE",
		DS:     "zset",
		UUID:   uuid,
		Args:   jArgs(`"scores"`, `"missing"`),
	})
	if !scoreResp.OK {
		t.Fatalf("ZSCORE failed: %s", scoreResp.Error)
	}
	if scoreResp.Float != nil {
		t.Fatalf("expected nil float for missing member, got %v", *scoreResp.Float)
	}

	rankResp := mustSend(t, conn, r, multiplexer.Request{
		Method: "ZRANK",
		DS:     "zset",
		UUID:   uuid,
		Args:   jArgs(`"scores"`, `"missing"`),
	})
	if !rankResp.OK {
		t.Fatalf("ZRANK failed: %s", rankResp.Error)
	}
	if rankResp.Integer != nil {
		t.Fatalf("expected nil integer for missing member, got %v", *rankResp.Integer)
	}

	revRankResp := mustSend(t, conn, r, multiplexer.Request{
		Method: "ZREVRANK",
		DS:     "zset",
		UUID:   uuid,
		Args:   jArgs(`"scores"`, `"missing"`),
	})
	if !revRankResp.OK {
		t.Fatalf("ZREVRANK failed: %s", revRankResp.Error)
	}
	if revRankResp.Integer != nil {
		t.Fatalf("expected nil integer for missing member, got %v", *revRankResp.Integer)
	}
}

// ── Edge cases: invalid pattern / missing args / bad UUID ────────────────────

func TestRouter_PatternOps_MissingArgs(t *testing.T) {
	_, conn, r := startTestServer(t)

	cases := []struct {
		ds     string
		method string
	}{
		{"list", "COUNT_PREFIX"}, {"list", "COUNT_SUFFIX"}, {"list", "COUNT_REGEX"},
		{"list", "FIND_PREFIX"}, {"list", "FIND_SUFFIX"}, {"list", "FIND_REGEX"},
		{"list", "DEL_PREFIX"}, {"list", "DEL_SUFFIX"}, {"list", "DEL_REGEX"},
		{"set", "COUNT_PREFIX"}, {"set", "COUNT_SUFFIX"}, {"set", "COUNT_REGEX"},
		{"set", "FIND_PREFIX"}, {"set", "FIND_SUFFIX"}, {"set", "FIND_REGEX"},
		{"set", "DEL_PREFIX"}, {"set", "DEL_SUFFIX"}, {"set", "DEL_REGEX"},
		{"set", "SINTERCARD"},
		{"zset", "COUNT_PREFIX"}, {"zset", "COUNT_SUFFIX"}, {"zset", "COUNT_REGEX"},
		{"zset", "FIND_PREFIX"}, {"zset", "FIND_SUFFIX"}, {"zset", "FIND_REGEX"},
		{"zset", "DEL_PREFIX"}, {"zset", "DEL_SUFFIX"}, {"zset", "DEL_REGEX"},
		{"kv", "MSET_TTL"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.ds+"/"+tc.method, func(t *testing.T) {
			uuid := mustCreate(t, conn, r, tc.ds)
			resp := mustSend(t, conn, r, multiplexer.Request{
				Method: tc.method, DS: tc.ds, UUID: uuid,
				Args: []json.RawMessage{}, // no args
			})
			if resp.OK {
				t.Errorf("%s/%s: expected failure with no args, got OK", tc.ds, tc.method)
			}
		})
	}
}

func TestRouter_PatternOps_InvalidRegex(t *testing.T) {
	_, conn, r := startTestServer(t)

	for _, ds := range []string{"list", "set", "zset", "kv"} {
		uuid := mustCreate(t, conn, r, ds)
		resp := mustSend(t, conn, r, multiplexer.Request{
			Method: "COUNT_REGEX", DS: ds, UUID: uuid,
			Args: jArgs(`"[invalid regex"`),
		})
		if resp.OK {
			t.Errorf("%s COUNT_REGEX: expected failure on invalid regex, got OK", ds)
		}
	}
}

func TestRouter_PatternOps_UnknownInstance(t *testing.T) {
	_, conn, r := startTestServer(t)

	for _, ds := range []string{"list", "set", "zset"} {
		resp := mustSend(t, conn, r, multiplexer.Request{
			Method: "COUNT_PREFIX", DS: ds, UUID: "no-such-uuid",
			Args: jArgs(`"prefix"`),
		})
		if resp.OK {
			t.Errorf("%s COUNT_PREFIX on unknown UUID: expected failure, got OK", ds)
		}
	}
}

// ── Pattern ops return correct data on empty store ────────────────────────────

func TestRouter_PatternOps_EmptyStore(t *testing.T) {
	_, conn, r := startTestServer(t)

	for _, ds := range []string{"list", "set", "zset"} {
		uuid := mustCreate(t, conn, r, ds)
		for _, method := range []string{"COUNT_PREFIX", "FIND_PREFIX", "DEL_PREFIX"} {
			resp := mustSend(t, conn, r, multiplexer.Request{
				Method: method, DS: ds, UUID: uuid, Args: jArgs(`"anything"`),
			})
			if !resp.OK {
				t.Errorf("%s/%s on empty store: expected OK, got %s", ds, method, resp.Error)
			}
			if method == "COUNT_PREFIX" || method == "DEL_PREFIX" {
				assertInt64(t, ds+"/"+method+" empty", resp, 0)
			}
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// jArgs converts string literals to []json.RawMessage.
func jArgs(args ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(args))
	for i, a := range args {
		out[i] = json.RawMessage(a)
	}
	return out
}

// assertInt64 reads a single int64 from resp.Data[0] and compares to want.
func assertInt64(t *testing.T, label string, resp multiplexer.Response, want int64) {
	t.Helper()
	if resp.Integer == nil {
		t.Errorf("%s: expected Integer response, got nil", label)
		return
	}
	if *resp.Integer != want {
		t.Errorf("%s: expected %d, got %d", label, want, *resp.Integer)
	}
}
