package memcache

import (
	"encoding/json"
	"expvar"
	"strings"
	"testing"
)

func TestExpvarFuncStringEncodesStats(t *testing.T) {
	c, err := New[string, int](
		WithMaxEntries(8),
		WithExpvar("memcache_test_string"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	_ = c.Set("a", 1)
	_, _ = c.Get("a")      // produces a hit
	_, _ = c.Get("absent") // produces a miss

	v := expvar.Get("memcache_test_string")
	if v == nil {
		t.Fatal("expvar.Get returned nil for published cache name")
	}
	got := v.String()
	// Must be JSON-shaped and contain core counter keys.
	if !strings.HasPrefix(got, "{") || !strings.HasSuffix(got, "}") {
		t.Fatalf("expvar string is not JSON-shaped: %q", got)
	}
	for _, key := range []string{
		"hits", "misses", "inserts", "updates", "deletes",
		"evictions", "expirations", "loads_total", "load_hits",
		"load_errors", "load_coalesced", "events_dropped",
		"entries", "bytes", "capacity", "hit_rate_permille",
	} {
		if !strings.Contains(got, "\""+key+"\":") {
			t.Errorf("expvar JSON missing key %q: %s", key, got)
		}
	}
	// Verify it parses as JSON for a stronger guarantee.
	var into map[string]any
	if err := json.Unmarshal([]byte(got), &into); err != nil {
		t.Errorf("expvar string does not parse as JSON: %v: %q", err, got)
	}
}

func TestExpvarPublishEmptyNameNoop(t *testing.T) {
	// No expvar name → publishExpvar should silently skip.
	c, err := New[string, int](WithMaxEntries(4))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	// No assertion beyond "doesn't panic and no name to look up".
}

func TestExpvarPublishDuplicateNameNoop(t *testing.T) {
	// Double registration must not panic; the second call no-ops.
	name := "memcache_test_dup"
	c1, err := New[string, int](
		WithMaxEntries(4),
		WithExpvar(name),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c1.Close()

	c2, err := New[string, int](
		WithMaxEntries(4),
		WithExpvar(name),
	)
	if err != nil {
		t.Fatalf("second New (duplicate name): %v", err)
	}
	defer c2.Close()
	// Both caches should still be usable; expvar entry exists.
	if expvar.Get(name) == nil {
		t.Fatalf("expvar.Get(%q) returned nil", name)
	}
}
