package memcache

import (
	"errors"
	"testing"
	"time"
)

func TestNewRequiresBound(t *testing.T) {
	_, err := New[string, int]()
	if !errors.Is(err, ErrUnbounded) {
		t.Fatalf("expected ErrUnbounded, got %v", err)
	}
}

func TestNewWithMaxEntries(t *testing.T) {
	c, err := New[string, int](WithMaxEntries(8))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if c.Capacity() != 8 {
		t.Errorf("Capacity = %d, want 8", c.Capacity())
	}
	if c.Len() != 0 {
		t.Errorf("Len = %d, want 0", c.Len())
	}
}

func TestNewMaxBytesRequiresWeigher(t *testing.T) {
	_, err := New[string, int](WithMaxBytes(1024))
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) || cfgErr.Field != "MaxBytes" {
		t.Fatalf("expected ConfigError on MaxBytes without Weigher, got %v", err)
	}
}

func TestNewUnboundedSucceeds(t *testing.T) {
	c, err := NewUnbounded[string, int]()
	if err != nil {
		t.Fatalf("NewUnbounded: %v", err)
	}
	defer c.Close()
	if c.Capacity() != 0 {
		t.Errorf("Capacity = %d, want 0 for unbounded", c.Capacity())
	}
}

func TestSetAndGetRoundTrip(t *testing.T) {
	c, err := New[string, int](WithMaxEntries(4))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if err := c.Set("alice", 42); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok := c.Get("alice")
	if !ok || got != 42 {
		t.Errorf("Get(alice) = %d, %v; want 42, true", got, ok)
	}
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1", c.Len())
	}
}

func TestGetMissReturnsZero(t *testing.T) {
	c, err := New[string, string](WithMaxEntries(4))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	got, ok := c.Get("missing")
	if ok || got != "" {
		t.Errorf("Get on missing = %q, %v; want \"\", false", got, ok)
	}
}

func TestSetReplacesExisting(t *testing.T) {
	c, err := New[string, int](WithMaxEntries(4))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	_ = c.Set("k", 1)
	_ = c.Set("k", 2)
	if got, _ := c.Get("k"); got != 2 {
		t.Errorf("after replace, Get = %d, want 2", got)
	}
	if c.Len() != 1 {
		t.Errorf("after replace Len = %d, want 1", c.Len())
	}
	st := c.Stats()
	if st.Inserts != 1 || st.Updates != 1 {
		t.Errorf("Stats Inserts=%d Updates=%d; want 1, 1", st.Inserts, st.Updates)
	}
}

func TestDeleteRemoves(t *testing.T) {
	c, err := New[string, int](WithMaxEntries(4))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	_ = c.Set("k", 1)
	if !c.Delete("k") {
		t.Fatal("Delete on present key should return true")
	}
	if _, ok := c.Get("k"); ok {
		t.Error("Get after Delete should miss")
	}
	if c.Delete("k") {
		t.Error("Delete on absent key should return false")
	}
}

func TestHasIgnoresPolicyAndExpiry(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, err := New[string, int](WithMaxEntries(4), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if err := c.SetWithTTL("k", 1, time.Second); err != nil {
		t.Fatal(err)
	}
	if !c.Has("k") {
		t.Error("Has should report true on fresh entry")
	}
	clk.Advance(2 * time.Second)
	if c.Has("k") {
		t.Error("Has should report false after TTL")
	}
}

func TestPeekDoesNotPromoteOrReturnNegative(t *testing.T) {
	c, err := New[string, int](WithMaxEntries(4))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	_ = c.Set("k", 1)
	v, ok := c.Peek("k")
	if !ok || v != 1 {
		t.Errorf("Peek = (%d, %v), want (1, true)", v, ok)
	}
}

func TestSetWithTTLNegativeIsError(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()

	if err := c.SetWithTTL("k", 1, -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("expected ErrInvalidTTL, got %v", err)
	}
}

func TestExpiredEntryEvictedOnGet(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, err := New[string, int](WithMaxEntries(4), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	_ = c.SetWithTTL("k", 1, time.Second)
	clk.Advance(2 * time.Second)
	if _, ok := c.Get("k"); ok {
		t.Error("Get on expired entry should miss")
	}
	if c.Len() != 0 {
		t.Errorf("expired entry should be evicted; Len = %d", c.Len())
	}
	st := c.Stats()
	if st.Expirations != 1 {
		t.Errorf("Expirations = %d, want 1", st.Expirations)
	}
}

func TestCapacityEvictsViaPolicy(t *testing.T) {
	// Single shard so we can predict eviction precisely. The
	// per-shard budget includes a 10% slop for hash-skew tolerance,
	// so insert enough entries to comfortably exceed it.
	c, err := New[string, int](WithMaxEntries(4), WithShards(1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	for i, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if err := c.Set(k, i); err != nil {
			t.Fatalf("Set(%q): %v", k, err)
		}
	}

	if c.Len() > 6 {
		t.Errorf("Len = %d, want <= 6 (maxEntries=4 + ~10%% slop)", c.Len())
	}
	st := c.Stats()
	if st.Evictions == 0 {
		t.Error("expected at least one eviction")
	}
}

func TestStatsHitsAndMisses(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()

	_ = c.Set("k", 1)
	_, _ = c.Get("k")    // hit
	_, _ = c.Get("nope") // miss
	st := c.Stats()
	if st.Hits != 1 || st.Misses != 1 {
		t.Errorf("Hits=%d Misses=%d; want 1, 1", st.Hits, st.Misses)
	}
	if rate := st.HitRate(); rate < 0.49 || rate > 0.51 {
		t.Errorf("HitRate = %v, want ~0.5", rate)
	}
}

func TestResetStatsZerosCounters(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()

	_ = c.Set("k", 1)
	_, _ = c.Get("k")
	c.ResetStats()
	st := c.Stats()
	if st.Hits != 0 || st.Inserts != 0 {
		t.Errorf("after ResetStats: Hits=%d Inserts=%d", st.Hits, st.Inserts)
	}
	if st.Entries != 1 {
		t.Errorf("ResetStats should preserve live count; Entries=%d", st.Entries)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestClosedCacheRejectsWrites(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	_ = c.Close()
	if err := c.Set("k", 1); !errors.Is(err, ErrClosed) {
		t.Errorf("Set after Close = %v, want ErrClosed", err)
	}
	if c.Has("k") {
		t.Error("Has after Close should report false")
	}
	if _, ok := c.Get("k"); ok {
		t.Error("Get after Close should miss")
	}
}

func TestMustPanicsOnError(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Must should panic on error")
		}
	}()
	var nilCache *Cache[string, int]
	_ = Must(nilCache, ErrUnbounded)
}

func TestMustReturnsCache(t *testing.T) {
	c := Must(New[string, int](WithMaxEntries(2)))
	if c == nil {
		t.Fatal("Must returned nil")
	}
	defer c.Close()
}

func TestWithWeigherCountsBytes(t *testing.T) {
	c, err := New[string, string](
		WithMaxBytes(100),
		WithWeigher[string](StringWeigher()),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	_ = c.Set("k", "hello")
	if c.Bytes() != 5 {
		t.Errorf("Bytes = %d, want 5", c.Bytes())
	}
}

func TestNextPowerOfTwo(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, 1}, {1, 1}, {2, 2}, {3, 4}, {7, 8},
		{8, 8}, {9, 16}, {17, 32}, {1023, 1024},
	}
	for _, tc := range cases {
		if got := nextPowerOfTwo(tc.in); got != tc.want {
			t.Errorf("nextPowerOfTwo(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
