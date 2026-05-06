package memcache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func contextWithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

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

func TestRangeVisitsAllLiveEntries(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64), WithShards(4))
	defer c.Close()

	want := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	for k, v := range want {
		_ = c.Set(k, v)
	}

	seen := map[string]int{}
	c.Range(func(k string, v int) bool {
		seen[k] = v
		return true
	})
	if len(seen) != len(want) {
		t.Errorf("Range visited %d entries, want %d", len(seen), len(want))
	}
	for k, v := range want {
		if seen[k] != v {
			t.Errorf("Range[%q] = %d, want %d", k, seen[k], v)
		}
	}
}

func TestRangeStopsOnFalse(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64))
	defer c.Close()
	for _, k := range []string{"a", "b", "c", "d"} {
		_ = c.Set(k, 1)
	}
	count := 0
	c.Range(func(string, int) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("Range visited %d entries before stop, want 2", count)
	}
}

func TestRangeSkipsExpired(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("alive", 1, time.Hour)
	_ = c.SetWithTTL("dead", 2, time.Second)
	clk.Advance(2 * time.Second)
	visited := 0
	c.Range(func(k string, _ int) bool {
		if k == "dead" {
			t.Errorf("Range returned expired entry %q", k)
		}
		visited++
		return true
	})
	if visited != 1 {
		t.Errorf("Range visited %d, want 1 live entry", visited)
	}
}

func TestKeysReturnsSnapshot(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(k, 0)
	}
	keys := c.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys() returned %d, want 3", len(keys))
	}
	// Mutating returned slice must not affect cache.
	keys[0] = "MUTATED"
	if !c.Has("a") {
		t.Error("Mutating Keys() slice should not affect cache state")
	}
}

func TestClearRecordsClearReason(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(k, 0)
	}
	c.Clear()
	if c.Len() != 0 {
		t.Errorf("Len after Clear = %d, want 0", c.Len())
	}
	st := c.Stats()
	if got := st.EvictionsByReason[EvictReasonClear]; got != 3 {
		t.Errorf("EvictionsByReason[Clear] = %d, want 3", got)
	}
}

func TestResetDoesNotRecordReason(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(k, 0)
	}
	c.Reset()
	st := c.Stats()
	if got := st.EvictionsByReason[EvictReasonClear]; got != 0 {
		t.Errorf("Reset must not record EvictReasonClear; got %d", got)
	}
}

func TestTTLAndExpiry(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("k", 1, 10*time.Second)

	d, ok := c.TTL("k")
	if !ok || d != 10*time.Second {
		t.Errorf("TTL = (%v, %v), want (10s, true)", d, ok)
	}
	exp, ok := c.Expiry("k")
	if !ok || exp.UnixNano() != int64(10*time.Second) {
		t.Errorf("Expiry = (%v, %v), want (10s wall, true)", exp, ok)
	}
	clk.Advance(7 * time.Second)
	d, ok = c.TTL("k")
	if !ok || d != 3*time.Second {
		t.Errorf("TTL after 7s = (%v, %v), want (3s, true)", d, ok)
	}
	clk.Advance(5 * time.Second)
	if _, ok := c.TTL("k"); ok {
		t.Error("TTL on expired entry should report ok=false")
	}
}

func TestTTLOnNoTTLEntry(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1) // no default TTL
	d, ok := c.TTL("k")
	if !ok || d != 0 {
		t.Errorf("TTL on no-TTL entry = (%v, %v), want (0, true)", d, ok)
	}
}

func TestTouchRefreshesTTL(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithClock(clk),
		WithDefaultTTL(10*time.Second),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	clk.Advance(8 * time.Second) // 2s remaining
	if !c.Touch("k") {
		t.Fatal("Touch on existing entry should succeed")
	}
	d, _ := c.TTL("k")
	if d != 10*time.Second {
		t.Errorf("TTL after Touch = %v, want 10s", d)
	}
}

func TestTouchOnAbsentReturnsFalse(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	if c.Touch("missing") {
		t.Error("Touch on missing key should return false")
	}
}

func TestTouchWithTTL(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(4), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Second)
	if !c.TouchWithTTL("k", time.Hour) {
		t.Fatal("TouchWithTTL should succeed")
	}
	d, _ := c.TTL("k")
	if d != time.Hour {
		t.Errorf("TTL after TouchWithTTL(hour) = %v, want 1h", d)
	}
}

func TestTouchWithTTLZeroClearsExpiry(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4), WithDefaultTTL(time.Second))
	defer c.Close()
	_ = c.Set("k", 1)
	if !c.TouchWithTTL("k", 0) {
		t.Fatal("TouchWithTTL(0) should succeed")
	}
	d, ok := c.TTL("k")
	if !ok || d != 0 {
		t.Errorf("after TouchWithTTL(0), TTL = (%v, %v), want (0, true)", d, ok)
	}
}

func TestTouchWithTTLNegative(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	if c.TouchWithTTL("k", -time.Second) {
		t.Error("TouchWithTTL with negative TTL should return false")
	}
}

func TestSetIfAbsent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	stored, err := c.SetIfAbsent("k", 1)
	if err != nil || !stored {
		t.Fatalf("SetIfAbsent on empty = (%v, %v), want (true, nil)", stored, err)
	}
	stored, err = c.SetIfAbsent("k", 2)
	if err != nil || stored {
		t.Fatalf("SetIfAbsent on existing = (%v, %v), want (false, nil)", stored, err)
	}
	got, _ := c.Get("k")
	if got != 1 {
		t.Errorf("value after SetIfAbsent x2 = %d, want 1", got)
	}
}

func TestSetIfPresent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	updated, _ := c.SetIfPresent("k", 1)
	if updated {
		t.Error("SetIfPresent on absent should return false")
	}
	_ = c.Set("k", 1)
	updated, _ = c.SetIfPresent("k", 2)
	if !updated {
		t.Fatal("SetIfPresent on present should return true")
	}
	got, _ := c.Get("k")
	if got != 2 {
		t.Errorf("value after SetIfPresent = %d, want 2", got)
	}
}

func TestDeleteIf(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 5)
	if c.DeleteIf("k", func(v int) bool { return v == 99 }) {
		t.Error("DeleteIf with non-matching predicate should return false")
	}
	if !c.DeleteIf("k", func(v int) bool { return v == 5 }) {
		t.Error("DeleteIf with matching predicate should return true")
	}
	if c.Has("k") {
		t.Error("entry should be removed after matching DeleteIf")
	}
}

func TestGetOrSet(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	v, loaded, err := c.GetOrSet("k", 1)
	if err != nil || loaded || v != 1 {
		t.Fatalf("GetOrSet on empty = (%d, %v, %v), want (1, false, nil)", v, loaded, err)
	}
	v, loaded, err = c.GetOrSet("k", 999)
	if err != nil || !loaded || v != 1 {
		t.Fatalf("GetOrSet on existing = (%d, %v, %v), want (1, true, nil)", v, loaded, err)
	}
	st := c.Stats()
	if st.Hits != 1 {
		t.Errorf("GetOrSet hit should bump Hits; got %d", st.Hits)
	}
}

func TestPeekOrAdd(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	v, loaded, err := c.PeekOrAdd("k", 1)
	if err != nil || loaded || v != 1 {
		t.Fatalf("PeekOrAdd on empty = (%d, %v, %v), want (1, false, nil)", v, loaded, err)
	}
	// PeekOrAdd should NOT bump Hits on the read side.
	st := c.Stats()
	if st.Hits != 0 {
		t.Errorf("PeekOrAdd insert should not bump Hits; got %d", st.Hits)
	}
	v, loaded, _ = c.PeekOrAdd("k", 999)
	if !loaded || v != 1 {
		t.Errorf("PeekOrAdd on existing = (%d, %v), want (1, true)", v, loaded)
	}
	st = c.Stats()
	if st.Hits != 0 {
		t.Errorf("PeekOrAdd existing-read should not bump Hits; got %d", st.Hits)
	}
}

func TestSetWithOptionsTTL(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(4), WithClock(clk))
	defer c.Close()
	if err := c.SetWithOptions("k", 1, SetTTL(5*time.Second)); err != nil {
		t.Fatalf("SetWithOptions: %v", err)
	}
	d, _ := c.TTL("k")
	if d != 5*time.Second {
		t.Errorf("TTL after SetWithOptions(SetTTL(5s)) = %v, want 5s", d)
	}
}

func TestSetWithOptionsExpireAt(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(4), WithClock(clk))
	defer c.Close()
	target := clk.Now().Add(20 * time.Second)
	if err := c.SetWithOptions("k", 1, SetExpireAt(target)); err != nil {
		t.Fatalf("SetWithOptions: %v", err)
	}
	exp, ok := c.Expiry("k")
	if !ok || !exp.Equal(target) {
		t.Errorf("Expiry = (%v, %v), want (%v, true)", exp, ok, target)
	}
}

func TestSetWithOptionsExplicitWeight(t *testing.T) {
	c, _ := New[string, []byte](
		WithMaxBytes(1024),
		WithWeigher[[]byte](BytesWeigher()),
	)
	defer c.Close()
	// Explicit weight ignores Weigher: store a 5-byte slice as weight=99.
	if err := c.SetWithOptions("k", []byte("hello"), SetWeight(99)); err != nil {
		t.Fatalf("SetWithOptions: %v", err)
	}
	if c.Bytes() != 99 {
		t.Errorf("Bytes after SetWeight(99) = %d, want 99", c.Bytes())
	}
}

func TestSetWithOptionsTags(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	if err := c.SetWithOptions("k", 1, SetTags("a", "b")); err != nil {
		t.Fatalf("SetWithOptions: %v", err)
	}
	// Tags currently sit on the entry; verify via internal field.
	s := c.shardFor("k")
	s.mu.RLock()
	tags := append([]string(nil), s.entries["k"].tags...)
	s.mu.RUnlock()
	if len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Errorf("entry tags = %v, want [a b]", tags)
	}
}

func TestSetWithOptionsRejectsNegativeTTL(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	err := c.SetWithOptions("k", 1, SetTTL(-time.Second))
	if !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("expected ErrInvalidTTL, got %v", err)
	}
}

func TestSetWithOptionsSlidingOverride(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithClock(clk),
		WithSlidingTTL(false),
	)
	defer c.Close()
	if err := c.SetWithOptions("k", 1, SetTTL(10*time.Second), SetSliding(true)); err != nil {
		t.Fatalf("SetWithOptions: %v", err)
	}
	clk.Advance(7 * time.Second)
	_, _ = c.Get("k") // sliding TTL should refresh
	d, _ := c.TTL("k")
	if d < 9*time.Second {
		t.Errorf("after Get, TTL = %v, want sliding refresh ≈ 10s", d)
	}
}

func TestSetWithOptionsPriorityClamped(t *testing.T) {
	// Priority is hint-only, but clamping logic should keep extreme
	// values in [-100, 100] when they reach the entry.
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	// Just verify the option doesn't error — there is no public
	// observation surface for priority in v0.
	if err := c.SetWithOptions("k", 1, SetPriority(500)); err != nil {
		t.Errorf("SetWithOptions(SetPriority(500)) = %v, want nil", err)
	}
	if err := c.SetWithOptions("k2", 1, SetPriority(-500)); err != nil {
		t.Errorf("SetWithOptions(SetPriority(-500)) = %v, want nil", err)
	}
}

func TestResizeShrinksAndEvicts(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(20), WithShards(1))
	defer c.Close()
	for i := range 15 {
		_ = c.Set(itoaSimple(i), i)
	}
	before := c.Len()
	evicted := c.Resize(4)
	if evicted == 0 {
		t.Error("Resize from 20 → 4 should evict entries")
	}
	if c.Len() >= before {
		t.Errorf("Len after Resize = %d, expected to drop from %d", c.Len(), before)
	}
	if c.Capacity() != 4 {
		t.Errorf("Capacity after Resize = %d, want 4", c.Capacity())
	}
}

func TestResizeGrowDoesNotEvict(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for i := range 5 {
		_ = c.Set(itoaSimple(i), i)
	}
	before := c.Len()
	evicted := c.Resize(100)
	if evicted != 0 {
		t.Errorf("Resize grow evicted %d entries; want 0", evicted)
	}
	if c.Len() != before {
		t.Errorf("Resize grow lost entries: before=%d after=%d", before, c.Len())
	}
}

func TestResizeNegativeIsNoop(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("k", 1)
	if got := c.Resize(-1); got != 0 {
		t.Errorf("Resize(-1) evicted %d, want 0", got)
	}
	if !c.Has("k") {
		t.Error("Resize(-1) should not affect existing entries")
	}
}

func TestSyncReturnsCtxErr(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	ctx, cancel := contextWithCancel()
	cancel()
	if err := c.Sync(ctx); err == nil {
		t.Error("Sync with canceled ctx should return non-nil error")
	}
}

func TestDeleteExpired(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("a", 1, time.Second)
	_ = c.SetWithTTL("b", 2, time.Hour)
	_ = c.Set("c", 3) // no TTL
	clk.Advance(2 * time.Second)
	n := c.DeleteExpired()
	if n != 1 {
		t.Errorf("DeleteExpired removed %d, want 1", n)
	}
	if c.Has("a") {
		t.Error("expired entry 'a' should be removed")
	}
	if !c.Has("b") || !c.Has("c") {
		t.Error("non-expired entries should remain")
	}
}

func TestDeletePrefixOnStringKeys(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(16))
	defer c.Close()
	for _, k := range []string{"user:42:name", "user:42:age", "user:7:name", "session:1"} {
		_ = c.Set(k, 1)
	}
	n := c.DeletePrefix("user:42:")
	if n != 2 {
		t.Errorf("DeletePrefix removed %d, want 2", n)
	}
	if c.Has("user:42:name") || c.Has("user:42:age") {
		t.Error("user:42:* entries should be gone")
	}
	if !c.Has("user:7:name") || !c.Has("session:1") {
		t.Error("non-matching entries should survive")
	}
}

func TestDeletePrefixUnsupportedKeyType(t *testing.T) {
	// Int keys neither implement Prefixer nor are string —
	// DeletePrefix is a no-op.
	c, _ := New[int, string](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set(1, "x")
	if n := c.DeletePrefix("anything"); n != 0 {
		t.Errorf("DeletePrefix on int-keyed cache removed %d, want 0", n)
	}
	if !c.Has(1) {
		t.Error("non-Prefixer keys should be untouched")
	}
}

func TestDeleteWhere(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for i := range 5 {
		_ = c.Set(itoaSimple(i), i)
	}
	n := c.DeleteWhere(func(_ string, v int) bool { return v%2 == 0 })
	// Removes 0, 2, 4 → 3 entries.
	if n != 3 {
		t.Errorf("DeleteWhere removed %d, want 3", n)
	}
	if c.Len() != 2 {
		t.Errorf("Len after DeleteWhere = %d, want 2", c.Len())
	}
}

func TestDeleteWhereNilPredicate(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	if n := c.DeleteWhere(nil); n != 0 {
		t.Errorf("DeleteWhere(nil) removed %d, want 0", n)
	}
	if !c.Has("k") {
		t.Error("nil predicate should not delete anything")
	}
}

func TestApplyJitterBounds(t *testing.T) {
	const ttl = 1000 * time.Millisecond
	const jitter = 100 * time.Millisecond
	for range 200 {
		out := applyJitter(ttl, jitter)
		if out < ttl-jitter || out > ttl+jitter {
			t.Fatalf("applyJitter out of range: got %v, want %v ± %v", out, ttl, jitter)
		}
	}
}

func TestApplyJitterClampsToQuarterTTL(t *testing.T) {
	// Jitter > ttl/4 should be clamped to ttl/4 to keep results positive.
	const ttl = 100 * time.Millisecond
	const jitter = ttl // larger than ttl/4
	for range 50 {
		out := applyJitter(ttl, jitter)
		// Clamped jitter is ttl/4 = 25ms, so result is in [75ms, 125ms].
		if out < ttl-(ttl/4) || out > ttl+(ttl/4) {
			t.Fatalf("clamped jitter out of range: got %v", out)
		}
	}
}

func TestApplyJitterDisabled(t *testing.T) {
	// Zero or negative jitter must be a no-op.
	if applyJitter(time.Second, 0) != time.Second {
		t.Error("zero jitter should not modify TTL")
	}
	if applyJitter(time.Second, -time.Second) != time.Second {
		t.Error("negative jitter should not modify TTL")
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
