package memcache

import (
	"container/heap"
	"errors"
	"sync"
	"testing"
	"time"
)

// makeTTLEntry constructs an entry with a given expireAt suitable as
// heap input.
func makeTTLEntry(key string, expireAt int64) *entry[string, int] {
	e := &entry[string, int]{key: key, heapIndex: -1}
	e.expireAt.Store(expireAt)
	return e
}

func TestExpiryHeapOrderedByExpireAt(t *testing.T) {
	var h expiryHeap[string, int]
	heap.Init(&h)

	heap.Push(&h, makeTTLEntry("c", 30))
	heap.Push(&h, makeTTLEntry("a", 10))
	heap.Push(&h, makeTTLEntry("b", 20))

	got := []string{}
	for h.Len() > 0 {
		e, _ := heap.Pop(&h).(*entry[string, int])
		got = append(got, e.key)
	}
	want := []string{"a", "b", "c"}
	for i, k := range want {
		if got[i] != k {
			t.Errorf("pop order [%d] = %q, want %q (full=%v)", i, got[i], k, got)
		}
	}
}

func TestExpiryHeapFix(t *testing.T) {
	var h expiryHeap[string, int]
	heap.Init(&h)
	a := makeTTLEntry("a", 100)
	b := makeTTLEntry("b", 200)
	heap.Push(&h, a)
	heap.Push(&h, b)
	if h[0] != a {
		t.Fatalf("heap min should be %q", a.key)
	}
	// Move a far into the future so b becomes the new min.
	a.expireAt.Store(1_000_000)
	heap.Fix(&h, a.heapIndex)
	if h[0] != b {
		t.Errorf("after Fix, heap min = %q, want %q", h[0].key, b.key)
	}
}

func TestExpiryHeapRemove(t *testing.T) {
	var h expiryHeap[string, int]
	heap.Init(&h)
	a := makeTTLEntry("a", 10)
	b := makeTTLEntry("b", 20)
	c := makeTTLEntry("c", 30)
	heap.Push(&h, a)
	heap.Push(&h, b)
	heap.Push(&h, c)
	heap.Remove(&h, b.heapIndex)
	if h.Len() != 2 {
		t.Errorf("after Remove, Len = %d, want 2", h.Len())
	}
	if b.heapIndex != -1 {
		t.Errorf("removed entry heapIndex = %d, want -1", b.heapIndex)
	}
}

func TestSetWithTTLPushesToHeap(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8), WithShards(1), WithClock(clk),
		WithJanitorInterval(time.Hour), // long interval so we drive sweeps manually
	)
	defer c.Close()
	if err := c.SetWithTTL("k", 1, time.Second); err != nil {
		t.Fatal(err)
	}
	s := c.shardFor("k")
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ttl.Len() != 1 {
		t.Errorf("heap.Len = %d, want 1 after TTL'd Set", s.ttl.Len())
	}
	e, _ := s.storage.get("k")
	if e.heapIndex != 0 {
		t.Errorf("entry heapIndex = %d, want 0", e.heapIndex)
	}
}

func TestSetWithoutTTLDoesNotTouchHeap(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8), WithShards(1))
	defer c.Close()
	_ = c.Set("k", 1) // no TTL
	s := c.shardFor("k")
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ttl.Len() != 0 {
		t.Errorf("heap.Len = %d, want 0 for no-TTL entry", s.ttl.Len())
	}
	e, _ := s.storage.get("k")
	if e.heapIndex != -1 {
		t.Errorf("no-TTL entry heapIndex = %d, want -1", e.heapIndex)
	}
}

func TestUpdateChangesExpireAtFixesHeap(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8), WithShards(1), WithClock(clk),
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()
	_ = c.SetWithTTL("a", 1, 100*time.Millisecond)
	_ = c.SetWithTTL("b", 2, 200*time.Millisecond)
	// Re-set "a" with a much longer TTL — heap should re-balance.
	_ = c.SetWithTTL("a", 1, time.Hour)
	s := c.shardFor("a")
	s.mu.RLock()
	hb, _ := s.ttl.(*expiryHeapBackend[string, int])
	headKey := hb.heap[0].key
	s.mu.RUnlock()
	if headKey != "b" {
		t.Errorf("heap min after update = %q, want %q", headKey, "b")
	}
}

func TestDeleteRemovesFromHeap(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8), WithShards(1),
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Hour)
	c.Delete("k")
	s := c.shardFor("k")
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ttl.Len() != 0 {
		t.Errorf("heap.Len after Delete = %d, want 0", s.ttl.Len())
	}
}

func TestJanitorSweepsExpired(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8), WithShards(1), WithClock(clk),
		WithJanitorInterval(100*time.Millisecond),
	)
	defer c.Close()
	_ = c.SetWithTTL("a", 1, 50*time.Millisecond)
	_ = c.SetWithTTL("b", 2, time.Hour)

	// Drive past TTL and the janitor interval, then wait for the
	// janitor goroutine to actually pop the entry from the heap.
	// Peek alone would return ok=false via the lazy TTL check
	// without proving the janitor ran, so we wait on Expirations.
	clk.Advance(150 * time.Millisecond)
	waitFor(t, time.Second, func() bool {
		return c.Stats().Expirations >= 1
	})
	if !c.Has("b") {
		t.Error("non-expired entry should remain after janitor sweep")
	}
}

func TestJanitorRepeats(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8), WithShards(1), WithClock(clk),
		WithJanitorInterval(100*time.Millisecond),
	)
	defer c.Close()
	// First sweep
	_ = c.SetWithTTL("a", 1, 50*time.Millisecond)
	clk.Advance(150 * time.Millisecond)
	waitFor(t, time.Second, func() bool { return !c.Has("a") })

	// Second sweep — janitor must rearm and fire again
	_ = c.SetWithTTL("b", 2, 50*time.Millisecond)
	clk.Advance(150 * time.Millisecond)
	waitFor(t, time.Second, func() bool { return !c.Has("b") })
}

func TestJanitorLazyStart(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8), WithShards(1),
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()
	s := c.shardFor("k")
	if s.janitor.running.Load() {
		t.Error("janitor should not be running before any TTL'd insert")
	}
	_ = c.Set("nottl", 1) // no TTL — janitor stays asleep
	if s.janitor.running.Load() {
		t.Error("no-TTL Set should not start the janitor")
	}
	_ = c.SetWithTTL("k", 1, time.Hour)
	if !s.janitor.running.Load() {
		t.Error("first TTL'd Set should start the janitor")
	}
}

func TestJanitorStopsOnClose(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8), WithShards(1), WithClock(clk),
		WithJanitorInterval(100*time.Millisecond),
	)
	_ = c.SetWithTTL("k", 1, time.Hour)
	s := c.shardFor("k")
	if !s.janitor.running.Load() {
		t.Fatal("janitor should be running")
	}
	_ = c.Close()
	if s.janitor.running.Load() {
		t.Error("Close should stop the janitor")
	}
}

func TestWithExpireFuncTreatsAsExpired(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithExpireFunc(func(key string, _ int, _ Metadata) bool {
			return key == "stale"
		}),
	)
	defer c.Close()
	_ = c.Set("fresh", 1)
	_ = c.Set("stale", 2)
	if !c.Has("fresh") {
		t.Error("fresh entry should be visible")
	}
	if c.Has("stale") {
		t.Error("stale entry should look expired via WithExpireFunc")
	}
	if _, ok := c.Get("stale"); ok {
		t.Error("Get on expire-func-marked entry should miss")
	}
	st := c.Stats()
	if st.Expirations < 1 {
		t.Errorf("Expirations = %d, want ≥ 1", st.Expirations)
	}
}

func TestWithExpireFuncPanicRecovered(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithExpireFunc(func(string, int, Metadata) bool {
			panic("expire func boom")
		}),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	// Get should not panic; entry should be treated as fresh.
	got, ok := c.Get("k")
	if !ok || got != 1 {
		t.Errorf("Get under panicking expire func = (%d, %v), want (1, true)", got, ok)
	}
}

func TestWithExpireFuncTypeMismatch(t *testing.T) {
	// Predicate over (string, int) won't satisfy a Cache[string, string].
	_, err := New[string, string](
		WithMaxEntries(4),
		WithExpireFunc(func(string, int, Metadata) bool { return false }),
	)
	var ce *ConfigError
	if !errors.As(err, &ce) || ce.Field != "ExpireFunc" {
		t.Errorf("expected ConfigError on ExpireFunc mismatch; got %v", err)
	}
}

func TestSlidingTTLCoalesces(t *testing.T) {
	// Sliding-TTL coalescing is observed via lastAccess: hot reads
	// within the slide/4 window should not advance lastAccess.
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithShards(1),
		WithClock(clk),
		WithSlidingTTL(true),
		WithDefaultTTL(time.Minute),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	s := c.shardFor("k")

	s.mu.RLock()
	e, _ := s.storage.get("k")
	first := e.lastAccess.Load()
	s.mu.RUnlock()

	// Advance by less than slide/4 (15s) and read — coalesced.
	clk.Advance(5 * time.Second)
	_, _ = c.Get("k")
	s.mu.RLock()
	e, _ = s.storage.get("k")
	got := e.lastAccess.Load()
	s.mu.RUnlock()
	if got != first {
		t.Errorf("coalesced sliding-TTL Get advanced lastAccess from %d to %d", first, got)
	}

	// Advance past slide/4 — next Get must update lastAccess.
	clk.Advance(20 * time.Second) // total 25s > 15s
	_, _ = c.Get("k")
	s.mu.RLock()
	e, _ = s.storage.get("k")
	got = e.lastAccess.Load()
	s.mu.RUnlock()
	if got == first {
		t.Errorf("sliding-TTL Get past slide/4 should have advanced lastAccess; still at %d", got)
	}
}

// waitFor polls cond up to timeout. Helps coordinate with goroutines
// (e.g., the janitor) that fire under FakeClock-driven timers.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("waitFor: condition not met within %v", timeout)
}

// TestJanitorConcurrentSetGet stresses the janitor under concurrent
// reads/writes. Race-detector confirms there are no data races.
func TestJanitorConcurrentSetGet(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithJanitorInterval(5*time.Millisecond),
	)
	defer c.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_ = c.SetWithTTL("k", 1, 10*time.Millisecond)
					_, _ = c.Get("k")
				}
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestExpireFuncHonoredBySetIfAbsent regression: SetIfAbsent and
// PeekOrAdd previously checked entry.expired(now) (TTL only),
// bypassing WithExpireFunc. A custom expire predicate's "this
// entry is stale" verdict was silently ignored on those paths.
func TestExpireFuncHonoredBySetIfAbsent(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithExpireFunc(func(string, int, Metadata) bool { return true }),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	// ExpireFunc returns true → entry is expired by user policy
	// → SetIfAbsent should INSERT (overwrite the stale entry).
	stored, err := c.SetIfAbsent("k", 2)
	if err != nil {
		t.Fatalf("SetIfAbsent: %v", err)
	}
	if !stored {
		t.Error("SetIfAbsent should insert when ExpireFunc says existing entry is expired")
	}
}

func TestExpireFuncHonoredByPeekOrAdd(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithExpireFunc(func(string, int, Metadata) bool { return true }),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	// ExpireFunc says expired → PeekOrAdd should NOT return the
	// stale value; it should add the new one.
	got, loaded, err := c.PeekOrAdd("k", 99)
	if err != nil {
		t.Fatalf("PeekOrAdd: %v", err)
	}
	if loaded {
		t.Error("PeekOrAdd should treat ExpireFunc-expired entry as absent")
	}
	if got != 99 {
		t.Errorf("PeekOrAdd value = %d, want 99 (the freshly inserted value)", got)
	}
}

// TestExpireFuncRecordsDistinctEvictionReason regression: removals
// driven by WithExpireFunc should land in
// Stats.EvictionsByReason[EvictReasonExpireFunc], not lumped with
// TTL expirations under EvictReasonExpired. Before the fix, the
// dedicated reason constant was dead code and the path through
// Get's slow-path expiry handling always passed EvictReasonExpired.
func TestExpireFuncRecordsDistinctEvictionReason(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithExpireFunc(func(key string, _ int, _ Metadata) bool {
			return key == "stale"
		}),
	)
	defer c.Close()
	_ = c.Set("stale", 1)            // no TTL — only the predicate marks it expired
	if _, ok := c.Get("stale"); ok { // triggers the predicate-driven eviction
		t.Fatal("Get on predicate-expired entry should miss")
	}
	st := c.Stats()
	if got := st.EvictionsByReason[EvictReasonExpireFunc]; got != 1 {
		t.Errorf("EvictionsByReason[EvictReasonExpireFunc] = %d, want 1", got)
	}
	// And TTL-only expirations should NOT have ticked.
	if got := st.EvictionsByReason[EvictReasonExpired]; got != 0 {
		t.Errorf("EvictionsByReason[EvictReasonExpired] = %d, want 0 "+
			"(predicate-driven removal should not count as TTL expiry)", got)
	}
}
