package memcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingLoader records call count + serves a fixed value.
type countingLoader struct {
	calls   atomic.Int64
	value   int
	delay   time.Duration
	err     error
	holdCh  chan struct{}   // when non-nil, blocks until receive on holdCh
	gateCh  chan struct{}   // signaled before the loader returns
	loadCtx context.Context //nolint:containedctx // testing-only state
}

func (l *countingLoader) Load(ctx context.Context, _ string) (int, time.Duration, error) {
	l.calls.Add(1)
	l.loadCtx = ctx
	if l.holdCh != nil {
		<-l.holdCh
	}
	if l.delay > 0 {
		select {
		case <-time.After(l.delay):
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
	if l.gateCh != nil {
		close(l.gateCh)
	}
	if l.err != nil {
		return 0, 0, l.err
	}
	return l.value, 0, nil
}

func TestGetOrLoadFastHit(t *testing.T) {
	loader := &countingLoader{value: 1}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader(loader),
	)
	defer c.Close()
	_ = c.Set("k", 99) // pre-populate; loader should NOT fire
	v, err := c.GetOrLoad(context.Background(), "k")
	if err != nil || v != 99 {
		t.Fatalf("GetOrLoad on hit = (%d, %v), want (99, nil)", v, err)
	}
	if loader.calls.Load() != 0 {
		t.Errorf("loader called %d times on hit; want 0", loader.calls.Load())
	}
}

func TestGetOrLoadInvokesLoader(t *testing.T) {
	loader := &countingLoader{value: 42}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader(loader),
	)
	defer c.Close()
	v, err := c.GetOrLoad(context.Background(), "k")
	if err != nil || v != 42 {
		t.Fatalf("GetOrLoad miss = (%d, %v), want (42, nil)", v, err)
	}
	if got := loader.calls.Load(); got != 1 {
		t.Errorf("loader called %d times; want 1", got)
	}
	// Subsequent GetOrLoad should hit the cache, not call again.
	_, _ = c.GetOrLoad(context.Background(), "k")
	if got := loader.calls.Load(); got != 1 {
		t.Errorf("after second GetOrLoad, calls = %d; want 1 (cache hit)", got)
	}
}

func TestGetOrLoadNoLoaderReturnsErr(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_, err := c.GetOrLoad(context.Background(), "k")
	if !errors.Is(err, ErrNoLoader) {
		t.Errorf("expected ErrNoLoader, got %v", err)
	}
}

func TestGetOrLoadFn(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	v, err := c.GetOrLoadFn(context.Background(), "k", func(_ context.Context, _ string) (int, time.Duration, error) {
		return 7, 0, nil
	})
	if err != nil || v != 7 {
		t.Fatalf("GetOrLoadFn = (%d, %v), want (7, nil)", v, err)
	}
	got, _ := c.Get("k")
	if got != 7 {
		t.Errorf("loaded value not cached; Get = %d, want 7", got)
	}
}

// TestStampede1000Goroutines: 1000 concurrent GetOrLoad on a
// missing key MUST invoke the Loader exactly once.
func TestStampede1000Goroutines(t *testing.T) {
	hold := make(chan struct{})
	loader := &countingLoader{value: 42, holdCh: hold}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader(loader),
	)
	defer c.Close()

	const G = 1000
	var wg sync.WaitGroup
	results := make([]int, G)
	errs := make([]error, G)
	start := make(chan struct{})
	for i := range G {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			v, err := c.GetOrLoad(context.Background(), "k")
			results[i] = v
			errs[i] = err
		}(i)
	}
	close(start)
	// Give goroutines a chance to enter GetOrLoad, then release loader.
	time.Sleep(20 * time.Millisecond)
	close(hold)
	wg.Wait()

	if got := loader.calls.Load(); got != 1 {
		t.Errorf("loader invoked %d times across 1000 goroutines; want 1", got)
	}
	for i, v := range results {
		if errs[i] != nil {
			t.Errorf("goroutine %d returned error %v", i, errs[i])
		}
		if v != 42 {
			t.Errorf("goroutine %d got %d, want 42", i, v)
		}
	}
	st := c.Stats()
	if st.LoadCoalesced == 0 {
		t.Error("expected LoadCoalesced > 0 to confirm singleflight savings")
	}
}

func TestCtxAggregationCancelsLoaderWhenAllCancel(t *testing.T) {
	// Two waiters; both cancel their ctx, so loader's ctx.Done
	// fires and the loader returns ctx.Canceled.
	cancelObserved := atomic.Bool{}
	loader := LoaderFunc[string, int](func(ctx context.Context, _ string) (int, time.Duration, error) {
		<-ctx.Done()
		cancelObserved.Store(true)
		return 0, 0, ctx.Err()
	})
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
	)
	defer c.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	r1 := make(chan error, 1)
	r2 := make(chan error, 1)
	go func() { _, err := c.GetOrLoad(ctx1, "k"); r1 <- err }()
	// Give first goroutine time to register the flight before
	// the second joins.
	time.Sleep(10 * time.Millisecond)
	go func() { _, err := c.GetOrLoad(ctx2, "k"); r2 <- err }()
	time.Sleep(10 * time.Millisecond)

	// Cancel both.
	cancel1()
	cancel2()

	for range 2 {
		select {
		case err := <-r1:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("waiter 1 = %v, want context.Canceled", err)
			}
			r1 = nil
		case err := <-r2:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("waiter 2 = %v, want context.Canceled", err)
			}
			r2 = nil
		case <-time.After(2 * time.Second):
			t.Fatal("waiters did not return")
		}
	}
	// Loader observation runs in a separate goroutine; poll briefly.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cancelObserved.Load() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Error("loader's ctx did not see Done after all waiters canceled")
}

func TestCtxPartialCancelKeepsLoaderRunning(t *testing.T) {
	// One waiter cancels; the other doesn't. Loader keeps running
	// and the surviving waiter receives the value.
	hold := make(chan struct{})
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		<-hold
		return 99, 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
	)
	defer c.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	r1 := make(chan error, 1)
	r2 := make(chan int, 1)
	go func() { _, err := c.GetOrLoad(ctx1, "k"); r1 <- err }()
	time.Sleep(10 * time.Millisecond)
	go func() {
		v, _ := c.GetOrLoad(context.Background(), "k")
		r2 <- v
	}()
	time.Sleep(10 * time.Millisecond)

	// Cancel only the first waiter.
	cancel1()
	if err := <-r1; !errors.Is(err, context.Canceled) {
		t.Errorf("waiter 1 = %v, want context.Canceled", err)
	}
	// Loader still alive; release it.
	close(hold)
	if v := <-r2; v != 99 {
		t.Errorf("surviving waiter = %d, want 99", v)
	}
}

func TestGetOrLoadCtxCancelDoesNotAbortLoader(t *testing.T) {
	hold := make(chan struct{})
	loader := &countingLoader{value: 1, holdCh: hold}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
	)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := c.GetOrLoad(ctx, "k")
		resultCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("GetOrLoad did not return after ctx cancel")
	}

	// Loader is still blocked on hold; release and confirm it
	// stored the value despite the canceled caller.
	close(hold)
	time.Sleep(20 * time.Millisecond)
	if v, ok := c.Get("k"); !ok || v != 1 {
		t.Errorf("loader should have stored value; Get = (%d, %v)", v, ok)
	}
}

func TestGetOrLoadLoaderTimeout(t *testing.T) {
	loader := &countingLoader{
		value: 1,
		delay: 100 * time.Millisecond,
	}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
		WithLoaderTimeout(10*time.Millisecond),
	)
	defer c.Close()
	_, err := c.GetOrLoad(context.Background(), "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestGetOrLoadNegativeCache(t *testing.T) {
	loader := &countingLoader{err: ErrNotFound}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
		WithNegativeCache(time.Hour),
	)
	defer c.Close()

	_, err := c.GetOrLoad(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("first GetOrLoad = %v, want ErrNotFound", err)
	}
	if loader.calls.Load() != 1 {
		t.Errorf("first call: loader calls = %d, want 1", loader.calls.Load())
	}

	// Second call should NOT invoke loader (negative cache hit).
	_, err = c.GetOrLoad(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("second GetOrLoad = %v, want ErrNotFound", err)
	}
	if loader.calls.Load() != 1 {
		t.Errorf("second call: loader still called; calls = %d, want 1", loader.calls.Load())
	}
}

func TestGetOrLoadErrorTTL(t *testing.T) {
	boom := errors.New("upstream broke")
	loader := &countingLoader{err: boom}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
		WithErrorTTL(time.Hour),
	)
	defer c.Close()

	_, err := c.GetOrLoad(context.Background(), "k")
	if !errors.Is(err, boom) {
		t.Fatalf("first call = %v, want %v", err, boom)
	}
	if loader.calls.Load() != 1 {
		t.Errorf("loader calls = %d, want 1", loader.calls.Load())
	}

	_, err = c.GetOrLoad(context.Background(), "k")
	if !errors.Is(err, boom) {
		t.Errorf("second call = %v, want cached %v", err, boom)
	}
	if loader.calls.Load() != 1 {
		t.Errorf("error should be cached; loader still called %d times", loader.calls.Load())
	}
}

func TestGetOrLoadErrorTTLDoesNotCacheNotFound(t *testing.T) {
	// WithErrorTTL is for OTHER errors. ErrNotFound goes to negative
	// cache (or, if WithNegativeCache is off, gets re-tried).
	loader := &countingLoader{err: ErrNotFound}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
		WithErrorTTL(time.Hour), // no WithNegativeCache
	)
	defer c.Close()
	_, _ = c.GetOrLoad(context.Background(), "k")
	_, _ = c.GetOrLoad(context.Background(), "k")
	if loader.calls.Load() < 2 {
		t.Errorf("ErrNotFound without WithNegativeCache should not be cached; calls = %d", loader.calls.Load())
	}
}

func TestRefresh(t *testing.T) {
	calls := atomic.Int64{}
	values := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		return int(values.Add(1)), 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
	)
	defer c.Close()

	// First load → value=1
	v, _ := c.GetOrLoad(context.Background(), "k")
	if v != 1 {
		t.Fatalf("initial load = %d, want 1", v)
	}
	// Refresh → loader fires again → value=2
	if err := c.Refresh(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	// Wait for the async refresh to complete.
	for range 100 {
		if got, _ := c.Get("k"); got == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if v, _ := c.Get("k"); v != 2 {
		t.Errorf("after Refresh: Get = %d, want 2", v)
	}
}

func TestRefreshNoLoader(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	if err := c.Refresh(context.Background(), "k"); !errors.Is(err, ErrNoLoader) {
		t.Errorf("expected ErrNoLoader, got %v", err)
	}
}

func TestRefreshAll(t *testing.T) {
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		return 0, 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader(loader),
	)
	defer c.Close()

	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(k, 0)
	}
	calls.Store(0)
	count := c.RefreshAll(context.Background())
	if count != 3 {
		t.Errorf("RefreshAll queued %d, want 3", count)
	}
	// Drain refreshes.
	for range 100 {
		if calls.Load() == 3 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if calls.Load() != 3 {
		t.Errorf("loader fired %d times; want 3", calls.Load())
	}
}

func TestRefreshAhead(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		return 99, 100 * time.Millisecond, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithClock(clk),
		WithLoader(loader),
		WithRefreshAhead(0.5), // refresh after 50% of TTL
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, 100*time.Millisecond)

	// Read at t=10ms (well below threshold); no refresh.
	clk.Advance(10 * time.Millisecond)
	_, _ = c.Get("k")
	if calls.Load() != 0 {
		t.Errorf("refresh-ahead fired too early; calls = %d", calls.Load())
	}

	// Read at t=60ms (past 50% threshold); should fire.
	clk.Advance(50 * time.Millisecond)
	_, _ = c.Get("k")
	for range 100 {
		if calls.Load() >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if calls.Load() < 1 {
		t.Errorf("refresh-ahead did not fire past 50%% threshold; calls = %d", calls.Load())
	}
}

func TestStaleWhileRevalidate(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		return 999, time.Hour, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithClock(clk),
		WithLoader(loader),
		WithStaleWhileRevalidate(time.Hour),
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 42, 100*time.Millisecond)

	// Past expiry but within staleFor window: Get returns stale.
	clk.Advance(150 * time.Millisecond)
	v, ok := c.Get("k")
	if !ok || v != 42 {
		t.Errorf("SWR Get = (%d, %v), want (42, true)", v, ok)
	}
	// Wait for the async refresh to fire.
	for range 100 {
		if calls.Load() >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if calls.Load() < 1 {
		t.Error("SWR did not trigger background refresh")
	}
}

func TestSWRBeyondStaleForNotServed(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		return 0, 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithClock(clk),
		WithLoader(loader),
		WithStaleWhileRevalidate(50*time.Millisecond),
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 42, 100*time.Millisecond)
	// Way past staleFor window.
	clk.Advance(time.Hour)
	if _, ok := c.Get("k"); ok {
		t.Error("Get past staleFor window should miss")
	}
}

func TestNegativeCacheRespected(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithNegativeCache(time.Hour),
	)
	defer c.Close()
	// Manually insert a negative tombstone via a loader.
	loader := &countingLoader{err: ErrNotFound}
	c2, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(loader),
		WithNegativeCache(time.Hour),
	)
	defer c2.Close()
	_, _ = c2.GetOrLoad(context.Background(), "k")
	// Has should report false (negative tombstone is hidden from
	// readers).
	if c2.Has("k") {
		t.Error("Has on negative tombstone should return false")
	}
	// Get also misses.
	if _, ok := c2.Get("k"); ok {
		t.Error("Get on negative tombstone should miss")
	}
	_ = c
}

// bulkLoaderFunc adapts a function to BulkLoader.
type bulkLoaderFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]LoadResult[V], error)

func (f bulkLoaderFunc[K, V]) LoadMulti(ctx context.Context, keys []K) (map[K]LoadResult[V], error) {
	return f(ctx, keys)
}

func TestGetMultiOrLoadHitsBypassLoader(t *testing.T) {
	calls := atomic.Int64{}
	bl := bulkLoaderFunc[string, int](func(_ context.Context, keys []string) (map[string]LoadResult[int], error) {
		calls.Add(1)
		out := map[string]LoadResult[int]{}
		for _, k := range keys {
			out[k] = LoadResult[int]{Value: len(k)}
		}
		return out, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithBulkLoader(bl),
	)
	defer c.Close()
	_ = c.Set("alice", 1)
	_ = c.Set("bob", 2)
	got, err := c.GetMultiOrLoad(context.Background(), []string{"alice", "bob"})
	if err != nil {
		t.Fatal(err)
	}
	if got["alice"] != 1 || got["bob"] != 2 || len(got) != 2 {
		t.Errorf("GetMultiOrLoad full hit = %v", got)
	}
	if calls.Load() != 0 {
		t.Errorf("bulk loader fired %d times on full-hit; want 0", calls.Load())
	}
}

func TestGetMultiOrLoadCoalescesMisses(t *testing.T) {
	calls := atomic.Int64{}
	var seenKeys atomic.Pointer[[]string]
	bl := bulkLoaderFunc[string, int](func(_ context.Context, keys []string) (map[string]LoadResult[int], error) {
		calls.Add(1)
		copyKeys := append([]string(nil), keys...)
		seenKeys.Store(&copyKeys)
		out := map[string]LoadResult[int]{}
		for _, k := range keys {
			out[k] = LoadResult[int]{Value: len(k)}
		}
		return out, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithBulkLoader(bl),
	)
	defer c.Close()
	got, err := c.GetMultiOrLoad(context.Background(), []string{"alice", "bob", "carol"})
	if err != nil {
		t.Fatal(err)
	}
	if got["alice"] != 5 || got["bob"] != 3 || got["carol"] != 5 {
		t.Errorf("GetMultiOrLoad = %v", got)
	}
	if calls.Load() != 1 {
		t.Errorf("bulk loader called %d times; want 1 (coalesce)", calls.Load())
	}
	// Loaded values cached for subsequent calls.
	v, _ := c.Get("alice")
	if v != 5 {
		t.Errorf("loaded value not cached; Get(alice) = %d", v)
	}
}

func TestGetMultiOrLoadFallsBackToSingleLoader(t *testing.T) {
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, k string) (int, time.Duration, error) {
		calls.Add(1)
		return len(k), 0, nil
	})
	// No WithBulkLoader → fall through to per-key Loader.
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader(loader),
	)
	defer c.Close()
	got, err := c.GetMultiOrLoad(context.Background(), []string{"alice", "bob"})
	if err != nil {
		t.Fatal(err)
	}
	if got["alice"] != 5 || got["bob"] != 3 {
		t.Errorf("fallback path produced %v", got)
	}
	if calls.Load() != 2 {
		t.Errorf("Loader fired %d times; want 2 (per-key)", calls.Load())
	}
}

func TestGetMultiOrLoadNoLoader(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	// Hit-only path: no loader configured but no misses either.
	got, err := c.GetMultiOrLoad(context.Background(), []string{"k"})
	if err != nil {
		t.Errorf("GetMultiOrLoad on full-hit without loader = %v; want nil", err)
	}
	if got["k"] != 1 {
		t.Errorf("got = %v", got)
	}
	// Miss-with-no-loader returns ErrNoLoader along with whatever
	// hits we collected.
	_, err = c.GetMultiOrLoad(context.Background(), []string{"k", "missing"})
	if !errors.Is(err, ErrNoLoader) {
		t.Errorf("missing-key without loader = %v; want ErrNoLoader", err)
	}
}

func TestLoaderRateLimitRejects(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		return 1, 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithClock(clk),
		WithLoader(loader),
		WithLoaderRateLimit(2), // 2 tokens/sec, bucket cap 2
	)
	defer c.Close()
	// First two consume the bucket.
	if _, err := c.GetOrLoad(context.Background(), "a"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := c.GetOrLoad(context.Background(), "b"); err != nil {
		t.Fatalf("second call: %v", err)
	}
	// Third should be rate-limited (no time advanced).
	_, err := c.GetOrLoad(context.Background(), "c")
	if !errors.Is(err, ErrLoaderRateLimited) {
		t.Errorf("third call = %v, want ErrLoaderRateLimited", err)
	}
}

func TestLoaderRateLimitRefillsOverTime(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		return 1, 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithClock(clk),
		WithLoader(loader),
		WithLoaderRateLimit(1),
	)
	defer c.Close()
	if _, err := c.GetOrLoad(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	// Bucket empty.
	if _, err := c.GetOrLoad(context.Background(), "b"); !errors.Is(err, ErrLoaderRateLimited) {
		t.Fatalf("expected rate-limit, got %v", err)
	}
	// After 2 seconds, at least one token should have refilled.
	clk.Advance(2 * time.Second)
	if _, err := c.GetOrLoad(context.Background(), "c"); err != nil {
		t.Errorf("after refill, GetOrLoad = %v; want nil", err)
	}
}

func TestMaxConcurrentLoadsCaps(t *testing.T) {
	hold := make(chan struct{})
	release := make(chan struct{})
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		hold <- struct{}{} // signal start
		<-release          // block until released
		return 1, 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithLoader(loader),
		WithMaxConcurrentLoads(2),
		WithLoaderTimeout(50*time.Millisecond),
	)
	defer c.Close()

	// Fire two loads to fill the slots.
	r1 := make(chan error, 1)
	r2 := make(chan error, 1)
	go func() { _, err := c.GetOrLoad(context.Background(), "a"); r1 <- err }()
	go func() { _, err := c.GetOrLoad(context.Background(), "b"); r2 <- err }()
	<-hold
	<-hold
	// Slots full. Third should time out waiting for a slot.
	_, err := c.GetOrLoad(context.Background(), "c")
	if !errors.Is(err, ErrLoaderTooManyInFlight) {
		t.Errorf("third loader = %v, want ErrLoaderTooManyInFlight", err)
	}
	// Release the held loaders.
	release <- struct{}{}
	release <- struct{}{}
	<-r1
	<-r2
}

func TestLoaderFuncAdapter(t *testing.T) {
	var fn LoaderFunc[string, int] = func(_ context.Context, key string) (int, time.Duration, error) {
		return len(key), 0, nil
	}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader(fn),
	)
	defer c.Close()
	v, err := c.GetOrLoad(context.Background(), "hello")
	if err != nil || v != 5 {
		t.Errorf("LoaderFunc adapter = (%d, %v), want (5, nil)", v, err)
	}
}

// TestSetClearsNegativeTombstone is a regression: Set over a
// negative-tombstone entry must clear flagNegative.
func TestSetClearsNegativeTombstone(t *testing.T) {
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		return 0, 0, ErrNotFound
	})
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader(loader),
		WithNegativeCache(time.Hour), // long enough to outlast the test
	)
	defer c.Close()

	// Trigger negative tombstone install.
	if _, err := c.GetOrLoad(context.Background(), "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetOrLoad: expected ErrNotFound, got %v", err)
	}
	// Overwrite via Set; MUST clear the tombstone.
	if err := c.Set("k", 42); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, ok := c.Get("k"); !ok || got != 42 {
		t.Errorf("Get(k) after Set-over-tombstone = (%d, %v), want (42, true)", got, ok)
	}
}
