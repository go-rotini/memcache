// contract_test.go checks promises that only break when features meet:
// loads racing writes, tags across delete and re-set, bounds at small
// sizes, snapshot integrity, and the like.

package memcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// startBlockedLoad starts GetOrLoadFn for key with a loader that returns
// value once release is closed, and returns a channel closed when the
// call returns. It waits until the load is in flight.
func startBlockedLoad(t *testing.T, c *Cache[string, string], key, value string, release <-chan struct{}) <-chan struct{} {
	t.Helper()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.GetOrLoadFn(context.Background(), key, func(context.Context, string) (string, time.Duration, error) {
			close(started)
			<-release
			return value, 0, nil
		})
	}()
	<-started
	return done
}

func TestLoadOvertakenByWriteIsNotStored(t *testing.T) {
	cases := []struct {
		name  string
		write func(c *Cache[string, string])
		want  string // "" means absent
	}{
		{"Set", func(c *Cache[string, string]) { _ = c.Set("k", "fresh") }, "fresh"},
		{"Delete", func(c *Cache[string, string]) { c.Delete("k") }, ""},
		{"Clear", func(c *Cache[string, string]) { c.Clear() }, ""},
		{"DeletePrefix", func(c *Cache[string, string]) { c.DeletePrefix("k") }, ""},
		{"Update", func(c *Cache[string, string]) {
			_ = c.Set("k", "a")
			_, _ = c.Update("k", func(string) string { return "updated" })
		}, "updated"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := New[string, string](WithMaxEntries(10))
			defer c.Close()
			release := make(chan struct{})
			done := startBlockedLoad(t, c, "k", "stale", release)
			tc.write(c)
			close(release)
			<-done
			v, ok := c.Get("k")
			if tc.want == "" && ok {
				t.Fatalf("Get = %q, want absent", v)
			}
			if tc.want != "" && v != tc.want {
				t.Fatalf("Get = %q, %v; want %q", v, ok, tc.want)
			}
		})
	}
}

func TestRefreshOvertakenByDeleteIsNotStored(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	loader := LoaderFunc[string, string](func(context.Context, string) (string, time.Duration, error) {
		started <- struct{}{}
		<-release
		return "refreshed", 0, nil
	})
	c, _ := New[string, string](WithMaxEntries(10), WithLoader(loader))
	defer c.Close()
	_ = c.Set("k", "v")
	if err := c.Refresh(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	<-started
	c.Delete("k")
	close(release)
	if err := eventually(func() bool { return c.Stats().LoadsTotal == 1 }); err != nil {
		t.Fatal(err)
	}
	if v, ok := c.Get("k"); ok {
		t.Fatalf("deleted key came back as %q", v)
	}
}

func eventually(cond func() bool) error {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("condition not met in time")
}

func TestTagIndexKeepsKeySetAgainAfterDelete(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(100))
	defer c.Close()
	for i := range 300 {
		_ = c.SetWithTags("k", i, "t")
		c.Delete("k")
		_ = c.SetWithTags("k", i, "t")
		if err := c.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if n := c.InvalidateTag("t"); n != 1 {
			t.Fatalf("round %d: InvalidateTag removed %d, want 1", i, n)
		}
		if c.Has("k") {
			t.Fatalf("round %d: key survived InvalidateTag", i)
		}
	}
}

func TestLoaderPanicReachesEveryWaiter(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(10), WithLogger(quietLogger))
	defer c.Close()
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	fn := func(context.Context, string) (int, time.Duration, error) {
		once.Do(func() { close(started) })
		<-release
		panic("boom")
	}
	errs := make(chan error, 3)
	go func() { _, err := c.GetOrLoadFn(context.Background(), "k", fn); errs <- err }()
	<-started
	for range 2 {
		go func() { _, err := c.GetOrLoadFn(context.Background(), "k", fn); errs <- err }()
	}
	if err := eventually(func() bool { return c.Stats().LoadCoalesced == 2 }); err != nil {
		t.Fatal(err)
	}
	close(release)
	for range 3 {
		err := <-errs
		if !errors.Is(err, ErrLoaderPanic) {
			t.Fatalf("err = %v, want ErrLoaderPanic", err)
		}
		if _, ok := errors.AsType[*LoadError](err); !ok || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("err = %v, want a *LoadError naming the panic", err)
		}
	}
	// The flight is gone: the next load runs.
	v, err := c.GetOrLoadFn(context.Background(), "k", func(context.Context, string) (int, time.Duration, error) {
		return 7, 0, nil
	})
	if err != nil || v != 7 {
		t.Fatalf("next load = %d, %v", v, err)
	}
}

var allPolicies = []Policy{PolicyS3FIFO, PolicyLRU, PolicyLFU, PolicyTinyLFU, PolicyFIFO, Policy2Q, PolicyARC}

func TestMaxEntriesIsACacheWideMaximum(t *testing.T) {
	for _, p := range allPolicies {
		for _, maxN := range []int{1, 10, 100, 1000} {
			t.Run(fmt.Sprintf("%v/%d", p, maxN), func(t *testing.T) {
				c, _ := New[int, int](WithMaxEntries(maxN), WithPolicy(p), WithShards(32))
				defer c.Close()
				for i := range 20 * maxN {
					_ = c.Set(i, i)
					if n := c.Len(); n > maxN {
						t.Fatalf("after %d sets: Len = %d, max %d", i+1, n, maxN)
					}
				}
				if got := c.Resize(int64(maxN / 2)); maxN > 1 && c.Len() > maxN/2 {
					t.Fatalf("after Resize: Len = %d (evicted %d), max %d", c.Len(), got, maxN/2)
				}
			})
		}
	}
}

func TestSetThenGetHitsOnEveryPolicy(t *testing.T) {
	for _, p := range allPolicies {
		t.Run(p.String(), func(t *testing.T) {
			c, _ := New[int, int](WithMaxEntries(100), WithPolicy(p), WithShards(1))
			defer c.Close()
			for i := range 1000 {
				_ = c.Set(i, i)
				_, _ = c.Get(i)
			}
			lost := 0
			for i := 1000; i < 1100; i++ {
				_ = c.Set(i, i)
				if _, ok := c.Get(i); !ok {
					lost++
				}
			}
			if lost > 0 {
				t.Fatalf("%d of 100 fresh Sets missed on the next Get", lost)
			}
		})
	}
}

// Under -race this also checks that Peek reads nothing after unlocking.
func TestPeekNeverReturnsAnotherKeysValue(t *testing.T) {
	c, _ := New[string, string](WithMaxEntries(64), WithShards(1))
	defer c.Close()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				k := strconv.Itoa((w*7 + i) % 32)
				_ = c.Set(k, k)
				c.Delete(k)
			}
		})
	}
	bad := atomic.Int64{}
	for range 4 {
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				k := strconv.Itoa(i % 32)
				if v, ok := c.Peek(k); ok && v != k {
					bad.Add(1)
				}
			}
		})
	}
	time.Sleep(200 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if bad.Load() > 0 {
		t.Fatalf("Peek returned another key's value %d times", bad.Load())
	}
}

// Under -race this checks that Resize doesn't race readers of the bounds.
func TestResizeConcurrentWithReaders(t *testing.T) {
	c, _ := New[int, int](WithMaxEntries(100))
	defer c.Close()
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for j := range 200 {
				c.Resize(int64(50 + (i*j)%100))
				_ = c.Set(j, j)
			}
		})
		wg.Go(func() {
			for range 200 {
				_ = c.Capacity()
				_ = c.Stats()
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			if cl, err := c.Clone(); err == nil {
				_ = cl.Close()
			}
		}
	})
	wg.Wait()
	c.Resize(10)
	if c.Len() > 10 || c.Capacity() != 10 {
		t.Fatalf("after Resize(10): Len=%d Capacity=%d", c.Len(), c.Capacity())
	}
}

func TestAbandonedFlightDoesNotEndTheNextOne(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(10))
	defer c.Close()
	var calls atomic.Int32
	release1 := make(chan struct{})
	release2 := make(chan struct{})
	fn := func(ctx context.Context, _ string) (int, time.Duration, error) {
		n := calls.Add(1)
		if n == 1 {
			<-release1 // ignores ctx, like many real loaders
			return 0, 0, errors.New("first failed")
		}
		<-release2
		return int(n), 0, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = c.GetOrLoadFn(ctx, "k", fn) }()
	if err := eventually(func() bool { return calls.Load() == 1 }); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := eventually(func() bool {
		s := c.shardFor("k")
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.inflight) == 0
	}); err != nil {
		t.Fatal(err)
	}
	done2 := make(chan struct{})
	go func() { _, _ = c.GetOrLoadFn(context.Background(), "k", fn); close(done2) }()
	if err := eventually(func() bool { return calls.Load() == 2 }); err != nil {
		t.Fatal(err)
	}
	close(release1) // flight 1 ends while flight 2 runs
	time.Sleep(20 * time.Millisecond)
	done3 := make(chan struct{})
	go func() { _, _ = c.GetOrLoadFn(context.Background(), "k", fn); close(done3) }()
	time.Sleep(20 * time.Millisecond)
	close(release2)
	<-done2
	<-done3
	if n := calls.Load(); n != 2 {
		t.Fatalf("loader ran %d times, want 2", n)
	}
}

func TestLockFreeReadSeesLoadedAndClonedEntries(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(100))
	for i := range 20 {
		_ = src.Set(strconv.Itoa(i), i)
	}
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	_ = src.Close()

	c, _ := New[string, int](WithMaxEntries(100), WithLockFreeRead())
	defer c.Close()
	if _, err := c.Load(&buf); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if _, ok := c.Get(strconv.Itoa(i)); !ok {
			t.Fatalf("Get(%d) missed after Load", i)
		}
	}
	cl, err := c.Clone()
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, ok := cl.Get("3"); !ok {
		t.Fatal("Get missed on a clone")
	}
}

// Under -race this checks the lock-free read path against writers.
func TestLockFreeReadConcurrentWithWriters(t *testing.T) {
	c, _ := New[int, int](WithMaxEntries(256), WithLockFreeRead(), WithShards(2))
	defer c.Close()
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; !stop.Load(); i++ {
			_ = c.Set(i%512, i)
			if i%3 == 0 {
				c.Delete((i / 3) % 512)
			}
		}
	})
	for range 3 {
		wg.Go(func() {
			for i := 0; !stop.Load(); i++ {
				_, _ = c.Get(i % 512)
			}
		})
	}
	time.Sleep(200 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

func TestErrorTTLKeepsABoundedNumberOfErrors(t *testing.T) {
	failing := errors.New("down")
	c, _ := New[int, int](WithMaxEntries(100), WithShards(1), WithErrorTTL(time.Hour))
	defer c.Close()
	fn := func(context.Context, int) (int, time.Duration, error) { return 0, 0, failing }
	for i := range 5000 {
		_, _ = c.GetOrLoadFn(context.Background(), i, fn)
	}
	s := c.shards[0]
	s.mu.Lock()
	n := len(s.errors)
	s.mu.Unlock()
	if limit := max(s.budget, errorCacheMinPerShard); n > limit {
		t.Fatalf("shard holds %d cached errors, limit %d", n, limit)
	}
	// The most recent error is still cached.
	if _, err := c.GetOrLoadFn(context.Background(), 4999, func(context.Context, int) (int, time.Duration, error) {
		return 1, 0, nil
	}); !errors.Is(err, failing) {
		t.Fatalf("err = %v, want the cached error", err)
	}
}

func TestUpdateKeepsTagsAndExpiry(t *testing.T) {
	clk := NewFakeClock(time.Unix(1000, 0))
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk))
	defer c.Close()
	_ = c.SetWithOptions("k", 1, SetTags("t"), SetTTL(time.Hour))
	exp, _ := c.Expiry("k")
	if _, err := c.Update("k", func(v int) int { return v + 1 }); err != nil {
		t.Fatal(err)
	}
	if _, err := Increment(c, "k"); err != nil {
		t.Fatal(err)
	}
	if !c.CompareAndSwap("k", 3, 4) {
		t.Fatal("CompareAndSwap failed")
	}
	if got, _ := c.Expiry("k"); !got.Equal(exp) {
		t.Fatalf("Expiry = %v, want %v", got, exp)
	}
	if n := c.InvalidateTag("t"); n != 1 {
		t.Fatalf("InvalidateTag removed %d, want 1", n)
	}
}

func TestComputeMayCallComputeOnAnotherCache(t *testing.T) {
	a, _ := New[string, int](WithMaxEntries(10))
	defer a.Close()
	b, _ := New[string, int](WithMaxEntries(10))
	defer b.Close()
	_, err := a.Compute("x", func(int, bool) (int, ComputeAction, error) {
		v, err := Increment(b, "y")
		return v, ComputeStore, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := a.Get("x"); v != 1 {
		t.Fatalf("a[x] = %d, want 1", v)
	}
}

func TestTieredPromotionKeepsTagsAndExpiry(t *testing.T) {
	clk := NewFakeClock(time.Unix(1000, 0))
	l1, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithDefaultTTL(time.Hour))
	l2, _ := New[string, int](WithMaxEntries(10), WithClock(clk))
	tc := NewTiered(l1, l2)
	defer tc.Close()
	_ = l2.SetWithOptions("k", 1, SetTags("user-1"), SetTTL(time.Second), SetSliding(false))
	if _, ok := tc.Get("k"); !ok {
		t.Fatal("Tiered.Get missed")
	}
	if tags := l1.Tags("k"); len(tags) != 1 || tags[0] != "user-1" {
		t.Fatalf("L1 tags = %v", tags)
	}
	clk.Advance(5 * time.Second)
	if _, ok := tc.Get("k"); ok {
		t.Fatal("promoted copy outlived its L2 expiry")
	}

	_ = l2.SetWithTags("j", 1, "grp")
	_, _ = tc.Get("j")
	tc.InvalidateTag("grp")
	if _, ok := tc.Get("j"); ok {
		t.Fatal("promoted copy survived InvalidateTag")
	}
}

func TestAsyncWritesSeenBySynchronousMethods(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(100), WithAsyncWrites(), WithDefaultTTL(time.Hour))
	defer c.Close()
	for i := range 200 {
		k := strconv.Itoa(i)
		_ = c.Set(k, 1)
		v, loaded, _ := c.GetOrSet(k, 2)
		if !loaded || v != 1 {
			t.Fatalf("GetOrSet after Set = %d, %v; want 1, true", v, loaded)
		}
		if _, ok := c.TTL(k); !ok {
			t.Fatal("TTL missed right after Set")
		}
		if err := c.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got, _ := c.Get(k); got != 1 {
			t.Fatalf("final value %d, want 1", got)
		}
	}
}

// probeStore wraps a MemoryStore and runs onDelete before each Delete.
type probeStore struct {
	*MemoryStore[string, int]
	onDelete func()
}

func (p probeStore) Delete(ctx context.Context, key string) (bool, error) {
	if p.onDelete != nil {
		p.onDelete()
	}
	return p.MemoryStore.Delete(ctx, key)
}

func TestStoreSeesEveryWriteAndDeleteDoesNotResurrect(t *testing.T) {
	ms := NewMemoryStore[string, int](nil)
	ps := &probeStore{MemoryStore: ms}
	c, _ := New[string, int](WithMaxEntries(100), WithStore(ps))
	defer c.Close()
	ctx := context.Background()

	_, _ = Increment(c, "inc")
	if _, _, err := c.GetOrSet("gos", 5); err != nil {
		t.Fatal(err)
	}
	_, _ = c.SetIfAbsent("sia", 6)
	_, _ = c.Compute("cmp", func(int, bool) (int, ComputeAction, error) { return 7, ComputeStore, nil })
	for k, want := range map[string]int{"inc": 1, "gos": 5, "sia": 6, "cmp": 7} {
		if v, ok, _ := ms.Get(ctx, k); !ok || v != want {
			t.Errorf("store[%s] = %d, %v; want %d", k, v, ok, want)
		}
	}

	_ = c.Set("d", 1)
	ps.onDelete = func() { _, _ = c.Get("d") } // a read in the middle of Delete
	c.Delete("d")
	ps.onDelete = nil
	if _, ok := c.Get("d"); ok {
		t.Fatal("deleted key is cached again")
	}
}

type tmplUnexported struct {
	ID     string `cache:"id,tag=user:{id}"`
	secret string `cache:"secret,tag=s:{secret}"`
}

type innerSecret struct {
	Token string `cache:"token,secret"`
}

type outerWithEmbedded struct {
	*innerSecret
	Name string
}

func TestStructTagsTolerateUnexportedAndNilEmbedded(t *testing.T) {
	c, _ := New[string, tmplUnexported](WithMaxEntries(10))
	defer c.Close()
	if err := c.Set("k", tmplUnexported{ID: "1", secret: "x"}); err != nil {
		t.Fatal(err)
	}
	if tags := c.Tags("k"); len(tags) != 2 || tags[1] != "s:x" {
		t.Fatalf("tags = %v", tags)
	}

	d, _ := New[string, outerWithEmbedded](WithMaxEntries(10), WithCodec(JSONCodec{}))
	defer d.Close()
	_ = d.Set("k", outerWithEmbedded{Name: "n"})
	if err := d.Save(io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestDoorkeeperKeepsRejectingOneHitKeys(t *testing.T) {
	d := NewDoorkeeper(1000, func(k int) uint64 { return uint64(k) * 0x9E3779B97F4A7C15 })
	for i := range 200_000 {
		d.Observe(i)
	}
	admitted := 0
	for i := 1_000_000; i < 1_001_000; i++ {
		if d.Admit(i) {
			admitted++
		}
	}
	if admitted > 100 {
		t.Fatalf("admitted %d of 1000 unseen keys", admitted)
	}
}

func TestFloatZeroKeysAreOneKey(t *testing.T) {
	negZero := 0.0
	negZero = -negZero
	c, _ := New[float64, string](WithMaxEntries(10))
	defer c.Close()
	_ = c.Set(0.0, "zero")
	if v, ok := c.Get(negZero); !ok || v != "zero" {
		t.Fatalf("Get(-0) = %q, %v", v, ok)
	}
	_ = c.Set(negZero, "neg")
	if c.Len() != 1 {
		t.Fatalf("Len = %d, want 1", c.Len())
	}

	type pt struct{ X, Y float64 }
	d, _ := New[pt, int](WithMaxEntries(10))
	defer d.Close()
	_ = d.Set(pt{0, 1}, 1)
	_ = d.Set(pt{negZero, 1}, 2)
	if d.Len() != 1 {
		t.Fatalf("struct keys: Len = %d, want 1", d.Len())
	}
}

func TestLoadFinishingAfterCloseIsDropped(t *testing.T) {
	c, _ := New[string, string](WithMaxEntries(10))
	release := make(chan struct{})
	done := startBlockedLoad(t, c, "k", "v", release)
	_ = c.Close()
	close(release)
	<-done
	if n := c.Len(); n != 0 {
		t.Fatalf("Len after Close = %d", n)
	}
}

func TestExpvarNameOwnedElsewhereIsAConfigError(t *testing.T) {
	_, err := New[string, int](WithMaxEntries(10), WithExpvar("memstats"))
	if _, ok := errors.AsType[*ConfigError](err); !ok {
		t.Fatalf("err = %v, want *ConfigError", err)
	}

	a, err := New[string, int](WithMaxEntries(10), WithExpvar("memcache_contract_reuse"))
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	b, _ := New[string, int](WithMaxEntries(10), WithExpvar("memcache_contract_reuse"))
	defer b.Close()
	_ = b.Set("k", 1)
	if got := expvarSlots["memcache_contract_reuse"].String(); !strings.Contains(got, `"entries":1`) {
		t.Fatalf("expvar = %s, want the new cache's stats", got)
	}
}

func TestMemoryStoreDropsExpiredEntries(t *testing.T) {
	clk := NewFakeClock(time.Unix(1000, 0))
	ms := NewMemoryStore[int, int](clk)
	ctx := context.Background()
	for i := range 10_000 {
		_ = ms.Set(ctx, i, i, time.Second)
	}
	clk.Advance(time.Minute)
	for i := range 10_000 {
		_ = ms.Set(ctx, -1-i%100, i, 0)
	}
	ms.mu.RLock()
	n := len(ms.entries)
	ms.mu.RUnlock()
	if n > 200 {
		t.Fatalf("store still holds %d entries", n)
	}
}

func TestTouchAppliesDefaultJitter(t *testing.T) {
	clk := NewFakeClock(time.Unix(1000, 0))
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk))
	defer c.Close()
	_ = c.Set("k", 1)
	distinct := map[time.Duration]bool{}
	for range 50 {
		c.TouchWithTTL("k", 100*time.Second)
		d, _ := c.TTL("k")
		if d < 95*time.Second || d > 105*time.Second {
			t.Fatalf("TTL %v outside ±5%%", d)
		}
		distinct[d] = true
	}
	if len(distinct) < 2 {
		t.Fatal("Touch applied no jitter")
	}
}

func TestGetOrLoadServesStaleAndRefreshesAhead(t *testing.T) {
	clk := NewFakeClock(time.Unix(1000, 0))
	var loads atomic.Int32
	loader := LoaderFunc[string, int](func(context.Context, string) (int, time.Duration, error) {
		return int(loads.Add(1)), 0, nil
	})
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithLoader(loader),
		WithDefaultTTL(10*time.Second), WithTTLJitter(0),
		WithRefreshAhead(0.5), WithStaleWhileRevalidate(time.Minute))
	defer c.Close()
	ctx := context.Background()
	if v, _ := c.GetOrLoad(ctx, "k"); v != 1 {
		t.Fatalf("first load = %d", v)
	}
	clk.Advance(6 * time.Second) // past refresh-ahead
	if v, _ := c.GetOrLoad(ctx, "k"); v != 1 {
		t.Fatalf("refresh-ahead hit = %d, want the cached 1", v)
	}
	if err := eventually(func() bool { return loads.Load() == 2 }); err != nil {
		t.Fatal("refresh-ahead did not start a load")
	}
	if err := eventually(func() bool { v, _ := c.Peek("k"); return v == 2 }); err != nil {
		t.Fatal(err)
	}
	clk.Advance(20 * time.Second) // expired, inside the stale window
	if v, err := c.GetOrLoad(ctx, "k"); err != nil || v != 2 {
		t.Fatalf("stale hit = %d, %v; want the stale 2", v, err)
	}
	if err := eventually(func() bool { return loads.Load() == 3 }); err != nil {
		t.Fatal("stale hit did not start a refresh")
	}
}

func TestErrorTextsLeaveKeysOut(t *testing.T) {
	err := error(&LoadError{Key: "session-secret", Err: errors.New("down")})
	if strings.Contains(err.Error(), "session-secret") {
		t.Fatalf("LoadError text %q contains the key", err)
	}
}

// --- snapshots ------------------------------------------------------------

// craftSnapshot encodes recs as a snapshot of the given format version,
// with a valid CRC, for codecs without an encryption layer.
func craftSnapshot(t *testing.T, codec Codec, version uint8, recs []entrySnapshot[string, string]) []byte {
	t.Helper()
	var body bytes.Buffer
	body.WriteString(snapshotMagic)
	body.WriteByte(version)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(writeString(&body, codec.Name()))
	must(writeString(&body, ""))
	must(writeMetadata(&body, nil))
	must(binary.Write(&body, binary.LittleEndian, int64(0)))
	must(binary.Write(&body, binary.LittleEndian, int64(len(recs))))
	for _, r := range recs {
		k, err := codec.Marshal(r.key)
		must(err)
		v, err := codec.Marshal(r.value)
		must(err)
		must(writeBytes(&body, k))
		must(writeBytes(&body, v))
		fields := []any{r.expireAt, r.weight, r.inserted, r.hits, r.flags}
		if version >= snapshotVersion {
			fields = append(fields, r.slidingTTL)
		}
		for _, f := range fields {
			must(binary.Write(&body, binary.LittleEndian, f))
		}
		body.WriteByte(uint8(len(r.tags)))
		for _, tag := range r.tags {
			must(writeString(&body, tag))
		}
	}
	return appendCRC(body.Bytes())
}

func appendCRC(b []byte) []byte {
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
}

// fixCRC recomputes the trailing CRC of a snapshot after tampering.
func fixCRC(b []byte) []byte {
	return appendCRC(append([]byte(nil), b[:len(b)-4]...))
}

func TestLoadLeavesCacheUntouchedOnCorruptInput(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(10), WithCodec(JSONCodec{}))
	_ = src.Set("balance", 100)
	var buf bytes.Buffer
	_ = src.Save(&buf)
	_ = src.Close()
	data := bytes.Replace(buf.Bytes(), []byte("100"), []byte("900"), 1)

	c, _ := New[string, int](WithMaxEntries(10), WithCodec(JSONCodec{}))
	defer c.Close()
	_ = c.Set("previous", 1)
	_, err := c.Load(bytes.NewReader(data))
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("err = %v, want ErrSnapshotCorrupt", err)
	}
	if _, ok := c.Get("balance"); ok {
		t.Fatal("corrupt record was applied")
	}
	if _, ok := c.Get("previous"); !ok {
		t.Fatal("failed Load dropped the previous contents")
	}
	se, _ := errors.AsType[*SnapshotError](err)
	if se == nil || se.Offset == 0 || !strings.Contains(err.Error(), ErrSnapshotCorrupt.Error()) {
		t.Fatalf("error %q lacks its cause or offset", err)
	}
}

func TestLoadBoundsAllocationByInputSize(t *testing.T) {
	var b bytes.Buffer
	b.WriteString(snapshotMagic)
	b.WriteByte(snapshotVersion)
	_ = binary.Write(&b, binary.LittleEndian, uint32(0xFFFFFFF0)) // codec name length
	c, _ := New[string, int](WithMaxEntries(10))
	defer c.Close()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := c.Load(bytes.NewReader(b.Bytes()))
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("err = %v, want ErrSnapshotCorrupt", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Fatalf("Load of %d bytes allocated %d bytes", b.Len(), grew)
	}

	path := t.TempDir() + "/snap"
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectSnapshot(path); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("InspectSnapshot err = %v, want ErrSnapshotCorrupt", err)
	}
}

func TestLoadCapsDecompression(t *testing.T) {
	codec := NewCompressedCodec(GobCodec{}, 9)
	src, _ := New[string, []byte](WithMaxEntries(10), WithCodec(codec))
	_ = src.Set("bomb", make([]byte, 32<<20))
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	_ = src.Close()

	c, _ := New[string, []byte](WithMaxEntries(10), WithCodec(codec), WithMaxSnapshotBytes(1<<20))
	defer c.Close()
	if buf.Len() > 1<<20 {
		t.Fatalf("compressed snapshot is %d bytes; the test needs it under the limit", buf.Len())
	}
	if _, err := c.Load(&buf); !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("err = %v, want ErrSnapshotTooLarge", err)
	}
}

func TestCompressedSnapshotOverSizeLimitRoundTrips(t *testing.T) {
	codec := NewCompressedCodec(GobCodec{}, 9)
	c, _ := New[string, []byte](WithMaxEntries(10), WithCodec(codec), WithMaxSnapshotBytes(1<<20))
	defer c.Close()
	_ = c.Set("big", make([]byte, 2<<20))
	var buf bytes.Buffer
	if err := c.Save(&buf); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Load(&buf); err != nil || n != 1 {
		t.Fatalf("a snapshot the cache saved fails its own Load: %d, %v", n, err)
	}
}

func TestSaveRefusesWhatLoadWouldRefuse(t *testing.T) {
	gz := NewCompressedCodec(GobCodec{}, 9)
	for name, opts := range map[string][]Option{
		"file over the limit":      {WithMaxSnapshotBytes(1 << 10)},
		"inflates past the budget": {WithMaxSnapshotBytes(1 << 20), WithCodec(gz)},
		"sealed records over":      {WithMaxSnapshotBytes(1 << 10), WithEncryptedCodec(GobCodec{}, bytes.Repeat([]byte{1}, 32))},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := New[string, []byte](append(opts, WithMaxEntries(10))...)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_ = c.Set("big", make([]byte, 17<<20))
			path := t.TempDir() + "/snap"
			if err := c.SaveFile(path); !errors.Is(err, ErrSnapshotTooLarge) {
				t.Fatalf("SaveFile err = %v, want ErrSnapshotTooLarge", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a refused save left a file: %v", err)
			}
		})
	}
}

func TestLoadRejectsBadWeightAndFlags(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rec   entrySnapshot[string, string]
		valid bool
	}{
		{"negative weight", entrySnapshot[string, string]{key: "k", value: "v", weight: -5}, false},
		{"zero weight", entrySnapshot[string, string]{key: "k", value: "v", weight: 0}, false},
		{"negative flag", entrySnapshot[string, string]{key: "k", value: "v", weight: 1, flags: uint16(flagNegative)}, false},
		{"unknown flag", entrySnapshot[string, string]{key: "k", value: "v", weight: 1, flags: 0x100}, false},
		{"sliding without TTL", entrySnapshot[string, string]{key: "k", value: "v", weight: 1, flags: uint16(flagSliding)}, false},
		{"plain", entrySnapshot[string, string]{key: "k", value: "v", weight: 1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := New[string, string](WithMaxEntries(10))
			defer c.Close()
			_, err := c.Load(bytes.NewReader(craftSnapshot(t, GobCodec{}, snapshotVersion, []entrySnapshot[string, string]{tc.rec})))
			if tc.valid != (err == nil) {
				t.Fatalf("err = %v", err)
			}
			if !tc.valid && !errors.Is(err, ErrSnapshotCorrupt) {
				t.Fatalf("err = %v, want ErrSnapshotCorrupt", err)
			}
		})
	}
}

func TestLoadAppliesTheCachesOwnLimits(t *testing.T) {
	recs := []entrySnapshot[string, string]{
		{key: "small", value: "v", weight: 1},
		{key: "lies", value: strings.Repeat("x", 50), weight: 1},
		{key: strings.Repeat("k", 40), value: "v", weight: 1},
		{key: "tagged", value: "v", weight: 1, tags: []string{"a", "b", "c"}},
	}
	c, _ := New[string, string](WithMaxEntries(10), WithMaxBytes(1000),
		WithWeigher(func(v string) int64 { return int64(len(v)) }),
		WithMaxValueWeight(20), WithMaxKeySize(32), WithMaxTagsPerEntry(2))
	defer c.Close()
	n, err := c.Load(bytes.NewReader(craftSnapshot(t, GobCodec{}, snapshotVersion, recs)))
	if err != nil || n != 1 {
		t.Fatalf("Load = %d, %v; want only the small record", n, err)
	}
	if c.Bytes() != 1 {
		t.Fatalf("Bytes = %d, want 1", c.Bytes())
	}
}

func TestSlidingTTLSurvivesSaveAndLoad(t *testing.T) {
	clk := NewFakeClock(time.Unix(1000, 0))
	src, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithTTLJitter(0))
	_ = src.SetWithOptions("k", 1, SetTTL(10*time.Second), SetSliding(true))
	var buf bytes.Buffer
	_ = src.Save(&buf)
	_ = src.Close()

	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithTTLJitter(0))
	defer c.Close()
	if _, err := c.Load(&buf); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		clk.Advance(6 * time.Second)
		if _, ok := c.Get("k"); !ok {
			t.Fatal("restored sliding entry expired while being read")
		}
	}
}

func TestLoadReadsThePreviousFormat(t *testing.T) {
	recs := []entrySnapshot[string, string]{
		{key: "a", value: "1", weight: 1, tags: []string{"t"}},
		{key: "s", value: "2", weight: 1, flags: uint16(flagSliding), expireAt: time.Now().Add(time.Hour).UnixNano()},
	}
	c, _ := New[string, string](WithMaxEntries(10))
	defer c.Close()
	n, err := c.Load(bytes.NewReader(craftSnapshot(t, GobCodec{}, snapshotVersionV2, recs)))
	if err != nil || n != 2 {
		t.Fatalf("Load = %d, %v", n, err)
	}
	if c.InvalidateTag("t") != 1 {
		t.Fatal("v2 tags not restored")
	}
	if m, _ := c.ItemMetadata("s"); m.Sliding {
		t.Fatal("v2 sliding entry without a saved TTL should load as absolute")
	}
}

func TestEncryptedSnapshotIsSealedAsAUnit(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	newCache := func() *Cache[string, string] {
		c, err := New[string, string](WithMaxEntries(10), WithEncryptedCodec(GobCodec{}, key), WithName("plain-name"))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	src := newCache()
	_ = src.SetWithTags("alice", "role=admin", "tenant-a")
	_ = src.SetWithTags("bob", "role=user", "tenant-b")
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	_ = src.Close()
	data := buf.Bytes()
	if bytes.Contains(data, []byte("tenant-a")) || bytes.Contains(data, []byte("alice")) {
		t.Fatal("tags or keys are in plaintext")
	}

	c := newCache()
	defer c.Close()
	if n, err := c.Load(bytes.NewReader(data)); err != nil || n != 2 {
		t.Fatalf("clean Load = %d, %v", n, err)
	}
	if v, _ := c.Get("alice"); v != "role=admin" {
		t.Fatalf("alice = %q", v)
	}

	// Flip one byte in the sealed records, and separately in the
	// header's cache name; fix the CRC each time.
	sealedByte := append([]byte(nil), data...)
	sealedByte[len(sealedByte)-10] ^= 1
	nameAt := bytes.Index(data, []byte("plain-name"))
	headerByte := append([]byte(nil), data...)
	headerByte[nameAt] ^= 1
	for name, bad := range map[string][]byte{"records": fixCRC(sealedByte), "header": fixCRC(headerByte)} {
		d := newCache()
		_ = d.Set("previous", "x")
		if _, err := d.Load(bytes.NewReader(bad)); !errors.Is(err, ErrSnapshotCorrupt) {
			t.Fatalf("tampered %s: err = %v, want ErrSnapshotCorrupt", name, err)
		}
		if _, ok := d.Get("previous"); !ok {
			t.Fatalf("tampered %s: cache changed", name)
		}
		_ = d.Close()
	}
}

func TestRefreshKeepsTags(t *testing.T) {
	loader := LoaderFunc[string, int](func(context.Context, string) (int, time.Duration, error) {
		return 2, 0, nil
	})
	c, _ := New[string, int](WithMaxEntries(10), WithLoader(loader))
	defer c.Close()
	_ = c.SetWithTags("k", 1, "t")
	if err := c.Refresh(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if err := eventually(func() bool { v, _ := c.Peek("k"); return v == 2 }); err != nil {
		t.Fatal(err)
	}
	if n := c.InvalidateTag("t"); n != 1 {
		t.Fatalf("InvalidateTag removed %d after a refresh, want 1", n)
	}
}

// yieldStore yields inside Set, widening the gap between the in-memory
// write and the Store write.
type yieldStore struct {
	*MemoryStore[string, int]
}

func (y yieldStore) Set(ctx context.Context, key string, value int, ttl time.Duration) error {
	runtime.Gosched()
	return y.MemoryStore.Set(ctx, key, value, ttl)
}

func TestConcurrentWritesReachMemoryAndStoreInOneOrder(t *testing.T) {
	ms := NewMemoryStore[string, int](nil)
	c, _ := New[string, int](WithMaxEntries(100), WithStore(yieldStore{ms}))
	defer c.Close()
	ctx := context.Background()
	writes := []func(int){
		func(v int) { _ = c.Set("k", v) },
		func(v int) { _ = c.SetWithTTL("k", v, time.Hour) },
		func(v int) { _ = c.SetWithOptions("k", v) },
		func(v int) {
			_, _ = c.Compute("k", func(int, bool) (int, ComputeAction, error) { return v, ComputeStore, nil })
		},
		func(int) { c.Delete("k") },
	}
	for round := range 2000 {
		var wg sync.WaitGroup
		for w := range 4 {
			wg.Go(func() { writes[(round+w)%len(writes)](round*10 + w) })
		}
		wg.Wait()
		mv, mok := c.Peek("k")
		sv, sok, _ := ms.Get(ctx, "k")
		if mok != sok || mv != sv {
			t.Fatalf("round %d: memory (%d, %v) and Store (%d, %v) disagree", round, mv, mok, sv, sok)
		}
	}
}

func TestTieredGetDoesNotRefillAfterInvalidation(t *testing.T) {
	cases := []struct {
		name       string
		invalidate func(tc *Tiered[string, int])
		want       int // 0 means absent
	}{
		{"Delete", func(tc *Tiered[string, int]) { tc.Delete("k") }, 0},
		{"InvalidateTag", func(tc *Tiered[string, int]) { tc.InvalidateTag("t") }, 0},
		{"Set", func(tc *Tiered[string, int]) { _ = tc.Set("k", 2) }, 2},
	}
	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			hold := make(chan struct{})
			reading := make(chan struct{})
			var once sync.Once
			l1, _ := New[string, int](WithMaxEntries(10))
			l2, _ := New[string, int](WithMaxEntries(10), WithPolicy(PolicyFIFO), WithOnHit(func(string, int) {
				once.Do(func() { close(reading); <-hold })
			}))
			tc := NewTiered(l1, l2)
			defer tc.Close()
			_ = l2.SetWithTags("k", 1, "t")

			done := make(chan struct{})
			go func() { _, _ = tc.Get("k"); close(done) }() // reads 1 from L2, then waits
			<-reading
			tcase.invalidate(tc)
			close(hold)
			<-done

			v, ok := l1.Peek("k")
			if tcase.want == 0 && ok {
				t.Fatalf("L1 holds %d after the invalidation", v)
			}
			if tcase.want != 0 && v != tcase.want {
				t.Fatalf("L1 = %d, %v; want %d", v, ok, tcase.want)
			}
		})
	}
}

func TestHookMayWriteItsOwnKeyWithAStore(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			ms := NewMemoryStore[string, int](nil)
			var c *Cache[string, int]
			opts := []Option{
				WithMaxEntries(100), WithStore(ms),
				WithOnEvict(func(k string, v int, r EvictionReason) {
					if v < 100 && (r == EvictReasonDeleted || r == EvictReasonComputed) {
						_ = c.Set(k, v+100) // writes the key whose removal fired it
					}
				}),
			}
			if async {
				opts = append(opts, WithAsyncWrites())
			}
			c, _ = New[string, int](opts...)
			defer c.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = c.Set("a", 1)
				_ = c.Sync(context.Background())
				c.Delete("a")
				_ = c.Set("b", 2)
				_ = c.Sync(context.Background())
				_, _ = c.Compute("b", func(int, bool) (int, ComputeAction, error) { return 0, ComputeDelete, nil })
				_ = c.Sync(context.Background())
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("deadlock: a hook writing its own key blocked")
			}
			for k, want := range map[string]int{"a": 101, "b": 102} {
				mv, _ := c.Peek(k)
				sv, _, _ := ms.Get(context.Background(), k)
				if mv != want || sv != want {
					t.Errorf("%s: memory %d, Store %d; want %d in both", k, mv, sv, want)
				}
			}
		})
	}
}
