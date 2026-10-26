package memcache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTieredDoesNotPromoteAStaleL2Value(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_000_000, 0))
	loader := LoaderFunc[string, string](func(context.Context, string) (string, time.Duration, error) {
		return "fresh", 0, nil
	})
	l1, _ := New[string, string](WithMaxEntries(100), WithClock(clk))
	l2, _ := New[string, string](WithMaxEntries(100), WithClock(clk), WithTTLJitter(0),
		WithLoader(loader), WithStaleWhileRevalidate(time.Minute))
	tr := NewTiered(l1, l2)
	defer tr.Close()
	_ = l2.SetWithTTL("k", "old", time.Second)
	clk.Advance(2 * time.Second) // expired in L2, inside its SWR window
	if v, ok := tr.Get("k"); !ok || v != "old" {
		t.Fatalf("Get = %q, %v; want the stale value", v, ok)
	}
	if _, ok := l1.Peek("k"); ok {
		t.Fatal("a value L2 served stale was copied into L1")
	}
	if err := l2.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if v, _ := tr.Get("k"); v == "fresh" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Tiered never served L2's refreshed value")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConcurrentTieredSetsLeaveTiersEqual(t *testing.T) {
	l1, _ := New[int, int](WithMaxEntries(100_000))
	l2, _ := New[int, int](WithMaxEntries(100_000))
	tr := NewTiered(l1, l2)
	defer tr.Close()
	const keys = 2000
	for round := range 5 {
		var wg sync.WaitGroup
		for w := range 2 {
			wg.Go(func() {
				for k := range keys {
					_ = tr.Set(k, round*10+w)
				}
			})
		}
		wg.Wait()
		for k := range keys {
			v1, _ := l1.Peek(k)
			v2, _ := l2.Peek(k)
			if v1 != v2 {
				t.Fatalf("round %d key %d: L1=%d L2=%d", round, k, v1, v2)
			}
		}
	}
}

func TestBulkLoadDoesNotOverwriteANewerWrite(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	bulk := bulkLoaderFunc[string, string](func(_ context.Context, keys []string) (map[string]LoadResult[string], error) {
		close(started)
		<-release
		out := make(map[string]LoadResult[string], len(keys))
		for _, k := range keys {
			out[k] = LoadResult[string]{Value: "stale"}
		}
		return out, nil
	})
	c, _ := New[string, string](WithMaxEntries(100), WithBulkLoader(bulk))
	defer c.Close()
	done := make(chan map[string]string)
	go func() {
		got, _ := c.GetMultiOrLoad(context.Background(), []string{"set", "deleted", "untouched"})
		done <- got
	}()
	<-started
	_ = c.Set("set", "fresh")
	_ = c.Set("deleted", "x")
	c.Delete("deleted")
	close(release)
	if got := <-done; got["set"] != "stale" {
		t.Fatalf("the caller should still see what it loaded: %v", got)
	}
	if v, _ := c.Get("set"); v != "fresh" {
		t.Fatalf("set = %q; the bulk load overwrote a newer Set", v)
	}
	if _, ok := c.Get("deleted"); ok {
		t.Fatal("the bulk load brought a deleted key back")
	}
	if v, _ := c.Get("untouched"); v != "stale" {
		t.Fatalf("untouched = %q; want the loaded value cached", v)
	}
}

func TestClearKeepsTagsOfAConcurrentSet(t *testing.T) {
	for _, method := range []string{"Clear", "Reset"} {
		t.Run(method, func(t *testing.T) {
			lost := 0
			for r := range 100 {
				c, _ := New[string, int](WithMaxEntries(100_000), WithShards(64))
				for i := range 5000 {
					_ = c.Set(fmt.Sprint("fill", i), i)
				}
				var wg sync.WaitGroup
				wg.Go(func() {
					if method == "Clear" {
						c.Clear()
					} else {
						c.Reset()
					}
				})
				wg.Go(func() {
					time.Sleep(time.Duration(r%20) * 25 * time.Microsecond)
					_ = c.SetWithTags("k", 1, "t")
				})
				wg.Wait()
				_ = c.Sync(context.Background())
				if _, ok := c.Peek("k"); ok {
					c.InvalidateTag("t")
					if _, still := c.Peek("k"); still {
						lost++
					}
				}
				_ = c.Close()
			}
			if lost > 0 {
				t.Fatalf("InvalidateTag missed a key tagged during %s in %d of 100 rounds", method, lost)
			}
		})
	}
}

func TestAsyncWriteRefusedOnceClosed(t *testing.T) {
	st := NewMemoryStore[string, int](nil)
	c, _ := New[string, int](WithMaxEntries(10), WithAsyncWrites(), WithStore(st))
	_ = c.Close()
	// A write that passed Set's closed check just before Close.
	s := c.shardFor("k")
	if err := c.enqueueAsyncSet(s, "k", 1, 1, 0, false, 0, nil, nil); err != ErrClosed {
		t.Fatalf("enqueue after Close = %v, want ErrClosed", err)
	}
	if err := c.enqueueAsyncDelete(s, "k"); err != ErrClosed {
		t.Fatalf("delete enqueue after Close = %v, want ErrClosed", err)
	}
	if len(s.pending) != 0 || c.asyncBacklog() != 0 {
		t.Fatal("a write was queued after Close's final drain")
	}
}

func TestAsyncSetsRacingCloseReachTheStore(t *testing.T) {
	for range 50 {
		st := NewMemoryStore[int, int](nil)
		c, _ := New[int, int](WithMaxEntries(100_000), WithAsyncWrites(), WithStore(st))
		var accepted sync.Map
		var n atomic.Int64
		var wg sync.WaitGroup
		for g := range 4 {
			wg.Go(func() {
				for i := range 2000 {
					k := g*10_000 + i
					if c.Set(k, 1) != nil {
						return
					}
					accepted.Store(k, true)
					n.Add(1)
				}
			})
		}
		for n.Load() < 100 {
			time.Sleep(10 * time.Microsecond)
		}
		_ = c.Close()
		wg.Wait()
		accepted.Range(func(k, _ any) bool {
			if _, ok, _ := st.Get(context.Background(), k.(int)); !ok {
				t.Fatalf("Set(%d) returned nil but never reached the Store", k)
			}
			return true
		})
	}
}

func TestMaxEntriesHoldsOnceConcurrentWritersReturn(t *testing.T) {
	for _, maxEntries := range []int{1, 4, 16} {
		for round := range 20 {
			c, _ := New[int, int](WithMaxEntries(maxEntries), WithShards(32))
			var wg sync.WaitGroup
			for g := range 16 {
				wg.Go(func() {
					for i := range 1000 {
						_ = c.Set(g*1_000_000+round*10_000+i, i)
					}
				})
			}
			wg.Wait()
			if n := c.Len(); n > maxEntries {
				t.Fatalf("WithMaxEntries(%d): Len = %d once every writer returned", maxEntries, n)
			}
			_ = c.Close()
		}
	}
}

// panicStore is a MemoryStore whose Set and Delete panic on demand.
type panicStore struct {
	*MemoryStore[string, int]
	panicSet, panicDel *atomic.Bool
}

func (p panicStore) Set(ctx context.Context, k string, v int, ttl time.Duration) error {
	if p.panicSet.Load() {
		panic("store Set")
	}
	return p.MemoryStore.Set(ctx, k, v, ttl)
}

func (p panicStore) Delete(ctx context.Context, k string) (bool, error) {
	if p.panicDel.Load() {
		panic("store Delete")
	}
	return p.MemoryStore.Delete(ctx, k)
}

// User code that panics while the cache holds a lock, recovered by the
// caller as net/http recovers a handler, leaves the cache usable.
func TestRecoveredPanicsInUserCodeReleaseLocks(t *testing.T) {
	type knobs struct {
		weigh, storeSet, storeDel atomic.Bool
	}
	cases := map[string]struct {
		storeOnly bool
		op        func(c *Cache[string, int], kn *knobs)
	}{
		"DeleteIf predicate": {op: func(c *Cache[string, int], _ *knobs) {
			c.DeleteIf("k", func(int) bool { panic("pred") })
		}},
		"Compute fn": {op: func(c *Cache[string, int], _ *knobs) {
			_, _ = c.Compute("k", func(int, bool) (int, ComputeAction, error) { panic("fn") })
		}},
		"Set weigher": {op: func(c *Cache[string, int], kn *knobs) {
			kn.weigh.Store(true)
			_ = c.Set("k", 2)
		}},
		"SetWithOptions weigher": {op: func(c *Cache[string, int], kn *knobs) {
			kn.weigh.Store(true)
			_ = c.SetWithOptions("k", 2)
		}},
		"CompareAndSwap weigher": {op: func(c *Cache[string, int], kn *knobs) {
			kn.weigh.Store(true)
			c.CompareAndSwap("k", 1, 2)
		}},
		"GetMultiOrLoad weigher": {op: func(c *Cache[string, int], kn *knobs) {
			c.Delete("k")
			kn.weigh.Store(true)
			_, _ = c.GetMultiOrLoad(context.Background(), []string{"k"})
		}},
		"Store Set": {storeOnly: true, op: func(c *Cache[string, int], kn *knobs) {
			kn.storeSet.Store(true)
			_ = c.Set("k", 2)
		}},
		"Store Set from SetWithOptions": {storeOnly: true, op: func(c *Cache[string, int], kn *knobs) {
			kn.storeSet.Store(true)
			_ = c.SetWithOptions("k", 2)
		}},
		"Store Delete": {storeOnly: true, op: func(c *Cache[string, int], kn *knobs) {
			kn.storeDel.Store(true)
			c.Delete("k")
		}},
		"Store Delete from DeleteIf": {storeOnly: true, op: func(c *Cache[string, int], kn *knobs) {
			kn.storeDel.Store(true)
			c.DeleteIf("k", func(int) bool { return true })
		}},
	}
	for name, tc := range cases {
		for _, withStore := range []bool{false, true} {
			if tc.storeOnly && !withStore {
				continue
			}
			t.Run(fmt.Sprint(name, "/store=", withStore), func(t *testing.T) {
				var kn knobs
				bulk := bulkLoaderFunc[string, int](func(_ context.Context, keys []string) (map[string]LoadResult[int], error) {
					out := make(map[string]LoadResult[int], len(keys))
					for _, k := range keys {
						out[k] = LoadResult[int]{Value: 3}
					}
					return out, nil
				})
				opts := []Option{WithMaxEntries(10), WithShards(1), WithBulkLoader(bulk),
					WithWeigher(func(int) int64 {
						if kn.weigh.Load() {
							panic("weigher")
						}
						return 1
					})}
				if withStore {
					opts = append(opts, WithStore[string, int](panicStore{
						NewMemoryStore[string, int](nil), &kn.storeSet, &kn.storeDel,
					}))
				}
				// Not closed on failure: Close would wait on the held lock.
				c, _ := New[string, int](opts...)
				_ = c.Set("k", 1)
				finishes(t, "the panicking call", func() {
					defer func() {
						if recover() == nil {
							t.Error("the user code didn't panic")
						}
					}()
					tc.op(c, &kn)
				})
				kn.weigh.Store(false)
				kn.storeSet.Store(false)
				kn.storeDel.Store(false)
				finishes(t, "using the cache after the panic", func() {
					c.Get("k")
					c.Get("other")
					_ = c.Set("k", 5)
					c.Delete("k")
				})
				_ = c.Close()
			})
		}
	}
}

// readPanicStore is a MemoryStore whose Get and TTL panic on demand.
type readPanicStore struct {
	*MemoryStore[string, int]
	panicGet, panicTTL *atomic.Bool
}

func (p readPanicStore) Get(ctx context.Context, k string) (int, bool, error) {
	if p.panicGet.Load() {
		panic("store Get")
	}
	return p.MemoryStore.Get(ctx, k)
}

func (p readPanicStore) TTL(ctx context.Context, k string) (time.Duration, bool, error) {
	if p.panicTTL.Load() {
		panic("store TTL")
	}
	return p.MemoryStore.TTL(ctx, k)
}

// A conditional write reads a key only the Store holds with the shard
// unlocked; a panic from the Store or the Weigher there, recovered by the
// caller, leaves the shard usable.
func TestRecoveredPanicsWhileReadingTheStoreReleaseLocks(t *testing.T) {
	ops := map[string]func(c *Cache[string, int]){
		"Compute": func(c *Cache[string, int]) {
			_, _ = c.Compute("s", func(v int, _ bool) (int, ComputeAction, error) { return v + 1, ComputeStore, nil })
		},
		"ComputeIfAbsent": func(c *Cache[string, int]) {
			_, _, _ = c.ComputeIfAbsent("s", func() (int, time.Duration, error) { return 1, 0, nil })
		},
		"ComputeIfPresent": func(c *Cache[string, int]) {
			_, _ = c.ComputeIfPresent("s", func(v int) (int, ComputeAction, error) { return v + 1, ComputeStore, nil })
		},
		"Update":         func(c *Cache[string, int]) { _, _ = c.Update("s", func(v int) int { return v + 1 }) },
		"CompareAndSwap": func(c *Cache[string, int]) { c.CompareAndSwap("s", 1, 2) },
		"Increment":      func(c *Cache[string, int]) { _, _ = Increment(c, "s") },
		"SetIfAbsent":    func(c *Cache[string, int]) { _, _ = c.SetIfAbsent("s", 2) },
		"SetIfPresent":   func(c *Cache[string, int]) { _, _ = c.SetIfPresent("s", 2) },
		"GetOrSet":       func(c *Cache[string, int]) { _, _, _ = c.GetOrSet("s", 2) },
		"PeekOrAdd":      func(c *Cache[string, int]) { _, _, _ = c.PeekOrAdd("s", 2) },
		"DeleteIf":       func(c *Cache[string, int]) { c.DeleteIf("s", func(int) bool { return true }) },
	}
	for _, source := range []string{"Get", "TTL", "Weigher"} {
		for name, op := range ops {
			t.Run(source+"/"+name, func(t *testing.T) {
				var panicGet, panicTTL, panicWeigh atomic.Bool
				st := NewMemoryStore[string, int](nil)
				c, _ := New[string, int](WithMaxEntries(10), WithShards(1),
					WithStore[string, int](readPanicStore{st, &panicGet, &panicTTL}),
					WithWeigher(func(int) int64 {
						if panicWeigh.Load() {
							panic("weigher")
						}
						return 1
					}))
				_ = st.Set(context.Background(), "s", 1, time.Hour)
				map[string]*atomic.Bool{"Get": &panicGet, "TTL": &panicTTL, "Weigher": &panicWeigh}[source].Store(true)
				finishes(t, "the panicking call", func() {
					defer func() {
						if recover() == nil {
							t.Error("nothing panicked")
						}
					}()
					op(c)
				})
				panicGet.Store(false)
				panicTTL.Store(false)
				panicWeigh.Store(false)
				finishes(t, "using the shard after the panic", func() {
					c.Get("s")
					_ = c.Set("x", 5)
					op(c)
					c.Delete("x")
				})
				_ = c.Close()
			})
		}
	}
}

var prefixPanics atomic.Bool

// panicPrefixKey is a Prefixer key whose HasPrefix panics on demand.
type panicPrefixKey string

func (k panicPrefixKey) HasPrefix(p string) bool {
	if prefixPanics.Load() {
		panic("HasPrefix")
	}
	return len(k) >= len(p) && string(k[:len(p)]) == p
}

func TestRecoveredDeletePrefixPanicReleasesLocks(t *testing.T) {
	for _, withStore := range []bool{false, true} {
		opts := []Option{WithMaxEntries(10), WithShards(1)}
		if withStore {
			opts = append(opts, WithStore[panicPrefixKey, int](NewMemoryStore[panicPrefixKey, int](nil)))
		}
		c, _ := New[panicPrefixKey, int](opts...)
		_ = c.Set("ab", 1)
		func() {
			defer func() { _ = recover() }()
			prefixPanics.Store(true)
			defer prefixPanics.Store(false)
			c.DeletePrefix("a")
		}()
		finishes(t, fmt.Sprint("using the cache after the panic, store=", withStore), func() {
			c.Get("ab")
			_ = c.Set("ab", 2)
			if n := c.DeletePrefix("a"); n != 1 {
				t.Errorf("DeletePrefix = %d, want 1", n)
			}
		})
		_ = c.Close()
	}
}

// Loaded values are weighed before the shard lock is taken, so a Weigher
// may read the cache.
func TestLoadWeigherMayReadTheCache(t *testing.T) {
	var cp atomic.Pointer[Cache[string, int]]
	loader := LoaderFunc[string, int](func(context.Context, string) (int, time.Duration, error) { return 1, 0, nil })
	c, _ := New[string, int](WithMaxEntries(10), WithShards(1), WithLoader(loader), WithBulkLoader(constBulk(2)),
		WithWeigher(func(int) int64 {
			if c := cp.Load(); c != nil {
				c.Get("other")
			}
			return 1
		}))
	cp.Store(c)
	finishes(t, "loads with a Weigher that reads the cache", func() {
		if _, err := c.GetMultiOrLoad(context.Background(), []string{"a", "b"}); err != nil {
			t.Error(err)
		}
		if _, err := c.GetOrLoad(context.Background(), "c"); err != nil {
			t.Error(err)
		}
	})
	if v, ok := c.Peek("a"); !ok || v != 2 {
		t.Fatalf("bulk-loaded a = %d, %v", v, ok)
	}
	_ = c.Close()
}
