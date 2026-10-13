package memcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// storeOnly returns a cache whose Store holds k=57 while memory is empty,
// as after an eviction.
func storeOnly(t *testing.T, opts ...Option) (*Cache[string, int], *MemoryStore[string, int]) {
	t.Helper()
	st := NewMemoryStore[string, int](nil)
	if err := st.Set(context.Background(), "k", 57, 0); err != nil {
		t.Fatal(err)
	}
	c, err := New[string, int](append(opts, WithMaxEntries(100), WithStore(st))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, st
}

func storeValue(t *testing.T, st Store[string, int]) (int, bool) {
	t.Helper()
	v, ok, err := st.Get(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	return v, ok
}

func TestConditionalWritesSeeTheStoreValue(t *testing.T) {
	type result struct {
		v  int
		ok bool
	}
	for _, tc := range []struct {
		name      string
		op        func(c *Cache[string, int]) (result, error)
		want      result
		wantStore int // -1: absent
	}{
		{"Increment", func(c *Cache[string, int]) (result, error) {
			v, err := Increment(c, "k")
			return result{v, true}, err
		}, result{58, true}, 58},
		{"SetIfAbsent", func(c *Cache[string, int]) (result, error) {
			ok, err := c.SetIfAbsent("k", 1)
			return result{0, ok}, err
		}, result{0, false}, 57},
		{"SetIfPresent", func(c *Cache[string, int]) (result, error) {
			ok, err := c.SetIfPresent("k", 9)
			return result{0, ok}, err
		}, result{0, true}, 9},
		{"GetOrSet", func(c *Cache[string, int]) (result, error) {
			v, hit, err := c.GetOrSet("k", 1)
			return result{v, hit}, err
		}, result{57, true}, 57},
		{"PeekOrAdd", func(c *Cache[string, int]) (result, error) {
			v, hit, err := c.PeekOrAdd("k", 1)
			return result{v, hit}, err
		}, result{57, true}, 57},
		{"Compute", func(c *Cache[string, int]) (result, error) {
			var saw result
			_, err := c.Compute("k", func(cur int, ok bool) (int, ComputeAction, error) {
				saw = result{cur, ok}
				return cur + 100, ComputeStore, nil
			})
			return saw, err
		}, result{57, true}, 157},
		{"Compute delete", func(c *Cache[string, int]) (result, error) {
			_, err := c.Compute("k", func(cur int, ok bool) (int, ComputeAction, error) {
				return 0, ComputeDelete, nil
			})
			return result{}, err
		}, result{}, -1},
		{"ComputeIfAbsent", func(c *Cache[string, int]) (result, error) {
			v, computed, err := c.ComputeIfAbsent("k", func() (int, time.Duration, error) { return 1, 0, nil })
			return result{v, computed}, err
		}, result{57, false}, 57},
		{"ComputeIfPresent", func(c *Cache[string, int]) (result, error) {
			v, err := c.ComputeIfPresent("k", func(cur int) (int, ComputeAction, error) { return cur * 2, ComputeStore, nil })
			return result{v, true}, err
		}, result{114, true}, 114},
		{"Update", func(c *Cache[string, int]) (result, error) {
			v, err := c.Update("k", func(cur int) int { return cur + 1 })
			return result{v, true}, err
		}, result{58, true}, 58},
		{"CompareAndSwap", func(c *Cache[string, int]) (result, error) {
			return result{0, c.CompareAndSwap("k", 57, 60)}, nil
		}, result{0, true}, 60},
		{"DeleteIf", func(c *Cache[string, int]) (result, error) {
			return result{0, c.DeleteIf("k", func(v int) bool { return v == 57 })}, nil
		}, result{0, true}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, st := storeOnly(t)
			got, err := tc.op(c)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("result = %+v, want %+v", got, tc.want)
			}
			sv, ok := storeValue(t, st)
			switch {
			case tc.wantStore < 0 && ok:
				t.Errorf("Store still holds %d", sv)
			case tc.wantStore >= 0 && (!ok || sv != tc.wantStore):
				t.Errorf("Store = %d, %v; want %d", sv, ok, tc.wantStore)
			}
			if v, ok := c.Get("k"); tc.wantStore >= 0 && (!ok || v != tc.wantStore) {
				t.Errorf("Get = %d, %v; want %d", v, ok, tc.wantStore)
			}
		})
	}
}

func TestConditionalWriteReturnsStoreReadError(t *testing.T) {
	ts := newTrackingStore(NewMemoryStore[string, int](nil))
	_ = ts.inner.Set(context.Background(), "k", 57, 0)
	c, _ := New[string, int](WithMaxEntries(10), WithStore(ts))
	defer c.Close()
	boom := errors.New("store down")
	ts.failNextGet.Store(&boom)
	called := false
	_, err := c.Compute("k", func(cur int, _ bool) (int, ComputeAction, error) {
		called = true
		return cur + 1, ComputeStore, nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the Store's error", err)
	}
	if called {
		t.Fatal("fn ran without the Store's value")
	}
	if v, _ := storeValue(t, ts.inner); v != 57 {
		t.Fatalf("Store = %d after a failed read, want 57", v)
	}
}

func TestConditionalWriteTrustsQueuedAsyncOpOverStore(t *testing.T) {
	c, st := storeOnly(t, WithAsyncWrites())
	c.Delete("k") // queued; the Store still holds 57 until it is applied
	stored, err := c.SetIfAbsent("k", 1)
	if err != nil || !stored {
		t.Fatalf("SetIfAbsent after a queued Delete = %v, %v; want true", stored, err)
	}
	if v, ok := storeValue(t, st); !ok || v != 1 {
		t.Fatalf("Store = %d, %v; want 1", v, ok)
	}
}

// gateStore reads its inner store, then blocks until released, so a
// test can act while a Store read is in flight.
type gateStore struct {
	*MemoryStore[string, string]
	gets    atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func newGateStore() *gateStore {
	return &gateStore{
		MemoryStore: NewMemoryStore[string, string](nil),
		entered:     make(chan struct{}, 100),
		release:     make(chan struct{}),
	}
}

func (g *gateStore) Get(ctx context.Context, key string) (string, bool, error) {
	v, ok, err := g.MemoryStore.Get(ctx, key)
	g.gets.Add(1)
	g.entered <- struct{}{}
	select {
	case <-g.release:
	case <-ctx.Done():
		return "", false, ctx.Err()
	}
	return v, ok, err
}

func TestReadThroughSharesOneStoreRead(t *testing.T) {
	gs := newGateStore()
	_ = gs.MemoryStore.Set(context.Background(), "k", "v", 0)
	c, _ := New[string, string](WithMaxEntries(10), WithStore(gs))
	defer c.Close()
	const readers = 50
	var wg sync.WaitGroup
	var hits atomic.Int32
	for range readers {
		wg.Go(func() {
			if v, ok := c.Get("k"); ok && v == "v" {
				hits.Add(1)
			}
		})
	}
	<-gs.entered
	time.Sleep(50 * time.Millisecond) // let the other readers queue up
	close(gs.release)
	wg.Wait()
	if hits.Load() != readers {
		t.Fatalf("%d of %d readers got the value", hits.Load(), readers)
	}
	if n := gs.gets.Load(); n > 5 {
		t.Fatalf("%d concurrent reads of one key made %d Store reads; want them shared", readers, n)
	}
}

func TestReadThroughWaitHonoursContext(t *testing.T) {
	gs := newGateStore()
	defer close(gs.release)
	c, _ := New[string, string](WithMaxEntries(10), WithStore(gs))
	defer c.Close()
	go c.Get("k")
	<-gs.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := c.GetCtx(ctx, "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("GetCtx waited %v past its 20ms deadline", d)
	}
}

func TestReadThroughRetriesWhenTheLeaderGivesUp(t *testing.T) {
	gs := newGateStore()
	_ = gs.MemoryStore.Set(context.Background(), "k", "v", 0)
	c, _ := New[string, string](WithMaxEntries(10), WithStore(gs))
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() {
		_, _, err := c.GetCtx(ctx, "k")
		leader <- err
	}()
	<-gs.entered
	follower := make(chan string, 1)
	go func() {
		v, _ := c.Get("k")
		follower <- v
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-leader; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader err = %v", err)
	}
	<-gs.entered // the follower reads again under its own context
	close(gs.release)
	if v := <-follower; v != "v" {
		t.Fatalf("follower got %q, want the Store's value", v)
	}
}

func TestReadThroughDoesNotPromoteOverANewerWrite(t *testing.T) {
	for name, write := range map[string]func(c *Cache[string, string]){
		"Set":    func(c *Cache[string, string]) { _ = c.Set("k", "new") },
		"Delete": func(c *Cache[string, string]) { c.Delete("k") },
	} {
		t.Run(name, func(t *testing.T) {
			gs := newGateStore()
			_ = gs.MemoryStore.Set(context.Background(), "k", "old", 0)
			c, _ := New[string, string](WithMaxEntries(10), WithStore(gs))
			defer c.Close()
			done := make(chan struct{})
			go func() { c.Get("k"); close(done) }()
			<-gs.entered // the read has the old value
			write(c)
			close(gs.release)
			<-done
			v, ok := c.Peek("k")
			if name == "Set" && v != "new" || name == "Delete" && ok {
				t.Fatalf("after the write, memory holds %q, %v", v, ok)
			}
		})
	}
}

func TestDeleteWhereAndPrefixDeleteFromTheStore(t *testing.T) {
	for name, del := range map[string]func(c *Cache[string, int]) int{
		"DeleteWhere":  func(c *Cache[string, int]) int { return c.DeleteWhere(func(string, int) bool { return true }) },
		"DeletePrefix": func(c *Cache[string, int]) int { return c.DeletePrefix("k") },
	} {
		t.Run(name, func(t *testing.T) {
			c, st := storeOnly(t)
			_ = c.Set("k", 1)
			if n := del(c); n != 1 {
				t.Fatalf("removed %d, want 1", n)
			}
			if _, ok := storeValue(t, st); ok {
				t.Fatal("the Store still holds the key")
			}
			if _, ok := c.Get("k"); ok {
				t.Fatal("the deleted key came back from the Store")
			}
			if c.Stats().Deletes != 0 {
				t.Fatalf("Deletes = %d; bulk deletes don't count", c.Stats().Deletes)
			}
		})
	}
}

func TestSetExpireAtInThePastDeletesFromTheStore(t *testing.T) {
	for name, opts := range map[string][]Option{"sync": nil, "async": {WithAsyncWrites()}} {
		t.Run(name, func(t *testing.T) {
			c, st := storeOnly(t, opts...)
			if err := c.SetWithOptions("k", 1, SetExpireAt(time.Now().Add(-time.Second))); err != nil {
				t.Fatal(err)
			}
			_ = c.Sync(context.Background())
			if v, ok := storeValue(t, st); ok {
				t.Fatalf("Store holds %d with no expiry", v)
			}
			if _, ok := c.Get("k"); ok {
				t.Fatal("an entry set to expire in the past is served")
			}
		})
	}
}

func TestStoreTTLUsesTheCacheClock(t *testing.T) {
	clk := NewFakeClock(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	st := NewMemoryStore[string, int](clk)
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithStore(st))
	defer c.Close()
	if err := c.SetWithOptions("k", 1, SetExpireAt(clk.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, ok := storeValue(t, st); !ok {
		t.Fatal("the Store lacks a key that expires in an hour")
	}
	clk.Advance(2 * time.Hour)
	if _, ok := storeValue(t, st); ok {
		t.Fatal("the Store keeps the key past its expiry")
	}
}

func TestTieredPromotesAStoreReadOfL2(t *testing.T) {
	st := NewMemoryStore[string, string](nil)
	_ = st.Set(context.Background(), "k", "v", 0)
	l1, _ := New[string, string](WithMaxEntries(10))
	l2, _ := New[string, string](WithMaxEntries(10), WithStore(st))
	tr := NewTiered(l1, l2)
	defer tr.Close()
	if v, ok := tr.Get("k"); !ok || v != "v" {
		t.Fatalf("Get = %q, %v", v, ok)
	}
	if _, ok := l1.Peek("k"); !ok || tr.Stats().Promotions != 1 {
		t.Fatalf("the read from L2's Store wasn't copied into L1 (promotions=%d)", tr.Stats().Promotions)
	}
}

func TestLoadsDoNotWriteTheStore(t *testing.T) {
	ts := newTrackingStore(NewMemoryStore[string, int](nil))
	loader := LoaderFunc[string, int](func(context.Context, string) (int, time.Duration, error) { return 1, 0, nil })
	bulk := bulkLoaderFunc[string, int](func(_ context.Context, keys []string) (map[string]LoadResult[int], error) {
		out := make(map[string]LoadResult[int], len(keys))
		for _, k := range keys {
			out[k] = LoadResult[int]{Value: 2}
		}
		return out, nil
	})
	c, _ := New[string, int](WithMaxEntries(10), WithStore(ts),
		WithLoader(loader), WithBulkLoader(bulk))
	defer c.Close()
	if _, err := c.GetOrLoad(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetMultiOrLoad(context.Background(), []string{"b", "c"}); err != nil {
		t.Fatal(err)
	}
	if n := ts.setCalls.Load(); n != 0 {
		t.Fatalf("loads wrote the Store %d times", n)
	}
	if v, ok := c.Peek("b"); !ok || v != 2 {
		t.Fatalf("bulk-loaded value not cached: %d, %v", v, ok)
	}
}
