package memcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
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

func TestGetOrLoadDoesNotWriteTheStore(t *testing.T) {
	ts := newTrackingStore(NewMemoryStore[string, int](nil))
	loader := LoaderFunc[string, int](func(context.Context, string) (int, time.Duration, error) { return 1, 0, nil })
	c, _ := New[string, int](WithMaxEntries(10), WithStore(ts), WithLoader(loader))
	defer c.Close()
	if _, err := c.GetOrLoad(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if n := ts.setCalls.Load(); n != 0 {
		t.Fatalf("GetOrLoad wrote the Store %d times", n)
	}
}

func constBulk(v int) BulkLoader[string, int] {
	return bulkLoaderFunc[string, int](func(_ context.Context, keys []string) (map[string]LoadResult[int], error) {
		out := make(map[string]LoadResult[int], len(keys))
		for _, k := range keys {
			out[k] = LoadResult[int]{Value: v, TTL: time.Minute}
		}
		return out, nil
	})
}

func TestBulkLoadsWriteThroughToTheStore(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_000_000, 0))
	st := NewMemoryStore[string, int](clk)
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithStore(st), WithBulkLoader(constBulk(2)))
	defer c.Close()
	if _, err := c.GetMultiOrLoad(context.Background(), []string{"b", "c"}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"b", "c"} {
		v, ttl, ok, err := recordTTL(st, k)
		if err != nil || !ok || v != 2 || ttl != time.Minute {
			t.Fatalf("Store %s = %d, %v, %v, %v; want 2 with the result's 1m TTL", k, v, ttl, ok, err)
		}
	}
	c.Clear()
	if v, ok := c.Get("b"); !ok || v != 2 {
		t.Fatalf("Get after Clear = %d, %v; want the Store's copy", v, ok)
	}
}

// Bulk loads racing Sets and Deletes of the same keys: memory and the
// Store must agree afterwards for every key.
func TestBulkLoadWriteThroughKeepsOrder(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprint("async=", async), func(t *testing.T) {
			for round := range 20 {
				st := NewMemoryStore[string, int](nil)
				opts := []Option{WithMaxEntries(1000), WithStore(st), WithBulkLoader(constBulk(-1))}
				if async {
					opts = append(opts, WithAsyncWrites())
				}
				c, _ := New[string, int](opts...)
				keys := []string{"a", "b", "c", "d", "e", "f"}
				var wg sync.WaitGroup
				for range 4 {
					wg.Go(func() {
						for range 200 {
							_, _ = c.GetMultiOrLoad(context.Background(), keys)
						}
					})
				}
				for range 2 {
					wg.Go(func() {
						for i := range 200 {
							k := keys[rand.IntN(len(keys))]
							if rand.IntN(2) == 0 {
								_ = c.Set(k, i)
							} else {
								c.Delete(k)
							}
						}
					})
				}
				wg.Wait()
				_ = c.Sync(context.Background())
				for _, k := range keys {
					mv, mok := c.Peek(k)
					sv, sok, _ := st.Get(context.Background(), k)
					if mok && (!sok || mv != sv) {
						t.Fatalf("round %d key %s: memory=%d store=%d,%v", round, k, mv, sv, sok)
					}
				}
				_ = c.Close()
			}
		})
	}
}

// A Set that lands while GetMultiOrLoad is still looking the key up
// (here, in a slow Store read) is newer than the bulk load that follows.
func TestBulkLoadDoesNotOverwriteAWriteDuringTheLookup(t *testing.T) {
	gs := newGateStore()
	bulk := bulkLoaderFunc[string, string](func(_ context.Context, keys []string) (map[string]LoadResult[string], error) {
		out := make(map[string]LoadResult[string], len(keys))
		for _, k := range keys {
			out[k] = LoadResult[string]{Value: "loaded"}
		}
		return out, nil
	})
	c, _ := New[string, string](WithMaxEntries(10), WithStore(gs), WithBulkLoader(bulk))
	defer c.Close()
	done := make(chan map[string]string, 1)
	go func() {
		out, _ := c.GetMultiOrLoad(context.Background(), []string{"k"})
		done <- out
	}()
	<-gs.entered // the lookup's Store read found nothing and is held
	if err := c.Set("k", "new"); err != nil {
		t.Fatal(err)
	}
	close(gs.release)
	if out := <-done; out["k"] != "loaded" {
		t.Fatalf("GetMultiOrLoad = %v; want the loaded value returned", out)
	}
	if v, ok := c.Peek("k"); !ok || v != "new" {
		t.Fatalf("memory holds %q, %v; want the newer Set", v, ok)
	}
	if v, _, _ := gs.MemoryStore.Get(context.Background(), "k"); v != "new" {
		t.Fatalf("Store holds %q; want the newer Set", v)
	}
}

// errGateStore is a gateStore whose reads fail with the Store's own
// deadline once released, while the callers' contexts are fine.
type errGateStore struct{ *gateStore }

func (g errGateStore) Get(ctx context.Context, key string) (string, bool, error) {
	if _, _, err := g.gateStore.Get(ctx, key); err != nil {
		return "", false, err
	}
	return "", false, context.DeadlineExceeded
}

func TestReadThroughSharesAStoreTimeout(t *testing.T) {
	gs := errGateStore{newGateStore()}
	c, _ := New[string, string](WithMaxEntries(10), WithStore[string, string](gs))
	defer c.Close()
	const readers = 20
	errs := make(chan error, readers)
	go func() {
		_, _, err := c.GetCtx(context.Background(), "k")
		errs <- err
	}()
	<-gs.entered
	for range readers - 1 {
		go func() {
			_, _, err := c.GetCtx(context.Background(), "k")
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond) // let the other readers join the read
	close(gs.release)
	for range readers {
		if err := <-errs; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want the Store's DeadlineExceeded", err)
		}
	}
	if n := gs.gets.Load(); n != 1 {
		t.Fatalf("%d readers made %d Store reads; a Store timeout should be shared, not retried", readers, n)
	}
}

// hideTTL is a Store that doesn't implement TTLStore.
type hideTTL[K comparable, V any] struct{ Store[K, V] }

func TestKeepWritesKeepTheStoreRecordTTL(t *testing.T) {
	writes := map[string]func(c *Cache[string, int]) error{
		"Increment": func(c *Cache[string, int]) error { _, err := Increment(c, "rl"); return err },
		"Compute": func(c *Cache[string, int]) error {
			_, err := c.Compute("rl", func(v int, _ bool) (int, ComputeAction, error) { return v + 1, ComputeStore, nil })
			return err
		},
		"Update": func(c *Cache[string, int]) error {
			_, err := c.Update("rl", func(v int) int { return v + 1 })
			return err
		},
		"CompareAndSwap": func(c *Cache[string, int]) error {
			if !c.CompareAndSwap("rl", 1, 2) {
				return errors.New("no swap")
			}
			return nil
		},
	}
	for name, write := range writes {
		for _, defTTL := range []time.Duration{0, time.Hour} {
			t.Run(fmt.Sprint(name, "/default=", defTTL), func(t *testing.T) {
				clk := NewFakeClock(time.Unix(1_000_000, 0))
				st := NewMemoryStore[string, int](clk)
				c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithStore(st),
					WithDefaultTTL(defTTL), WithTTLJitter(0))
				defer c.Close()
				if err := c.SetWithTTL("rl", 1, time.Minute); err != nil {
					t.Fatal(err)
				}
				clk.Advance(10 * time.Second)
				c.Clear() // memory only: the Store keeps the record and its TTL
				if err := write(c); err != nil {
					t.Fatal(err)
				}
				v, ttl, ok, _ := recordTTL(st, "rl")
				if !ok || v != 2 || ttl != 50*time.Second {
					t.Fatalf("Store = %d, %v, %v; want 2 with the record's 50s left", v, ttl, ok)
				}
				if left, ok := c.TTL("rl"); !ok || left <= 0 || left > 50*time.Second {
					t.Fatalf("memory TTL = %v, %v; want no later than the Store record", left, ok)
				}
			})
		}
	}
}

// A Get that promotes a key gives the copy the shorter default TTL; a
// later keep-expiry write still keeps the Store record's longer TTL.
func TestKeepWritesAfterPromotionKeepTheStoreRecordTTL(t *testing.T) {
	writes := map[string]func(c *Cache[string, int], key string) error{
		"Increment": func(c *Cache[string, int], key string) error { _, err := Increment(c, key); return err },
		"Compute": func(c *Cache[string, int], key string) error {
			_, err := c.Compute(key, func(v int, _ bool) (int, ComputeAction, error) { return v + 1, ComputeStore, nil })
			return err
		},
		"ComputeIfPresent": func(c *Cache[string, int], key string) error {
			_, err := c.ComputeIfPresent(key, func(v int) (int, ComputeAction, error) { return v + 1, ComputeStore, nil })
			return err
		},
		"Update": func(c *Cache[string, int], key string) error {
			_, err := c.Update(key, func(v int) int { return v + 1 })
			return err
		},
		"CompareAndSwap": func(c *Cache[string, int], key string) error {
			if !c.CompareAndSwap(key, 1, 2) {
				return errors.New("no swap")
			}
			return nil
		},
	}
	for name, write := range writes {
		for record, want := range map[time.Duration]time.Duration{time.Hour: time.Hour - 10*time.Second, 0: 0} {
			t.Run(fmt.Sprint(name, "/record=", record), func(t *testing.T) {
				clk := NewFakeClock(time.Unix(1_000_000, 0))
				st := NewMemoryStore[string, int](clk)
				c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithStore(st),
					WithDefaultTTL(time.Minute), WithTTLJitter(0))
				defer c.Close()
				_ = st.Set(context.Background(), "rl", 1, record)
				if _, ok := c.Get("rl"); !ok {
					t.Fatal("Get missed")
				}
				clk.Advance(10 * time.Second)
				if err := write(c, "rl"); err != nil {
					t.Fatal(err)
				}
				if v, ttl, ok, _ := recordTTL(st, "rl"); !ok || v != 2 || ttl != want {
					t.Fatalf("Store = %d, %v, %v; want 2 with %v left", v, ttl, ok, want)
				}
				if left, ok := c.TTL("rl"); !ok || left != 50*time.Second {
					t.Fatalf("memory TTL = %v, %v; want the copy's 50s", left, ok)
				}
			})
		}
	}
}

func TestStorePromotionExpiresWithTheStoreRecord(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_000_000, 0))
	st := NewMemoryStore[string, int](clk)
	_ = st.Set(context.Background(), "short", 1, time.Minute)
	_ = st.Set(context.Background(), "long", 1, 2*time.Hour)
	_ = st.Set(context.Background(), "forever", 1, 0)
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithStore(st),
		WithDefaultTTL(time.Hour), WithTTLJitter(0))
	defer c.Close()
	for key, want := range map[string]time.Duration{"short": time.Minute, "long": time.Hour, "forever": time.Hour} {
		if _, ok := c.Get(key); !ok {
			t.Fatalf("Get(%s) missed", key)
		}
		if got, _ := c.TTL(key); got != want {
			t.Errorf("%s: promoted TTL = %v, want %v", key, got, want)
		}
	}
}

func TestKeepWriteWithoutTTLStoreUsesTheCopysTTL(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_000_000, 0))
	st := NewMemoryStore[string, int](clk)
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithStore[string, int](hideTTL[string, int]{st}),
		WithDefaultTTL(time.Hour), WithTTLJitter(0))
	defer c.Close()
	_ = c.SetWithTTL("rl", 1, time.Minute)
	c.Clear()
	if _, err := Increment(c, "rl"); err != nil {
		t.Fatal(err)
	}
	if v, ttl, ok, _ := recordTTL(st, "rl"); !ok || v != 2 || ttl != time.Hour {
		t.Fatalf("Store = %d, %v, %v; want 2 with the promoted copy's 1h", v, ttl, ok)
	}
}

// recordTTL reads key's value and remaining TTL from st.
func recordTTL[V any](st *MemoryStore[string, V], key string) (V, time.Duration, bool, error) {
	v, ok, err := st.Get(context.Background(), key)
	if err != nil || !ok {
		return v, 0, ok, err
	}
	ttl, ok, err := st.TTL(context.Background(), key)
	return v, ttl, ok, err
}

func TestMemoryStoreTTLLookup(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_000_000, 0))
	st := NewMemoryStore[string, int](clk)
	ctx := context.Background()
	_ = st.Set(ctx, "ttl", 1, time.Minute)
	_ = st.Set(ctx, "none", 2, 0)
	clk.Advance(20 * time.Second)
	if ttl, ok, err := st.TTL(ctx, "ttl"); err != nil || !ok || ttl != 40*time.Second {
		t.Fatalf("ttl = %v, %v, %v", ttl, ok, err)
	}
	if ttl, ok, err := st.TTL(ctx, "none"); err != nil || !ok || ttl != 0 {
		t.Fatalf("none = %v, %v, %v", ttl, ok, err)
	}
	if _, ok, _ := st.TTL(ctx, "absent"); ok {
		t.Fatal("absent key found")
	}
	clk.Advance(time.Minute)
	if _, ok, _ := st.TTL(ctx, "ttl"); ok {
		t.Fatal("expired record found")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := st.TTL(canceled, "none"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx err = %v", err)
	}
	_ = st.Close()
	if _, _, err := st.TTL(ctx, "none"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed err = %v", err)
	}
}

// countingGetStore embeds MemoryStore and overrides only Get, as a
// metrics or auth wrapper would.
type countingGetStore struct {
	*MemoryStore[string, int]
	gets atomic.Int32
}

func (s *countingGetStore) Get(ctx context.Context, key string) (int, bool, error) {
	s.gets.Add(1)
	return s.MemoryStore.Get(ctx, key)
}

// The cache reads values with Get even from a TTLStore, so a Get
// override on a type embedding MemoryStore is still called, and the
// inherited TTL lookup still keeps the record's expiry.
func TestTTLStoreValuesAreReadWithGet(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_000_000, 0))
	st := &countingGetStore{MemoryStore: NewMemoryStore[string, int](clk)}
	_ = st.MemoryStore.Set(context.Background(), "k", 1, time.Minute)
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithStore[string, int](st), WithTTLJitter(0))
	defer c.Close()
	if v, ok := c.Get("k"); !ok || v != 1 {
		t.Fatalf("Get = %d, %v", v, ok)
	}
	c.Clear()
	if _, err := Increment(c, "k"); err != nil {
		t.Fatal(err)
	}
	if n := st.gets.Load(); n != 2 {
		t.Fatalf("the Store's Get was called %d times, want 2", n)
	}
	if v, ttl, ok, _ := recordTTL(st.MemoryStore, "k"); !ok || v != 2 || ttl != time.Minute {
		t.Fatalf("Store = %d, %v, %v; want 2 with the record's 1m", v, ttl, ok)
	}
}

// failTTLStore is a TTLStore whose TTL lookup fails.
type failTTLStore struct{ *MemoryStore[string, int] }

func (failTTLStore) TTL(context.Context, string) (time.Duration, bool, error) {
	return 0, false, errors.New("ttl down")
}

// goneTTLStore is a TTLStore whose records are gone by the TTL lookup.
type goneTTLStore struct{ *MemoryStore[string, int] }

func (goneTTLStore) TTL(context.Context, string) (time.Duration, bool, error) {
	return 0, false, nil
}

// A record that expires between the Get and the TTL lookup is returned
// but not copied into memory.
func TestStoreRecordGoneAtTheTTLLookupIsNotPromoted(t *testing.T) {
	ms := NewMemoryStore[string, int](nil)
	_ = ms.Set(context.Background(), "k", 1, time.Minute)
	_ = ms.Set(context.Background(), "n", 1, time.Minute)
	c, _ := New[string, int](WithMaxEntries(10), WithStore[string, int](goneTTLStore{ms}),
		WithDefaultTTL(time.Hour))
	defer c.Close()
	if v, ok := c.Get("k"); !ok || v != 1 {
		t.Fatalf("Get = %d, %v; want the value the Store returned", v, ok)
	}
	if v, ok, err := c.GetOrSet("n", 5); err != nil || !ok || v != 1 {
		t.Fatalf("GetOrSet = %d, %v, %v; want the Store's value", v, ok, err)
	}
	if n := c.Len(); n != 0 {
		t.Fatalf("Len = %d; a record the Store reports gone was promoted", n)
	}
}

func TestFailedTTLLookupFallsBackToTheDefaultTTL(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_000_000, 0))
	ms := NewMemoryStore[string, int](clk)
	_ = ms.Set(context.Background(), "k", 1, time.Minute)
	c, _ := New[string, int](WithMaxEntries(10), WithClock(clk), WithStore[string, int](failTTLStore{ms}),
		WithDefaultTTL(time.Hour), WithTTLJitter(0))
	defer c.Close()
	if v, ok := c.Get("k"); !ok || v != 1 {
		t.Fatalf("Get = %d, %v; a failed TTL lookup must not fail the read", v, ok)
	}
	if ttl, _ := c.TTL("k"); ttl != time.Hour {
		t.Fatalf("promoted TTL = %v, want the default 1h", ttl)
	}
	c.Clear()
	if _, err := Increment(c, "k"); err != nil {
		t.Fatal(err)
	}
	if v, ttl, ok, _ := recordTTL(ms, "k"); !ok || v != 2 || ttl != time.Hour {
		t.Fatalf("Store = %d, %v, %v; want 2 with the promoted copy's 1h", v, ttl, ok)
	}
}

// A bulk-loaded value that reaches a cache closed meanwhile is dropped
// without a warning.
func TestBulkLoadedValueAfterCloseIsNotWarned(t *testing.T) {
	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	c, _ := New[string, int](WithMaxEntries(10), WithLogger(logger),
		WithStore[string, int](NewMemoryStore[string, int](nil)))
	_ = c.Close()
	c.storeBulkLoaded("k", 1, 0, &watchToken{})
	if out := buf.String(); out != "" {
		t.Fatalf("logged %q; want nothing", out)
	}
}
