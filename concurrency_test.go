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
	for _, clear := range []string{"Clear", "Reset"} {
		t.Run(clear, func(t *testing.T) {
			lost := 0
			for r := range 100 {
				c, _ := New[string, int](WithMaxEntries(100_000), WithShards(64))
				for i := range 5000 {
					_ = c.Set(fmt.Sprint("fill", i), i)
				}
				var wg sync.WaitGroup
				wg.Go(func() {
					if clear == "Clear" {
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
				t.Fatalf("InvalidateTag missed a key tagged during %s in %d of 100 rounds", clear, lost)
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
