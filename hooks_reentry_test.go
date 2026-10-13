package memcache

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// within fails the test if fn doesn't return within d.
func within(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("still running after %v", d)
	}
}

func TestHookMayComputeOnTheShardItFiredFrom(t *testing.T) {
	var c *Cache[string, int]
	var evictions atomic.Int32
	c, _ = New[string, int](WithMaxEntries(2), WithShards(1),
		WithOnEvict(func(k string, _ int, _ EvictionReason) {
			if k == "evictions" {
				return
			}
			if _, err := Increment(c, "evictions"); err == nil {
				evictions.Add(1)
			}
		}))
	defer c.Close()
	_ = c.Set("a", 1)
	_ = c.Set("b", 1)
	if _, err := Increment(c, "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Compute("a", func(int, bool) (int, ComputeAction, error) { return 0, ComputeDelete, nil }); err != nil {
		t.Fatal(err)
	}
	if evictions.Load() == 0 {
		t.Fatal("the hook's Increment never ran")
	}
}

func TestComputeStillPanicsOnReentry(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(10), WithShards(1))
	defer c.Close()
	defer func() {
		if r := recover(); r != ErrComputeReentrant {
			t.Fatalf("recovered %v, want ErrComputeReentrant", r)
		}
	}()
	_, _ = c.Compute("a", func(int, bool) (int, ComputeAction, error) {
		_, _ = Increment(c, "b")
		return 0, ComputeNoOp, nil
	})
}

func TestRangeCallbackMayWriteTheCache(t *testing.T) {
	c, _ := New[int, int](WithMaxEntries(100), WithShards(1))
	defer c.Close()
	for i := range 10 {
		_ = c.Set(i, i)
	}
	within(t, 2*time.Second, func() {
		c.Range(func(k, v int) bool {
			if k%2 == 0 {
				c.Delete(k)
			} else {
				_ = c.Set(k, v*10)
			}
			_, _ = c.Get(k)
			return true
		})
	})
	if c.Len() != 5 {
		t.Fatalf("Len = %d, want 5", c.Len())
	}
	if v, _ := c.Get(3); v != 30 {
		t.Fatalf("Get(3) = %d, want 30", v)
	}
}

func TestPanickingHookIsRecoveredAndLogged(t *testing.T) {
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	var expired atomic.Int32
	c, _ := New[string, int](WithMaxEntries(10), WithLogger(logger),
		WithJanitorInterval(5*time.Millisecond),
		WithOnExpire(func(k string, _ int) {
			expired.Add(1)
			if k == "boom" {
				panic("hook boom")
			}
		}),
		WithOnEvict(func(string, int, EvictionReason) { panic("evict boom") }))
	defer c.Close()
	_ = c.SetWithTTL("boom", 1, time.Millisecond)
	_ = c.SetWithTTL("next", 1, time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for expired.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if expired.Load() < 2 {
		t.Fatal("the janitor stopped firing OnExpire after a hook panicked")
	}
	_ = c.Set("x", 1)
	c.Delete("x") // OnEvict panics on the caller's goroutine
	if !strings.Contains(logs.String(), "hook panicked") || !strings.Contains(logs.String(), "evict boom") {
		t.Fatalf("panics not logged: %s", logs.String())
	}
}

func TestSyncFromAHookAnAsyncApplyFired(t *testing.T) {
	var c *Cache[string, int]
	var syncErr atomic.Value
	var fired atomic.Bool
	c, _ = New[string, int](WithMaxEntries(2), WithShards(1), WithAsyncWrites(),
		WithOnEvict(func(string, int, EvictionReason) {
			if !fired.CompareAndSwap(false, true) {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := c.Sync(ctx); err != nil {
				syncErr.Store(err)
			}
		}))
	defer c.Close()
	within(t, 5*time.Second, func() {
		_ = c.Set("a", 1)
		_ = c.Set("b", 1)
		_ = c.Set("c", 1)
		_ = c.Sync(context.Background())
	})
	if !fired.Load() {
		t.Fatal("no eviction hook fired")
	}
	if err := syncErr.Load(); err != nil {
		t.Fatalf("Sync from the hook: %v", err)
	}
}

func TestSyncWaitsForHooksOfAppliedWrites(t *testing.T) {
	var fired atomic.Int32
	c, _ := New[string, int](WithMaxEntries(2), WithShards(1), WithAsyncWrites(),
		WithStore(NewMemoryStore[string, int](nil)),
		WithOnEvict(func(string, int, EvictionReason) {
			time.Sleep(10 * time.Millisecond)
			fired.Add(1)
		}))
	defer c.Close()
	for range 20 {
		_ = c.Set("a", 1)
		_ = c.Set("b", 1)
		_ = c.Set("c", 1)
		before := fired.Load()
		_ = c.Sync(context.Background())
		if fired.Load() == before {
			t.Fatal("Sync returned before the hooks of the writes it waited for ran")
		}
		c.Clear()
	}
}
