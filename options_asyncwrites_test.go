package memcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAsyncWritesSetThenGetReturnsValueImmediately(t *testing.T) {
	// Visibility contract: a Set followed by a Get on the same key
	// must return the new value, even before the apply goroutine
	// has drained.
	c, err := New[string, int](
		WithMaxEntries(64),
		WithAsyncWrites(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if err := c.Set("k", 42); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, ok := c.Get("k"); !ok || got != 42 {
		t.Errorf("Get(k) = (%d, %v), want (42, true)", got, ok)
	}
}

func TestAsyncWritesSetThenDeleteThenGetReturnsMiss(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithAsyncWrites(),
	)
	defer c.Close()

	_ = c.Set("k", 1)
	c.Delete("k")
	if _, ok := c.Get("k"); ok {
		t.Error("Get(k) returned hit after pending Set+Delete")
	}
}

func TestAsyncWritesCoalescesRepeatedSetsOnSameKey(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithAsyncWrites(),
	)
	defer c.Close()

	for i := 0; i < 100; i++ {
		_ = c.Set("k", i)
	}
	if got, ok := c.Get("k"); !ok || got != 99 {
		t.Errorf("Get(k) after 100 coalesced Sets = (%d, %v), want (99, true)", got, ok)
	}
}

func TestAsyncWritesSyncDrainsPending(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(1024),
		WithShards(2),
		WithAsyncWrites(),
	)
	defer c.Close()

	for i := 0; i < 32; i++ {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	if err := c.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := c.asyncBacklog(); got != 0 {
		t.Errorf("asyncBacklog after Sync = %d, want 0", got)
	}
	// After Sync, every entry should be in storage (not pending).
	for i := 0; i < 32; i++ {
		k := fmt.Sprintf("k%d", i)
		s := c.shardFor(k)
		s.mu.RLock()
		_, inPending := s.pending[k]
		_, inStorage := s.storage.get(k)
		s.mu.RUnlock()
		if inPending {
			t.Errorf("key %q still in pending map after Sync", k)
		}
		if !inStorage {
			t.Errorf("key %q not in storage after Sync", k)
		}
	}
}

func TestAsyncWritesCloseDrainsPending(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(1024),
		WithShards(2),
		WithAsyncWrites(),
	)
	for i := 0; i < 32; i++ {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After Close the apply goroutine must have exited cleanly.
	select {
	case <-c.async.exited:
	case <-time.After(time.Second):
		t.Fatal("apply goroutine did not exit within 1s after Close")
	}
}

func TestAsyncWritesSetWithTTLHonorsTTLAfterApply(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithClock(clk),
		WithAsyncWrites(),
	)
	defer c.Close()

	if err := c.SetWithTTL("k", 1, 100*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	// Drain so the entry materializes in storage with its TTL.
	if err := c.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got, ok := c.Get("k"); !ok || got != 1 {
		t.Fatalf("Get(k) = (%d, %v), want (1, true)", got, ok)
	}
	clk.Advance(200 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Error("Get(k) returned hit after TTL expired")
	}
}

func TestAsyncWritesPropagatesToStore(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
		WithAsyncWrites(),
	)
	defer c.Close()

	_ = c.Set("k", 99)
	if err := c.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if store.setCalls.Load() != 1 {
		t.Errorf("Store.Set calls after async Set + Sync = %d, want 1",
			store.setCalls.Load())
	}
	v, ok, _ := inner.Get(context.Background(), "k")
	if !ok || v != 99 {
		t.Errorf("Store.Get(k) = (%d, %v), want (99, true)", v, ok)
	}
}

func TestAsyncWritesStoreErrorDoesNotBlockCaller(t *testing.T) {
	// When async writes are on, a Store failure during apply must
	// not propagate back to the caller — it's logged. The caller
	// has already moved on.
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)

	failure := errors.New("simulated store failure")
	store.failNextSet.Store(&failure)

	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
		WithAsyncWrites(),
	)
	defer c.Close()

	if err := c.Set("k", 1); err != nil {
		t.Fatalf("Set returned %v, want nil (async writes swallow store errors)", err)
	}
	// The in-memory entry is still there — apply ran and the
	// upsert succeeded; the Store call failed but apply doesn't
	// roll back in the async path.
	_ = c.Sync(context.Background())
	if got, ok := c.Get("k"); !ok || got != 1 {
		t.Errorf("post-Sync Get(k) = (%d, %v), want (1, true)", got, ok)
	}
}

func TestAsyncWritesValidationStillRunsSynchronously(t *testing.T) {
	// Even with async writes, capacity / TTL / tag-limit
	// validation must run synchronously so users see configuration
	// errors at the call site, not asynchronously.
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithAsyncWrites(),
	)
	defer c.Close()

	if err := c.SetWithTTL("k", 1, -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("SetWithTTL with negative TTL = %v, want ErrInvalidTTL", err)
	}
}

func TestAsyncWritesPeekSeesPendingValue(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithAsyncWrites(),
	)
	defer c.Close()

	_ = c.Set("k", 7)
	got, ok := c.Peek("k")
	if !ok || got != 7 {
		t.Errorf("Peek(k) = (%d, %v), want (7, true)", got, ok)
	}
}

func TestAsyncWritesHasSeesPendingValue(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithAsyncWrites(),
	)
	defer c.Close()

	_ = c.Set("k", 1)
	if !c.Has("k") {
		t.Error("Has(k) = false after pending Set")
	}
	c.Delete("k")
	if c.Has("k") {
		t.Error("Has(k) = true after pending Delete")
	}
}

func TestAsyncWritesConcurrentSetGetUnderRace(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(2048),
		WithShards(4),
		WithAsyncWrites(),
	)
	defer c.Close()

	const goroutines = 8
	const opsPerG = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				k := fmt.Sprintf("g%d-k%d", g, i)
				_ = c.Set(k, g*1000+i)
				if got, ok := c.Get(k); !ok || got != g*1000+i {
					t.Errorf("Get(%s) = (%d, %v), want (%d, true)",
						k, got, ok, g*1000+i)
					return
				}
			}
		}()
	}
	wg.Wait()
	_ = c.Sync(context.Background())
}

func TestAsyncWritesSetWithOptionsAbsoluteExpiry(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 100_000_000_000))
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithClock(clk),
		WithAsyncWrites(),
	)
	defer c.Close()

	expireAt := clk.Now().Add(50 * time.Millisecond)
	if err := c.SetWithOptions("k", 1, SetExpireAt(expireAt)); err != nil {
		t.Fatalf("SetWithOptions: %v", err)
	}
	if got, ok := c.Get("k"); !ok || got != 1 {
		t.Errorf("Get(k) before drain = (%d, %v), want (1, true)", got, ok)
	}
	if err := c.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	clk.Advance(100 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Error("Get(k) returned hit after absolute-expiry passed")
	}
}

func TestAsyncWritesDefaultIsOff(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	if c.async != nil {
		t.Error("async is non-nil without WithAsyncWrites")
	}
	for _, sh := range c.shards {
		if sh.pending != nil {
			t.Error("shard.pending is non-nil without WithAsyncWrites")
		}
	}
}
