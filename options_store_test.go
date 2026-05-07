package memcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// trackingStore is a [Store] decorator that counts each method
// invocation. Tests use it to assert the cache routed to the Store
// at the expected times.
type trackingStore[K comparable, V any] struct {
	inner    Store[K, V]
	getCalls atomic.Uint64
	setCalls atomic.Uint64
	delCalls atomic.Uint64
	// failNext, when set, causes the next matching call to return
	// the configured error. The flag is consumed on first use.
	failNextSet atomic.Pointer[error]
	failNextDel atomic.Pointer[error]
	failNextGet atomic.Pointer[error]
}

func newTrackingStore[K comparable, V any](inner Store[K, V]) *trackingStore[K, V] {
	return &trackingStore[K, V]{inner: inner}
}

func (s *trackingStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	s.getCalls.Add(1)
	if eptr := s.failNextGet.Swap(nil); eptr != nil {
		var zero V
		return zero, false, *eptr
	}
	return s.inner.Get(ctx, key)
}

func (s *trackingStore[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	s.setCalls.Add(1)
	if eptr := s.failNextSet.Swap(nil); eptr != nil {
		return *eptr
	}
	return s.inner.Set(ctx, key, value, ttl)
}

func (s *trackingStore[K, V]) Delete(ctx context.Context, key K) (bool, error) {
	s.delCalls.Add(1)
	if eptr := s.failNextDel.Swap(nil); eptr != nil {
		return false, *eptr
	}
	return s.inner.Delete(ctx, key)
}

func (s *trackingStore[K, V]) Iterate(ctx context.Context, fn func(K, V) bool) error {
	return s.inner.Iterate(ctx, fn)
}

func (s *trackingStore[K, V]) Len(ctx context.Context) (int, error) {
	return s.inner.Len(ctx)
}

func (s *trackingStore[K, V]) Close() error {
	return s.inner.Close()
}

func TestWithStoreTypeMismatchReturnsConfigError(t *testing.T) {
	// A Store[int, int] cannot back a Cache[string, int].
	store := NewMemoryStore[int, int](nil)
	_, err := New[string, int](
		WithMaxEntries(8),
		WithStore[int, int](store),
	)
	if err == nil {
		t.Fatal("expected ConfigError on K type mismatch")
	}
	var ce *ConfigError
	if !errors.As(err, &ce) {
		t.Errorf("expected *ConfigError, got %T: %v", err, err)
	}
}

func TestWithStoreSetWritesThroughToStore(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)

	c, err := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if err := c.Set("k", 42); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if store.setCalls.Load() != 1 {
		t.Errorf("Store.Set calls = %d, want 1", store.setCalls.Load())
	}
	// Confirm the value actually reached the underlying store.
	v, ok, _ := inner.Get(context.Background(), "k")
	if !ok || v != 42 {
		t.Errorf("Store.Get(k) = (%d, %v), want (42, true)", v, ok)
	}
}

func TestWithStoreReadHitDoesNotConsultStore(t *testing.T) {
	store := newTrackingStore[string, int](NewMemoryStore[string, int](nil))
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	defer c.Close()

	_ = c.Set("k", 1)
	store.getCalls.Store(0) // reset post-Set baseline

	if got, ok := c.Get("k"); !ok || got != 1 {
		t.Fatalf("Get(k) = (%d, %v), want (1, true)", got, ok)
	}
	if store.getCalls.Load() != 0 {
		t.Errorf("Store.Get called %d times for in-memory hit, want 0",
			store.getCalls.Load())
	}
}

func TestWithStoreReadMissFallsThroughToStore(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	// Pre-populate the store directly (bypassing the cache).
	_ = inner.Set(context.Background(), "k", 99, 0)
	store := newTrackingStore[string, int](inner)

	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	defer c.Close()

	got, ok := c.Get("k")
	if !ok || got != 99 {
		t.Fatalf("Get(k) after store-side write = (%d, %v), want (99, true)", got, ok)
	}
	if store.getCalls.Load() == 0 {
		t.Error("Store.Get not called on in-memory miss")
	}

	// Promotion: the next Get should be served from in-memory.
	store.getCalls.Store(0)
	if got, ok := c.Get("k"); !ok || got != 99 {
		t.Fatalf("post-promotion Get(k) = (%d, %v), want (99, true)", got, ok)
	}
	if store.getCalls.Load() != 0 {
		t.Errorf("Store.Get called %d times after promotion, want 0",
			store.getCalls.Load())
	}
}

func TestWithStoreDeleteWritesThroughToStore(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)

	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	defer c.Close()

	_ = c.Set("k", 1)
	store.delCalls.Store(0)
	if !c.Delete("k") {
		t.Error("Delete reported nothing removed")
	}
	if store.delCalls.Load() != 1 {
		t.Errorf("Store.Delete calls = %d, want 1", store.delCalls.Load())
	}
	if _, ok, _ := inner.Get(context.Background(), "k"); ok {
		t.Error("Store still has key after Delete")
	}
}

func TestWithStoreSetErrorRollsBackInMemory(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)

	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	defer c.Close()

	failure := errors.New("simulated store failure")
	store.failNextSet.Store(&failure)

	err := c.Set("k", 1)
	if err == nil || !errors.Is(err, failure) {
		t.Errorf("Set returned %v, want %v", err, failure)
	}
	// In-memory must NOT have the entry; the rollback ran.
	if _, ok := c.Get("k"); ok {
		t.Error("in-memory cache holds entry after Store-write rollback")
	}
}

func TestWithStoreDeleteCtxSurfacesStoreError(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)

	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	defer c.Close()

	_ = c.Set("k", 1)
	failure := errors.New("simulated delete failure")
	store.failNextDel.Store(&failure)

	removed, err := c.DeleteCtx(context.Background(), "k")
	if !removed {
		t.Error("DeleteCtx removed=false, want true (in-memory delete ran)")
	}
	if !errors.Is(err, failure) {
		t.Errorf("DeleteCtx err = %v, want %v", err, failure)
	}
}

func TestWithStoreHasFallsThroughToStore(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	_ = inner.Set(context.Background(), "store-only", 42, 0)
	store := newTrackingStore[string, int](inner)

	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	defer c.Close()

	if !c.Has("store-only") {
		t.Error("Has returned false for key that lives only in the Store")
	}
	if c.Has("nowhere") {
		t.Error("Has returned true for key that exists in neither layer")
	}
}

func TestWithStoreCloseDoesNotCloseStore(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)

	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Store should still be usable.
	if err := store.Set(context.Background(), "post-close", 1, 0); err != nil {
		t.Errorf("Store.Set after cache.Close failed: %v", err)
	}
}

func TestWithStoreEvictionDoesNotDeleteFromStore(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)

	// Tight bound: in-memory evictions must NOT reach the Store.
	c, _ := New[string, int](
		WithMaxEntries(2),
		WithShards(1),
		WithPolicy(PolicyLRU),
		WithStore[string, int](store),
	)
	defer c.Close()

	for i, k := range []string{"a", "b", "c", "d"} {
		if err := c.Set(k, i); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	store.delCalls.Store(0)
	// Force in-memory misses on the evicted keys by reading them.
	// These should hit the Store, not return false.
	for _, k := range []string{"a", "b", "c", "d"} {
		if _, ok := c.Get(k); !ok {
			t.Errorf("post-eviction Get(%s) = false; Store should have promoted it", k)
		}
	}
	if store.delCalls.Load() != 0 {
		t.Errorf("Store.Delete called %d times during eviction, want 0",
			store.delCalls.Load())
	}
}

func TestWithStoreSetCtxCancelledBeforeWriteSurfacesError(t *testing.T) {
	store := newTrackingStore[string, int](NewMemoryStore[string, int](nil))
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithStore[string, int](store),
	)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.SetCtx(ctx, "k", 1)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("SetCtx with canceled ctx err = %v, want context.Canceled", err)
	}
}

func TestWithStoreConcurrentSetGetThroughStore(t *testing.T) {
	inner := NewMemoryStore[string, int](nil)
	store := newTrackingStore[string, int](inner)

	c, _ := New[string, int](
		WithMaxEntries(2048),
		WithShards(4),
		WithStore[string, int](store),
	)
	defer c.Close()

	const goroutines = 8
	const opsPerG = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				k := storeKeyFor(g, i)
				if err := c.Set(k, g*1000+i); err != nil {
					t.Errorf("Set(%s): %v", k, err)
					return
				}
				if got, ok := c.Get(k); !ok || got != g*1000+i {
					t.Errorf("Get(%s) = (%d, %v), want (%d, true)",
						k, got, ok, g*1000+i)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// storeKeyFor composes a deterministic per-goroutine key for the
// concurrent test above.
func storeKeyFor(g, i int) string {
	return "g" + storeIntToString(g) + "-k" + storeIntToString(i)
}

func storeIntToString(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	negative := false
	if n < 0 {
		negative = true
		n = -n
	}
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
