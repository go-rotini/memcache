package memcache

import (
	"context"
	"sync/atomic"
	"time"
)

// TieredStats reports observability for a [Tiered] cache split
// between its two tiers. L1Hits are hits served from the fast
// (in-memory) tier; L2Hits are served from the slower-but-larger
// tier and trigger a promotion. Misses are queries that found
// neither tier.
type TieredStats struct {
	// L1 is the L1 cache's full Stats snapshot.
	L1 Stats
	// L2 is the L2 cache's full Stats snapshot.
	L2 Stats
	// L1Hits is the count of Tiered.Get calls served from L1.
	L1Hits uint64
	// L2Hits is the count of Tiered.Get calls served from L2
	// (with subsequent promotion to L1).
	L2Hits uint64
	// Misses is the count of Tiered.Get calls that found neither
	// tier.
	Misses uint64
	// Promotions is L2Hits but recorded after the L1 promotion
	// actually wrote successfully (failed Sets do not bump it).
	Promotions uint64
}

// Tiered combines two [Cache] instances into an L1/L2 hierarchy. Reads
// hit L1 first; L2 hits are promoted to L1. Writes are write-through.
// Tiered exposes the high-traffic methods directly; reach the rest via
// [Tiered.L1] / [Tiered.L2]. Both caches must be non-nil.
type Tiered[K comparable, V any] struct {
	l1 *Cache[K, V]
	l2 *Cache[K, V]

	l1Hits     atomic.Uint64
	l2Hits     atomic.Uint64
	misses     atomic.Uint64
	promotions atomic.Uint64

	closed atomic.Bool
}

// NewTiered constructs a [Tiered] cache from two non-nil [Cache]
// instances. [Tiered.Close] closes both; share an underlying cache only
// if you do not need to outlive Tiered.Close.
func NewTiered[K comparable, V any](l1, l2 *Cache[K, V]) *Tiered[K, V] {
	if l1 == nil {
		panic("memcache: NewTiered: l1 is nil")
	}
	if l2 == nil {
		panic("memcache: NewTiered: l2 is nil")
	}
	return &Tiered[K, V]{l1: l1, l2: l2}
}

// Get returns the cached value for key. Lookup order: L1, then L2 with
// best-effort promotion to L1, then miss. A failed L1 promotion still
// returns the L2 hit.
func (t *Tiered[K, V]) Get(key K) (V, bool) {
	var zero V
	if t.closed.Load() {
		return zero, false
	}
	if v, ok := t.l1.Get(key); ok {
		t.l1Hits.Add(1)
		return v, true
	}
	if v, ok := t.l2.Get(key); ok {
		t.l2Hits.Add(1)
		// Promotion: best-effort write into L1. We treat any L1
		// Set failure (e.g., MaxValueWeight rejection) as a
		// non-fatal hint and still return the L2 value.
		if err := t.l1.Set(key, v); err == nil {
			t.promotions.Add(1)
		}
		return v, true
	}
	t.misses.Add(1)
	return zero, false
}

// Set writes value to BOTH tiers (write-through). Returns the
// first error encountered; partial failure is possible but
// uncommon (e.g., L1 succeeds but L2 hits a weight limit). In
// that case L1 may carry a fresher value than L2 until the next
// Set rebalances.
func (t *Tiered[K, V]) Set(key K, value V) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if err := t.l1.Set(key, value); err != nil {
		return err
	}
	return t.l2.Set(key, value)
}

// Delete removes key from BOTH tiers. Returns true when at least
// one tier had an entry to remove.
func (t *Tiered[K, V]) Delete(key K) bool {
	if t.closed.Load() {
		return false
	}
	d1 := t.l1.Delete(key)
	d2 := t.l2.Delete(key)
	return d1 || d2
}

// Has reports whether either tier has a fresh entry for key.
func (t *Tiered[K, V]) Has(key K) bool {
	if t.closed.Load() {
		return false
	}
	return t.l1.Has(key) || t.l2.Has(key)
}

// GetCtx is the context-aware variant of [Tiered.Get]. The ctx is
// threaded through to each tier's [Cache.GetCtx]; either tier's
// [WithStore] read-through honors cancellation. The boolean
// reports whether either tier had the entry; the error surfaces
// any underlying ctx or Store failure.
func (t *Tiered[K, V]) GetCtx(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if t.closed.Load() {
		return zero, false, ErrClosed
	}
	if v, ok, err := t.l1.GetCtx(ctx, key); err != nil || ok {
		if ok {
			t.l1Hits.Add(1)
		}
		return v, ok, err
	}
	v, ok, err := t.l2.GetCtx(ctx, key)
	if err != nil {
		return zero, false, err
	}
	if !ok {
		t.misses.Add(1)
		return zero, false, nil
	}
	t.l2Hits.Add(1)
	if perr := t.l1.SetCtx(ctx, key, v); perr == nil {
		t.promotions.Add(1)
	}
	return v, true, nil
}

// SetWithTTL writes value to BOTH tiers with the given TTL.
// Returns the first error encountered.
func (t *Tiered[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if err := t.l1.SetWithTTL(key, value, ttl); err != nil {
		return err
	}
	return t.l2.SetWithTTL(key, value, ttl)
}

// SetWithOptions writes value to both tiers with the same options. For
// per-tier overrides, call into [Tiered.L1] / [Tiered.L2] directly.
func (t *Tiered[K, V]) SetWithOptions(key K, value V, opts ...SetOption) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if err := t.l1.SetWithOptions(key, value, opts...); err != nil {
		return err
	}
	return t.l2.SetWithOptions(key, value, opts...)
}

// InvalidateTag removes every entry tagged with t from BOTH tiers.
// Returns the total number of entries removed across both tiers.
// Negative-cache tombstones and invalidated read-snapshot entries
// are not counted by either tier's InvalidateTag.
func (t *Tiered[K, V]) InvalidateTag(tag string) int {
	if t.closed.Load() {
		return 0
	}
	return t.l1.InvalidateTag(tag) + t.l2.InvalidateTag(tag)
}

// InvalidateTags removes every entry tagged with at least one of
// tags from BOTH tiers. Returns the total removed across both.
func (t *Tiered[K, V]) InvalidateTags(tags ...string) int {
	if t.closed.Load() {
		return 0
	}
	return t.l1.InvalidateTags(tags...) + t.l2.InvalidateTags(tags...)
}

// Sync waits for any background work in BOTH tiers (tag-cleanup
// drainer, async-writes apply goroutine) to drain. Useful in tests
// before reading state that depends on completed writes.
func (t *Tiered[K, V]) Sync(ctx context.Context) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if err := t.l1.Sync(ctx); err != nil {
		return err
	}
	return t.l2.Sync(ctx)
}

// Len returns the number of entries in L2. Because writes are
// write-through, L1 ⊆ L2 in steady state, so reporting L2's Len
// is the most accurate "how many distinct keys does this tiered
// cache hold" measurement. Use [Tiered.L1]/[Tiered.L2] and their
// own Len() if you need per-tier counts.
func (t *Tiered[K, V]) Len() int {
	if t.closed.Load() {
		return 0
	}
	return t.l2.Len()
}

// Stats returns the combined observability snapshot.
func (t *Tiered[K, V]) Stats() TieredStats {
	return TieredStats{
		L1:         t.l1.Stats(),
		L2:         t.l2.Stats(),
		L1Hits:     t.l1Hits.Load(),
		L2Hits:     t.l2Hits.Load(),
		Misses:     t.misses.Load(),
		Promotions: t.promotions.Load(),
	}
}

// L1 returns the underlying L1 [Cache]. Useful for callers that
// need to apply L1-specific operations (e.g., InvalidateTag) that
// the Tiered wrapper does not expose directly.
func (t *Tiered[K, V]) L1() *Cache[K, V] { return t.l1 }

// L2 returns the underlying L2 [Cache]. Same caveats as [Tiered.L1].
func (t *Tiered[K, V]) L2() *Cache[K, V] { return t.l2 }

// Close closes BOTH tiers. The first non-nil error encountered is
// returned; subsequent close errors are silently swallowed (the
// caller cannot act on more than one shutdown failure usefully).
// Idempotent: subsequent Close calls return nil.
func (t *Tiered[K, V]) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return nil
	}
	err1 := t.l1.Close()
	err2 := t.l2.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
