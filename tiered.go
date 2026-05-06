package memcache

import (
	"sync/atomic"
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

// Tiered combines two [Cache] instances into a hierarchy. Reads
// hit L1 first; on miss they fall through to L2 and on L2 hit the
// value is promoted to L1 so subsequent reads bypass L2 entirely.
// Writes are write-through: every Set lands in both caches.
//
// The L1/L2 split is the canonical "small fast in-memory cache +
// larger warm pool" pattern. Typical CLI usage gives L1 a tight
// entry budget and lets L2 carry the long-lived working set —
// possibly backed by a disk-backed [Store] in a future revision.
//
// Tiered is safe for concurrent use; thread-safety follows from
// the underlying [Cache] thread-safety. Both caches must be
// non-nil and use the same K/V types (enforced statically by Go
// generics).
type Tiered[K comparable, V any] struct {
	l1 *Cache[K, V]
	l2 *Cache[K, V]

	l1Hits     atomic.Uint64
	l2Hits     atomic.Uint64
	misses     atomic.Uint64
	promotions atomic.Uint64

	closed atomic.Bool
}

// NewTiered constructs a [Tiered] cache from two [Cache] instances.
// Both must be non-nil; passing a nil cache panics. The Tiered
// wrapper does NOT take ownership of either cache for purposes of
// re-configuration, but [Tiered.Close] DOES close both — callers
// who need to keep one cache alive after Tiered.Close should not
// share it with a Tiered.
func NewTiered[K comparable, V any](l1, l2 *Cache[K, V]) *Tiered[K, V] {
	if l1 == nil {
		panic("memcache: NewTiered: l1 is nil")
	}
	if l2 == nil {
		panic("memcache: NewTiered: l2 is nil")
	}
	return &Tiered[K, V]{l1: l1, l2: l2}
}

// Get returns the cached value for key. Lookup order:
//
//  1. L1 is consulted via [Cache.Get]. On hit, return immediately.
//  2. On L1 miss, L2 is consulted via [Cache.Get]. On hit, the
//     value is written into L1 (best-effort — a failed L1 Set is
//     silently swallowed; the L2 hit is still returned) and
//     returned.
//  3. Both miss → return the zero value with ok=false.
//
// Negative-cache and refresh-ahead semantics on either underlying
// Cache are honored — those signals come through normal Get.
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
