package memcache

// evictionPolicy is the per-shard interface implemented by every
// eviction algorithm in the package. Implementations are NOT safe for
// concurrent use; the owning shard's mutex serializes all calls.
//
// The policy is informed of every cache state change so it can
// maintain its own ordering structure. When the shard exceeds its
// budget, the cache repeatedly calls Victim until the budget is met.
//
// The interface intentionally exceeds the linter's 8-method
// preference: each method covers a distinct responsibility (insert,
// access, update, remove, victim selection, fast-path eligibility,
// budget reconfiguration, capacity introspection, diagnostics,
// reset) and splitting them produces interface acrobatics with no
// composition benefit.
//
//nolint:interfacebloat // see comment above
type evictionPolicy[K comparable, V any] interface {
	// OnInsert is called after a new entry is added to the shard's
	// map.
	OnInsert(e *entry[K, V])

	// OnAccess is called when an existing entry is read (Get hit).
	// Implementations that promote on access (LRU, LFU, S3-FIFO)
	// adjust their state here.
	OnAccess(e *entry[K, V])

	// OnUpdate is called when an existing key has its value
	// replaced. Most policies treat this identically to OnAccess.
	OnUpdate(e *entry[K, V])

	// OnRemove is called after the entry is removed from the shard's
	// map (whether by Delete, expiry, or eviction). Implementations
	// must drop any internal references to the entry.
	OnRemove(e *entry[K, V])

	// Victim returns the next entry the policy would evict, or nil
	// when the policy has no more entries to give up. Calling Victim
	// does NOT remove the entry from the shard's map; the caller is
	// responsible for completing the eviction.
	Victim() *entry[K, V]

	// Len returns the number of entries currently tracked by the
	// policy. Used by sanity checks; the cache's authoritative count
	// is the shard's map.
	Len() int

	// Reset clears all policy state. Used by [Cache.Reset] and
	// [Cache.Clear].
	Reset()

	// SetBudget updates the policy's notion of capacity at runtime.
	// Capacity-aware policies (S3-FIFO, TinyLFU, 2Q, ARC) recompute
	// sub-budgets; simple policies treat this as a no-op.
	// Implementations MUST NOT trigger immediate eviction; the cache
	// drives that through subsequent Victim calls.
	SetBudget(budget int)

	// Snapshot returns a per-policy diagnostic struct describing the
	// policy's current state (queue sizes, sketch fill, etc.).
	// Surfaced via [Stats.PolicyDetail] for `:cache stats`-style
	// REPL diagnostics. Implementations should return concrete
	// structs by value; nil is acceptable when the policy has no
	// useful detail to report. Called under the shard's read lock.
	Snapshot() any

	// PromotionNeeded reports whether OnAccess(e) would mutate state.
	// Returning false lets [Cache.Get] complete the hit under a read
	// lock. Called under the shard's read lock; implementations should
	// err on the side of returning true (false positives only cost an
	// unnecessary write lock).
	PromotionNeeded(e *entry[K, V]) bool
}

// PolicyDetailLRU is the [Stats.PolicyDetail] value for [PolicyLRU].
// Size is the shard's entry count.
type PolicyDetailLRU struct {
	Size int
}

// PolicyDetailFIFO is the [Stats.PolicyDetail] value for [PolicyFIFO].
type PolicyDetailFIFO struct {
	Size int
}

// PolicyDetailLFU is the [Stats.PolicyDetail] value for [PolicyLFU].
// DistinctBuckets is the number of populated frequency buckets.
type PolicyDetailLFU struct {
	Size            int
	DistinctBuckets int
}

// PolicyDetailS3FIFO is the [Stats.PolicyDetail] value for
// [PolicyS3FIFO]: the entry counts and budgets of the small and main
// queues, and the size of the key-only ghost queue.
type PolicyDetailS3FIFO struct {
	SmallSize, MainSize, GhostSize int
	SmallBudget, MainBudget        int
}

// PolicyDetailTinyLFU is the [Stats.PolicyDetail] value for
// [PolicyTinyLFU]. SketchOps counts frequency-sketch increments since
// the sketch was last aged.
type PolicyDetailTinyLFU struct {
	WindowSize, MainSize int
	SketchOps            uint64
}

// PolicyDetail2Q is the [Stats.PolicyDetail] value for [Policy2Q].
type PolicyDetail2Q struct {
	A1inSize, AmSize, A1outSize int
}

// PolicyDetailARC is the [Stats.PolicyDetail] value for [PolicyARC]:
// the sizes of the T1/T2 lists and B1/B2 ghost lists, and P, the
// adaptive target size of T1.
type PolicyDetailARC struct {
	T1Size, T2Size, B1Size, B2Size int
	P                              int
}

// policyConfig bundles the construction-time parameters that
// individual eviction policies may consume.
//
// `budget` is the shard's target entry count (0 for byte-bounded or
// unbounded caches). Capacity-aware policies (S3-FIFO, TinyLFU, 2Q,
// ARC) use it to derive their internal size splits; simple policies
// (LRU, LFU, FIFO) ignore it.
//
// `hasher` is the typed key hasher used by frequency-sketch-backed
// policies (TinyLFU). Policies that don't need it ignore the field.
type policyConfig[K comparable] struct {
	budget int
	hasher func(K) uint64
}

// newPolicy constructs the eviction policy implementation for the
// requested [Policy] enum. The returned implementation is fresh; each
// shard owns its own. Unrecognized policies fall back to LRU so the
// cache remains usable.
func newPolicy[K comparable, V any](p Policy, cfg policyConfig[K]) evictionPolicy[K, V] {
	switch p {
	case PolicyLRU:
		return newLRU[K, V]()
	case PolicyFIFO:
		return newFIFO[K, V]()
	case PolicyS3FIFO:
		return newS3FIFO[K, V](cfg.budget)
	case PolicyLFU:
		return newLFU[K, V]()
	case PolicyTinyLFU:
		return newTinyLFU[K, V](cfg.budget, cfg.hasher)
	case Policy2Q:
		return newTwoQ[K, V](cfg.budget)
	case PolicyARC:
		return newARC[K, V](cfg.budget)
	default:
		return newLRU[K, V]()
	}
}

// shedVictim returns an entry to evict when the cache is over its byte
// budget. Unlike Victim it ignores the policy's entry sub-budgets, so it
// yields a candidate whenever the policy holds one. With keepRecent,
// S3-FIFO still refuses to give up the entry inserted by the current
// Set.
func shedVictim[K comparable, V any](p evictionPolicy[K, V], keepRecent bool) *entry[K, V] {
	if v := p.Victim(); v != nil {
		return v
	}
	switch p := p.(type) {
	case *s3fifoPolicy[K, V]:
		return p.shed(keepRecent)
	case *tinyLFUPolicy[K, V]:
		return p.shed()
	}
	return nil
}
