package memcache

// evictionPolicy is the per-shard interface implemented by every
// eviction algorithm in the package. Implementations are NOT safe for
// concurrent use; the owning shard's mutex serializes all calls.
//
// The policy is informed of every cache state change so it can
// maintain its own ordering structure. When the shard exceeds its
// budget, the cache repeatedly calls Victim until the budget is met.
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
	// Used by [Cache.Resize] so capacity-aware policies (S3-FIFO,
	// TinyLFU, 2Q, ARC) can recompute their internal sub-budgets.
	// Simple policies (LRU, LFU, FIFO) treat this as a no-op.
	//
	// Implementations are responsible for keeping their internal
	// sub-budgets consistent — they may NOT cause an immediate
	// eviction; that remains the cache's responsibility through
	// subsequent calls to Victim.
	SetBudget(budget int)
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
