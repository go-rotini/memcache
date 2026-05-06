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
}

// newPolicy constructs the eviction policy implementation for the
// requested [Policy] enum. The returned implementation is fresh; each
// shard owns its own.
//
// Unsupported or not-yet-implemented policies fall back to FIFO so
// that the cache remains usable while implementations are filled in.
// New users select the policy through [WithPolicy], so this default
// behavior is documented in the package overview.
func newPolicy[K comparable, V any](p Policy) evictionPolicy[K, V] {
	switch p {
	case PolicyLRU:
		return newLRU[K, V]()
	case PolicyFIFO:
		return newFIFO[K, V]()
	case PolicyS3FIFO:
		// S3-FIFO falls back to LRU until the dedicated
		// implementation lands. Hit-rate guarantees in the spec
		// apply only after the S3-FIFO policy is enabled.
		return newLRU[K, V]()
	case PolicyLFU, PolicyTinyLFU, PolicyARC, Policy2Q:
		// Pending implementations; fall through to LRU.
		return newLRU[K, V]()
	default:
		return newLRU[K, V]()
	}
}
