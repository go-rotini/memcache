package memcache

import "sync"

// shard is one of the cache's hash-routed partitions. Each shard owns
// its own map, eviction policy, and sync.Pool of entries; cross-shard
// coordination is the cache-level concern (tags, snapshots, stats).
//
// Concurrency: shard.mu protects all mutable state. The hot path
// takes a single Lock or RLock — there are no nested shard locks
// taken anywhere in the package.
type shard[K comparable, V any] struct {
	mu      sync.RWMutex
	entries map[K]*entry[K, V]
	policy  evictionPolicy[K, V]
	pool    *entryPool[K, V]

	// budget is the per-shard target entry count. The cache divides
	// the global maxEntries across all shards (with a 10% slop) so
	// that one shard exceeding budget evicts only its own entries,
	// not from other shards.
	budget int
}

// newShard constructs a shard with the given policy and budget.
func newShard[K comparable, V any](p evictionPolicy[K, V], budget int) *shard[K, V] {
	return &shard[K, V]{
		entries: make(map[K]*entry[K, V]),
		policy:  p,
		pool:    newEntryPool[K, V](),
		budget:  budget,
	}
}
