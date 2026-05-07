package memcache

import (
	"sync"
	"sync/atomic"
)

// shard is one of the cache's hash-routed partitions. Each shard owns
// its own storage, eviction policy, and sync.Pool of entries;
// cross-shard coordination is the cache-level concern (tags,
// snapshots, stats).
//
// Concurrency: shard.mu protects all mutable state. The hot path
// takes a single Lock or RLock — there are no nested shard locks
// taken anywhere in the package.
type shard[K comparable, V any] struct {
	mu sync.RWMutex
	// storage is the entry-table abstraction. Default is a
	// [mapStore] over `map[K]*entry[K, V]`; opt-in alternatives
	// (currently [flatStore] via [WithFlatStorage]) implement the
	// same [shardStore] interface.
	storage shardStore[K, V]
	policy  evictionPolicy[K, V]
	pool    *entryPool[K, V]

	// ttl tracks pending expirations. Concrete type depends on
	// [WithTTLBuckets] — the heap-backed backend is the default;
	// the wheel-backed backend is opt-in via WithTTLBuckets.
	// Always non-nil after newShard.
	ttl ttlBackend[K, V]

	// janitor coordinates the per-shard expiry sweep goroutine.
	// Started lazily when the first TTL'd entry is inserted; stopped
	// when the cache is closed.
	janitor janitorState

	// inflight tracks in-progress Loader invocations for
	// singleflight semantics. Entries are removed by the loader
	// goroutine after it stores the result.
	inflight map[K]*flightCall[V]

	// errors caches Loader errors when [WithErrorTTL] is enabled.
	// Negative-cache (ErrNotFound) tombstones live on the storage
	// table with flagNegative set, NOT here.
	errors map[K]*cachedError

	// budget is the per-shard target entry count. The cache divides
	// the global maxEntries across all shards (with a 10% slop) so
	// that one shard exceeding budget evicts only its own entries,
	// not from other shards.
	budget int

	// hashIndex is populated only when [WithCollisionTracking] is
	// enabled. It maps the cache's hasher output to the most recent
	// key seen at that hash; on insert, a different key at the same
	// hash bumps Stats.HashCollisions. Accessed under shard.mu.
	hashIndex map[uint64]any

	// pending is populated only when [WithAsyncWrites] is enabled.
	// Keys map to the latest queued [pendingOp]; coalescing turns
	// rapid same-key churn into a single applied op. Read paths
	// consult this map under shard.mu.RLock before falling through
	// to storage; the apply goroutine drains it under shard.mu.Lock.
	// nil when async writes are off.
	pending map[K]pendingOp[K, V]

	// read is the lock-free read snapshot of this shard's storage,
	// published behind an atomic pointer. nil when [WithLockFreeRead]
	// is off; non-nil (possibly empty) when the option is on. The
	// pointer is replaced wholesale on promotion — the underlying
	// readMap is never mutated after construction.
	read atomic.Pointer[readMap[K, V]]

	// readMisses counts how many times reads observed a miss in
	// the snapshot but a hit (or amended state) in dirty since the
	// last promotion. Used to drive lazy snapshot rebuilding.
	readMisses atomic.Int64
}

// newShard constructs a shard with the given policy, budget, TTL
// backend, and storage. trackCollisions opts the shard into the
// per-insert [WithCollisionTracking] check.
func newShard[K comparable, V any](p evictionPolicy[K, V], budget int, trackCollisions bool, ttl ttlBackend[K, V], storage shardStore[K, V]) *shard[K, V] {
	s := &shard[K, V]{
		storage:  storage,
		policy:   p,
		pool:     newEntryPool[K, V](),
		budget:   budget,
		inflight: make(map[K]*flightCall[V]),
		errors:   make(map[K]*cachedError),
		ttl:      ttl,
	}
	if trackCollisions {
		s.hashIndex = make(map[uint64]any)
	}
	return s
}
