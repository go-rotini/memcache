package memcache

import (
	"sync"
	"sync/atomic"
)

// shard is one of the cache's hash-routed partitions. shard.mu protects
// all mutable state; the hot path takes a single Lock/RLock and the
// package never nests shard locks.
type shard[K comparable, V any] struct {
	mu sync.RWMutex
	// storage is the entry-table abstraction. Default is a
	// [mapStore] over `map[K]*entry[K, V]`; opt-in alternatives
	// (currently [flatStore] via [WithFlatStorage]) implement the
	// same [shardStore] interface.
	storage shardStore[K, V]
	policy  evictionPolicy[K, V]
	pool    *entryPool[K, V]

	// ttl tracks pending expirations; heap-backed by default, wheel-backed
	// opt-in via [WithTTLBuckets]. Always non-nil after newShard.
	ttl ttlBackend[K, V]

	// janitor coordinates the per-shard expiry sweep goroutine.
	// Started lazily when the first TTL'd entry is inserted; stopped
	// when the cache is closed.
	janitor janitorState

	// inflight tracks in-progress Loader invocations for
	// singleflight semantics. Entries are removed by the loader
	// goroutine after it stores the result.
	inflight map[K]*flightCall[V]

	// orphans holds loads whose waiters all gave up, so a write to the
	// key can still stop them storing a stale result. Created lazily.
	orphans map[K]*flightCall[V]

	// errors caches Loader errors when [WithErrorTTL] is enabled.
	// Negative-cache (ErrNotFound) tombstones live on the storage
	// table with flagNegative set, NOT here.
	errors map[K]*cachedError

	// budget is the per-shard target entry count: the global
	// maxEntries divided across all shards, plus a 10% slop for hash
	// skew. It sizes the policy's queues and keeps one shard from
	// growing far past its share; the global maxEntries is enforced
	// separately, cache-wide.
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

	// read is the lock-free read snapshot, replaced wholesale on
	// promotion; the underlying readMap is immutable.
	read atomic.Pointer[readMap[K, V]]

	// readMisses counts how many times reads observed a miss in
	// the snapshot but a hit (or amended state) in dirty since the
	// last promotion. Used to drive lazy snapshot rebuilding.
	readMisses atomic.Int64

	// watches holds the [watchToken] of each key a [Tiered] promotion
	// or a Store read is copying, so a write to the key can void the
	// copy. Created lazily.
	watches map[K]*watchToken

	// keyLocks serializes, per key, an in-memory write with its Store
	// write, so writes to one key reach both in the same order. Only
	// used with [WithStore].
	keyLocks keyLockTable[K]

	// storeReads holds the Store read in progress for each key, so
	// concurrent read-throughs of the key share one Store call. Only
	// used with [WithStore]; created lazily.
	storeReads map[K]*storeRead[V]

	// computing holds the goroutine ID of the Compute-family call that
	// holds mu while running user code, or 0. A nested call from that
	// goroutine panics instead of deadlocking on mu.
	computing atomic.Uint64

	// pendingCallbacks queues user-callback closures (OnEvict, OnExpire,
	// events, invalidation) deferred from removeLocked; they fire after
	// the shard lock is released to prevent re-entry deadlock.
	pendingCallbacks []func()
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

// keyLockTable holds a mutex for each key that is locked or awaited.
// The zero value is ready to use.
type keyLockTable[K comparable] struct {
	mu sync.Mutex
	m  map[K]*keyLock
}

// keyLock is one key's entry in a keyLockTable; refs counts the
// goroutines holding or waiting for it.
type keyLock struct {
	mu   sync.Mutex
	refs int
}

// lock locks key and returns the unlock.
func (t *keyLockTable[K]) lock(key K) func() {
	t.mu.Lock()
	if t.m == nil {
		t.m = make(map[K]*keyLock)
	}
	l := t.m[key]
	if l == nil {
		l = &keyLock{}
		t.m[key] = l
	}
	l.refs++
	t.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		t.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(t.m, key)
		}
		t.mu.Unlock()
	}
}

// noUnlock is lockKey's result when there is nothing to unlock.
func noUnlock() {}

// lockKey serializes the caller with every other Store-backed write of
// key and returns the unlock. Without [WithStore] it does nothing. Take
// it before the shard lock and release it after the Store call; collect
// the callbacks of every shard release in between with
// [Cache.releaseShardInto] and fire them only after unlocking, so a hook
// that writes the key doesn't block on this lock.
func (c *Cache[K, V]) lockKey(key K) func() {
	if c.store == nil {
		return noUnlock
	}
	return c.shardFor(key).keyLocks.lock(key)
}
