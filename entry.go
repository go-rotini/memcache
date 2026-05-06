package memcache

import (
	"sync"
	"sync/atomic"
	"time"
)

// entry is the internal record stored in a shard. Field order is chosen
// to keep frequently-accessed fields (key, value, expireAt) on the same
// cache line.
//
// Concurrency: an entry's mutable fields are protected by the owning
// shard's mutex. Atomic loads/stores are used for hits/lastAccess so
// the read path can update them while holding only the shard read lock.
type entry[K comparable, V any] struct {
	// Identity
	key K

	// Value
	value V

	// Lifecycle timestamps (unix nanos; 0 means "no TTL" for expireAt).
	expireAt   atomic.Int64
	lastAccess atomic.Int64
	inserted   int64

	// Hit count since insertion. Atomic-updated.
	hits atomic.Uint32

	// Generation increments on every mutation that is not a pure read;
	// used by refresh-ahead and singleflight to detect "value changed
	// since I started loading."
	generation atomic.Uint32

	// Cost / weight of this entry per the cache's Weigher (clamped to
	// >= 1). Captured at insert time; changes to the underlying value
	// do not update this field.
	weight int64

	// Sliding TTL duration (only meaningful if flags has flagSliding
	// set). Stored so the read path can compute the new expireAt
	// without consulting the cache config.
	slidingTTL int64

	// heapIndex is the entry's position in its shard's expiry heap,
	// or -1 when the entry has no TTL (and therefore is not tracked
	// by the heap). Maintained by the heap's Swap/Push/Pop methods.
	// Used only by the heap-based ttlBackend; nil-valued (-1) when
	// the wheel-based backend is active.
	heapIndex int

	// wheelHandle is the back-pointer to the wheel entry tracking
	// this expiration. Used only by the wheel-based ttlBackend
	// ([WithTTLBuckets]); nil when the heap-based backend is
	// active. The field is `any` rather than the concrete wheel
	// type to keep entry independent of the wheel package — the
	// wheel backend type-asserts to recover the *wheel.Entry.
	wheelHandle any

	// Tags (nil if untagged).
	tags []string

	// Policy hookup. The eviction-policy implementation may store a
	// node pointer or index here. Opaque to the cache.
	policyData any

	// Bit flags (saves memory vs separate bool fields).
	flags entryFlags
}

// entryFlags is a bit field of per-entry state.
type entryFlags uint8

const (
	// flagSliding marks the entry as having sliding-TTL semantics.
	flagSliding entryFlags = 1 << iota
	// flagNegative marks the entry as a negative-cache tombstone (the
	// value field is the zero V; gets return ErrNotFound).
	flagNegative
)

// has reports whether all f bits are set.
func (e entryFlags) has(f entryFlags) bool { return e&f == f }

// expired reports whether the entry's absolute expiry time has passed.
// now is unix nanos. Returns false for entries with no TTL (expireAt == 0).
func (e *entry[K, V]) expired(now int64) bool {
	exp := e.expireAt.Load()
	return exp != 0 && now >= exp
}

// touchAccess updates lastAccess. For sliding-TTL entries it also
// pushes the expireAt forward to `now + slidingTTL`. To avoid a
// write-storm on hot keys, the sliding refresh is COALESCED: it
// only fires when the access is more than `slidingTTL/4` newer than
// the previously recorded lastAccess. Non-sliding entries always
// update lastAccess (the cost is one atomic store per Get).
//
// Returns true when the entry's expireAt was actually moved — the
// caller can use this signal to drive [Cache.expiryFix].
//
// Multiple concurrent readers may race here under the shard read
// lock; the race is benign because all racers write approximately
// the same value.
func (e *entry[K, V]) touchAccess(nowNanos int64) (expiryShifted bool) {
	if !e.flags.has(flagSliding) || e.slidingTTL <= 0 {
		e.lastAccess.Store(nowNanos)
		return false
	}
	prev := e.lastAccess.Load()
	if nowNanos-prev < e.slidingTTL/4 {
		// Access too close to the previous one — skip the
		// expireAt write entirely. Sliding semantics are
		// preserved because the recorded expireAt already
		// covers `now`. (If prev == 0 this is the post-insert
		// read, which is by definition close to insertion and
		// safe to coalesce.)
		return false
	}
	e.lastAccess.Store(nowNanos)
	e.expireAt.Store(nowNanos + e.slidingTTL)
	return true
}

// metadata returns a snapshot of the entry's metadata for read-only
// consumers (Range, ItemMetadata, snapshots).
func (e *entry[K, V]) metadata() Metadata {
	expNanos := e.expireAt.Load()
	var expiry time.Time
	if expNanos != 0 {
		expiry = time.Unix(0, expNanos)
	}
	var lastAccess time.Time
	if la := e.lastAccess.Load(); la != 0 {
		lastAccess = time.Unix(0, la)
	}
	tags := e.tags
	if len(tags) > 0 {
		// Defensive copy to keep callers from racing with InvalidateTag.
		dup := make([]string, len(tags))
		copy(dup, tags)
		tags = dup
	}
	return Metadata{
		Expiry:     expiry,
		Inserted:   time.Unix(0, e.inserted),
		LastAccess: lastAccess,
		Hits:       e.hits.Load(),
		Weight:     e.weight,
		Tags:       tags,
		Sliding:    e.flags.has(flagSliding),
	}
}

// reset zeros out the entry's mutable fields so it can be returned to a
// sync.Pool. Pointer-bearing fields are explicitly cleared so the GC can
// reclaim the prior value.
func (e *entry[K, V]) reset() {
	var zeroK K
	var zeroV V
	e.key = zeroK
	e.value = zeroV
	e.expireAt.Store(0)
	e.inserted = 0
	e.lastAccess.Store(0)
	e.hits.Store(0)
	e.generation.Store(0)
	e.weight = 0
	e.tags = nil
	e.flags = 0
	e.slidingTTL = 0
	e.heapIndex = -1
	e.policyData = nil
}

// entryPool is a per-cache sync.Pool of entries. Allocating a fresh pool
// per cache (rather than sharing one across all caches in a process)
// keeps entries of one Cache[K, V] instantiation from leaking into
// another.
type entryPool[K comparable, V any] struct {
	pool sync.Pool
}

// newEntryPool constructs an empty entry pool.
func newEntryPool[K comparable, V any]() *entryPool[K, V] {
	return &entryPool[K, V]{
		pool: sync.Pool{
			New: func() any { return &entry[K, V]{heapIndex: -1} },
		},
	}
}

// get returns a fresh or recycled entry. heapIndex is reset to -1
// so the entry begins life "not in the expiry heap".
func (p *entryPool[K, V]) get() *entry[K, V] {
	e, _ := p.pool.Get().(*entry[K, V])
	if e == nil {
		return &entry[K, V]{heapIndex: -1}
	}
	e.heapIndex = -1
	return e
}

// put returns an entry to the pool after resetting it.
func (p *entryPool[K, V]) put(e *entry[K, V]) {
	if e == nil {
		return
	}
	e.reset()
	p.pool.Put(e)
}
