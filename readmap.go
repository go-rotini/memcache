package memcache

// readMap is the immutable read-side snapshot of a shard's storage,
// published behind an [atomic.Pointer] so [Cache.Get] can consult
// it without taking any shard lock. It is replaced wholesale on
// promotion — never mutated after construction.
//
// `amended` is true when the live shard storage holds at least one
// key not present in this snapshot, signaling readers that a miss
// must fall through to the locked path.
type readMap[K comparable, V any] struct {
	m       map[K]*entry[K, V]
	amended bool
}

// newEmptyReadMap returns an empty, non-amended snapshot. Used as
// the initial value when [WithLockFreeRead] is enabled so the very
// first Get can load a non-nil pointer without a nil check.
func newEmptyReadMap[K comparable, V any]() *readMap[K, V] {
	return &readMap[K, V]{m: nil, amended: false}
}

// readMissThreshold returns the miss count at which a shard
// promotes its live storage into a fresh read snapshot. We base
// the threshold on the SNAPSHOT's size rather than the dirty's
// because the snapshot is immutable (safe to read without a lock)
// while the dirty map can race with concurrent writers. The
// heuristic still tracks the relevant ratio: if a snapshot of N
// entries has been missed N times, the dirty has drifted enough
// to justify rebuilding.
func (s *shard[K, V]) readMissThreshold() int64 {
	rm := s.read.Load()
	if rm == nil {
		return 8
	}
	n := int64(len(rm.m))
	if n < 8 {
		return 8
	}
	return n
}

// markAmended is called from the write path under shard.Lock when
// a key is inserted that wasn't previously in the read snapshot.
// Idempotent — once true the flag stays true until the next
// promotion. Storing a fresh `*readMap` with amended=true keeps
// the snapshot's mutability invariant: we never mutate the
// existing struct.
func (s *shard[K, V]) markAmended() {
	cur := s.read.Load()
	if cur == nil || cur.amended {
		return
	}
	s.read.Store(&readMap[K, V]{m: cur.m, amended: true})
}

// recordReadMiss bumps the per-shard miss counter ONLY when the
// snapshot truly didn't have the key — not when the snapshot had
// it but the fast path declined (e.g. policy promotion needed).
// Promotion fires once the threshold is crossed.
func (c *Cache[K, V]) recordReadMiss(s *shard[K, V], snapshotHadKey bool) {
	if s.read.Load() == nil || snapshotHadKey {
		return
	}
	if s.readMisses.Add(1) >= s.readMissThreshold() {
		c.promoteReadMap(s)
	}
}

// snapshotHasKey reports whether the live read snapshot contains
// key. Cheap atomic load + map lookup; safe to call from any
// goroutine without a lock.
func (s *shard[K, V]) snapshotHasKey(key K) bool {
	rm := s.read.Load()
	if rm == nil {
		return false
	}
	_, ok := rm.m[key]
	return ok
}

// promoteReadMap rebuilds the shard's read snapshot from its live
// storage and atomically publishes it. The shard write lock is
// taken so the storage iteration and snapshot construction see a
// consistent view; existing readers holding the previous snapshot
// see no change until they re-Load.
//
// Entries that are already invalidated (removed from storage but
// still tracked by the previous snapshot) are excluded from the
// new snapshot. Entries surviving into the new snapshot have their
// invalidatedFlag left as-is; a fresh insert resets it via
// upsertLocked, while an in-place update keeps it cleared.
func (c *Cache[K, V]) promoteReadMap(s *shard[K, V]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.promoteReadMapLocked(s)
}

// promoteReadMapLocked is the under-the-shard-lock body of
// promoteReadMap. Exposed separately so write paths that already
// hold the lock can promote without re-acquiring.
//
// Idempotent: if the snapshot is already non-amended and no fresh
// misses have accumulated, the function returns without rebuilding.
// This shields the lock from a thundering-herd of concurrent
// promote callers when several Gets cross the miss threshold at
// once.
func (c *Cache[K, V]) promoteReadMapLocked(s *shard[K, V]) {
	cur := s.read.Load()
	if cur == nil {
		return
	}
	if !cur.amended && s.readMisses.Load() == 0 {
		return
	}
	m := make(map[K]*entry[K, V], s.storage.length())
	s.storage.each(func(e *entry[K, V]) bool {
		m[e.key] = e
		return true
	})
	s.read.Store(&readMap[K, V]{m: m, amended: false})
	s.readMisses.Store(0)
}

// maybeInitReadSnapshot installs an empty [readMap] on every
// shard when [WithLockFreeRead] is enabled, so the lock-free Get
// path can blindly Load a non-nil pointer. Factored out of [build]
// to keep that function under the project's funlen budget.
func (c *Cache[K, V]) maybeInitReadSnapshot(cfg *config) {
	if !cfg.lockFreeRead {
		return
	}
	for _, s := range c.shards {
		s.read.Store(newEmptyReadMap[K, V]())
	}
}

// maybePromoteOnSlowHit bumps the under-the-snapshot miss counter
// from inside the shard write lock and triggers promotion in place
// when the threshold trips. No-op when [WithLockFreeRead] is off
// or when the snapshot already had the key (the slow path was
// taken for policy reasons, not snapshot drift).
// Caller holds shard.mu.
func (c *Cache[K, V]) maybePromoteOnSlowHit(s *shard[K, V], key K) {
	if !c.cfg.lockFreeRead {
		return
	}
	if s.snapshotHasKey(key) {
		return
	}
	if s.readMisses.Add(1) >= s.readMissThreshold() {
		c.promoteReadMapLocked(s)
	}
}

// tryServeFromReadSnapshot is the [Cache.getCtx] entry point for
// the lock-free fast path. Returns (val, hit, terminal) where
// `terminal` is true when the snapshot answer is authoritative —
// the caller can return immediately. When `terminal` is false the
// caller must continue to the locked path.
func (c *Cache[K, V]) tryServeFromReadSnapshot(s *shard[K, V], key K, now int64) (V, bool, bool) {
	var zero V
	if s.read.Load() == nil {
		return zero, false, false
	}
	val, hit, decisive := c.tryLockFreeGet(s, key, now)
	if !decisive {
		return zero, false, false
	}
	if hit {
		c.recordHitObserve(key)
		c.fireHit(key, val)
		return c.returnValue(val), true, true
	}
	c.recordMiss()
	c.fireMiss(key)
	return zero, false, true
}

// tryLockFreeGet attempts to serve key from the read snapshot
// without any shard lock. Returns (value, true, true) on a fast-
// path hit, (zero, false, true) when the snapshot guaranteed a
// miss (no key, snapshot not amended), and (zero, false, false)
// when the caller must fall through to the locked path.
//
// Fast-path conditions mirror the existing slow path: entry must
// be non-expired, non-negative, non-invalidated, non-sliding, no
// refresh-ahead window, and the policy must not need promotion.
// Anything else routes through the lock so policy state can be
// updated correctly.
//
// Hits bump e.hits atomically — the existing fast path already
// does this, and atomic adds are safe regardless of any concurrent
// writer that might be mutating other fields under the shard lock.
func (c *Cache[K, V]) tryLockFreeGet(s *shard[K, V], key K, now int64) (V, bool, bool) {
	var zero V
	rm := s.read.Load()
	if rm == nil {
		return zero, false, false
	}
	e, ok := rm.m[key]
	if !ok {
		if !rm.amended {
			// The snapshot is authoritative — this is a true miss.
			return zero, false, true
		}
		// The snapshot is missing keys present in the live shard;
		// caller must check storage under the lock.
		return zero, false, false
	}
	if e.invalidated() {
		// Entry was removed from storage; treat as a true miss
		// and route the caller to the locked path so a fresh
		// promote can drop the stale entry from the snapshot.
		return zero, false, false
	}
	if c.entryExpiredLocked(e, now) {
		return zero, false, false
	}
	if e.flags.has(flagNegative) || e.flags.has(flagSliding) {
		return zero, false, false
	}
	if s.policy.PromotionNeeded(e) || c.shouldRefreshAhead(e, now) {
		return zero, false, false
	}
	e.hits.Add(1)
	return e.loadValue(), true, true
}
