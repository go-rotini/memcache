package memcache

// gateLockFreeRead disables [WithLockFreeRead] when the configuration
// would make the fast path unsafe; logs an info message instead of
// failing [New].
func gateLockFreeRead(cfg *config) {
	if !cfg.lockFreeRead || lockFreeReadSupported(cfg) {
		return
	}
	if cfg.logger != nil {
		cfg.logger.Info("memcache: WithLockFreeRead disabled (requires S3-FIFO policy and no WithExpireFunc)")
	}
	cfg.lockFreeRead = false
}

// lockFreeReadSupported reports whether the cache can safely run the
// [WithLockFreeRead] fast path. Requires PolicyS3FIFO (the only policy
// with atomic per-entry state for PromotionNeeded) and no WithExpireFunc.
func lockFreeReadSupported(cfg *config) bool {
	if cfg.policy != PolicyS3FIFO {
		return false
	}
	if cfg.expireFunc != nil {
		return false
	}
	return true
}

// readMap is the immutable read-side snapshot of a shard's storage,
// published behind an atomic.Pointer. Replaced wholesale on promotion;
// never mutated after construction. amended=true means a miss must fall
// through to the locked path.
type readMap[K comparable, V any] struct {
	m       map[K]*entry[K, V]
	amended bool
}

func newEmptyReadMap[K comparable, V any]() *readMap[K, V] {
	return &readMap[K, V]{m: nil, amended: false}
}

// readMissThreshold returns the miss count for promoting the live
// storage into a fresh snapshot; based on the (immutable) snapshot size.
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

// markAmended must be called under shard.Lock when a key is inserted
// that wasn't in the snapshot. Stores a fresh *readMap with amended=true
// to preserve the snapshot's immutability invariant.
func (s *shard[K, V]) markAmended() {
	cur := s.read.Load()
	if cur == nil || cur.amended {
		return
	}
	s.read.Store(&readMap[K, V]{m: cur.m, amended: true})
}

// recordReadMiss bumps the per-shard miss counter only when the snapshot
// truly didn't have the key. Triggers promotion once the threshold trips.
func (c *Cache[K, V]) recordReadMiss(s *shard[K, V], snapshotHadKey bool) {
	if s.read.Load() == nil || snapshotHadKey {
		return
	}
	if s.readMisses.Add(1) >= s.readMissThreshold() {
		c.promoteReadMap(s)
	}
}

// snapshotHasKey reports whether the live snapshot contains key. Cheap
// atomic load + map lookup; lock-free.
func (s *shard[K, V]) snapshotHasKey(key K) bool {
	rm := s.read.Load()
	if rm == nil {
		return false
	}
	_, ok := rm.m[key]
	return ok
}

// promoteReadMap rebuilds the shard's read snapshot under s.mu.Lock and
// atomically publishes it.
func (c *Cache[K, V]) promoteReadMap(s *shard[K, V]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.promoteReadMapLocked(s)
}

// promoteReadMapLocked is promoteReadMap under s.mu. Idempotent so
// concurrent callers across the miss threshold don't thunder.
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

func (c *Cache[K, V]) maybeInitReadSnapshot(cfg *config) {
	if !cfg.lockFreeRead {
		return
	}
	for _, s := range c.shards {
		s.read.Store(newEmptyReadMap[K, V]())
	}
}

// maybePromoteOnSlowHit bumps the miss counter and may trigger
// promotion. Caller MUST hold s.mu.
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

// tryServeFromReadSnapshot is the lock-free entry point. Returns
// terminal=true when the snapshot answer is authoritative.
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

// tryLockFreeGet serves key from the read snapshot without a lock.
// Returns terminal=false when the caller must fall through to the locked
// path (snapshot amended, entry invalidated/expired/negative/sliding,
// refresh-ahead due, or policy promotion needed).
//
// e.flags is a non-atomic uint8; the read is tear-free on supported
// architectures and gated to configurations where flag mutations are
// extremely rare (S3-FIFO + no WithExpireFunc).
func (c *Cache[K, V]) tryLockFreeGet(s *shard[K, V], key K, now int64) (V, bool, bool) {
	var zero V
	rm := s.read.Load()
	if rm == nil {
		return zero, false, false
	}
	e, ok := rm.m[key]
	if !ok {
		if !rm.amended {
			// Snapshot is authoritative; true miss.
			return zero, false, true
		}
		// Snapshot is missing keys present in the live shard.
		return zero, false, false
	}
	if e.invalidated() {
		// Entry removed from storage; route to locked path so a fresh
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
