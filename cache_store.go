package memcache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

//nolint:nilnil // (nil, nil) signals no Store configured.
func resolveStore[K comparable, V any](raw any) (Store[K, V], error) {
	if raw == nil {
		return nil, nil
	}
	s, ok := raw.(Store[K, V])
	if !ok {
		return nil, &ConfigError{
			Field:   "Store",
			Message: fmt.Sprintf("type mismatch: store does not match cache K/V (%T)", raw),
		}
	}
	return s, nil
}

// storeRead is one Store read shared by every concurrent read-through
// of a key. Its result fields are written before done is closed; done is
// made, under the shard lock, only once a second reader joins.
type storeRead[V any] struct {
	done chan struct{}
	// watch is superseded by any write of the key after the read
	// began; a superseded read is neither joined nor promoted. It is
	// own unless the key was already watched.
	watch *watchToken
	own   watchToken
	value V
	ok    bool
	err   error
	// aborted is set when the Store call panicked, and canceled when
	// the leader's ctx ended before the read finished; waiters read
	// again rather than share a result that is the leader's alone.
	aborted  bool
	canceled bool
}

// readThroughStore is consulted by Get-style methods after an in-memory
// miss. A Store hit is promoted into the in-memory cache and returned.
// Concurrent calls for a key share one Store read; each waits only as
// long as its own ctx allows.
func (c *Cache[K, V]) readThroughStore(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if c.store == nil {
		return zero, false, nil
	}
	s := c.shardFor(key)
	for {
		s.mu.Lock()
		if r := s.storeReads[key]; r != nil && !r.watch.superseded {
			if r.done == nil {
				r.done = make(chan struct{})
			}
			done := r.done
			s.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return zero, false, ctx.Err() //nolint:wrapcheck // pass ctx.Err verbatim
			}
			if r.aborted || (r.canceled && r.err != nil && ctx.Err() == nil) {
				// The read ended for its leader's own reason, not this
				// caller's; read again. A Store error, including one
				// from the Store's own deadline, is shared.
				continue
			}
			if r.err != nil || !r.ok {
				return zero, false, r.err
			}
			return c.returnValue(r.value), true, nil
		}
		r := &storeRead[V]{}
		r.watch = s.watchLockedSpare(key, &r.own)
		if s.storeReads == nil {
			s.storeReads = make(map[K]*storeRead[V])
		}
		s.storeReads[key] = r
		s.mu.Unlock()
		return c.leadStoreRead(ctx, s, key, r)
	}
}

// leadStoreRead runs the Store read r for key and promotes a hit, unless
// a write of the key superseded r or memory gained a fresh entry for it
// meanwhile: either one is newer than the value read. The watch, not the
// key's lock, keeps the read ordered with writes, so readers never wait
// behind a write's Store call or each other.
func (c *Cache[K, V]) leadStoreRead(ctx context.Context, s *shard[K, V], key K, r *storeRead[V]) (V, bool, error) {
	var zero V
	finished := false
	defer func() {
		if !finished {
			s.mu.Lock()
			r.aborted = true
			done := c.endStoreReadLocked(s, key, r)
			s.mu.Unlock()
			if done != nil {
				close(done)
			}
		}
	}()
	value, ok, expireAt, err := c.storeGet(ctx, key)
	var p promotion[V]
	if err == nil && ok {
		p = c.preparePromotion(key, value, expireAt)
	}
	var cbs []func()
	s.mu.Lock()
	if err == nil && ok && !r.watch.superseded && c.freshEntryLocked(s, key) == nil {
		c.promoteLocked(s, key, &p)
	}
	r.value, r.ok, r.err = value, ok, err
	r.canceled = ctx.Err() != nil
	done := c.endStoreReadLocked(s, key, r)
	finished = true
	c.releaseShardInto(s, &cbs)
	if done != nil {
		close(done)
	}
	c.runCallbacks(cbs, s)
	if err != nil || !ok {
		return zero, false, err
	}
	return c.returnValue(value), true, nil
}

// endStoreReadLocked unregisters r and returns the channel to close for
// its waiters, if any joined. Caller MUST hold s.mu (write).
func (c *Cache[K, V]) endStoreReadLocked(s *shard[K, V], key K, r *storeRead[V]) chan struct{} {
	if s.storeReads[key] == r {
		delete(s.storeReads, key)
	}
	s.unwatchLocked(key, r.watch)
	return r.done
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// freshEntryLocked returns key's in-memory entry if it is fresh and not
// a negative-cache tombstone, else nil. Caller MUST hold s.mu.
func (c *Cache[K, V]) freshEntryLocked(s *shard[K, V], key K) *entry[K, V] {
	e, ok := s.storage.get(key)
	if !ok || e.flags.has(flagNegative) || c.entryExpiredLocked(e, c.cfg.clock.Now().UnixNano()) {
		return nil
	}
	return e
}

// storeGet reads key from the Store with Get. On a hit from a
// [TTLStore], expireAt is when the record expires on the cache's clock,
// 0 when it doesn't, or storeRecordGone when the TTL lookup finds no
// record; otherwise, or when the TTL lookup fails, it is -1, for not
// known.
func (c *Cache[K, V]) storeGet(ctx context.Context, key K) (value V, ok bool, expireAt int64, err error) {
	value, ok, err = c.store.Get(ctx, key)
	if err != nil || !ok {
		return value, ok, -1, err
	}
	return value, true, c.storeExpireAt(ctx, key), nil
}

// storeExpireAt is the expiry of key's Store record as storeGet reports
// it. A failed lookup only loses the expiry, so its error is dropped.
func (c *Cache[K, V]) storeExpireAt(ctx context.Context, key K) int64 {
	if c.ttlStore == nil {
		return -1
	}
	ttl, found, err := c.ttlStore.TTL(ctx, key)
	switch {
	case err != nil:
		return -1
	case !found:
		return storeRecordGone
	case ttl > 0:
		return c.cfg.clock.Now().UnixNano() + int64(ttl)
	}
	return 0
}

// storeRecordGone is storeExpireAt's result when the record expired or
// was deleted between the Get and the TTL lookup.
const storeRecordGone = -2

// errStoreRecordGone declines the promotion of a value whose Store record
// is gone.
var errStoreRecordGone = errors.New("memcache: store record expired before promotion")

// promotion is a Store value checked against the cache's limits, ready
// to be inserted under the shard lock. err is why it can't be.
type promotion[V any] struct {
	value  V
	weight int64
	tags   []string
	err    error
	// expireAt is the Store record's expiry, as storeGet reports it.
	expireAt int64
}

// preparePromotion weighs and tags a Store value outside the shard lock,
// since both may run user code.
func (c *Cache[K, V]) preparePromotion(key K, value V, expireAt int64) promotion[V] {
	if expireAt == storeRecordGone {
		return promotion[V]{value: value, expireAt: -1, err: errStoreRecordGone}
	}
	p := promotion[V]{value: value, tags: c.autoTags(value), expireAt: expireAt}
	if p.err = c.checkKeySize(key); p.err != nil {
		return p
	}
	if p.weight, p.err = c.computeWeight(key, value); p.err != nil {
		return p
	}
	p.err = c.checkTagLimits(key, p.tags)
	return p
}

// promoteLocked inserts a Store value with [WithDefaultTTL], or, when
// the Store record expires sooner, with the record's expiry. Promotion
// is best-effort: a value over the cache's limits, or one admission
// turns away, is not cached. Watches on the key are left alone, since
// the value is the Store's current one rather than a newer write.
// Eviction does not cascade to the Store. Caller MUST hold s.mu (write).
func (c *Cache[K, V]) promoteLocked(s *shard[K, V], key K, p *promotion[V]) {
	if p.err != nil || c.closed.Load() {
		if p.err != nil && c.cfg.logger != nil {
			c.cfg.logger.Debug("memcache: store promotion declined by in-memory cache", "err", p.err)
		}
		return
	}
	w := s.watches[key]
	if w != nil {
		delete(s.watches, key)
	}
	if left := time.Duration(p.expireAt - c.cfg.clock.Now().UnixNano()); p.expireAt > 0 &&
		(c.cfg.defaultTTL <= 0 || left < c.cfg.defaultTTL) {
		left = max(left, 1)
		c.upsertLocked(s, key, p.value, p.weight, left, false, int64(left), p.tags)
	} else {
		c.upsertLocked(s, key, p.value, p.weight,
			c.effectiveTTL(c.cfg.defaultTTL),
			c.cfg.slidingTTL, int64(c.cfg.defaultTTL), p.tags)
	}
	if w != nil {
		s.watches[key] = w
	}
}

// currentLocked returns key's value for a method that acts on whether
// the key is present: the fresh in-memory entry e, or with a Store, the
// Store's value, promoted into memory when the cache admits it (e is nil
// when it doesn't). The Store is not read while an async op queued for
// key is newer than it. A Store read error is returned. Caller MUST hold
// key's lock and s.mu (write) and have applied key's queued op into sm;
// s.mu is released during the Store read.
func (c *Cache[K, V]) currentLocked(
	s *shard[K, V], key K, sm *storeMirror[K, V], cbs *[]func(),
) (e *entry[K, V], value V, present bool, err error) {
	var zero V
	if e := c.freshEntryLocked(s, key); e != nil {
		v := e.loadValue()
		return e, v, true, nil
	}
	if c.store == nil || sm.hasPending {
		return nil, zero, false, nil
	}
	c.releaseShardInto(s, cbs)
	// The caller's deferred release unlocks s.mu, so a panic in the
	// Store or in user code must not leave it unlocked.
	relocked := false
	defer func() {
		if !relocked {
			s.mu.Lock()
		}
	}()
	value, ok, expireAt, err := c.storeGet(context.Background(), key)
	var p promotion[V]
	if err == nil && ok {
		p = c.preparePromotion(key, value, expireAt)
	}
	s.mu.Lock()
	relocked = true
	// An async write queued meanwhile is newer than the Store's value,
	// and so is anything a loader stored.
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	if e := c.freshEntryLocked(s, key); e != nil {
		v := e.loadValue()
		return e, v, true, nil
	}
	if sm.hasPending {
		return nil, zero, false, nil
	}
	if err != nil {
		return nil, zero, false, err
	}
	if !ok {
		return nil, zero, false, nil
	}
	sm.fromStore, sm.storeExpireAt = true, expireAt
	c.promoteLocked(s, key, &p)
	if e, ok := s.storage.get(key); ok {
		return e, value, true, nil
	}
	return nil, value, true, nil
}

// writeThroughStore propagates a Set to the Store. ttl is the resolved
// per-call TTL so Store mirror agrees with in-memory expiry. Sliding
// TTLs are NOT propagated.
func (c *Cache[K, V]) writeThroughStore(ctx context.Context, key K, value V, ttl time.Duration) error {
	if c.store == nil {
		return nil
	}
	return c.store.Set(ctx, key, value, ttl)
}

// writeThroughStoreUntil writes value to the Store to expire at
// expireAt, timed by the cache's [Clock]. An expiry already reached
// deletes the key from the Store instead: a TTL of 0 would mean no
// expiry.
func (c *Cache[K, V]) writeThroughStoreUntil(ctx context.Context, key K, value V, expireAt time.Time) error {
	if c.store == nil {
		return nil
	}
	ttl := expireAt.Sub(c.cfg.clock.Now())
	if ttl <= 0 {
		_, err := c.store.Delete(ctx, key)
		return err
	}
	return c.store.Set(ctx, key, value, ttl)
}

// deleteThroughStore propagates a Delete to the Store; errors are
// logged. Use [Cache.DeleteCtx] to surface them.
func (c *Cache[K, V]) deleteThroughStore(ctx context.Context, key K) error {
	if c.store == nil {
		return nil
	}
	_, err := c.store.Delete(ctx, key)
	if err != nil && c.cfg.logger != nil {
		if isContextErr(err) {
			c.cfg.logger.Debug("memcache: store Delete canceled", "err", err)
		} else {
			c.cfg.logger.Warn("memcache: store Delete failed", "err", err)
		}
	}
	return err
}

// rollbackInMemory removes the in-memory entry for key after a
// write-through Store failure so the two sides do not drift.
func (c *Cache[K, V]) rollbackInMemory(key K, cbs *[]func()) {
	s := c.shardFor(key)
	s.mu.Lock()
	if e, ok := s.storage.get(key); ok {
		c.removeLocked(s, e, EvictReasonStoreRollback)
	}
	c.releaseShardInto(s, cbs)
}

// propagateSetToStore mirrors the write to the Store; on Store failure
// the in-memory entry is rolled back so the sides do not drift.
// The rollback's callbacks are added to cbs.
func (c *Cache[K, V]) propagateSetToStore(ctx context.Context, key K, value V, ttl time.Duration, cbs *[]func()) error {
	if err := c.writeThroughStore(ctx, key, value, ttl); err != nil {
		c.rollbackInMemory(key, cbs)
		return err
	}
	return nil
}

// storeMirror records a write made under a shard lock so it can be
// mirrored to the Store once the lock is released.
type storeMirror[K comparable, V any] struct {
	set, del bool
	value    V
	ttl      time.Duration
	// pending is a queued async op applied by the same call; it reaches
	// the Store first.
	pending    pendingOp[K, V]
	hasPending bool
	// fromStore is set when the call read the key's value from the
	// Store; storeExpireAt is then the record's expiry, as storeGet
	// reports it.
	fromStore     bool
	storeExpireAt int64
	// keepStoreTTL is set for a keep-expiry write of a key this call
	// didn't read from the [TTLStore]: the set keeps the record's TTL,
	// looked up before the write.
	keepStoreTTL bool
}

// noteStoreSetLocked records that value was stored under key, with the
// entry's remaining TTL, or [WithDefaultTTL] when memory didn't keep the
// entry. A no-op without a Store. Caller MUST hold s.mu.
func (c *Cache[K, V]) noteStoreSetLocked(m *storeMirror[K, V], s *shard[K, V], key K, value V) {
	if c.store == nil {
		return
	}
	m.set, m.value, m.ttl = true, value, c.cfg.defaultTTL
	if e, ok := s.storage.get(key); ok {
		m.ttl = 0
		if exp := e.expireAt.Load(); exp > 0 {
			m.ttl = max(time.Duration(exp-c.cfg.clock.Now().UnixNano()), 1)
		}
	}
}

// noteStoreKeepLocked is noteStoreSetLocked for a write that keeps the
// key's expiry: with a [TTLStore], the write keeps the Store record's
// TTL, which may differ from the in-memory copy's. A key the same call
// read from the Store uses the TTL it read; otherwise mirrorToStore
// looks the TTL up.
func (c *Cache[K, V]) noteStoreKeepLocked(m *storeMirror[K, V], s *shard[K, V], key K, value V) {
	if !m.fromStore || m.storeExpireAt < 0 {
		c.noteStoreSetLocked(m, s, key, value)
		m.keepStoreTTL = !m.fromStore && c.ttlStore != nil
		return
	}
	m.set, m.value, m.ttl = true, value, 0
	if m.storeExpireAt > 0 {
		m.ttl = max(time.Duration(m.storeExpireAt-c.cfg.clock.Now().UnixNano()), 1)
	}
}

// mirrorToStore applies the writes m records to the Store. A failed set
// rolls back the in-memory entry, as [Cache.Set] does, adding the
// rollback's callbacks to cbs.
func (c *Cache[K, V]) mirrorToStore(key K, m *storeMirror[K, V], cbs *[]func()) error {
	if m.hasPending {
		c.asyncStoreEffect(m.pending)
	}
	switch {
	case m.set:
		if m.keepStoreTTL {
			// A missing record or a failed lookup keeps the in-memory TTL.
			if ttl, found, err := c.ttlStore.TTL(context.Background(), key); err == nil && found {
				m.ttl = ttl
			}
		}
		return c.propagateSetToStore(context.Background(), key, m.value, m.ttl, cbs)
	case m.del:
		return c.deleteThroughStore(context.Background(), key)
	}
	return nil
}
