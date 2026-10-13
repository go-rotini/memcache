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
	// aborted is set when the Store call panicked; waiters read again.
	aborted bool
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
			if r.aborted || (r.err != nil && isContextErr(r.err) && ctx.Err() == nil) {
				// The read ended for its leader's own reason, not this
				// caller's; read again.
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
	value, ok, err := c.store.Get(ctx, key)
	var p promotion[V]
	if err == nil && ok {
		p = c.preparePromotion(key, value)
	}
	var cbs []func()
	s.mu.Lock()
	if err == nil && ok && !r.watch.superseded && c.freshEntryLocked(s, key) == nil {
		c.promoteLocked(s, key, &p)
	}
	r.value, r.ok, r.err = value, ok, err
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

// promotion is a Store value checked against the cache's limits, ready
// to be inserted under the shard lock. err is why it can't be.
type promotion[V any] struct {
	value  V
	weight int64
	tags   []string
	err    error
}

// preparePromotion weighs and tags a Store value outside the shard lock,
// since both may run user code.
func (c *Cache[K, V]) preparePromotion(key K, value V) promotion[V] {
	p := promotion[V]{value: value, tags: c.autoTags(value)}
	if p.err = c.checkKeySize(key); p.err != nil {
		return p
	}
	if p.weight, p.err = c.computeWeight(key, value); p.err != nil {
		return p
	}
	p.err = c.checkTagLimits(key, p.tags)
	return p
}

// promoteLocked inserts a Store value with [WithDefaultTTL] (a Store
// does not return TTLs). Promotion is best-effort: a value over the
// cache's limits, or one admission turns away, is not cached. Watches on
// the key are left alone, since the value is the Store's current one
// rather than a newer write. Eviction does not cascade to the Store.
// Caller MUST hold s.mu (write).
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
	c.upsertLocked(s, key, p.value, p.weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), p.tags)
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
	value, ok, err := c.store.Get(context.Background(), key)
	var p promotion[V]
	if err == nil && ok {
		p = c.preparePromotion(key, value)
	}
	s.mu.Lock()
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
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
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
}

// noteStoreSetLocked records that value was stored under key, with the
// entry's remaining TTL. A no-op without a Store. Caller MUST hold s.mu.
func (c *Cache[K, V]) noteStoreSetLocked(m *storeMirror[K, V], s *shard[K, V], key K, value V) {
	if c.store == nil {
		return
	}
	m.set, m.value, m.ttl = true, value, 0
	if e, ok := s.storage.get(key); ok {
		if exp := e.expireAt.Load(); exp > 0 {
			m.ttl = max(time.Duration(exp-c.cfg.clock.Now().UnixNano()), 1)
		}
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
		return c.propagateSetToStore(context.Background(), key, m.value, m.ttl, cbs)
	case m.del:
		return c.deleteThroughStore(context.Background(), key)
	}
	return nil
}
