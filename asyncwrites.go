package memcache

import (
	"context"
	"sync/atomic"
	"time"
)

// pendingOpKind tags the operation a [pendingOp] records.
type pendingOpKind uint8

const (
	pendingOpSet pendingOpKind = iota + 1
	pendingOpDelete
)

// pendingOp captures everything the apply goroutine needs to
// faithfully reproduce a Set or Delete that the caller has already
// returned from. Pending ops coalesce by key — a second op on the
// same key replaces the first, so churn collapses to its final
// state.
//
// Field reuse follows the upsertLocked signature so apply can call
// the same path the synchronous Set would take.
type pendingOp[K comparable, V any] struct {
	kind         pendingOpKind
	key          K
	value        V
	weight       int64
	effectiveTTL time.Duration // post-jitter, relative; ignored when expireAt > 0
	rawTTL       int64         // pre-jitter, persisted on the entry for sliding/Touch
	sliding      bool
	expireAt     int64 // absolute, 0 means "use effectiveTTL"
	tags         []string
}

// asyncWrites holds the per-cache state for [WithAsyncWrites]. nil
// when the option is off. The apply goroutine is the sole writer
// to the per-shard pending maps from the storage side; producers
// write under the shard lock so the data-race detector is happy.
type asyncWrites struct {
	// signal collapses wakeup notifications. Producers do a non-
	// blocking send; the apply goroutine receives and immediately
	// drains every shard. Capacity 1 is sufficient — multiple
	// pending ops between two drains coalesce into a single
	// drain.
	signal chan struct{}

	// stop is closed by [Cache.Close] to make the apply goroutine
	// return. exited is closed by the goroutine on its way out so
	// Close can wait for the drain to finish.
	stop   chan struct{}
	exited chan struct{}

	// inflight is the count of pending ops the cache has accepted
	// but the apply goroutine has not yet drained. Bumped at
	// enqueue, decremented at apply. [Cache.Sync] polls it for the
	// drained signal.
	inflight atomic.Int64
}

// newAsyncWrites constructs the per-cache async-writes state. It
// allocates the signal/stop/exited channels and the per-shard
// pending maps. The apply goroutine starts in [Cache.startAsyncApply].
func newAsyncWrites[K comparable, V any](shards []*shard[K, V]) *asyncWrites {
	for _, s := range shards {
		s.pending = make(map[K]pendingOp[K, V])
	}
	return &asyncWrites{
		signal: make(chan struct{}, 1),
		stop:   make(chan struct{}),
		exited: make(chan struct{}),
	}
}

// enqueueAsyncSet places a Set op in the target shard's pending map
// and signals the apply goroutine. The pending map is keyed; a prior
// pending op for the same key is overwritten (coalescing). The
// shard lock is held only for the map write; everything else
// happens off-lock.
func (c *Cache[K, V]) enqueueAsyncSet(s *shard[K, V], key K, value V, weight int64,
	effectiveTTL time.Duration, sliding bool, rawTTL int64, tags []string,
) {
	op := pendingOp[K, V]{
		kind:         pendingOpSet,
		key:          key,
		value:        value,
		weight:       weight,
		effectiveTTL: effectiveTTL,
		rawTTL:       rawTTL,
		sliding:      sliding,
		tags:         tags,
	}
	s.mu.Lock()
	if _, existed := s.pending[key]; !existed {
		c.async.inflight.Add(1)
	}
	s.pending[key] = op
	s.mu.Unlock()
	c.signalAsyncApply()
}

// enqueueAsyncSetWithExpiry is the absolute-expiry variant for the
// [SetExpireAt] path.
func (c *Cache[K, V]) enqueueAsyncSetWithExpiry(s *shard[K, V], key K, value V, weight int64,
	expireAt int64, sliding bool, rawTTL int64, tags []string,
) {
	op := pendingOp[K, V]{
		kind:     pendingOpSet,
		key:      key,
		value:    value,
		weight:   weight,
		expireAt: expireAt,
		rawTTL:   rawTTL,
		sliding:  sliding,
		tags:     tags,
	}
	s.mu.Lock()
	if _, existed := s.pending[key]; !existed {
		c.async.inflight.Add(1)
	}
	s.pending[key] = op
	s.mu.Unlock()
	c.signalAsyncApply()
}

// enqueueAsyncDelete records a delete tombstone in the pending map.
// Like Set, this coalesces with any earlier op on the same key —
// the final apply just removes the entry.
func (c *Cache[K, V]) enqueueAsyncDelete(s *shard[K, V], key K) {
	op := pendingOp[K, V]{
		kind: pendingOpDelete,
		key:  key,
	}
	s.mu.Lock()
	if _, existed := s.pending[key]; !existed {
		c.async.inflight.Add(1)
	}
	s.pending[key] = op
	s.mu.Unlock()
	c.signalAsyncApply()
}

// signalAsyncApply wakes the apply goroutine. Non-blocking: a
// pending wakeup already in the channel covers the new op.
func (c *Cache[K, V]) signalAsyncApply() {
	select {
	case c.async.signal <- struct{}{}:
	default:
	}
}

// startAsyncApply launches the per-cache apply goroutine. Called
// from [build] when [WithAsyncWrites] is configured.
func (c *Cache[K, V]) startAsyncApply() {
	go c.runAsyncApply()
}

// runAsyncApply is the apply loop. It blocks on the signal channel,
// drains every shard's pending map under that shard's write lock,
// and exits when [Cache.Close] closes async.stop.
//
// Drain order: shards are walked in slice order (deterministic).
// Within a shard the pending map is walked in Go's map-iteration
// order (non-deterministic) — applying each op via the standard
// upsertLocked / removeLocked path, then propagating to the
// configured Store.
func (c *Cache[K, V]) runAsyncApply() {
	defer close(c.async.exited)
	for {
		select {
		case <-c.async.stop:
			c.drainAllShards()
			return
		case <-c.async.signal:
			c.drainAllShards()
		}
	}
}

// drainAllShards walks every shard's pending map and applies the
// queued ops. Returns the number of ops applied (currently unused
// but useful in future telemetry hooks).
func (c *Cache[K, V]) drainAllShards() int {
	applied := 0
	for _, s := range c.shards {
		applied += c.drainShard(s)
	}
	return applied
}

// drainShard moves every pending op on s into storage. The shard
// write lock is held for the duration so reads see a consistent
// snapshot — they either observe the pending op (before the drain
// fires) or the storage state (after).
func (c *Cache[K, V]) drainShard(s *shard[K, V]) int {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return 0
	}
	ops := make([]pendingOp[K, V], 0, len(s.pending))
	for _, op := range s.pending {
		ops = append(ops, op)
	}
	clear(s.pending)
	for i := range ops {
		op := ops[i]
		switch op.kind {
		case pendingOpSet:
			if op.expireAt > 0 {
				c.applyAsyncSetWithExpiryLocked(s, op)
			} else {
				c.upsertLocked(s, op.key, op.value, op.weight,
					op.effectiveTTL, op.sliding, op.rawTTL, op.tags)
			}
		case pendingOpDelete:
			if e, ok := s.storage.get(op.key); ok {
				c.removeLocked(s, e, EvictReasonDeleted)
				c.counters.deletes.Add(1)
			}
		}
	}
	c.flushPendingCallbacks(s)

	// Phase 2 (off-shard-lock): propagate each op to the
	// configured Store + enforce group budgets. Failing fast on
	// Store errors is impossible — the caller's already returned —
	// so we log and move on.
	for i := range ops {
		op := ops[i]
		c.applyAsyncSideEffects(op)
	}
	c.async.inflight.Add(-int64(len(ops)))
	return len(ops)
}

// applyAsyncSetWithExpiryLocked is the absolute-expiry equivalent
// of upsertLocked, mirroring [Cache.upsertWithAbsoluteExpiryLocked]
// but driven by a pendingOp instead of a setConfig. Caller holds
// shard.mu.
func (c *Cache[K, V]) applyAsyncSetWithExpiryLocked(s *shard[K, V], op pendingOp[K, V]) {
	sc := setConfig{
		ttl:       0,
		weight:    op.weight,
		hasWeight: true,
		sliding:   op.sliding,
		hasExpiry: true,
		tags:      op.tags,
	}
	sc.expireAt = time.Unix(0, op.expireAt)
	c.upsertWithAbsoluteExpiryLocked(s, op.key, op.value, op.weight, sc)
}

// applyAsyncSideEffects runs the post-shard-lock work for a drained
// op: Store propagation and group budgets. Errors from the Store are
// logged but cannot be surfaced to the original caller.
func (c *Cache[K, V]) applyAsyncSideEffects(op pendingOp[K, V]) {
	switch op.kind {
	case pendingOpSet:
		if c.store != nil {
			ttl := op.effectiveTTL
			if op.expireAt > 0 {
				ttl = max(time.Until(time.Unix(0, op.expireAt)), 0)
			}
			if err := c.writeThroughStore(context.Background(), op.key, op.value, ttl); err != nil && c.cfg.logger != nil {
				c.cfg.logger.Warn("memcache: async write-through to store failed",
					"key", op.key, "err", err)
			}
		}
		c.enforceGroupBudgets(op.tags)
	case pendingOpDelete:
		if c.store != nil {
			if _, err := c.store.Delete(context.Background(), op.key); err != nil && c.cfg.logger != nil {
				c.cfg.logger.Warn("memcache: async delete-through to store failed",
					"key", op.key, "err", err)
			}
		}
	}
}

// stopAsyncApply signals the apply goroutine to drain remaining
// work and exit. Blocks until the goroutine has returned. Called
// from [Cache.Close].
func (c *Cache[K, V]) stopAsyncApply() {
	if c.async == nil {
		return
	}
	close(c.async.stop)
	<-c.async.exited
}

// asyncBacklog returns the number of pending ops the cache has
// accepted but not yet applied. Used by [Cache.Sync] to detect
// drained state. Safe to call without locks (atomic).
func (c *Cache[K, V]) asyncBacklog() int64 {
	if c.async == nil {
		return 0
	}
	return c.async.inflight.Load()
}

// asyncSet runs the synchronous validation portion of Set —
// closed-check, weight, tag limits — and then enqueues a pending
// Set op. The caller has already extracted TTL and tags.
func (c *Cache[K, V]) asyncSet(key K, value V, ttl time.Duration, sliding bool, tags []string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if err := c.checkKeySize(key); err != nil {
		return err
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return err
	}
	if err := c.checkTagLimits(key, tags); err != nil {
		return err
	}
	s := c.shardFor(key)
	c.enqueueAsyncSet(s, key, value, weight, c.effectiveTTL(ttl), sliding, int64(ttl), tags)
	return nil
}

// asyncSetWithExpiry mirrors [Cache.asyncSet] for the absolute-
// expiry path used by [Cache.SetWithOptions] when [SetExpireAt] is
// supplied.
func (c *Cache[K, V]) asyncSetWithExpiry(key K, value V, weight int64, sc setConfig) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if err := c.checkKeySize(key); err != nil {
		return err
	}
	if err := c.checkTagLimits(key, sc.tags); err != nil {
		return err
	}
	s := c.shardFor(key)
	expireAt := int64(0)
	if !sc.expireAt.IsZero() {
		expireAt = sc.expireAt.UnixNano()
	}
	c.enqueueAsyncSetWithExpiry(s, key, value, weight, expireAt, sc.sliding, int64(sc.ttl), sc.tags)
	return nil
}

// asyncDelete is the [Cache.Delete] counterpart of [Cache.asyncSet].
// Returns false when the cache is closed; otherwise queues the
// delete and returns true (the caller cannot know whether an entry
// existed without inspecting state, which would defeat the async
// optimization).
func (c *Cache[K, V]) asyncDelete(key K) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	c.enqueueAsyncDelete(s, key)
	c.counters.deletes.Add(1)
	return true
}

// tryServeFromAsyncPending is the [Cache.getCtx] entry point for
// the async-writes visibility check. Returns (val, hit, terminal)
// where `terminal` is true when a pending Set/Delete covers the
// key and the caller can return immediately. When terminal is
// false the caller falls through to the storage path.
func (c *Cache[K, V]) tryServeFromAsyncPending(s *shard[K, V], key K, now int64) (V, bool, bool) {
	var zero V
	if s.pending == nil {
		return zero, false, false
	}
	s.mu.RLock()
	val, kind, hit := c.asyncReadHit(s, key, now)
	s.mu.RUnlock()
	if !hit {
		return zero, false, false
	}
	if kind == pendingOpDelete {
		c.recordMiss()
		c.fireMiss(key)
		return zero, false, true
	}
	c.recordHitObserve(key)
	c.fireHit(key, val)
	return c.returnValue(val), true, true
}

// asyncReadHit is consulted by every Get-style read path before
// looking at the shard's storage. Returns (value, kind, true) when
// a pending op covers the key — Set ops yield the pending value,
// Delete ops yield the zero value with kind=pendingOpDelete so the
// caller can short-circuit to a miss. The third return is false
// when no pending op exists.
//
// Caller must hold shard.mu.RLock (or stronger).
func (c *Cache[K, V]) asyncReadHit(s *shard[K, V], key K, now int64) (V, pendingOpKind, bool) {
	var zero V
	if s.pending == nil {
		return zero, 0, false
	}
	op, ok := s.pending[key]
	if !ok {
		return zero, 0, false
	}
	switch op.kind {
	case pendingOpDelete:
		return zero, pendingOpDelete, true
	case pendingOpSet:
		// Honor any expiry encoded on the pending op so
		// readers don't see "fresh" data that the apply path
		// would have rejected.
		if op.expireAt > 0 && now >= op.expireAt {
			return zero, pendingOpDelete, true
		}
		// Pending Set ops with a relative effectiveTTL are
		// treated as fresh — the apply pass will materialize the
		// absolute expireAt and the next Get will see it.
		return op.value, pendingOpSet, true
	}
	return zero, 0, false
}
