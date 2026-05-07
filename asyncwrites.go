package memcache

import (
	"context"
	"sync/atomic"
	"time"
)

type pendingOpKind uint8

const (
	pendingOpSet pendingOpKind = iota + 1
	pendingOpDelete
)

// pendingOp captures a Set or Delete the caller has already returned
// from. Ops coalesce by key: a second op on the same key replaces the
// first.
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

// asyncWrites holds per-cache state for [WithAsyncWrites].
type asyncWrites struct {
	// signal collapses wakeup notifications; capacity 1 because
	// multiple ops between drains coalesce into one drain.
	signal chan struct{}

	stop   chan struct{}
	exited chan struct{}

	// inflight tracks accepted-but-not-applied ops; [Cache.Sync] polls.
	inflight atomic.Int64
}

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

func (c *Cache[K, V]) signalAsyncApply() {
	select {
	case c.async.signal <- struct{}{}:
	default:
	}
}

func (c *Cache[K, V]) startAsyncApply() {
	go c.runAsyncApply()
}

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

func (c *Cache[K, V]) drainAllShards() int {
	applied := 0
	for _, s := range c.shards {
		applied += c.drainShard(s)
	}
	return applied
}

// drainShard moves every pending op into storage under the shard write
// lock so reads observe either the pending op or the post-drain state.
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
	c.flushAndUnlock(s)

	// Off-shard-lock: propagate to the Store and enforce group
	// budgets. Store errors are logged; the caller has already
	// returned and there is no fast-fail path.
	for i := range ops {
		op := ops[i]
		c.applyAsyncSideEffects(op)
	}
	c.async.inflight.Add(-int64(len(ops)))
	return len(ops)
}

// applyAsyncSetWithExpiryLocked is the absolute-expiry equivalent of
// upsertLocked, driven by a pendingOp. Caller MUST hold s.mu.
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

// stopAsyncApply signals the apply goroutine to drain and exit, blocking
// until it has returned.
func (c *Cache[K, V]) stopAsyncApply() {
	if c.async == nil {
		return
	}
	close(c.async.stop)
	<-c.async.exited
}

func (c *Cache[K, V]) asyncBacklog() int64 {
	if c.async == nil {
		return 0
	}
	return c.async.inflight.Load()
}

// asyncSet validates the Set synchronously and enqueues a pending op.
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

// asyncDelete queues a delete and returns true unless the cache is
// closed. The result cannot reflect whether an entry existed without
// defeating the async optimization.
func (c *Cache[K, V]) asyncDelete(key K) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	c.enqueueAsyncDelete(s, key)
	c.counters.deletes.Add(1)
	return true
}

// tryServeFromAsyncPending checks pending ops for the visibility
// contract. Returns terminal=true when a pending Set/Delete covers the
// key and the caller can return immediately.
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

// asyncReadHit returns (value, kind, true) when a pending op covers key:
// Set yields the pending value, Delete yields zero+pendingOpDelete so
// the caller short-circuits to a miss. Caller MUST hold s.mu (read or
// write).
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
		// Honor any absolute expiry on the pending op so readers
		// don't see "fresh" data the apply path would reject.
		if op.expireAt > 0 && now >= op.expireAt {
			return zero, pendingOpDelete, true
		}
		// Pending Sets with a relative TTL are treated as fresh; apply
		// materializes the absolute expireAt later.
		return op.value, pendingOpSet, true
	}
	return zero, 0, false
}
