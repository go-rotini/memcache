package memcache

import (
	"context"
	"slices"
	"sync"
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

	// applyGID is the apply goroutine's ID, so [Cache.Sync] can tell
	// when a hook that goroutine runs calls it.
	applyGID atomic.Uint64

	// held counts, per goroutine ID, the applied ops whose hooks that
	// goroutine is running; they can't complete while one of those
	// hooks waits in [Cache.Sync]. waiting holds the goroutines whose
	// hooks are waiting there. heldMu guards both.
	heldMu  sync.Mutex
	held    map[uint64]int64
	waiting map[uint64]bool
}

// hold records that goroutine gid is running the hooks of n applied
// ops, and returns the release.
func (a *asyncWrites) hold(gid uint64, n int64) func() {
	a.heldMu.Lock()
	if a.held == nil {
		a.held = make(map[uint64]int64)
	}
	a.held[gid] += n
	a.heldMu.Unlock()
	return func() {
		a.heldMu.Lock()
		if a.held[gid] -= n; a.held[gid] <= 0 {
			delete(a.held, gid)
		}
		a.heldMu.Unlock()
	}
}

// waitFromHook reports whether goroutine gid is running hooks of applied
// ops. If so it records gid as waiting in Sync until release is called.
func (a *asyncWrites) waitFromHook(gid uint64) (release func(), ok bool) {
	a.heldMu.Lock()
	defer a.heldMu.Unlock()
	if a.held[gid] == 0 {
		return nil, false
	}
	if a.waiting == nil {
		a.waiting = make(map[uint64]bool)
	}
	a.waiting[gid] = true
	return func() {
		a.heldMu.Lock()
		delete(a.waiting, gid)
		a.heldMu.Unlock()
	}, true
}

// heldByWaiters returns how many applied ops are held up by hooks
// waiting in Sync; those can't complete until the waits end.
func (a *asyncWrites) heldByWaiters() int64 {
	a.heldMu.Lock()
	defer a.heldMu.Unlock()
	var n int64
	for gid := range a.waiting {
		n += a.held[gid]
	}
	return n
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
	effectiveTTL time.Duration, sliding bool, rawTTL int64, tags []string, guard func() bool,
) error {
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
	return c.enqueuePending(s, op, guard)
}

func (c *Cache[K, V]) enqueueAsyncSetWithExpiry(s *shard[K, V], key K, value V, weight int64,
	expireAt int64, sliding bool, rawTTL int64, tags []string, guard func() bool,
) error {
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
	return c.enqueuePending(s, op, guard)
}

// enqueuePending queues op. It returns [ErrClosed] once the cache is
// closed, checked under the shard lock so an op is either queued before
// Close's final drain of s or refused, and errWriteSuperseded when
// guard, also checked under the lock, says the write is no longer
// wanted.
func (c *Cache[K, V]) enqueuePending(s *shard[K, V], op pendingOp[K, V], guard func() bool) error {
	s.mu.Lock()
	if c.closed.Load() {
		s.mu.Unlock()
		return ErrClosed
	}
	if guard != nil && !guard() {
		s.mu.Unlock()
		return errWriteSuperseded
	}
	if _, existed := s.pending[op.key]; !existed {
		c.async.inflight.Add(1)
	}
	s.pending[op.key] = op
	s.mu.Unlock()
	c.signalAsyncApply()
	return nil
}

func (c *Cache[K, V]) enqueueAsyncDelete(s *shard[K, V], key K) error {
	return c.enqueuePending(s, pendingOp[K, V]{kind: pendingOpDelete, key: key}, nil)
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
	c.async.applyGID.Store(goroutineID())
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
	if c.store != nil {
		return c.drainShardByKey(s)
	}
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
		c.applyPendingOpLocked(s, ops[i])
	}
	if len(s.pendingCallbacks) > 0 {
		// Sync, called from a hook these ops fire, doesn't wait for
		// them.
		defer c.async.hold(goroutineID(), int64(len(ops)))()
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

// drainShardByKey is drainShard for a cache with a [Store]: each key's
// op is applied, and written to the Store, under that key's lock, so it
// stays ordered with synchronous writes of the key. Ops stay visible in
// s.pending until applied.
func (c *Cache[K, V]) drainShardByKey(s *shard[K, V]) int {
	s.mu.RLock()
	keys := make([]K, 0, len(s.pending))
	for k := range s.pending {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	for _, k := range keys {
		c.applyPendingKey(s, k)
	}
	return len(keys)
}

// applyPendingOpLocked applies one queued op to storage. Caller MUST
// hold s.mu (write).
func (c *Cache[K, V]) applyPendingOpLocked(s *shard[K, V], op pendingOp[K, V]) {
	switch op.kind {
	case pendingOpSet:
		if op.expireAt > 0 {
			c.applyAsyncSetWithExpiryLocked(s, op)
		} else {
			c.upsertLocked(s, op.key, op.value, op.weight,
				op.effectiveTTL, op.sliding, op.rawTTL, op.tags)
		}
	case pendingOpDelete:
		s.supersedeFlight(op.key)
		if e, ok := s.storage.get(op.key); ok {
			c.removeLocked(s, e, EvictReasonDeleted)
			c.counters.deletes.Add(1)
		}
	}
}

// applyPendingKeyLocked applies the queued op for key, if any, so a
// method that works on storage sees what Get sees and the op can't
// overwrite that method's result later. It returns the op: with a
// Store, the caller writes it through with [Cache.asyncStoreEffect]
// while holding the key's lock. Group budgets and the backlog count are
// settled once s is unlocked. Caller MUST hold s.mu (write).
func (c *Cache[K, V]) applyPendingKeyLocked(s *shard[K, V], key K) (pendingOp[K, V], bool) {
	if len(s.pending) == 0 {
		return pendingOp[K, V]{}, false
	}
	op, ok := s.pending[key]
	if !ok {
		return op, false
	}
	delete(s.pending, key)
	n := len(s.pendingCallbacks)
	c.applyPendingOpLocked(s, op)
	// The op completes once the hooks it fired have run; Sync, called
	// from one of them, doesn't wait for it.
	hooks := slices.Clone(s.pendingCallbacks[n:])
	s.pendingCallbacks = append(s.pendingCallbacks[:n], func() {
		if len(hooks) > 0 {
			release := c.async.hold(goroutineID(), 1)
			defer release()
			for _, h := range hooks {
				h()
			}
		}
		if op.kind == pendingOpSet {
			c.enforceGroupBudgets(op.tags)
		}
		c.async.inflight.Add(-1)
	})
	return op, true
}

// applyPendingKey is applyPendingKeyLocked for callers that don't hold
// s.mu or the key's lock. A no-op without [WithAsyncWrites].
func (c *Cache[K, V]) applyPendingKey(s *shard[K, V], key K) {
	if s.pending == nil {
		return
	}
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	s.mu.Lock()
	op, ok := c.applyPendingKeyLocked(s, key)
	c.releaseShardInto(s, &cbs)
	if ok {
		c.asyncStoreEffect(op)
	}
}

// applyAllPending applies every queued op before a method that works
// across keys. A no-op without [WithAsyncWrites].
func (c *Cache[K, V]) applyAllPending() {
	if c.async != nil {
		c.drainAllShards()
	}
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
	c.asyncStoreEffect(op)
	if op.kind == pendingOpSet {
		c.enforceGroupBudgets(op.tags)
	}
}

// asyncStoreEffect writes an applied op through to the Store. With a
// Store, callers hold the key's lock ([Cache.lockKey]).
func (c *Cache[K, V]) asyncStoreEffect(op pendingOp[K, V]) {
	if c.store == nil {
		return
	}
	switch op.kind {
	case pendingOpSet:
		var err error
		if op.expireAt > 0 {
			err = c.writeThroughStoreUntil(context.Background(), op.key, op.value, time.Unix(0, op.expireAt))
		} else {
			err = c.writeThroughStore(context.Background(), op.key, op.value, op.effectiveTTL)
		}
		if err != nil && c.cfg.logger != nil {
			c.cfg.logger.Warn("memcache: async write-through to store failed", "err", err)
		}
	case pendingOpDelete:
		if _, err := c.store.Delete(context.Background(), op.key); err != nil && c.cfg.logger != nil {
			c.cfg.logger.Warn("memcache: async delete-through to store failed", "err", err)
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
func (c *Cache[K, V]) asyncSet(key K, value V, ttl time.Duration, sliding bool, tags []string, guard func() bool) error {
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
	return c.enqueueAsyncSet(s, key, value, weight, c.effectiveTTL(ttl), sliding, int64(ttl), tags, guard)
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
	return c.enqueueAsyncSetWithExpiry(s, key, value, weight, expireAt, sc.sliding, int64(sc.ttl), sc.tags, sc.guard)
}

// asyncDelete queues a delete and returns true unless the cache is
// closed. The result cannot reflect whether an entry existed without
// defeating the async optimization.
func (c *Cache[K, V]) asyncDelete(key K) bool {
	if c.closed.Load() {
		return false
	}
	if c.enqueueAsyncDelete(c.shardFor(key), key) != nil {
		return false
	}
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
