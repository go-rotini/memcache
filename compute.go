package memcache

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"
)

var (
	errNilComputeFn         = errors.New("memcache: nil compute function")
	errUnknownComputeAction = errors.New("memcache: unknown ComputeAction")
)

// stackBufs holds the buffers goroutineID reads the stack header into.
var stackBufs = sync.Pool{New: func() any { return new([64]byte) }}

// goroutineID returns the calling goroutine's ID, or 0 when it can't be
// read.
func goroutineID() uint64 {
	buf := stackBufs.Get().(*[64]byte) //nolint:forcetypeassert // the pool holds only *[64]byte
	defer stackBufs.Put(buf)
	n := runtime.Stack(buf[:], false)
	// Layout: "goroutine N [status]:\n..."
	const prefix = "goroutine "
	if n < len(prefix) || string(buf[:len(prefix)]) != prefix {
		return 0
	}
	var id uint64
	for _, b := range buf[len(prefix):n] {
		if b < '0' || b > '9' {
			break
		}
		id = id*10 + uint64(b-'0')
	}
	return id
}

// enterCompute panics with [ErrComputeReentrant] when the calling
// goroutine is already running user code under s's lock in a
// Compute-family call; nested calls for other shards or caches are
// allowed. It returns the goroutine ID, which the caller stores in
// s.computing while it runs user code under s.mu.
func (s *shard[K, V]) enterCompute() uint64 {
	gid := goroutineID()
	if gid != 0 && s.computing.Load() == gid {
		panic(ErrComputeReentrant)
	}
	return gid
}

// storeComputedLocked stores a computed value: an existing fresh entry
// keeps its tags and expiry ([Cache.replaceValueLocked]); otherwise the
// value is inserted with the default TTL and no tags. Caller MUST hold
// s.mu (write).
func (c *Cache[K, V]) storeComputedLocked(s *shard[K, V], key K, value V, weight int64) {
	if e, ok := s.storage.get(key); ok && !e.flags.has(flagNegative) &&
		!c.entryExpiredLocked(e, c.cfg.clock.Now().UnixNano()) {
		c.replaceValueLocked(s, e, value, weight)
		return
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
}

// replaceValueLocked swaps the value of the live entry e, keeping its
// tags, expiry and sliding TTL. Caller MUST hold s.mu (write).
func (c *Cache[K, V]) replaceValueLocked(s *shard[K, V], e *entry[K, V], value V, weight int64) {
	s.supersedeFlight(e.key)
	c.counters.bytes.Add(weight - e.weight)
	e.storeValue(value)
	e.weight = weight
	e.lastAccess.Store(c.cfg.clock.Now().UnixNano())
	e.hits.Store(0)
	s.policy.OnUpdate(e)
	if c.cfg.statsEnabled {
		c.counters.updates.Add(1)
	}
	if c.events.active() {
		c.publishEvent(Event[K, V]{
			Kind:  EventUpdate,
			Key:   e.key,
			Value: value,
			At:    c.cfg.clock.Now(),
			Tags:  append([]string(nil), e.tags...),
		})
	}
}

// replaceOrStoreLocked stores a computed value over the key's current
// value: in place when e, the live entry, is in memory, otherwise as a
// new entry. Caller MUST hold s.mu (write).
func (c *Cache[K, V]) replaceOrStoreLocked(s *shard[K, V], e *entry[K, V], key K, value V, weight int64) {
	if e != nil {
		c.replaceValueLocked(s, e, value, weight)
		return
	}
	c.storeComputedLocked(s, key, value, weight)
}

// Compute atomically applies fn to the entry for key. fn receives the
// current value (zero V when absent) and a presence bool; it returns the
// new value, a [ComputeAction], and an optional error.
//
// Actions: [ComputeStore] stores the value, [ComputeDelete] removes the
// entry (under [EvictReasonComputed]), [ComputeNoOp] discards the
// returned value. Storing over an existing entry keeps its tags and
// expiry; a new entry gets the default TTL and no tags. The same holds
// for [Cache.ComputeIfPresent], [Cache.Update], [Cache.CompareAndSwap]
// and [Increment].
//
// With [WithStore], a key missing from memory is read from the Store
// first, so fn sees the Store's value; a Store read error is returned
// and fn is not called.
//
// fn runs under the shard write lock and MUST NOT call into the cache
// for any key on the same shard.
func (c *Cache[K, V]) Compute(
	key K,
	fn func(cur V, ok bool) (V, ComputeAction, error),
) (_ V, err error) {
	var zero V
	if c.closed.Load() {
		return zero, ErrClosed
	}
	if fn == nil {
		return zero, errNilComputeFn
	}
	s := c.shardFor(key)
	gid := s.enterCompute()
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	var sm storeMirror[K, V]
	defer func() {
		if merr := c.mirrorToStore(key, &sm, &cbs); merr != nil && err == nil {
			err = merr
		}
	}()
	s.mu.Lock()
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	defer c.releaseShardInto(s, &cbs)
	e, current, present, err := c.currentLocked(s, key, &sm, &cbs)
	if err != nil {
		return zero, err
	}
	s.computing.Store(gid)
	defer s.computing.Store(0)

	newValue, action, err := fn(current, present)
	if err != nil {
		return zero, err
	}

	switch action {
	case ComputeStore:
		weight, werr := c.computeWeight(key, newValue)
		if werr != nil {
			return zero, werr
		}
		c.replaceOrStoreLocked(s, e, key, newValue, weight)
		c.noteStoreSetLocked(&sm, s, key, newValue)
		return newValue, nil
	case ComputeDelete:
		// Only fire EvictReasonComputed when fn saw the entry as
		// present; expired/negative entries are not user-driven deletes.
		if !present {
			return zero, nil
		}
		if e != nil {
			c.removeLocked(s, e, EvictReasonComputed)
		}
		sm.del = c.store != nil
		return zero, nil
	case ComputeNoOp:
		if present {
			return current, nil
		}
		return zero, nil
	default:
		return zero, fmt.Errorf("%w: %d", errUnknownComputeAction, action)
	}
}

// ComputeIfAbsent invokes fn only when the key is absent or expired. On
// a hit, the existing value is returned with computed=false. fn's TTL
// of 0 means use [WithDefaultTTL]; a negative TTL returns [ErrInvalidTTL].
// fn errors are propagated and no entry is stored. With [WithStore], a
// key the Store holds is present.
func (c *Cache[K, V]) ComputeIfAbsent(
	key K, fn func() (V, time.Duration, error),
) (value V, computed bool, err error) {
	var zero V
	if c.closed.Load() {
		return zero, false, ErrClosed
	}
	if fn == nil {
		return zero, false, errNilComputeFn
	}
	s := c.shardFor(key)
	gid := s.enterCompute()
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	var sm storeMirror[K, V]
	defer func() {
		if merr := c.mirrorToStore(key, &sm, &cbs); merr != nil && err == nil {
			err = merr
		}
	}()
	s.mu.Lock()
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	defer c.releaseShardInto(s, &cbs)
	_, current, present, err := c.currentLocked(s, key, &sm, &cbs)
	if err != nil {
		return zero, false, err
	}
	if present {
		return current, false, nil
	}
	s.computing.Store(gid)
	defer s.computing.Store(0)

	v, ttl, ferr := fn()
	if ferr != nil {
		return zero, false, ferr
	}
	if ttl < 0 {
		return zero, false, ErrInvalidTTL
	}
	weight, werr := c.computeWeight(key, v)
	if werr != nil {
		return zero, false, werr
	}
	if ttl == 0 {
		ttl = c.cfg.defaultTTL
	}
	c.upsertLocked(s, key, v, weight,
		c.effectiveTTL(ttl),
		c.cfg.slidingTTL, int64(ttl), nil)
	c.noteStoreSetLocked(&sm, s, key, v)
	return v, true, nil
}

// ComputeIfPresent invokes fn only when the key is present and fresh.
// On miss, returns (zero, nil) without error. With [WithStore], a key
// the Store holds is present.
func (c *Cache[K, V]) ComputeIfPresent(
	key K, fn func(cur V) (V, ComputeAction, error),
) (_ V, err error) {
	var zero V
	if c.closed.Load() {
		return zero, ErrClosed
	}
	if fn == nil {
		return zero, errNilComputeFn
	}
	s := c.shardFor(key)
	gid := s.enterCompute()
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	var sm storeMirror[K, V]
	defer func() {
		if merr := c.mirrorToStore(key, &sm, &cbs); merr != nil && err == nil {
			err = merr
		}
	}()
	s.mu.Lock()
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	defer c.releaseShardInto(s, &cbs)
	e, current, present, err := c.currentLocked(s, key, &sm, &cbs)
	if err != nil || !present {
		return zero, err
	}
	s.computing.Store(gid)
	defer s.computing.Store(0)

	newValue, action, err := fn(current)
	if err != nil {
		return zero, err
	}
	switch action {
	case ComputeStore:
		weight, werr := c.computeWeight(key, newValue)
		if werr != nil {
			return zero, werr
		}
		c.replaceOrStoreLocked(s, e, key, newValue, weight)
		c.noteStoreSetLocked(&sm, s, key, newValue)
		return newValue, nil
	case ComputeDelete:
		if e != nil {
			c.removeLocked(s, e, EvictReasonComputed)
		}
		sm.del = c.store != nil
		return zero, nil
	case ComputeNoOp:
		return current, nil
	default:
		return zero, fmt.Errorf("%w: %d", errUnknownComputeAction, action)
	}
}

// Update is shorthand for [Cache.ComputeIfPresent] that always stores
// fn's result. Returns (zero, [ErrNotFound]) when the entry is absent.
func (c *Cache[K, V]) Update(key K, fn func(cur V) V) (_ V, err error) {
	var zero V
	if fn == nil {
		return zero, errNilComputeFn
	}
	if c.closed.Load() {
		return zero, ErrClosed
	}
	s := c.shardFor(key)
	gid := s.enterCompute()
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	var sm storeMirror[K, V]
	defer func() {
		if merr := c.mirrorToStore(key, &sm, &cbs); merr != nil && err == nil {
			err = merr
		}
	}()
	s.mu.Lock()
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	defer c.releaseShardInto(s, &cbs)
	e, current, present, err := c.currentLocked(s, key, &sm, &cbs)
	if err != nil {
		return zero, err
	}
	if !present {
		return zero, ErrNotFound
	}
	s.computing.Store(gid)
	defer s.computing.Store(0)
	newValue := fn(current)
	weight, werr := c.computeWeight(key, newValue)
	if werr != nil {
		return zero, werr
	}
	c.replaceOrStoreLocked(s, e, key, newValue, weight)
	c.noteStoreSetLocked(&sm, s, key, newValue)
	return newValue, nil
}

// CompareAndSwap atomically replaces the value for key with newValue
// when the current value equals old, using [reflect.DeepEqual] for
// comparison. Returns true when the swap happened.
func (c *Cache[K, V]) CompareAndSwap(key K, old, newValue V) (swapped bool) {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	var sm storeMirror[K, V]
	defer func() {
		if c.mirrorToStore(key, &sm, &cbs) != nil {
			swapped = false
		}
	}()
	s.mu.Lock()
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	defer c.releaseShardInto(s, &cbs)

	e, current, present, err := c.currentLocked(s, key, &sm, &cbs)
	if err != nil || !present {
		return false
	}
	if !reflect.DeepEqual(current, old) {
		return false
	}
	weight, err := c.computeWeight(key, newValue)
	if err != nil {
		return false
	}
	c.replaceOrStoreLocked(s, e, key, newValue, weight)
	c.noteStoreSetLocked(&sm, s, key, newValue)
	return true
}

// IncrementBy atomically adds delta to the integer value at key. If
// absent, the entry is created with value 0+delta. Returns the
// post-increment value.
func IncrementBy[K comparable, V Number](
	c *Cache[K, V], key K, delta V,
) (V, error) {
	var zero V
	if c == nil {
		return zero, ErrClosed
	}
	return c.Compute(key, func(cur V, _ bool) (V, ComputeAction, error) {
		return cur + delta, ComputeStore, nil
	})
}

// Increment is IncrementBy(c, key, 1).
func Increment[K comparable, V Number](c *Cache[K, V], key K) (V, error) {
	return IncrementBy(c, key, 1)
}

// Decrement is IncrementBy(c, key, -1). For unsigned V, zero wraps per
// Go integer rules; use [Cache.Compute] for saturation.
func Decrement[K comparable, V Number](c *Cache[K, V], key K) (V, error) {
	return IncrementBy(c, key, decrementDelta[V]())
}

// decrementDelta returns -1 typed as V; computed as 0-1 so the literal
// type-checks under unsigned V.
func decrementDelta[V Number]() V {
	var one V = 1
	return 0 - one
}
