package memcache

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var (
	errNilComputeFn         = errors.New("memcache: nil compute function")
	errUnknownComputeAction = errors.New("memcache: unknown ComputeAction")
)

// activeCompute tracks goroutines inside a Compute callback so the
// re-entrancy detector can panic with [ErrComputeReentrant] instead of
// deadlocking on the shard mutex.
var activeCompute sync.Map

func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Layout: "goroutine N [status]:\n..."
	const prefix = "goroutine "
	if n < len(prefix) || string(buf[:len(prefix)]) != prefix {
		return 0
	}
	rest := buf[len(prefix):n]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	id, err := strconv.ParseUint(string(rest[:end]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// reentryGuard registers the current goroutine as in-compute. Caller
// MUST defer the returned cleanup. Panics with [ErrComputeReentrant]
// when a Compute callback re-enters Compute on the cache.
func reentryGuard() func() {
	gid := goroutineID()
	if gid == 0 {
		return func() {}
	}
	if _, loaded := activeCompute.LoadOrStore(gid, struct{}{}); loaded {
		panic(ErrComputeReentrant)
	}
	return func() { activeCompute.Delete(gid) }
}

// Compute atomically applies fn to the entry for key. fn receives the
// current value (zero V when absent) and a presence bool; it returns the
// new value, a [ComputeAction], and an optional error.
//
// Actions: [ComputeStore] stores with default TTL, [ComputeDelete]
// removes (under [EvictReasonComputed]), [ComputeNoOp] discards the
// returned value.
//
// fn runs under the shard write lock and MUST NOT call into the cache
// for any key on the same shard.
func (c *Cache[K, V]) Compute(
	key K,
	fn func(cur V, ok bool) (V, ComputeAction, error),
) (V, error) {
	var zero V
	if c.closed.Load() {
		return zero, ErrClosed
	}
	if fn == nil {
		return zero, errNilComputeFn
	}
	defer reentryGuard()()

	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)

	var current V
	var present bool
	if e, ok := s.storage.get(key); ok && !c.entryExpiredLocked(e, now) && !e.flags.has(flagNegative) {
		current = e.loadValue()
		present = true
	}

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
		c.upsertLocked(s, key, newValue, weight,
			c.effectiveTTL(c.cfg.defaultTTL),
			c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
		return newValue, nil
	case ComputeDelete:
		// Only fire EvictReasonComputed when fn saw the entry as
		// present; expired/negative entries are not user-driven deletes.
		if !present {
			return zero, nil
		}
		if e, ok := s.storage.get(key); ok {
			c.removeLocked(s, e, EvictReasonComputed)
		}
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
// fn errors are propagated and no entry is stored.
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
	defer reentryGuard()()

	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)

	if e, ok := s.storage.get(key); ok && !c.entryExpiredLocked(e, now) && !e.flags.has(flagNegative) {
		return e.loadValue(), false, nil
	}

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
	return v, true, nil
}

// ComputeIfPresent invokes fn only when the key is present and fresh.
// On miss, returns (zero, nil) without error.
func (c *Cache[K, V]) ComputeIfPresent(
	key K, fn func(cur V) (V, ComputeAction, error),
) (V, error) {
	var zero V
	if c.closed.Load() {
		return zero, ErrClosed
	}
	if fn == nil {
		return zero, errNilComputeFn
	}
	defer reentryGuard()()

	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)

	e, ok := s.storage.get(key)
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return zero, nil
	}
	current := e.loadValue()

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
		c.upsertLocked(s, key, newValue, weight,
			c.effectiveTTL(c.cfg.defaultTTL),
			c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
		return newValue, nil
	case ComputeDelete:
		c.removeLocked(s, e, EvictReasonComputed)
		return zero, nil
	case ComputeNoOp:
		return current, nil
	default:
		return zero, fmt.Errorf("%w: %d", errUnknownComputeAction, action)
	}
}

// Update is shorthand for [Cache.ComputeIfPresent] that always stores
// fn's result. Returns (zero, [ErrNotFound]) when the entry is absent.
func (c *Cache[K, V]) Update(key K, fn func(cur V) V) (V, error) {
	var zero V
	if fn == nil {
		return zero, errNilComputeFn
	}
	if c.closed.Load() {
		return zero, ErrClosed
	}
	defer reentryGuard()()
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)

	e, ok := s.storage.get(key)
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return zero, ErrNotFound
	}
	newValue := fn(e.loadValue())
	weight, werr := c.computeWeight(key, newValue)
	if werr != nil {
		return zero, werr
	}
	c.upsertLocked(s, key, newValue, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
	return newValue, nil
}

// CompareAndSwap atomically replaces the value for key with newValue
// when the current value equals old, using [reflect.DeepEqual] for
// comparison. Returns true when the swap happened.
func (c *Cache[K, V]) CompareAndSwap(key K, old, newValue V) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)

	e, ok := s.storage.get(key)
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return false
	}
	if !reflect.DeepEqual(e.loadValue(), old) {
		return false
	}
	weight, err := c.computeWeight(key, newValue)
	if err != nil {
		return false
	}
	c.upsertLocked(s, key, newValue, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
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
