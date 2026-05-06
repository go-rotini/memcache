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
	// errNilComputeFn is returned (or wrapped) when a caller passes
	// a nil function to one of the Compute family methods. Treated
	// as a programming error.
	errNilComputeFn = errors.New("memcache: nil compute function")

	// errUnknownComputeAction is returned when a Compute callback
	// returns a [ComputeAction] outside the defined set.
	errUnknownComputeAction = errors.New("memcache: unknown ComputeAction")
)

// activeCompute is a process-wide registry of goroutines currently
// inside a Compute callback. Maps `goroutineID -> struct{}`. The
// re-entrancy detector consults this on every Compute entry — if
// the current goroutine is already in the map, the callback has
// re-entered the cache and we panic with [ErrComputeReentrant]
// rather than deadlocking on the shard mutex.
//
// The cost is one `runtime.Stack` parse per Compute call (to
// extract the goroutine ID). Acceptable per spec §19.2.6 given
// how nasty the deadlock would otherwise be to debug.
var activeCompute sync.Map

// goroutineID returns the calling goroutine's ID by parsing
// `runtime.Stack`'s "goroutine N [...]:" prefix. Allocates a small
// scratch buffer; cost is dominated by stack-trace formatting.
//
// Returns 0 if parsing fails (defensive — the package would still
// detect re-entrancy at the next Compute frame, just with
// different IDs).
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

// reentryGuard registers the current goroutine as "in compute" and
// returns an unregister closure for the deferred cleanup. The
// caller MUST `defer` the returned function; otherwise nested
// Computes leak guard slots.
//
// Panics with [ErrComputeReentrant] when the current goroutine is
// already in the registry — i.e., a Compute callback called back
// into Compute on the cache.
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

// Compute atomically applies fn to the entry for key. fn receives
// the current value (or the zero V when absent) and a presence
// boolean; it returns the new value, an action describing what to
// do with that value, and an optional error.
//
// Action handling:
//   - [ComputeStore]  — store the returned value with the cache's
//     default TTL (sliding flag inherited from the cache config).
//   - [ComputeDelete] — remove the entry, recording the eviction
//     under [EvictReasonComputed].
//   - [ComputeNoOp]   — leave the entry as it was; the returned
//     value is discarded.
//
// fn runs under the shard write lock. It MUST NOT call back into
// the cache for the same key (deadlock) and SHOULD be fast — any
// time spent in fn blocks every other Compute/Set/Delete on the
// same shard. Calls into the cache for keys hashing to a different
// shard are safe; calls for keys that hash to the same shard
// deadlock the same way.
//
// Compute is the canonical atomic update primitive; the other
// Compute* methods and the numeric helpers are sugar over it.
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
	defer s.mu.Unlock()

	var current V
	var present bool
	if e, ok := s.entries[key]; ok && !c.entryExpiredLocked(e, now) && !e.flags.has(flagNegative) {
		current = e.value
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
			effectiveTTL(c.cfg.defaultTTL, c.cfg.ttlJitter),
			c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
		return newValue, nil
	case ComputeDelete:
		if e, ok := s.entries[key]; ok {
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

// ComputeIfAbsent invokes fn only when the key is absent (or has
// expired). On a hit, the existing value is returned with
// computed=false and no fn call is made. On a miss, fn produces the
// new value and a TTL; storing is atomic with respect to other
// writers on the same key.
//
// A fn-returned TTL of 0 means "use the cache's [WithDefaultTTL]";
// negative TTLs surface as [ErrInvalidTTL]. fn returning an error
// is propagated as-is (no entry is stored).
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
	defer s.mu.Unlock()

	if e, ok := s.entries[key]; ok && !c.entryExpiredLocked(e, now) && !e.flags.has(flagNegative) {
		return e.value, false, nil
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
		effectiveTTL(ttl, c.cfg.ttlJitter),
		c.cfg.slidingTTL, int64(ttl), nil)
	return v, true, nil
}

// ComputeIfPresent invokes fn only when the key is present (and
// fresh). fn returns the new value plus a [ComputeAction]; the
// caller can store, delete, or no-op via the action discriminator.
// Returns the post-update value (or the zero V on Delete/no-entry)
// and any error fn produced.
//
// On miss, ComputeIfPresent returns ([zero V], nil) — not an
// error — matching the spirit of [Cache.SetIfPresent].
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
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return zero, nil
	}
	current := e.value

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
			effectiveTTL(c.cfg.defaultTTL, c.cfg.ttlJitter),
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

// Update is shorthand for [Cache.ComputeIfPresent] that always
// stores fn's result. Returns the new value and nil on success;
// returns ([zero V], [ErrNotFound]) when the entry is absent.
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
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return zero, ErrNotFound
	}
	newValue := fn(e.value)
	weight, werr := c.computeWeight(key, newValue)
	if werr != nil {
		return zero, werr
	}
	c.upsertLocked(s, key, newValue, weight,
		effectiveTTL(c.cfg.defaultTTL, c.cfg.ttlJitter),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
	return newValue, nil
}

// CompareAndSwap atomically replaces the value for key with new only
// when the current value equals old. Returns true when the swap
// happened.
//
// Equality uses [reflect.DeepEqual], so non-comparable V types (slices,
// maps) are supported but at higher cost. For comparable V the
// comparison reduces to a `==` check internally.
func (c *Cache[K, V]) CompareAndSwap(key K, old, newValue V) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return false
	}
	if !reflect.DeepEqual(e.value, old) {
		return false
	}
	weight, err := c.computeWeight(key, newValue)
	if err != nil {
		return false
	}
	c.upsertLocked(s, key, newValue, weight,
		effectiveTTL(c.cfg.defaultTTL, c.cfg.ttlJitter),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
	return true
}

// IncrementBy atomically adds delta to the integer value at key. If
// the entry is absent, it is created with value `0 + delta`. Returns
// the post-increment value.
//
// IncrementBy is a top-level function rather than a method because
// Go generics do not allow per-method type-parameter constraints
// beyond those of the receiver: the cache's V is `any`, so the
// Number constraint must live at the function level.
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

// Decrement is IncrementBy(c, key, -1).
//
// For unsigned V types (`uint`, `uint8`, …), wraparound at zero is
// the documented Go integer behavior; callers that need saturation
// should use [Cache.Compute] directly.
func Decrement[K comparable, V Number](c *Cache[K, V], key K) (V, error) {
	return IncrementBy(c, key, decrementDelta[V]())
}

// decrementDelta returns -1 typed as V. Implemented as a tiny helper
// because the literal `-1` fails to type-check under unsigned
// instantiations of V; we instead compute it as `0 - 1` so the
// Go compiler resolves the unsigned wraparound at instantiation
// time.
func decrementDelta[V Number]() V {
	var one V = 1
	return 0 - one
}
