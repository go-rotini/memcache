package memcache

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-rotini/memcache/internal/sketch"
)

// Cache is a generic, bounded, thread-safe in-memory cache.
//
// The zero value of Cache is not usable. Construct one with [New].
//
// All methods are safe for concurrent use. Per-shard locking keeps
// contention bounded; each Get acquires at most one shard's RLock.
type Cache[K comparable, V any] struct {
	cfg *config

	// hasher is the typed key hasher resolved from cfg.hasher (or
	// the package default when none was supplied).
	hasher func(K) uint64

	// weigher is the typed value weigher; nil when no weigher was
	// supplied. The cache treats a missing weigher as "weight = 1
	// per entry" via [clampWeight] applied to the unit weight.
	weigher Weigher[V]

	// loader is the typed loader resolved from cfg.loader, or nil.
	loader Loader[K, V]

	shards    []*shard[K, V]
	shardMask uint64

	// counters is the cache's atomic stats accumulator. It is
	// allocated even when stats are disabled so that hot-path code
	// can unconditionally bump entries/bytes; the disable flag
	// suppresses Hits/Misses/Inserts updates.
	counters *statsCounters

	closed atomic.Bool
}

// New constructs a [Cache]. At least one of [WithMaxEntries] or
// [WithMaxBytes] must be supplied; otherwise New returns
// [ErrUnbounded]. Returned errors of type [*ConfigError] indicate a
// specific option that was rejected.
func New[K comparable, V any](opts ...Option) (*Cache[K, V], error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	return build[K, V](cfg, false)
}

// NewUnbounded constructs a [Cache] with no size bound. Use only when
// the key space is provably bounded by something else; an unbounded
// cache is otherwise a memory leak in disguise.
func NewUnbounded[K comparable, V any](opts ...Option) (*Cache[K, V], error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	return build[K, V](cfg, true)
}

// Must wraps a call to [New] and panics if err is non-nil. Intended
// for package-level variable initialization.
func Must[K comparable, V any](c *Cache[K, V], err error) *Cache[K, V] {
	if err != nil {
		panic(err)
	}
	return c
}

// build validates cfg and constructs a [Cache]. allowUnbounded
// permits a cache with no entry/byte limit; otherwise an unbounded
// configuration is rejected with [ErrUnbounded].
func build[K comparable, V any](cfg *config, allowUnbounded bool) (*Cache[K, V], error) {
	if !allowUnbounded && cfg.maxEntries <= 0 && cfg.maxBytes <= 0 {
		return nil, ErrUnbounded
	}
	if cfg.maxEntries < 0 {
		return nil, &ConfigError{Field: "MaxEntries", Message: "must be non-negative"}
	}
	if cfg.maxBytes < 0 {
		return nil, &ConfigError{Field: "MaxBytes", Message: "must be non-negative"}
	}

	// Resolve the typed weigher, hasher, loader from cfg's
	// type-erased fields.
	weigher, err := resolveWeigher[V](cfg.weigher)
	if err != nil {
		return nil, err
	}
	hasher, err := resolveHasher[K](cfg.hasher)
	if err != nil {
		return nil, err
	}
	loader, err := resolveLoader[K, V](cfg.loader)
	if err != nil {
		return nil, err
	}

	if cfg.maxBytes > 0 && weigher == nil {
		return nil, &ConfigError{
			Field:   "MaxBytes",
			Message: "WithMaxBytes requires WithWeigher",
		}
	}

	shardCount := nextPowerOfTwo(max(cfg.shards, 1))

	c := &Cache[K, V]{
		cfg:       cfg,
		hasher:    hasher,
		weigher:   weigher,
		loader:    loader,
		shards:    make([]*shard[K, V], shardCount),
		shardMask: uint64(shardCount - 1),
		counters:  &statsCounters{},
	}

	perShard := perShardBudget(cfg.maxEntries, shardCount)
	for i := range c.shards {
		c.shards[i] = newShard(newPolicy[K, V](cfg.policy), perShard)
	}

	return c, nil
}

// perShardBudget computes the per-shard target entry count from a
// global maxEntries. A 10% slop factor accommodates hash-skew between
// shards. Returns 0 when maxEntries is 0 (i.e. byte-bounded or
// unbounded caches).
func perShardBudget(maxEntries, shardCount int) int {
	if maxEntries <= 0 || shardCount <= 0 {
		return 0
	}
	per := (maxEntries + shardCount - 1) / shardCount
	// 10% slop, rounded up.
	slop := max((per*10)/100, 1)
	return per + slop
}

// resolveWeigher returns the typed Weigher[V] from a type-erased any,
// or nil when none is configured. A nil weigher is a valid state, not
// an error: callers default to unit weight.
//
//nolint:nilnil // (nil, nil) signals "no weigher configured" — both fields are meaningful.
func resolveWeigher[V any](raw any) (Weigher[V], error) {
	if raw == nil {
		return nil, nil
	}
	w, ok := raw.(Weigher[V])
	if !ok {
		return nil, &ConfigError{
			Field:   "Weigher",
			Message: fmt.Sprintf("type mismatch: weigher does not match cache value type (%T)", raw),
		}
	}
	return w, nil
}

// resolveLoader returns the typed Loader[K, V] from a type-erased
// any, or nil when none is configured. A nil loader is a valid state.
//
//nolint:nilnil // (nil, nil) signals "no loader configured".
func resolveLoader[K comparable, V any](raw any) (Loader[K, V], error) {
	if raw == nil {
		return nil, nil
	}
	l, ok := raw.(Loader[K, V])
	if !ok {
		return nil, &ConfigError{
			Field:   "Loader",
			Message: fmt.Sprintf("type mismatch: loader does not match cache type parameters (%T)", raw),
		}
	}
	return l, nil
}

// resolveHasher returns the typed key hasher. When the user supplies
// none, a defaultHasher specialized to common K types is returned.
func resolveHasher[K comparable](raw any) (func(K) uint64, error) {
	if raw != nil {
		fn, ok := raw.(func(K) uint64)
		if !ok {
			return nil, &ConfigError{
				Field:   "Hasher",
				Message: fmt.Sprintf("type mismatch: hasher does not match cache key type (%T)", raw),
			}
		}
		return fn, nil
	}
	return defaultHasher[K](), nil
}

// defaultHasher returns a hasher specialized for common K types. For
// string keys it uses SipHash-2-4 with a fixed key seed; for other
// types the fallback is fmt-based and slow but correct. A future
// refinement may add fast paths for integer-like K via type
// switching.
func defaultHasher[K comparable]() func(K) uint64 {
	const k0 = uint64(0x0706050403020100)
	const k1 = uint64(0x0f0e0d0c0b0a0908)
	return func(k K) uint64 {
		switch v := any(k).(type) {
		case string:
			return sketch.HashString(k0, k1, v)
		case []byte:
			return sketch.SipHash24(k0, k1, v)
		default:
			s := fmt.Sprintf("%v", k)
			return sketch.HashString(k0, k1, s)
		}
	}
}

// shardFor returns the shard responsible for key. Always non-nil; the
// shard count is fixed at construction.
func (c *Cache[K, V]) shardFor(key K) *shard[K, V] {
	return c.shards[c.hasher(key)&c.shardMask]
}

// Len returns the current number of entries across all shards. O(1).
func (c *Cache[K, V]) Len() int {
	return int(c.counters.entries.Load())
}

// Bytes returns the current total weight of all entries. When no
// [Weigher] is configured, this equals [Cache.Len].
func (c *Cache[K, V]) Bytes() int64 {
	return c.counters.bytes.Load()
}

// Capacity returns the cache's effective configured bound. Entries
// when [WithMaxEntries] is set, otherwise bytes when [WithMaxBytes]
// is set, otherwise 0 (unbounded).
func (c *Cache[K, V]) Capacity() int64 {
	if c.cfg.maxEntries > 0 {
		return int64(c.cfg.maxEntries)
	}
	return c.cfg.maxBytes
}

// Stats returns a snapshot of counters and live state.
func (c *Cache[K, V]) Stats() Stats {
	s := c.counters.snapshot(c.cfg.clock.Now())
	s.Capacity = c.Capacity()
	return s
}

// ResetStats zeros counters. Live entry/byte counts are preserved.
func (c *Cache[K, V]) ResetStats() {
	c.counters.reset()
}

// Has reports whether the cache contains a fresh entry for key. It
// does NOT promote the entry in the eviction policy and never
// invokes the configured Loader.
func (c *Cache[K, V]) Has(key K) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	if e.expired(now) {
		return false
	}
	if e.flags.has(flagNegative) {
		return false
	}
	return true
}

// Peek returns the value without affecting eviction policy state. A
// negative-cache tombstone is reported as a miss.
func (c *Cache[K, V]) Peek(key K) (V, bool) {
	var zero V
	if c.closed.Load() {
		return zero, false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok || e.expired(now) || e.flags.has(flagNegative) {
		return zero, false
	}
	return e.value, true
}

// Get returns the value stored for key, or the zero value of V and
// false if absent or expired. Get does NOT invoke a configured
// [Loader]; use [Cache.GetOrLoad] for that.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	var zero V
	if c.closed.Load() {
		return zero, false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok {
		c.recordMiss()
		return zero, false
	}
	if e.expired(now) {
		c.removeLocked(s, e, EvictReasonExpired)
		c.counters.expirations.Add(1)
		c.recordMiss()
		return zero, false
	}
	if e.flags.has(flagNegative) {
		c.recordMiss()
		return zero, false
	}
	e.hits.Add(1)
	e.touchAccess(now)
	s.policy.OnAccess(e)
	c.recordHit()
	return e.value, true
}

// GetWithExpiry returns the value, its absolute expiry time, and a
// hit boolean. expiry is the zero time when the entry has no TTL.
func (c *Cache[K, V]) GetWithExpiry(key K) (V, time.Time, bool) {
	var zero V
	if c.closed.Load() {
		return zero, time.Time{}, false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok {
		c.recordMiss()
		return zero, time.Time{}, false
	}
	if e.expired(now) {
		c.removeLocked(s, e, EvictReasonExpired)
		c.counters.expirations.Add(1)
		c.recordMiss()
		return zero, time.Time{}, false
	}
	if e.flags.has(flagNegative) {
		c.recordMiss()
		return zero, time.Time{}, false
	}
	e.hits.Add(1)
	e.touchAccess(now)
	s.policy.OnAccess(e)
	c.recordHit()
	expNanos := e.expireAt.Load()
	if expNanos == 0 {
		return e.value, time.Time{}, true
	}
	return e.value, time.Unix(0, expNanos), true
}

// Set stores value under key with the cache's default TTL.
func (c *Cache[K, V]) Set(key K, value V) error {
	return c.setLocked(key, value, c.cfg.defaultTTL, c.cfg.slidingTTL, nil)
}

// SetWithTTL stores value under key with the given TTL. A TTL of 0
// means "no expiry"; a negative TTL returns [ErrInvalidTTL].
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	if ttl < 0 {
		return ErrInvalidTTL
	}
	return c.setLocked(key, value, ttl, c.cfg.slidingTTL, nil)
}

// Delete removes key from the cache. Returns true when an entry was
// removed.
func (c *Cache[K, V]) Delete(key K) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok {
		return false
	}
	c.removeLocked(s, e, EvictReasonDeleted)
	c.counters.deletes.Add(1)
	return true
}

// Reset removes all entries without firing eviction callbacks.
func (c *Cache[K, V]) Reset() {
	for _, s := range c.shards {
		s.mu.Lock()
		for _, e := range s.entries {
			c.counters.entries.Add(-1)
			c.counters.bytes.Add(-e.weight)
			s.policy.OnRemove(e)
			s.pool.put(e)
		}
		clear(s.entries)
		s.policy.Reset()
		s.mu.Unlock()
	}
}

// Close releases all resources and disables further operations.
// Subsequent calls return [ErrClosed]. Calling Close more than once
// returns nil on subsequent calls (it is idempotent).
func (c *Cache[K, V]) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.Reset()
	return nil
}

// recordHit increments the hit counter when stats are enabled.
func (c *Cache[K, V]) recordHit() {
	if c.cfg.statsEnabled {
		c.counters.hits.Add(1)
	}
}

// recordMiss increments the miss counter when stats are enabled.
func (c *Cache[K, V]) recordMiss() {
	if c.cfg.statsEnabled {
		c.counters.misses.Add(1)
	}
}

// setLocked performs the shared write-path used by Set / SetWithTTL.
// extraTags is ignored for now; the tagging system lands in a future
// commit.
func (c *Cache[K, V]) setLocked(key K, value V, ttl time.Duration, sliding bool, _ []string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	weight := c.weighOf(value)
	if c.cfg.maxValueWeight > 0 && weight > c.cfg.maxValueWeight {
		return &CapacityError{
			Key:        key,
			Reason:     "value weight exceeds MaxValueWeight",
			LimitField: "MaxValueWeight",
		}
	}

	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	var expireAt int64
	if ttl > 0 {
		expireAt = now + int64(ttl)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.entries[key]; ok {
		// Update path: replace value, refresh expiry.
		c.counters.bytes.Add(-existing.weight + weight)
		existing.value = value
		existing.weight = weight
		existing.expireAt.Store(expireAt)
		existing.lastAccess.Store(now)
		existing.hits.Store(0)
		existing.generation.Add(1)
		if sliding {
			existing.flags |= flagSliding
			existing.slidingTTL = int64(ttl)
		} else {
			existing.flags &^= flagSliding
			existing.slidingTTL = 0
		}
		s.policy.OnUpdate(existing)
		if c.cfg.statsEnabled {
			c.counters.updates.Add(1)
		}
		return nil
	}

	// Insert path.
	e := s.pool.get()
	e.key = key
	e.value = value
	e.weight = weight
	e.inserted = now
	e.lastAccess.Store(now)
	e.expireAt.Store(expireAt)
	if sliding {
		e.flags |= flagSliding
		e.slidingTTL = int64(ttl)
	}
	s.entries[key] = e
	s.policy.OnInsert(e)
	c.counters.entries.Add(1)
	c.counters.bytes.Add(weight)
	if c.cfg.statsEnabled {
		c.counters.inserts.Add(1)
	}

	c.evictWhileOverBudgetLocked(s)
	return nil
}

// weighOf returns the configured weight of value, defaulting to 1
// when no [Weigher] was supplied. Always >= 1 (clamped).
func (c *Cache[K, V]) weighOf(v V) int64 {
	var w int64 = 1
	if c.weigher != nil {
		w = c.weigher(v)
	}
	return clampWeight(w)
}

// evictWhileOverBudgetLocked evicts policy-chosen victims until the
// shard is within its budget. The caller must hold s.mu.
func (c *Cache[K, V]) evictWhileOverBudgetLocked(s *shard[K, V]) {
	if s.budget <= 0 {
		return
	}
	for len(s.entries) > s.budget {
		victim := s.policy.Victim()
		if victim == nil {
			return
		}
		c.removeLocked(s, victim, EvictReasonCapacity)
		c.counters.evictions.Add(1)
	}
}

// removeLocked deletes e from the shard and notifies the policy. The
// caller must hold s.mu.
func (c *Cache[K, V]) removeLocked(s *shard[K, V], e *entry[K, V], reason EvictionReason) {
	delete(s.entries, e.key)
	s.policy.OnRemove(e)
	c.counters.entries.Add(-1)
	c.counters.bytes.Add(-e.weight)
	if c.cfg.statsEnabled {
		c.counters.evictionsByReason[reason].Add(1)
	}
	s.pool.put(e)
}
