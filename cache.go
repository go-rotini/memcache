package memcache

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
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

	// expireFunc is the typed per-entry expiry predicate resolved
	// from cfg.expireFunc, or nil when none is configured.
	expireFunc func(key K, value V, meta Metadata) bool

	// tags is the cache-level inverted index used by SetWithTags
	// and InvalidateTag. Always non-nil; an empty index has zero
	// memory cost beyond the struct itself.
	tags *tagIndex[K]

	// events is the cache-level fan-out bus used by Subscribe.
	// Always non-nil; with no subscribers, publish acquires the
	// bus's RLock and immediately returns.
	events *eventBus[K, V]

	// Resolved hook callbacks. Any of these may be nil when the
	// corresponding [WithOnXxx] option was not supplied.
	onHit    func(K, V)
	onMiss   func(K)
	onEvict  func(K, V, EvictionReason)
	onExpire func(K, V)
	onLoad   func(K, V, time.Duration, error)

	shards    []*shard[K, V]
	shardMask uint64

	// counters is the cache's atomic stats accumulator. It is
	// allocated even when stats are disabled so that hot-path code
	// can unconditionally bump entries/bytes; the disable flag
	// suppresses Hits/Misses/Inserts updates.
	counters *statsCounters

	// autoSaveStop signals the auto-save goroutine to exit;
	// autoSaveDone closes once the goroutine has returned.
	// Both are nil when WithAutoSave is not configured.
	autoSaveStop chan struct{}
	autoSaveDone chan struct{}

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

// configFieldMaxBytes is the [ConfigError.Field] value reported when
// [WithMaxBytes] is misconfigured. Used in multiple call sites so a
// constant keeps them in sync.
const configFieldMaxBytes = "MaxBytes"

// build validates cfg and constructs a [Cache]. allowUnbounded
// permits a cache with no entry/byte limit; otherwise an unbounded
// configuration is rejected with [ErrUnbounded].
func build[K comparable, V any](cfg *config, allowUnbounded bool) (*Cache[K, V], error) {
	// Validate non-negativity before the unbounded check so callers
	// who supply a negative value get a precise ConfigError rather
	// than a generic ErrUnbounded.
	if cfg.maxEntries < 0 {
		return nil, &ConfigError{Field: "MaxEntries", Message: "must be non-negative"}
	}
	if cfg.maxBytes < 0 {
		return nil, &ConfigError{Field: configFieldMaxBytes, Message: "must be non-negative"}
	}
	if !allowUnbounded && cfg.maxEntries <= 0 && cfg.maxBytes <= 0 {
		return nil, ErrUnbounded
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
	expireFunc, err := resolveExpireFunc[K, V](cfg.expireFunc)
	if err != nil {
		return nil, err
	}
	hooks, err := resolveHooks[K, V](cfg)
	if err != nil {
		return nil, err
	}

	if cfg.maxBytes > 0 && weigher == nil {
		return nil, &ConfigError{
			Field:   configFieldMaxBytes,
			Message: "WithMaxBytes requires WithWeigher",
		}
	}

	shardCount := nextPowerOfTwo(max(cfg.shards, 1))

	c := &Cache[K, V]{
		cfg:        cfg,
		hasher:     hasher,
		weigher:    weigher,
		loader:     loader,
		expireFunc: expireFunc,
		tags:       newTagIndex[K](),
		events:     newEventBus[K, V](),
		onHit:      hooks.onHit,
		onMiss:     hooks.onMiss,
		onEvict:    hooks.onEvict,
		onExpire:   hooks.onExpire,
		onLoad:     hooks.onLoad,
		shards:     make([]*shard[K, V], shardCount),
		shardMask:  uint64(shardCount - 1),
		counters:   &statsCounters{},
	}

	perShard := perShardBudget(cfg.maxEntries, shardCount)
	pcfg := policyConfig[K]{budget: perShard, hasher: hasher}
	for i := range c.shards {
		c.shards[i] = newShard(newPolicy[K, V](cfg.policy, pcfg), perShard)
	}

	if err := c.applyAutoLoad(); err != nil {
		return nil, err
	}
	c.startAutoSave()

	return c, nil
}

// applyAutoLoad attempts a one-shot LoadFile when [WithAutoLoad]
// is configured. Missing files are treated as "no warm state to
// load" — not an error. Any other error fails New unless
// [WithAutoLoadIgnoreErrors] is set, in which case it is logged
// and swallowed.
func (c *Cache[K, V]) applyAutoLoad() error {
	if c.cfg.autoLoadPath == "" {
		return nil
	}
	if _, statErr := os.Stat(c.cfg.autoLoadPath); errors.Is(statErr, os.ErrNotExist) {
		return nil
	}
	_, err := c.LoadFile(c.cfg.autoLoadPath)
	if err == nil {
		return nil
	}
	if c.cfg.autoLoadIgnore {
		if c.cfg.logger != nil {
			c.cfg.logger.Warn("memcache: auto-load failed, continuing with empty cache",
				"path", c.cfg.autoLoadPath, "err", err)
		}
		return nil
	}
	return err
}

// startAutoSave launches the periodic snapshot goroutine when
// [WithAutoSave] is configured. The goroutine ticks at the
// configured interval, writes a snapshot, and exits cleanly when
// Cache.Close fires the autoSaveStop channel.
func (c *Cache[K, V]) startAutoSave() {
	if c.cfg.autoSavePath == "" || c.cfg.autoSaveInterval <= 0 {
		return
	}
	c.autoSaveStop = make(chan struct{})
	c.autoSaveDone = make(chan struct{})
	go c.runAutoSave()
}

// runAutoSave is the periodic-snapshot goroutine body.
func (c *Cache[K, V]) runAutoSave() {
	defer close(c.autoSaveDone)
	tick := make(chan struct{}, 1)
	fire := func() {
		select {
		case tick <- struct{}{}:
		default:
		}
	}
	timer := c.cfg.clock.AfterFunc(c.cfg.autoSaveInterval, fire)
	defer timer.Stop()
	for {
		select {
		case <-c.autoSaveStop:
			return
		case <-tick:
			if err := c.SaveFile(c.cfg.autoSavePath); err != nil && c.cfg.logger != nil {
				c.cfg.logger.Warn("memcache: auto-save failed",
					"path", c.cfg.autoSavePath, "err", err)
			}
			timer.Reset(c.cfg.autoSaveInterval)
		}
	}
}

// applyJitter returns ttl with uniform jitter in [-j, +j]. Per the
// spec, jitter is clamped to ttl/4 to keep the resulting expiry
// strictly positive even on aggressive jitter settings. Returns ttl
// unmodified when j is non-positive.
func applyJitter(ttl, j time.Duration) time.Duration {
	if j <= 0 {
		return ttl
	}
	if j > ttl/4 {
		j = ttl / 4
	}
	if j <= 0 {
		return ttl
	}
	// rand.Int64N(2*j) is in [0, 2*j); subtract j to get [-j, +j-1].
	delta := rand.Int64N(int64(2*j)) - int64(j)
	return ttl + time.Duration(delta)
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

// resolveExpireFunc returns the typed per-entry expiry predicate
// from a type-erased any, or nil when none is configured.
//
//nolint:nilnil // (nil, nil) signals "no expire func configured".
func resolveExpireFunc[K comparable, V any](raw any) (func(K, V, Metadata) bool, error) {
	if raw == nil {
		return nil, nil
	}
	fn, ok := raw.(func(K, V, Metadata) bool)
	if !ok {
		return nil, &ConfigError{
			Field:   "ExpireFunc",
			Message: fmt.Sprintf("type mismatch: expire-func does not match cache type parameters (%T)", raw),
		}
	}
	return fn, nil
}

// resolvedHooks bundles every typed hook callback resolved from the
// type-erased fields on config. Each field may be nil when the
// corresponding [WithOnXxx] option was not supplied.
type resolvedHooks[K comparable, V any] struct {
	onHit    func(K, V)
	onMiss   func(K)
	onEvict  func(K, V, EvictionReason)
	onExpire func(K, V)
	onLoad   func(K, V, time.Duration, error)
}

// resolveHooks type-asserts every WithOn* option from cfg into its
// typed shape. A type-mismatch in any one of them surfaces as a
// [*ConfigError] keyed on the misconfigured option name.
func resolveHooks[K comparable, V any](cfg *config) (resolvedHooks[K, V], error) {
	var out resolvedHooks[K, V]
	if cfg.onHit != nil {
		fn, ok := cfg.onHit.(func(K, V))
		if !ok {
			return out, &ConfigError{
				Field:   "OnHit",
				Message: fmt.Sprintf("type mismatch: hook does not match cache type parameters (%T)", cfg.onHit),
			}
		}
		out.onHit = fn
	}
	if cfg.onMiss != nil {
		fn, ok := cfg.onMiss.(func(K))
		if !ok {
			return out, &ConfigError{
				Field:   "OnMiss",
				Message: fmt.Sprintf("type mismatch: hook does not match cache key type (%T)", cfg.onMiss),
			}
		}
		out.onMiss = fn
	}
	if cfg.onEvict != nil {
		fn, ok := cfg.onEvict.(func(K, V, EvictionReason))
		if !ok {
			return out, &ConfigError{
				Field:   "OnEvict",
				Message: fmt.Sprintf("type mismatch: hook does not match cache type parameters (%T)", cfg.onEvict),
			}
		}
		out.onEvict = fn
	}
	if cfg.onExpire != nil {
		fn, ok := cfg.onExpire.(func(K, V))
		if !ok {
			return out, &ConfigError{
				Field:   "OnExpire",
				Message: fmt.Sprintf("type mismatch: hook does not match cache type parameters (%T)", cfg.onExpire),
			}
		}
		out.onExpire = fn
	}
	if cfg.onLoad != nil {
		fn, ok := cfg.onLoad.(func(K, V, time.Duration, error))
		if !ok {
			return out, &ConfigError{
				Field:   "OnLoad",
				Message: fmt.Sprintf("type mismatch: hook does not match cache type parameters (%T)", cfg.onLoad),
			}
		}
		out.onLoad = fn
	}
	return out, nil
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
	if c.entryExpiredLocked(e, now) {
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
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return zero, false
	}
	return e.value, true
}

// Get returns the value stored for key, or the zero value of V and
// false if absent or expired. Get does NOT invoke a configured
// [Loader]; use [Cache.GetOrLoad] for that.
//
// When [WithRefreshAhead] is enabled, a successful hit on an entry
// past `refreshAt × TTL` triggers an asynchronous Loader call (the
// cached value continues to be returned).
//
// When [WithStaleWhileRevalidate] is enabled, a hit on an entry
// whose TTL has elapsed by less than `staleFor` returns the stale
// value AND triggers an asynchronous Loader refresh.
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
		c.fireMiss(key)
		return zero, false
	}
	if c.entryExpiredLocked(e, now) {
		// Stale-while-revalidate: serve stale + trigger refresh.
		if !e.flags.has(flagNegative) && c.shouldServeStale(e, now) {
			c.triggerAsyncRefreshLocked(s, key)
			e.hits.Add(1)
			s.policy.OnAccess(e)
			c.recordHit()
			c.fireHit(key, e.value)
			return e.value, true
		}
		c.removeLocked(s, e, EvictReasonExpired)
		c.counters.expirations.Add(1)
		c.recordMiss()
		c.fireMiss(key)
		return zero, false
	}
	if e.flags.has(flagNegative) {
		c.recordMiss()
		c.fireMiss(key)
		return zero, false
	}
	e.hits.Add(1)
	if e.touchAccess(now) {
		s.expiryFix(e)
	}
	s.policy.OnAccess(e)
	if c.shouldRefreshAhead(e, now) {
		c.triggerAsyncRefreshLocked(s, key)
	}
	c.recordHit()
	c.fireHit(key, e.value)
	return e.value, true
}

// fireHit invokes the configured OnHit hook (if any) and publishes
// no event — there is no EventHit kind in the spec; subscribers
// derive hit info from Stats. The synchronous hook still runs
// since it gives callers raw observability.
func (c *Cache[K, V]) fireHit(key K, value V) {
	if c.onHit != nil {
		c.onHit(key, value)
	}
}

// fireMiss invokes the configured OnMiss hook.
func (c *Cache[K, V]) fireMiss(key K) {
	if c.onMiss != nil {
		c.onMiss(key)
	}
}

// shouldRefreshAhead reports whether the entry has aged past the
// configured refresh-ahead threshold.
func (c *Cache[K, V]) shouldRefreshAhead(e *entry[K, V], now int64) bool {
	if c.loader == nil {
		return false
	}
	if c.cfg.refreshAheadAt <= 0 || c.cfg.refreshAheadAt >= 1 {
		return false
	}
	if e.flags.has(flagNegative) {
		return false
	}
	exp := e.expireAt.Load()
	if exp == 0 {
		return false
	}
	age := now - e.inserted
	ttl := exp - e.inserted
	if ttl <= 0 {
		return false
	}
	threshold := int64(c.cfg.refreshAheadAt * float64(ttl))
	return age >= threshold
}

// shouldServeStale reports whether SWR applies — the entry's TTL
// has elapsed by less than the configured staleFor window. Returns
// false for entries with no TTL or that are still fresh.
func (c *Cache[K, V]) shouldServeStale(e *entry[K, V], now int64) bool {
	if c.loader == nil {
		return false
	}
	if c.cfg.swrStaleFor <= 0 {
		return false
	}
	if e.flags.has(flagNegative) {
		return false
	}
	exp := e.expireAt.Load()
	if exp == 0 || exp > now {
		return false
	}
	return now-exp < int64(c.cfg.swrStaleFor)
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
	if c.entryExpiredLocked(e, now) {
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
	if e.touchAccess(now) {
		s.expiryFix(e)
	}
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

// SetWithOptions stores value under key with per-call overrides
// supplied via [SetOption]. The cache's defaults (TTL, sliding,
// Weigher) apply to fields no option touches.
//
// Per-call options that conflict with cache configuration are
// resolved in favor of the option (e.g., [SetTTL](0) explicitly
// stores an entry with no expiry even when the cache has a non-zero
// [WithDefaultTTL]).
func (c *Cache[K, V]) SetWithOptions(key K, value V, opts ...SetOption) error {
	if c.closed.Load() {
		return ErrClosed
	}
	sc := defaultSetConfig(c.cfg)
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	if sc.hasTTL && sc.ttl < 0 {
		return ErrInvalidTTL
	}

	// Determine effective weight.
	var weight int64
	if sc.hasWeight {
		weight = clampWeight(sc.weight)
		if c.cfg.maxValueWeight > 0 && weight > c.cfg.maxValueWeight {
			return &CapacityError{
				Key:        key,
				Reason:     "explicit weight exceeds MaxValueWeight",
				LimitField: "MaxValueWeight",
			}
		}
	} else {
		w, err := c.computeWeight(key, value)
		if err != nil {
			return err
		}
		weight = w
	}

	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if sc.hasExpiry {
		c.upsertWithAbsoluteExpiryLocked(s, key, value, weight, sc)
	} else {
		c.upsertLocked(s, key, value, weight,
			effectiveTTL(sc.ttl, c.cfg.ttlJitter),
			sc.sliding, int64(sc.ttl), sc.tags)
	}
	return nil
}

// upsertWithAbsoluteExpiryLocked is the [SetExpireAt] variant of
// upsertLocked: it stores the entry with an absolute expiry time
// rather than a TTL-derived one. No jitter is applied (the caller
// chose an exact moment).
func (c *Cache[K, V]) upsertWithAbsoluteExpiryLocked(
	s *shard[K, V], key K, value V, weight int64, sc setConfig,
) {
	now := c.cfg.clock.Now().UnixNano()
	var expireAt int64
	if !sc.expireAt.IsZero() {
		expireAt = sc.expireAt.UnixNano()
	}
	if existing, ok := s.entries[key]; ok {
		var oldTags []string
		if c.tags != nil && len(existing.tags) > 0 {
			oldTags = append([]string(nil), existing.tags...)
		}
		c.counters.bytes.Add(-existing.weight + weight)
		existing.value = value
		existing.weight = weight
		existing.expireAt.Store(expireAt)
		existing.lastAccess.Store(now)
		existing.hits.Store(0)
		existing.generation.Add(1)
		// SetExpireAt always implies absolute, never sliding.
		existing.flags &^= flagSliding
		existing.slidingTTL = 0
		if len(sc.tags) > 0 {
			existing.tags = append(existing.tags[:0], sc.tags...)
		} else {
			existing.tags = existing.tags[:0]
		}
		c.retagLocked(key, oldTags, sc.tags)
		s.expiryFix(existing)
		s.policy.OnUpdate(existing)
		if c.cfg.statsEnabled {
			c.counters.updates.Add(1)
		}
		if expireAt > 0 {
			c.startJanitorLocked(s)
		}
		return
	}
	e := s.pool.get()
	e.key = key
	e.value = value
	e.weight = weight
	e.inserted = now
	e.lastAccess.Store(now)
	e.expireAt.Store(expireAt)
	if len(sc.tags) > 0 {
		e.tags = append(e.tags[:0], sc.tags...)
	}
	s.entries[key] = e
	s.expiryAdd(e)
	s.policy.OnInsert(e)
	c.retagLocked(key, nil, sc.tags)
	c.counters.entries.Add(1)
	c.counters.bytes.Add(weight)
	if c.cfg.statsEnabled {
		c.counters.inserts.Add(1)
	}
	if expireAt > 0 {
		c.startJanitorLocked(s)
	}
	c.evictWhileOverBudgetLocked(s)
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
		// Drop heap state: every entry was returned to the pool so
		// the slice's pointers must not be reused.
		for i := range s.expHeap {
			s.expHeap[i] = nil
		}
		s.expHeap = s.expHeap[:0]
		s.mu.Unlock()
	}
	if c.tags != nil {
		c.tags.reset()
	}
}

// Close releases all resources, stops every shard's janitor goroutine,
// stops the auto-save goroutine after writing a final snapshot,
// closes every active subscriber channel, and disables further
// operations. Subsequent calls return nil — Close is idempotent.
// Reads and writes after Close return their zero/error path
// ([ErrClosed] for writes; the zero value with ok=false for reads).
//
// A final auto-save error is logged via the configured slog.Logger
// but does not propagate from Close — there is no useful action a
// caller can take at shutdown time.
func (c *Cache[K, V]) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Final snapshot BEFORE stopping janitors / clearing entries.
	// saveFileTo bypasses the closed check (closed is already true
	// here) so the final auto-save can persist live cache state.
	if c.cfg.autoSavePath != "" {
		if err := c.saveFileTo(c.cfg.autoSavePath); err != nil && c.cfg.logger != nil {
			c.cfg.logger.Warn("memcache: final auto-save failed",
				"path", c.cfg.autoSavePath, "err", err)
		}
	}
	if c.autoSaveStop != nil {
		close(c.autoSaveStop)
		<-c.autoSaveDone
	}
	c.stopAllJanitors()
	c.Reset()
	if c.events != nil {
		c.events.close()
	}
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
func (c *Cache[K, V]) setLocked(key K, value V, ttl time.Duration, sliding bool, tags []string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return err
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	c.upsertLocked(s, key, value, weight, effectiveTTL(ttl, c.cfg.ttlJitter), sliding, int64(ttl), tags)
	return nil
}

// computeWeight returns the configured weight of value and validates
// it against [WithMaxValueWeight]. Returns a [*CapacityError] when
// the value exceeds the limit.
func (c *Cache[K, V]) computeWeight(key K, value V) (int64, error) {
	weight := c.weighOf(value)
	if c.cfg.maxValueWeight > 0 && weight > c.cfg.maxValueWeight {
		return 0, &CapacityError{
			Key:        key,
			Reason:     "value weight exceeds MaxValueWeight",
			LimitField: "MaxValueWeight",
		}
	}
	return weight, nil
}

// effectiveTTL returns ttl with cache-level jitter applied. A non-
// positive ttl is returned unchanged; the caller interprets 0 as "no
// expiry".
func effectiveTTL(ttl, jitter time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	return applyJitter(ttl, jitter)
}

// upsertLocked is the shared insert-or-update routine. The caller
// must hold s.mu (write). effectiveTTL has already had jitter applied
// and is converted to an absolute expireAt internally; rawTTL is the
// original (pre-jitter) duration, persisted on the entry so [Touch]
// and the sliding-TTL path can re-derive an expireAt.
func (c *Cache[K, V]) upsertLocked(
	s *shard[K, V], key K, value V, weight int64,
	effectiveTTL time.Duration, sliding bool, rawTTL int64, tags []string,
) {
	now := c.cfg.clock.Now().UnixNano()
	var expireAt int64
	if effectiveTTL > 0 {
		expireAt = now + int64(effectiveTTL)
	}
	if existing, ok := s.entries[key]; ok {
		// Capture the prior tag set before we overwrite the
		// slice; retagLocked needs the full old set to untag.
		var oldTags []string
		if c.tags != nil && len(existing.tags) > 0 {
			oldTags = append([]string(nil), existing.tags...)
		}
		c.counters.bytes.Add(-existing.weight + weight)
		existing.value = value
		existing.weight = weight
		existing.expireAt.Store(expireAt)
		existing.lastAccess.Store(now)
		existing.hits.Store(0)
		existing.generation.Add(1)
		if sliding {
			existing.flags |= flagSliding
			existing.slidingTTL = rawTTL
		} else {
			existing.flags &^= flagSliding
			existing.slidingTTL = 0
		}
		if len(tags) > 0 {
			existing.tags = append(existing.tags[:0], tags...)
		} else {
			existing.tags = existing.tags[:0]
		}
		c.retagLocked(key, oldTags, tags)
		s.expiryFix(existing)
		s.policy.OnUpdate(existing)
		if c.cfg.statsEnabled {
			c.counters.updates.Add(1)
		}
		if expireAt > 0 {
			c.startJanitorLocked(s)
		}
		c.publishEvent(Event[K, V]{
			Kind:  EventUpdate,
			Key:   key,
			Value: value,
			At:    c.cfg.clock.Now(),
			Tags:  tags,
		})
		return
	}
	e := s.pool.get()
	e.key = key
	e.value = value
	e.weight = weight
	e.inserted = now
	e.lastAccess.Store(now)
	e.expireAt.Store(expireAt)
	if sliding {
		e.flags |= flagSliding
		e.slidingTTL = rawTTL
	}
	if len(tags) > 0 {
		e.tags = append(e.tags[:0], tags...)
	}
	s.entries[key] = e
	s.expiryAdd(e)
	s.policy.OnInsert(e)
	c.retagLocked(key, nil, tags)
	c.counters.entries.Add(1)
	c.counters.bytes.Add(weight)
	if c.cfg.statsEnabled {
		c.counters.inserts.Add(1)
	}
	if expireAt > 0 {
		c.startJanitorLocked(s)
	}
	c.evictWhileOverBudgetLocked(s)
	c.publishEvent(Event[K, V]{
		Kind:  EventInsert,
		Key:   key,
		Value: value,
		At:    c.cfg.clock.Now(),
		Tags:  tags,
	})
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

// entryExpiredLocked reports whether e should be treated as expired
// at `now`. Combines the entry's TTL check with the cache's
// optional [WithExpireFunc] predicate; the predicate is recovered
// on panic and the entry is treated as fresh in that case (a
// deliberately defensive default — preserve data over loss).
//
// Caller must hold the entry's shard lock (read or write) so the
// metadata snapshot it builds reflects a consistent point in time.
func (c *Cache[K, V]) entryExpiredLocked(e *entry[K, V], now int64) bool {
	if e.expired(now) {
		return true
	}
	if c.expireFunc == nil {
		return false
	}
	expired, panicked := c.callExpireFunc(e)
	if panicked && c.cfg.logger != nil {
		c.cfg.logger.Warn("memcache: WithExpireFunc panicked; entry treated as fresh",
			"name", c.cfg.name)
	}
	return expired
}

// callExpireFunc invokes the configured expire predicate with panic
// recovery. Returns (expired, panicked). On panic returns (false,
// true) so callers can keep the entry alive AND log the issue.
func (c *Cache[K, V]) callExpireFunc(e *entry[K, V]) (expired, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			expired = false
			panicked = true
		}
	}()
	return c.expireFunc(e.key, e.value, e.metadata()), false
}

// removeLocked deletes e from the shard and notifies the policy. The
// caller must hold s.mu. Tags carried by the entry are unindexed
// from the cache-level tagIndex so future InvalidateTag calls do
// not see this key.
//
// Fires `OnExpire` + publishes `EventExpire` when reason is
// `EvictReasonExpired` or `EvictReasonExpireFunc`; otherwise fires
// `OnEvict` + publishes `EventEvict`. Callbacks run synchronously
// under the shard write lock — slow callbacks block writes for that
// shard.
func (c *Cache[K, V]) removeLocked(s *shard[K, V], e *entry[K, V], reason EvictionReason) {
	// Capture key/value/tags BEFORE returning the entry to the pool.
	key := e.key
	value := e.value
	var tagsCopy []string
	if len(e.tags) > 0 {
		tagsCopy = append([]string(nil), e.tags...)
	}
	negative := e.flags.has(flagNegative)

	delete(s.entries, e.key)
	s.expiryRemove(e)
	s.policy.OnRemove(e)
	if c.tags != nil && len(e.tags) > 0 {
		c.tags.untag(e.key, e.tags)
	}
	c.counters.entries.Add(-1)
	c.counters.bytes.Add(-e.weight)
	if c.cfg.statsEnabled {
		c.counters.evictionsByReason[reason].Add(1)
	}
	s.pool.put(e)

	// Negative tombstones are an internal artifact of the loader
	// path; suppress callbacks/events for them so subscribers
	// don't see "evictions" they never asked for.
	if negative {
		return
	}

	switch reason {
	case EvictReasonExpired, EvictReasonExpireFunc:
		if c.onExpire != nil {
			c.onExpire(key, value)
		}
		c.publishEvent(Event[K, V]{
			Kind:   EventExpire,
			Key:    key,
			Value:  value,
			Reason: reason,
			At:     c.cfg.clock.Now(),
			Tags:   tagsCopy,
		})
	default:
		if c.onEvict != nil {
			c.onEvict(key, value, reason)
		}
		c.publishEvent(Event[K, V]{
			Kind:   EventEvict,
			Key:    key,
			Value:  value,
			Reason: reason,
			At:     c.cfg.clock.Now(),
			Tags:   tagsCopy,
		})
	}
}

// Range calls fn for every entry in the cache. Iteration is shard by
// shard under each shard's read lock; expired and negative-cache
// entries are skipped. The visit set is NOT a consistent snapshot —
// concurrent inserts and deletes may or may not be observed. fn
// returning false stops iteration immediately.
//
// Long Range scans on huge caches block writes for the duration of
// each shard's pass; callers who need an iteration that does not
// hold locks should use [Cache.Keys] and re-Get individually.
func (c *Cache[K, V]) Range(fn func(key K, value V) bool) {
	if c.closed.Load() || fn == nil {
		return
	}
	now := c.cfg.clock.Now().UnixNano()
	for _, s := range c.shards {
		stop := c.rangeShard(s, now, fn)
		if stop {
			return
		}
	}
}

// rangeShard runs fn over a single shard's live entries. Returns true
// when fn requested an early stop.
func (c *Cache[K, V]) rangeShard(s *shard[K, V], now int64, fn func(K, V) bool) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.entries {
		if c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
			continue
		}
		if !fn(e.key, e.value) {
			return true
		}
	}
	return false
}

// Keys returns a freshly-allocated snapshot of every key currently
// in the cache. O(n) time and allocation; intended for diagnostics
// and bulk operations on small caches. Use [Cache.Range] for
// non-allocating iteration.
func (c *Cache[K, V]) Keys() []K {
	if c.closed.Load() {
		return nil
	}
	keys := make([]K, 0, c.Len())
	c.Range(func(k K, _ V) bool {
		keys = append(keys, k)
		return true
	})
	return keys
}

// Clear removes every entry, recording each removal under
// [EvictReasonClear] in stats. Hooks (Phase 8) will fire here once
// they are wired up; today Clear behaves like [Cache.Reset] but
// updates per-reason eviction counters.
func (c *Cache[K, V]) Clear() {
	if c.closed.Load() {
		return
	}
	for _, s := range c.shards {
		s.mu.Lock()
		for _, e := range s.entries {
			c.counters.entries.Add(-1)
			c.counters.bytes.Add(-e.weight)
			if c.cfg.statsEnabled {
				c.counters.evictionsByReason[EvictReasonClear].Add(1)
			}
			s.policy.OnRemove(e)
			s.pool.put(e)
		}
		clear(s.entries)
		s.policy.Reset()
		for i := range s.expHeap {
			s.expHeap[i] = nil
		}
		s.expHeap = s.expHeap[:0]
		s.mu.Unlock()
	}
	if c.tags != nil {
		c.tags.reset()
	}
}

// TTL returns the remaining time-to-live for the entry at key. The
// boolean is true when the entry exists and is fresh; (0, true)
// means the entry has no TTL configured. (0, false) means the entry
// is absent or already expired.
func (c *Cache[K, V]) TTL(key K) (time.Duration, bool) {
	if c.closed.Load() {
		return 0, false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return 0, false
	}
	exp := e.expireAt.Load()
	if exp == 0 {
		return 0, true
	}
	remaining := exp - now
	if remaining < 0 {
		return 0, false
	}
	return time.Duration(remaining), true
}

// Expiry returns the absolute expiry time for the entry at key, or
// the zero time when the entry has no TTL. The boolean is true when
// the entry exists and is fresh.
func (c *Cache[K, V]) Expiry(key K) (time.Time, bool) {
	if c.closed.Load() {
		return time.Time{}, false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return time.Time{}, false
	}
	exp := e.expireAt.Load()
	if exp == 0 {
		return time.Time{}, true
	}
	return time.Unix(0, exp), true
}

// Touch refreshes the TTL of the entry at key without modifying its
// value. The new TTL is the entry's recorded sliding-TTL when set,
// otherwise the cache's [WithDefaultTTL]. Returns true when the
// entry exists and was refreshed.
func (c *Cache[K, V]) Touch(key K) bool {
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
	ttl := c.cfg.defaultTTL
	if e.flags.has(flagSliding) && e.slidingTTL > 0 {
		ttl = time.Duration(e.slidingTTL)
	}
	c.refreshExpiryLocked(s, e, now, ttl)
	return true
}

// TouchWithTTL resets the TTL of the entry at key to the given
// duration. A TTL of 0 clears the expiry; a negative TTL is a no-op
// and returns false. Returns true when the entry exists and was
// refreshed.
func (c *Cache[K, V]) TouchWithTTL(key K, ttl time.Duration) bool {
	if c.closed.Load() || ttl < 0 {
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
	c.refreshExpiryLocked(s, e, now, ttl)
	return true
}

// refreshExpiryLocked updates the entry's expireAt based on the
// supplied TTL (with cache-level jitter applied when ttl > 0). The
// caller must hold the entry's shard write lock. Adjusts the heap
// position and starts the shard's janitor if a TTL was applied.
func (c *Cache[K, V]) refreshExpiryLocked(s *shard[K, V], e *entry[K, V], now int64, ttl time.Duration) {
	var expireAt int64
	if ttl > 0 {
		expireAt = now + int64(applyJitter(ttl, c.cfg.ttlJitter))
	}
	e.expireAt.Store(expireAt)
	e.lastAccess.Store(now)
	s.expiryFix(e)
	if expireAt > 0 {
		c.startJanitorLocked(s)
	}
}

// SetIfAbsent stores value under key only when the key is absent (or
// expired). Returns stored=true when the value was inserted.
func (c *Cache[K, V]) SetIfAbsent(key K, value V) (bool, error) {
	if c.closed.Load() {
		return false, ErrClosed
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.entries[key]; ok && !existing.expired(now) && !existing.flags.has(flagNegative) {
		return false, nil
	}
	c.upsertLocked(s, key, value, weight,
		effectiveTTL(c.cfg.defaultTTL, c.cfg.ttlJitter),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
	return true, nil
}

// SetIfPresent updates the value under key only when an entry already
// exists (and is fresh). Returns updated=true when the entry was
// replaced.
func (c *Cache[K, V]) SetIfPresent(key K, value V) (bool, error) {
	if c.closed.Load() {
		return false, ErrClosed
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.entries[key]
	if !ok || c.entryExpiredLocked(existing, now) || existing.flags.has(flagNegative) {
		return false, nil
	}
	c.upsertLocked(s, key, value, weight,
		effectiveTTL(c.cfg.defaultTTL, c.cfg.ttlJitter),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
	return true, nil
}

// DeleteIf removes the entry for key only when pred returns true for
// the current value. pred is invoked under the shard write lock and
// must therefore be fast and must not call back into the cache for
// the same key. Returns true when the entry was removed.
func (c *Cache[K, V]) DeleteIf(key K, pred func(V) bool) bool {
	if c.closed.Load() || pred == nil {
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
	if !pred(e.value) {
		return false
	}
	c.removeLocked(s, e, EvictReasonDeleted)
	c.counters.deletes.Add(1)
	return true
}

// GetOrSet returns the cached value for key when present (and fresh)
// or stores value and returns it otherwise. The boolean reports
// loaded=true when the returned value came from the cache; loaded=
// false when a new entry was inserted.
//
// GetOrSet is atomic with respect to concurrent Set on the same key.
// It does not invoke a configured Loader; for that, see [Cache.GetOrLoad].
func (c *Cache[K, V]) GetOrSet(key K, value V) (V, bool, error) {
	var zero V
	if c.closed.Load() {
		return zero, false, ErrClosed
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return zero, false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.entries[key]; ok && !c.entryExpiredLocked(existing, now) && !existing.flags.has(flagNegative) {
		existing.hits.Add(1)
		if existing.touchAccess(now) {
			s.expiryFix(existing)
		}
		s.policy.OnAccess(existing)
		c.recordHit()
		return existing.value, true, nil
	}
	c.upsertLocked(s, key, value, weight,
		effectiveTTL(c.cfg.defaultTTL, c.cfg.ttlJitter),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
	return value, false, nil
}

// PeekOrAdd is [Cache.GetOrSet] that does not promote the existing
// entry in the eviction policy on the read side. Newly inserted
// entries follow the policy's normal placement.
func (c *Cache[K, V]) PeekOrAdd(key K, value V) (V, bool, error) {
	var zero V
	if c.closed.Load() {
		return zero, false, ErrClosed
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return zero, false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.entries[key]; ok && !existing.expired(now) && !existing.flags.has(flagNegative) {
		// Peek semantics: do NOT call OnAccess and do not bump hits.
		return existing.value, true, nil
	}
	c.upsertLocked(s, key, value, weight,
		effectiveTTL(c.cfg.defaultTTL, c.cfg.ttlJitter),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), nil)
	return value, false, nil
}

// Resize changes the cache's bound at runtime. When MaxEntries is the
// configured bound, newSize is interpreted as the new entry budget;
// when MaxBytes is the configured bound, newSize is the new byte
// budget. Resize returns the number of entries evicted as a result
// of shrinking; growing the cache evicts nothing.
//
// Per-shard sub-budgets are recomputed and propagated to each shard
// and its eviction policy via [evictionPolicy.SetBudget]. Subsequent
// Set/Compute calls drive any further eviction the policies need.
func (c *Cache[K, V]) Resize(newSize int64) int {
	if c.closed.Load() {
		return 0
	}
	if newSize < 0 {
		return 0
	}

	if c.cfg.maxEntries > 0 || c.cfg.maxBytes <= 0 {
		c.cfg.maxEntries = int(newSize)
	} else {
		c.cfg.maxBytes = newSize
	}

	perShard := perShardBudget(c.cfg.maxEntries, len(c.shards))
	evicted := 0
	for _, s := range c.shards {
		s.mu.Lock()
		s.budget = perShard
		s.policy.SetBudget(perShard)
		evicted += c.shrinkShardLocked(s)
		s.mu.Unlock()
	}
	c.publishEvent(Event[K, V]{
		Kind: EventResize,
		At:   c.cfg.clock.Now(),
	})
	return evicted
}

// shrinkShardLocked evicts policy-chosen victims until the shard is
// within budget, recording each eviction under [EvictReasonResize].
// Caller must hold s.mu.
func (c *Cache[K, V]) shrinkShardLocked(s *shard[K, V]) int {
	if s.budget <= 0 {
		return 0
	}
	count := 0
	for len(s.entries) > s.budget {
		victim := s.policy.Victim()
		if victim == nil {
			return count
		}
		c.removeLocked(s, victim, EvictReasonResize)
		c.counters.evictions.Add(1)
		count++
	}
	return count
}

// Sync drains pending background work — async refreshes, async tag
// cleanup, async tier-2 writes — and returns when the cache is in a
// quiescent state. Returns the context's error if it cancels first.
//
// In v0 there is no background work to drain (refresh-ahead, async
// writes, etc. are not yet implemented), so Sync just respects the
// context. The signature is stable so callers can integrate today.
func (c *Cache[K, V]) Sync(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // pass through ctx.Err verbatim
	}
	return nil
}

// DeleteExpired performs an immediate sweep over every shard,
// removing each entry whose TTL has elapsed. Returns the number of
// entries removed. Useful in tests, in REPL `:gc`-style commands,
// and after wall-clock jumps where the lazy/janitor paths might lag.
func (c *Cache[K, V]) DeleteExpired() int {
	if c.closed.Load() {
		return 0
	}
	now := c.cfg.clock.Now().UnixNano()
	count := 0
	for _, s := range c.shards {
		s.mu.Lock()
		for _, e := range s.entries {
			if e.expired(now) {
				c.removeLocked(s, e, EvictReasonExpired)
				c.counters.expirations.Add(1)
				count++
			}
		}
		s.mu.Unlock()
	}
	return count
}

// DeletePrefix removes every entry whose key begins with the given
// prefix. Only valid when K is `string` or implements [Prefixer]. For
// caches with other K types DeletePrefix is a no-op and returns 0;
// callers can detect this case via [Prefixer] type assertions on
// their own K type before calling.
//
// Iteration is O(n) shard-by-shard. Tag-based invalidation
// ([Cache.InvalidateTag], Phase 7) is the preferred mechanism when
// applicable.
func (c *Cache[K, V]) DeletePrefix(prefix string) int {
	if c.closed.Load() {
		return 0
	}
	matcher := prefixMatcher[K](prefix)
	if matcher == nil {
		return 0
	}
	count := 0
	for _, s := range c.shards {
		s.mu.Lock()
		for _, e := range s.entries {
			if matcher(e.key) {
				c.removeLocked(s, e, EvictReasonDeletedPrefix)
				count++
			}
		}
		s.mu.Unlock()
	}
	return count
}

// prefixMatcher returns a function that reports whether a K-typed
// key matches the supplied prefix, or nil when K is neither `string`
// nor a [Prefixer]-implementing type.
func prefixMatcher[K comparable](prefix string) func(K) bool {
	var zero K
	if _, ok := any(zero).(string); ok {
		return func(k K) bool {
			s, _ := any(k).(string)
			return strings.HasPrefix(s, prefix)
		}
	}
	if _, ok := any(zero).(Prefixer); ok {
		return func(k K) bool {
			p, ok := any(k).(Prefixer)
			return ok && p.HasPrefix(prefix)
		}
	}
	return nil
}

// DeleteWhere removes every entry for which pred returns true when
// called with the entry's (key, value). pred runs OUTSIDE the shard
// write lock so it may freely call cache methods on other keys —
// callers should still keep pred fast and side-effect-free.
//
// Iteration is shard-by-shard. Within each shard the cache snapshots
// matching candidates under a read lock, evaluates pred outside any
// lock, then re-acquires the write lock to delete the matches. As a
// consequence, an entry that was modified between snapshot and
// delete will still be removed if its pre-modification value
// satisfied pred — documented eventual semantics.
func (c *Cache[K, V]) DeleteWhere(pred func(key K, value V) bool) int {
	if c.closed.Load() || pred == nil {
		return 0
	}
	count := 0
	type candidate struct {
		key   K
		value V
	}
	now := c.cfg.clock.Now().UnixNano()
	for _, s := range c.shards {
		var pairs []candidate
		s.mu.RLock()
		for _, e := range s.entries {
			if c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
				continue
			}
			pairs = append(pairs, candidate{key: e.key, value: e.value})
		}
		s.mu.RUnlock()

		var doomed []K
		for _, p := range pairs {
			if pred(p.key, p.value) {
				doomed = append(doomed, p.key)
			}
		}
		if len(doomed) == 0 {
			continue
		}
		s.mu.Lock()
		for _, k := range doomed {
			if e, ok := s.entries[k]; ok {
				c.removeLocked(s, e, EvictReasonDeletedWhere)
				count++
			}
		}
		s.mu.Unlock()
	}
	return count
}

// GetOrLoad returns the cached value for key, or invokes the
// configured [Loader] when the key is absent (or expired, or marked
// negative-cached past its window). Concurrent callers for the same
// missing key share a single Loader invocation (singleflight); the
// load count is bumped once and waiters that joined an in-flight
// call increment Stats.LoadCoalesced.
//
// Returns [ErrNoLoader] when no [WithLoader] is configured,
// [ErrClosed] after [Cache.Close], the caller's ctx error if ctx
// cancels before the load resolves, or whatever the Loader returns.
// On Loader [ErrNotFound] with [WithNegativeCache] active, a
// negative-cache tombstone is recorded and the same error is
// returned.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K) (V, error) {
	var zero V
	if c.closed.Load() {
		return zero, ErrClosed
	}
	if c.loader == nil {
		return zero, ErrNoLoader
	}
	if err := ctx.Err(); err != nil {
		return zero, err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	return c.loadOrJoin(ctx, key, c.loader.Load)
}

// GetOrLoadFn is [Cache.GetOrLoad] with a per-call loader function.
// Useful when a one-off load needs different semantics than the
// cache-level [WithLoader]. fn is invoked at most once per
// concurrent miss (singleflight by key).
func (c *Cache[K, V]) GetOrLoadFn(
	ctx context.Context, key K,
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) (V, error) {
	var zero V
	if c.closed.Load() {
		return zero, ErrClosed
	}
	if fn == nil {
		return zero, ErrNoLoader
	}
	if err := ctx.Err(); err != nil {
		return zero, err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	return c.loadOrJoin(ctx, key, fn)
}

// Refresh asynchronously triggers a Loader call for key, replacing
// the cached value when the load completes. Joins an existing
// in-flight call instead of starting a duplicate. Returns
// immediately with [ErrNoLoader] when no [WithLoader] is configured,
// [ErrClosed] when the cache is closed, or ctx.Err if ctx is
// already canceled.
func (c *Cache[K, V]) Refresh(ctx context.Context, key K) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if c.loader == nil {
		return ErrNoLoader
	}
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	c.triggerAsyncRefresh(c.shardFor(key), key)
	return nil
}

// RefreshAll triggers a Loader call for every key currently in the
// cache. Returns the number of refresh flights queued (skipping
// keys that already have an in-flight load). Negative-cache
// tombstones are NOT refreshed.
//
// Snapshot-and-iterate semantics: the key set is captured under
// per-shard read locks, then refreshes are queued outside any lock.
// Keys deleted between the snapshot and the queue attempt are
// silently skipped.
func (c *Cache[K, V]) RefreshAll(ctx context.Context) int {
	if c.closed.Load() || c.loader == nil {
		return 0
	}
	if err := ctx.Err(); err != nil {
		_ = err
		return 0
	}
	keys := c.Keys()
	count := 0
	for _, k := range keys {
		s := c.shardFor(k)
		s.mu.Lock()
		if _, busy := s.inflight[k]; !busy {
			//nolint:contextcheck // refresh runs detached
			c.triggerAsyncRefreshLocked(s, k)
			count++
		}
		s.mu.Unlock()
	}
	return count
}

// loadOrJoin is the singleflight core. Returns the cached value on
// hit (avoiding singleflight altogether); otherwise either creates
// a new flight or joins an existing one and waits for it.
func (c *Cache[K, V]) loadOrJoin(
	ctx context.Context, key K,
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) (V, error) {
	var zero V
	s := c.shardFor(key)
	s.mu.Lock()
	now := c.cfg.clock.Now().UnixNano()

	// Cache hit on a fresh, non-negative entry.
	if e, ok := s.entries[key]; ok && !c.entryExpiredLocked(e, now) && !e.flags.has(flagNegative) {
		v := e.value
		e.hits.Add(1)
		if e.touchAccess(now) {
			s.expiryFix(e)
		}
		s.policy.OnAccess(e)
		s.mu.Unlock()
		c.recordHit()
		return v, nil
	}

	// Negative-cache hit: short-circuit without invoking loader.
	if e, ok := s.entries[key]; ok && e.flags.has(flagNegative) && !c.entryExpiredLocked(e, now) {
		s.mu.Unlock()
		c.recordMiss()
		return zero, ErrNotFound
	}

	// Cached error: short-circuit and surface the same error.
	if c.cfg.errorTTL > 0 {
		if ce, ok := s.errors[key]; ok {
			if ce.expireAt > now {
				err := ce.err
				s.mu.Unlock()
				c.recordMiss()
				return zero, err
			}
			delete(s.errors, key)
		}
	}

	c.recordMiss()

	// Already-in-flight: join the existing call.
	if flight, ok := s.inflight[key]; ok {
		s.mu.Unlock()
		if c.cfg.statsEnabled {
			c.counters.loadCoalesced.Add(1)
		}
		return waitForFlight(ctx, flight)
	}

	// Leader path: create a new flight, launch the loader.
	flight := newFlightCall[V]()
	s.inflight[key] = flight
	s.mu.Unlock()

	// runLoader manages its own context (Background +
	// WithLoaderTimeout). Detaching from the caller's ctx is
	// intentional: a single canceling caller must not abort the
	// load while other waiters still want it.
	go c.runLoader(s, key, flight, fn) //nolint:contextcheck // detached by design
	return waitForFlight(ctx, flight)
}

// runLoader executes fn in a fresh goroutine, persists the result
// (or error tombstone) in the cache, removes the flight from the
// inflight map, and signals waiters via close(flight.done).
//
// Concurrent waiters block on flight.done so they observe the
// stored val/err under happens-before guarantees from the channel
// close.
func (c *Cache[K, V]) runLoader(
	s *shard[K, V], key K, flight *flightCall[V],
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) {
	defer close(flight.done)

	loaderCtx := context.Background()
	if c.cfg.loaderTimeout > 0 {
		var cancel context.CancelFunc
		loaderCtx, cancel = context.WithTimeout(loaderCtx, c.cfg.loaderTimeout)
		defer cancel()
	}

	val, ttl, err := fn(loaderCtx, key)
	flight.val = val
	flight.ttl = ttl
	flight.err = err

	s.mu.Lock()
	delete(s.inflight, key)
	if c.cfg.statsEnabled {
		c.counters.loadsTotal.Add(1)
	}

	switch {
	case err == nil:
		c.storeLoadedLocked(s, key, val, ttl)
		if c.cfg.statsEnabled {
			c.counters.loadHits.Add(1)
		}
	case errors.Is(err, ErrNotFound) && c.cfg.negativeTTL > 0:
		c.insertNegativeTombstoneLocked(s, key)
		if c.cfg.statsEnabled {
			c.counters.loadErrors.Add(1)
		}
	case c.cfg.errorTTL > 0 && !errors.Is(err, ErrNotFound):
		// WithErrorTTL caches "the loader broke" errors but
		// explicitly skips ErrNotFound so that
		// WithNegativeCache remains the only path for
		// not-found tombstones.
		s.errors[key] = &cachedError{
			err:      err,
			expireAt: c.cfg.clock.Now().UnixNano() + int64(c.cfg.errorTTL),
		}
		if c.cfg.statsEnabled {
			c.counters.loadErrors.Add(1)
		}
	default:
		if c.cfg.statsEnabled {
			c.counters.loadErrors.Add(1)
		}
	}
	s.mu.Unlock()

	// OnLoad / EventLoad fire AFTER the shard lock is released so
	// the callback can call back into the cache without
	// re-entering the same shard's mutex.
	if c.onLoad != nil {
		c.onLoad(key, val, ttl, err)
	}
	if err == nil {
		c.publishEvent(Event[K, V]{
			Kind:  EventLoad,
			Key:   key,
			Value: val,
			At:    c.cfg.clock.Now(),
		})
	} else {
		kind := EventLoadError
		if errors.Is(err, context.DeadlineExceeded) {
			kind = EventLoadTimeout
		}
		c.publishEvent(Event[K, V]{
			Kind: kind,
			Key:  key,
			Err:  err,
			At:   c.cfg.clock.Now(),
		})
	}
}

// storeLoadedLocked applies the Loader's result to the cache. ttl=0
// inherits the cache's [WithDefaultTTL]; weight comes from the
// configured Weigher with the standard [WithMaxValueWeight] guard.
// Caller must hold s.mu.
func (c *Cache[K, V]) storeLoadedLocked(s *shard[K, V], key K, val V, ttl time.Duration) {
	weight, err := c.computeWeight(key, val)
	if err != nil {
		// Loader produced a value too big to cache; leave it
		// uncached but report no error to the caller (the
		// returned val is still useful).
		return
	}
	if ttl == 0 {
		ttl = c.cfg.defaultTTL
	}
	c.upsertLocked(s, key, val, weight,
		effectiveTTL(ttl, c.cfg.ttlJitter),
		c.cfg.slidingTTL, int64(ttl), nil)
}

// insertNegativeTombstoneLocked records a negative-cache entry for
// key, reusing the existing entry slot when possible to keep the
// shard's bookkeeping consistent. Caller must hold s.mu.
func (c *Cache[K, V]) insertNegativeTombstoneLocked(s *shard[K, V], key K) {
	now := c.cfg.clock.Now().UnixNano()
	expireAt := now + int64(c.cfg.negativeTTL)
	if existing, ok := s.entries[key]; ok {
		// Negative tombstones carry no tags; untag the prior set.
		if c.tags != nil && len(existing.tags) > 0 {
			c.tags.untag(key, existing.tags)
			existing.tags = existing.tags[:0]
		}
		var zero V
		existing.value = zero
		existing.weight = 1
		existing.expireAt.Store(expireAt)
		existing.lastAccess.Store(now)
		existing.hits.Store(0)
		existing.generation.Add(1)
		existing.flags = (existing.flags &^ flagSliding) | flagNegative
		existing.slidingTTL = 0
		s.expiryFix(existing)
		s.policy.OnUpdate(existing)
		if c.cfg.statsEnabled {
			c.counters.updates.Add(1)
		}
		c.startJanitorLocked(s)
		return
	}
	e := s.pool.get()
	e.key = key
	e.weight = 1
	e.inserted = now
	e.lastAccess.Store(now)
	e.expireAt.Store(expireAt)
	e.flags |= flagNegative
	s.entries[key] = e
	s.expiryAdd(e)
	s.policy.OnInsert(e)
	c.counters.entries.Add(1)
	c.counters.bytes.Add(1)
	if c.cfg.statsEnabled {
		c.counters.inserts.Add(1)
	}
	c.startJanitorLocked(s)
	c.evictWhileOverBudgetLocked(s)
}

// triggerAsyncRefresh kicks off a Loader call for key in the
// background. Acquires s.mu briefly to register the flight; no-op
// when one is already in flight or no Loader is configured.
func (c *Cache[K, V]) triggerAsyncRefresh(s *shard[K, V], key K) {
	if c.loader == nil {
		return
	}
	s.mu.Lock()
	c.triggerAsyncRefreshLocked(s, key)
	s.mu.Unlock()
}

// triggerAsyncRefreshLocked is the same as [Cache.triggerAsyncRefresh]
// but assumes the caller already holds s.mu.
func (c *Cache[K, V]) triggerAsyncRefreshLocked(s *shard[K, V], key K) {
	if c.loader == nil {
		return
	}
	if _, busy := s.inflight[key]; busy {
		return
	}
	flight := newFlightCall[V]()
	s.inflight[key] = flight
	go c.runLoader(s, key, flight, c.loader.Load)
}

// waitForFlight blocks until the flight resolves or ctx is canceled.
// On ctx cancel the loader keeps running (other waiters may still
// want the result); the canceling waiter just returns ctx.Err.
func waitForFlight[V any](ctx context.Context, flight *flightCall[V]) (V, error) {
	var zero V
	select {
	case <-flight.done:
		if flight.err != nil {
			return zero, flight.err
		}
		return flight.val, nil
	case <-ctx.Done():
		return zero, ctx.Err() //nolint:wrapcheck // pass ctx.Err verbatim
	}
}

// GetMulti returns the cached value for each key in keys. Missing
// or expired keys are absent from the returned map. The returned
// map is freshly allocated; callers may mutate it freely.
//
// Each lookup goes through [Cache.Get], so refresh-ahead /
// stale-while-revalidate / sliding-TTL touch all behave the same
// as for individual gets.
func (c *Cache[K, V]) GetMulti(keys []K) map[K]V {
	if c.closed.Load() || len(keys) == 0 {
		return map[K]V{}
	}
	out := make(map[K]V, len(keys))
	for _, k := range keys {
		if v, ok := c.Get(k); ok {
			out[k] = v
		}
	}
	return out
}

// SetMulti stores every (key, value) in items. On the first error,
// SetMulti returns immediately; entries successfully stored before
// the failure remain in the cache (no rollback). Useful when callers
// can tolerate partial state and want lower per-call overhead than
// many [Cache.Set] invocations.
func (c *Cache[K, V]) SetMulti(items map[K]V) error {
	if c.closed.Load() {
		return ErrClosed
	}
	for k, v := range items {
		if err := c.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

// DeleteMulti removes every key in keys. Returns the number of
// entries actually removed (absent keys are silently skipped).
func (c *Cache[K, V]) DeleteMulti(keys []K) int {
	if c.closed.Load() || len(keys) == 0 {
		return 0
	}
	count := 0
	for _, k := range keys {
		if c.Delete(k) {
			count++
		}
	}
	return count
}
