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

	// bulkLoader is the typed bulk-loader resolved from
	// cfg.bulkLoader, or nil. When nil, [Cache.GetMultiOrLoad]
	// falls back to a single-loader fan-out.
	bulkLoader BulkLoader[K, V]

	// loaderLimiter throttles loader invocations when
	// [WithLoaderRateLimit] is configured. nil means "unlimited".
	loaderLimiter *rateLimiter

	// loadSlots is a counting semaphore that caps in-flight loader
	// calls when [WithMaxConcurrentLoads] is configured. nil
	// means "unlimited".
	loadSlots chan struct{}

	// expireFunc is the typed per-entry expiry predicate resolved
	// from cfg.expireFunc, or nil when none is configured.
	expireFunc func(key K, value V, meta Metadata) bool

	// store is the optional source-of-truth backend supplied by
	// [WithStore]. When non-nil, [Cache.Get]/[Cache.Set]/
	// [Cache.Delete] (and their Ctx variants) read-through, write-
	// through, and delete-through respectively. The in-memory
	// shards behave as a write-through cache of this backend.
	store Store[K, V]

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
	onHit        func(K, V)
	onMiss       func(K)
	onEvict      func(K, V, EvictionReason)
	onExpire     func(K, V)
	onLoad       func(K, V, time.Duration, error)
	purgeVisitor func(K, V) error
	copyOnGet    func(V) V

	// admission is the resolved [AdmissionPolicy]. Always non-nil;
	// defaults to [AdmitAlways] when no [WithAdmissionPolicy] /
	// [WithDoorkeeper] is configured.
	admission AdmissionPolicy[K]

	// invalidationPublisher fires on every removal; nil when
	// not configured.
	invalidationPublisher func(K, EvictionReason)

	// invalidationSubscriber drives the consumer goroutine that
	// turns remote-channel sends into local Deletes. nil when
	// not configured. invalidationSubscriberDone is closed by
	// Close to stop the goroutine; invalidationSubscriberExited is
	// closed by the goroutine on its way out so Close can wait for
	// it to finish before returning.
	invalidationSubscriber       <-chan K
	invalidationSubscriberDone   chan struct{}
	invalidationSubscriberExited chan struct{}

	// tagCleanupQueue receives untag ops from removeLocked; the
	// drainer goroutine batches them and applies under a single
	// tagIndex.mu acquisition. nil when c.tags is nil.
	// tagCleanupDone signals shutdown; tagCleanupExited closes
	// when the drainer has fully drained and returned.
	tagCleanupQueue     chan untagOp[K]
	tagCleanupDone      chan struct{}
	tagCleanupExited    chan struct{}
	tagCleanupOverflows atomic.Uint64
	tagCleanupInflight  atomic.Int64 // ops pulled off queue but not yet applied

	// tracer is always non-nil. Resolves from cfg.tracer or
	// falls back to [noopTracer].
	tracer Tracer

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

	// async holds the [WithAsyncWrites] state. nil when async writes
	// are off.
	async *asyncWrites

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

func initShards[K comparable, V any](c *Cache[K, V], cfg *config, hasher func(K) uint64) {
	perShard := perShardBudget(cfg.maxEntries, len(c.shards))
	pcfg := policyConfig[K]{budget: perShard, hasher: hasher}
	for i := range c.shards {
		c.shards[i] = newShard(
			newPolicy[K, V](cfg.policy, pcfg),
			perShard,
			cfg.collisionTracking,
			newTTLBackend[K, V](cfg),
			newShardStore[K, V](cfg, hasher),
		)
	}
}

func build[K comparable, V any](cfg *config, allowUnbounded bool) (*Cache[K, V], error) {
	// Surface deferred constructor errors (e.g., bad AES key) before
	// other validation; the rest of build assumes cfg.codec is usable.
	if cfg.codecCtorErr != nil {
		return nil, &ConfigError{Field: "Codec", Message: cfg.codecCtorErr.Error()}
	}
	if cfg.maxEntries < 0 {
		return nil, &ConfigError{Field: "MaxEntries", Message: "must be non-negative"}
	}
	if cfg.maxBytes < 0 {
		return nil, &ConfigError{Field: configFieldMaxBytes, Message: "must be non-negative"}
	}
	if !allowUnbounded && cfg.maxEntries <= 0 && cfg.maxBytes <= 0 {
		return nil, ErrUnbounded
	}

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
	bulkLoader, err := resolveBulkLoader[K, V](cfg.bulkLoader)
	if err != nil {
		return nil, err
	}
	expireFunc, err := resolveExpireFunc[K, V](cfg.expireFunc)
	if err != nil {
		return nil, err
	}
	store, err := resolveStore[K, V](cfg.store)
	if err != nil {
		return nil, err
	}
	hooks, err := resolveHooks[K, V](cfg)
	if err != nil {
		return nil, err
	}
	admission, err := resolveAdmissionPolicy(cfg, hasher)
	if err != nil {
		return nil, err
	}
	publisher, err := resolveInvalidationPublisher[K](cfg.invalidationPublisher)
	if err != nil {
		return nil, err
	}
	subscriber, err := resolveInvalidationSubscriber[K](cfg.invalidationSubscriber)
	if err != nil {
		return nil, err
	}
	safeKeysCheck[K](cfg)
	if cfg.ttlBuckets > 0 && cfg.logger != nil {
		cfg.logger.Debug("memcache: WithTTLBuckets active; using hashed-wheel TTL backend",
			"slots", cfg.ttlBuckets,
			"tickPerBucket", cfg.ttlBucketsTickPerBucket)
	}

	if cfg.maxBytes > 0 && weigher == nil {
		return nil, &ConfigError{
			Field:   configFieldMaxBytes,
			Message: "WithMaxBytes requires WithWeigher",
		}
	}
	gateLockFreeRead(cfg)

	shardCount := nextPowerOfTwo(max(cfg.shards, 1))

	var loadSlots chan struct{}
	if cfg.maxConcurrentLoads > 0 {
		loadSlots = make(chan struct{}, cfg.maxConcurrentLoads)
	}
	tracer := cfg.tracer
	if tracer == nil {
		tracer = noopTracer{}
	}
	c := &Cache[K, V]{
		cfg:                        cfg,
		hasher:                     hasher,
		weigher:                    weigher,
		loader:                     loader,
		bulkLoader:                 bulkLoader,
		loaderLimiter:              newRateLimiter(cfg.loaderRatePerSecond, cfg.clock),
		loadSlots:                  loadSlots,
		expireFunc:                 expireFunc,
		store:                      store,
		tags:                       newTagIndex[K](),
		events:                     newEventBus[K, V](),
		onHit:                      hooks.onHit,
		onMiss:                     hooks.onMiss,
		onEvict:                    hooks.onEvict,
		onExpire:                   hooks.onExpire,
		onLoad:                     hooks.onLoad,
		purgeVisitor:               hooks.purgeVisitor,
		copyOnGet:                  hooks.copyOnGet,
		admission:                  admission,
		invalidationPublisher:      publisher,
		invalidationSubscriber:     subscriber,
		invalidationSubscriberDone: make(chan struct{}),
		tracer:                     tracer,
		shards:                     make([]*shard[K, V], shardCount),
		shardMask:                  uint64(shardCount - 1),
		counters:                   newStatsCounters(cfg.shardedStats, cfg.clock.Now()),
	}

	initShards(c, cfg, hasher)
	c.maybeInitReadSnapshot(cfg)
	c.maybeStartAsync(cfg)

	if err := c.applyAutoLoad(); err != nil {
		return nil, err
	}
	c.startAutoSave()
	c.startInvalidationSubscriber()
	c.startTagCleanup()
	publishExpvar(c)

	return c, nil
}

func (c *Cache[K, V]) maybeStartAsync(cfg *config) {
	if !cfg.asyncWrites {
		return
	}
	c.async = newAsyncWrites(c.shards)
	c.startAsyncApply()
}

// applyAutoLoad attempts a one-shot LoadFile when [WithAutoLoad] is
// configured. Missing files are not treated as errors.
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

func (c *Cache[K, V]) startAutoSave() {
	if c.cfg.autoSavePath == "" || c.cfg.autoSaveInterval <= 0 {
		return
	}
	c.autoSaveStop = make(chan struct{})
	c.autoSaveDone = make(chan struct{})
	go c.runAutoSave()
}

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

// applyJitter returns ttl with uniform jitter in [-j, +j]. Jitter is
// clamped to ttl/4 to keep the resulting expiry positive.
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
	delta := rand.Int64N(int64(2*j)+1) - int64(j)
	return ttl + time.Duration(delta)
}

// perShardBudget computes the per-shard target entry count from a global
// maxEntries, with a 10% slop factor to accommodate hash skew.
func perShardBudget(maxEntries, shardCount int) int {
	if maxEntries <= 0 || shardCount <= 0 {
		return 0
	}
	per := (maxEntries + shardCount - 1) / shardCount
	slop := max((per*10)/100, 1)
	return per + slop
}

//nolint:nilnil // (nil, nil) signals no weigher configured.
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

//nolint:nilnil // (nil, nil) signals no expire func configured.
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

type resolvedHooks[K comparable, V any] struct {
	onHit        func(K, V)
	onMiss       func(K)
	onEvict      func(K, V, EvictionReason)
	onExpire     func(K, V)
	onLoad       func(K, V, time.Duration, error)
	purgeVisitor func(K, V) error
	copyOnGet    func(V) V
}

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
	if cfg.purgeVisitor != nil {
		fn, ok := cfg.purgeVisitor.(func(K, V) error)
		if !ok {
			return out, &ConfigError{
				Field:   "PurgeVisitor",
				Message: fmt.Sprintf("type mismatch: visitor does not match cache type parameters (%T)", cfg.purgeVisitor),
			}
		}
		out.purgeVisitor = fn
	}
	if cfg.copyOnGet != nil {
		fn, ok := cfg.copyOnGet.(func(V) V)
		if !ok {
			return out, &ConfigError{
				Field:   "CopyOnGet",
				Message: fmt.Sprintf("type mismatch: copy func does not match cache value type (%T)", cfg.copyOnGet),
			}
		}
		out.copyOnGet = fn
	}
	return out, nil
}

// resolveBulkLoader returns the typed BulkLoader[K, V] from a
// type-erased any, or nil when none is configured.
//
//nolint:nilnil // (nil, nil) signals "no bulk loader configured".
func resolveBulkLoader[K comparable, V any](raw any) (BulkLoader[K, V], error) {
	if raw == nil {
		return nil, nil
	}
	l, ok := raw.(BulkLoader[K, V])
	if !ok {
		return nil, &ConfigError{
			Field:   "BulkLoader",
			Message: fmt.Sprintf("type mismatch: bulk loader does not match cache type parameters (%T)", raw),
		}
	}
	return l, nil
}

//nolint:nilnil // (nil, nil) signals no loader configured.
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

// defaultHasher returns a hasher specialized for common K types: SipHash-2-4
// for string and []byte keys, splitmix64 for integer/bool keys, and a
// fmt-stringification fallback for anything else.
func defaultHasher[K comparable]() func(K) uint64 {
	const k0 = uint64(0x0706050403020100)
	const k1 = uint64(0x0f0e0d0c0b0a0908)
	return func(k K) uint64 {
		switch v := any(k).(type) {
		case string:
			return sketch.HashString(k0, k1, v)
		case []byte:
			return sketch.SipHash24(k0, k1, v)
		case int:
			return sketch.MixUint64(uint64(v))
		case int8:
			return sketch.MixUint64(uint64(v))
		case int16:
			return sketch.MixUint64(uint64(v))
		case int32:
			return sketch.MixUint64(uint64(v))
		case int64:
			return sketch.MixUint64(uint64(v))
		case uint:
			return sketch.MixUint64(uint64(v))
		case uint8:
			return sketch.MixUint64(uint64(v))
		case uint16:
			return sketch.MixUint64(uint64(v))
		case uint32:
			return sketch.MixUint64(uint64(v))
		case uint64:
			return sketch.MixUint64(v)
		case uintptr:
			return sketch.MixUint64(uint64(v))
		case bool:
			if v {
				return sketch.MixUint64(1)
			}
			return sketch.MixUint64(0)
		default:
			s := fmt.Sprintf("%v", k)
			return sketch.HashString(k0, k1, s)
		}
	}
}

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

// Capacity returns the cache's effective configured bound: entries when
// [WithMaxEntries] is set, bytes when [WithMaxBytes] is set, or 0 when
// unbounded.
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
	s.PolicyName = c.cfg.policy.String()
	if c.tags != nil {
		s.TagsTracked = c.tags.distinctTagCount()
	}
	s.TagCleanupBacklog = c.tagCleanupBacklog()
	for _, sh := range c.shards {
		s.Compactions += sh.storage.compactions()
	}
	// PolicyDetail samples shard 0; values describe a single shard's
	// state and should be read as representative rather than cache-wide.
	if len(c.shards) > 0 {
		sh := c.shards[0]
		sh.mu.RLock()
		s.PolicyDetail = sh.policy.Snapshot()
		sh.mu.RUnlock()
	}
	return s
}

// ResetStats zeros counters and stamps [Stats.LastResetAt]. Live
// entry/byte counts are preserved.
func (c *Cache[K, V]) ResetStats() {
	c.counters.reset()
	c.counters.stampReset(c.cfg.clock.Now())
}

// Has reports whether the cache contains a fresh entry for key. It does
// NOT promote the entry in the eviction policy and never invokes the
// configured Loader. The shard RLock MUST be held through every field
// read on the resolved entry to avoid racing with eviction's pool
// recycling.
func (c *Cache[K, V]) Has(key K) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	if s.pending != nil {
		s.mu.RLock()
		_, kind, hit := c.asyncReadHit(s, key, now)
		s.mu.RUnlock()
		if hit {
			return kind == pendingOpSet
		}
	}
	s.mu.RLock()
	e, ok := s.storage.get(key)
	if ok {
		expired := c.entryExpiredLocked(e, now)
		negative := e.flags.has(flagNegative)
		s.mu.RUnlock()
		if expired || negative {
			// Fall through to the store check (if any).
		} else {
			return true
		}
	} else {
		s.mu.RUnlock()
	}
	if c.store != nil {
		v, found, err := c.readThroughStore(context.Background(), key)
		_ = v
		return err == nil && found
	}
	return false
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
	if s.pending != nil {
		s.mu.RLock()
		val, kind, hit := c.asyncReadHit(s, key, now)
		s.mu.RUnlock()
		if hit {
			if kind == pendingOpDelete {
				return zero, false
			}
			return c.returnValue(val), true
		}
	}
	s.mu.RLock()
	e, ok := s.storage.get(key)
	s.mu.RUnlock()
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return zero, false
	}
	return c.returnValue(e.loadValue()), true
}

// Get returns the value stored for key, or the zero value of V and false
// if absent or expired. Get does NOT invoke a configured [Loader]; use
// [Cache.GetOrLoad] for that.
//
// When [WithRefreshAhead] is enabled, a successful hit on an entry past
// refreshAt*TTL triggers an asynchronous Loader call. When
// [WithStaleWhileRevalidate] is enabled, a hit on an entry within the
// staleFor window returns the stale value and triggers an asynchronous
// refresh. When [WithStore] is configured, an in-memory miss falls
// through to the Store and a hit is promoted into the in-memory cache.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	v, ok, err := c.getCtx(context.Background(), key)
	if err != nil && c.cfg.logger != nil {
		c.cfg.logger.Debug("memcache: store Get failed during read-through",
			"err", err)
	}
	return v, ok
}

//nolint:contextcheck // refresh-ahead spawns its own ctx by design (see triggerAsyncRefreshLocked)
func (c *Cache[K, V]) getCtx(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if c.closed.Load() {
		return zero, false, nil
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()

	if val, hit, terminal := c.tryServeFromAsyncPending(s, key, now); terminal {
		return val, hit, nil
	}
	if val, hit, terminal := c.tryServeFromReadSnapshot(s, key, now); terminal {
		return val, hit, nil
	}

	// Fast path: try to serve under the shard read lock.
	s.mu.RLock()
	if e, ok := s.storage.get(key); ok &&
		!c.entryExpiredLocked(e, now) &&
		!e.flags.has(flagNegative) &&
		!e.flags.has(flagSliding) &&
		!s.policy.PromotionNeeded(e) &&
		!c.shouldRefreshAhead(e, now) {
		// hits is atomic; copy value while still locked so a concurrent
		// eviction cannot pool the entry mid-read.
		e.hits.Add(1)
		val := e.loadValue()
		s.mu.RUnlock()
		// Record the under-the-snapshot miss so a stale read
		// snapshot eventually gets rebuilt.
		if c.cfg.lockFreeRead {
			c.recordReadMiss(s, s.snapshotHasKey(key))
		}
		c.recordHitObserve(key)
		c.fireHit(key, val)
		return c.returnValue(val), true, nil
	}
	s.mu.RUnlock()

	// Slow path: full Get under the shard write lock. Re-validates
	// every condition because the entry may have been evicted or
	// mutated between RUnlock and Lock.
	s.mu.Lock()
	e, ok := s.storage.get(key)
	if !ok {
		c.flushAndUnlock(s)
		// Fall through to the Store after the unlock to keep slow Store
		// calls off the shard's hot path.
		if v, found, err := c.readThroughStore(ctx, key); err != nil || found {
			if found {
				c.recordHitObserve(key)
				c.fireHit(key, v)
			}
			return v, found, err
		}
		c.recordMiss()
		c.fireMiss(key)
		return zero, false, nil
	}
	if expired, reason := c.entryExpiredLockedReason(e, now); expired {
		if !e.flags.has(flagNegative) && c.shouldServeStale(e, now) {
			c.triggerAsyncRefreshLocked(s, key)
			c.counters.staleWhileRevalidate.Add(1)
			e.hits.Add(1)
			s.policy.OnAccess(e)
			val := e.loadValue()
			c.flushAndUnlock(s)
			c.recordHitObserve(key)
			c.fireHit(key, val)
			return c.returnValue(val), true, nil
		}
		c.removeLocked(s, e, reason)
		c.counters.expirations.Add(1)
		c.flushAndUnlock(s)
		if v, found, err := c.readThroughStore(ctx, key); err != nil || found {
			if found {
				c.recordHitObserve(key)
				c.fireHit(key, v)
			}
			return v, found, err
		}
		c.recordMiss()
		c.fireMiss(key)
		return zero, false, nil
	}
	if e.flags.has(flagNegative) {
		c.counters.negativeHits.Add(1)
		c.flushAndUnlock(s)
		c.recordMiss()
		c.fireMiss(key)
		return zero, false, nil
	}
	defer c.unlockShard(s)
	e.hits.Add(1)
	if e.touchAccess(now) {
		s.expiryFix(e)
	}
	s.policy.OnAccess(e)
	if c.shouldRefreshAhead(e, now) {
		c.counters.refreshAhead.Add(1)
		c.triggerAsyncRefreshLocked(s, key)
	}
	c.maybePromoteOnSlowHit(s, key)
	c.recordHitObserve(key)
	c.fireHit(key, e.loadValue())
	return c.returnValue(e.loadValue()), true, nil
}

func (c *Cache[K, V]) fireHit(key K, value V) {
	if c.onHit != nil {
		c.runHook("OnHit", func() { c.onHit(key, value) })
	}
}

func (c *Cache[K, V]) returnValue(value V) V {
	if c.copyOnGet != nil {
		return c.copyOnGet(value)
	}
	return value
}

func (c *Cache[K, V]) fireMiss(key K) {
	if c.onMiss != nil {
		c.runHook("OnMiss", func() { c.onMiss(key) })
	}
}

func (c *Cache[K, V]) runHook(name string, fn func()) {
	deadline := c.cfg.callbackTimeout
	if deadline <= 0 {
		fn()
		return
	}
	done := make(chan struct{})
	start := c.cfg.clock.Now()
	go func() {
		t := time.NewTimer(deadline)
		defer t.Stop()
		select {
		case <-done:
		case <-t.C:
			if c.cfg.logger != nil {
				c.cfg.logger.Warn("memcache: callback exceeded WithCallbackTimeout",
					"hook", name, "deadline", deadline)
			}
		}
	}()
	fn()
	close(done)
	_ = start
}

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
	defer c.unlockShard(s)

	e, ok := s.storage.get(key)
	if !ok {
		c.recordMiss()
		return zero, time.Time{}, false
	}
	if expired, reason := c.entryExpiredLockedReason(e, now); expired {
		c.removeLocked(s, e, reason)
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
		return e.loadValue(), time.Time{}, true
	}
	return e.loadValue(), time.Unix(0, expNanos), true
}

// Set stores value under key with the cache's default TTL. If V
// implements [CacheTTLer] the value's TTL overrides [WithDefaultTTL]; if V
// implements [CacheTagger] its tags are merged into the entry's tag set.
// After insertion, [WithGroup]-bounded groups named by the entry's tags
// are shrunk back to capacity.
func (c *Cache[K, V]) Set(key K, value V) error {
	return c.setCtx(context.Background(), key, value)
}

func (c *Cache[K, V]) setCtx(ctx context.Context, key K, value V) error {
	ttl := extractCacheableTTL(value, c.cfg.defaultTTL)
	tags := deriveAutoTags(value)
	if c.async != nil {
		return c.asyncSet(key, value, ttl, c.cfg.slidingTTL, tags)
	}
	if err := c.setLocked(key, value, ttl, c.cfg.slidingTTL, tags); err != nil {
		return err
	}
	if err := c.propagateSetToStore(ctx, key, value, ttl); err != nil {
		return err
	}
	c.enforceGroupBudgets(tags)
	return nil
}

// SetWithTTL stores value under key with the given TTL. A TTL of 0 means
// no expiry; a negative TTL returns [ErrInvalidTTL].
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	if ttl < 0 {
		return ErrInvalidTTL
	}
	tags := deriveAutoTags(value)
	if c.async != nil {
		return c.asyncSet(key, value, ttl, c.cfg.slidingTTL, tags)
	}
	if err := c.setLocked(key, value, ttl, c.cfg.slidingTTL, tags); err != nil {
		return err
	}
	c.enforceGroupBudgets(tags)
	return c.propagateSetToStore(context.Background(), key, value, ttl)
}

// SetWithOptions stores value under key with per-call overrides supplied
// via [SetOption]. Cache defaults apply to fields no option touches; when
// they conflict, the per-call option wins.
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
	if err := c.checkKeySize(key); err != nil {
		return err
	}
	// Auto-tag from CacheTagger / template tags ONLY when the
	// caller didn't invoke SetTags at all. SetTags() with no args
	// means "explicitly no tags" (sc.hasTags=true) and bypasses
	// auto-tag derivation. Per spec: explicit per-call options
	// always win.
	if !sc.hasTags {
		sc.tags = deriveAutoTags(value)
	}
	if err := c.checkTagLimits(key, sc.tags); err != nil {
		return err
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
				Cause:      ErrValueTooLarge,
			}
		}
	} else {
		w, err := c.computeWeight(key, value)
		if err != nil {
			return err
		}
		weight = w
	}

	if c.async != nil {
		if sc.hasExpiry {
			return c.asyncSetWithExpiry(key, value, weight, sc)
		}
		return c.asyncSet(key, value, sc.ttl, sc.sliding, sc.tags)
	}

	s := c.shardFor(key)
	s.mu.Lock()
	if sc.hasExpiry {
		c.upsertWithAbsoluteExpiryLocked(s, key, value, weight, sc)
	} else {
		c.upsertLocked(s, key, value, weight,
			c.effectiveTTL(sc.ttl),
			sc.sliding, int64(sc.ttl), sc.tags)
	}
	c.flushAndUnlock(s)
	if c.store != nil {
		ttl := sc.ttl
		if sc.hasExpiry && !sc.expireAt.IsZero() {
			ttl = max(time.Until(sc.expireAt), 0)
		}
		if err := c.propagateSetToStore(context.Background(), key, value, ttl); err != nil {
			return err
		}
	}
	// Group enforcement runs OUTSIDE the shard lock to avoid inverting
	// the standard shard-then-index lock order.
	c.enforceGroupBudgets(sc.tags)
	return nil
}

func (c *Cache[K, V]) upsertWithAbsoluteExpiryLocked(
	s *shard[K, V], key K, value V, weight int64, sc setConfig,
) {
	now := c.cfg.clock.Now().UnixNano()
	var expireAt int64
	if !sc.expireAt.IsZero() {
		expireAt = sc.expireAt.UnixNano()
	}
	if existing, ok := s.storage.get(key); ok {
		var oldTags []string
		if c.tags != nil && len(existing.tags) > 0 {
			oldTags = append([]string(nil), existing.tags...)
		}
		c.counters.bytes.Add(-existing.weight + weight)
		existing.storeValue(value)
		existing.weight = weight
		existing.expireAt.Store(expireAt)
		existing.lastAccess.Store(now)
		existing.hits.Store(0)
		existing.generation.Add(1)
		// SetExpireAt always implies absolute, never sliding; also clear
		// any negative-cache tombstone (see upsertLocked).
		existing.flags &^= (flagSliding | flagNegative)
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
	e.storeValue(value)
	e.weight = weight
	e.inserted = now
	e.lastAccess.Store(now)
	e.expireAt.Store(expireAt)
	if len(sc.tags) > 0 {
		e.tags = append(e.tags[:0], sc.tags...)
	}
	s.storage.set(key, e)
	s.markAmended()
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

// Delete removes key from the cache and returns true when an entry was
// removed. With [WithStore] the delete is mirrored through; Store errors
// are logged but do not affect the returned bool. Use [Cache.DeleteCtx]
// to surface Store errors. With [WithAsyncWrites] the delete is queued
// and the bool is true except on a closed cache; call [Cache.Sync] first
// to observe the post-delete state.
func (c *Cache[K, V]) Delete(key K) bool {
	if c.async != nil {
		return c.asyncDelete(key)
	}
	return c.deleteCtx(context.Background(), key, EvictReasonDeleted)
}

func (c *Cache[K, V]) deleteCtx(ctx context.Context, key K, reason EvictionReason) bool {
	removed := c.deleteWithReason(key, reason)
	if c.store != nil {
		_ = c.deleteThroughStore(ctx, key) //nolint:errcheck // surfaced via DeleteCtx
	}
	return removed
}

func (c *Cache[K, V]) deleteWithReason(key K, reason EvictionReason) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer c.unlockShard(s)
	e, ok := s.storage.get(key)
	if !ok {
		return false
	}
	c.removeLocked(s, e, reason)
	c.counters.deletes.Add(1)
	return true
}

// Reset removes all entries without firing eviction callbacks.
func (c *Cache[K, V]) Reset() {
	if c.admission != nil {
		c.admission.Reset()
	}
	for _, s := range c.shards {
		s.mu.Lock()
		s.storage.each(func(e *entry[K, V]) bool {
			c.counters.entries.Add(-1)
			c.counters.bytes.Add(-e.weight)
			s.policy.OnRemove(e)
			c.recycleOrInvalidate(s, e)
			return true
		})
		s.storage.clearAll()
		s.policy.Reset()
		s.ttl.Reset()
		if c.cfg.lockFreeRead {
			s.read.Store(newEmptyReadMap[K, V]())
			s.readMisses.Store(0)
		}
		c.flushAndUnlock(s)
	}
	if c.tags != nil {
		c.tags.reset()
	}
}

func (c *Cache[K, V]) recycleOrInvalidate(s *shard[K, V], e *entry[K, V]) {
	if c.cfg.lockFreeRead {
		e.markInvalidated()
		return
	}
	s.pool.put(e)
}

// Close releases all resources, stops the auto-save goroutine after
// writing a final snapshot, stops every shard's janitor, closes every
// active subscriber channel, and disables further operations. Close is
// idempotent. After Close, writes return [ErrClosed] and reads return
// the zero value with ok=false. A final auto-save error is logged but
// not returned.
func (c *Cache[K, V]) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Stop auto-save first so its tick handler cannot observe closed=true
	// mid-Close; saveFileTo below bypasses the closed check explicitly.
	if c.autoSaveStop != nil {
		close(c.autoSaveStop)
		<-c.autoSaveDone
	}
	// Drain pending async writes BEFORE snapshot so the final on-disk
	// state reflects every Set/Delete the caller returned from.
	c.stopAsyncApply()
	// Final snapshot BEFORE stopping janitors / clearing entries.
	if c.cfg.autoSavePath != "" {
		if err := c.saveFileTo(c.cfg.autoSavePath); err != nil && c.cfg.logger != nil {
			c.cfg.logger.Warn("memcache: final auto-save failed",
				"path", c.cfg.autoSavePath, "err", err)
		}
	}
	c.stopAllJanitors()
	if c.invalidationSubscriberDone != nil {
		close(c.invalidationSubscriberDone)
		if c.invalidationSubscriberExited != nil {
			<-c.invalidationSubscriberExited
		}
	}
	c.runPurgeVisitor()
	c.Reset()
	c.stopTagCleanup()
	if c.events != nil {
		c.events.close()
	}
	return nil
}

func (c *Cache[K, V]) runPurgeVisitor() {
	if c.purgeVisitor == nil {
		return
	}
	type kv struct {
		k K
		v V
	}
	var staged []kv
	for _, s := range c.shards {
		s.mu.RLock()
		s.storage.each(func(e *entry[K, V]) bool {
			if !e.flags.has(flagNegative) {
				staged = append(staged, kv{k: e.key, v: c.returnValue(e.loadValue())})
			}
			return true
		})
		s.mu.RUnlock()
	}
	for _, p := range staged {
		c.runHook("PurgeVisitor", func() {
			if err := c.purgeVisitor(p.k, p.v); err != nil && c.cfg.logger != nil {
				c.cfg.logger.Warn("memcache: PurgeVisitor returned error",
					"err", err)
			}
		})
	}
}

func (c *Cache[K, V]) recordHit() {
	if c.cfg.statsEnabled {
		c.counters.addHit()
	}
}

// recordHashCollisionLocked bumps [Stats.HashCollisions] when key collides
// with a previously-inserted distinct key on the same shard. Caller MUST
// hold s.mu (write).
func (c *Cache[K, V]) recordHashCollisionLocked(s *shard[K, V], key K) {
	if s.hashIndex == nil {
		return
	}
	h := c.hasher(key)
	if prev, exists := s.hashIndex[h]; exists {
		if pk, ok := prev.(K); ok && pk != key {
			if c.cfg.statsEnabled {
				c.counters.hashCollisions.Add(1)
			}
		}
	}
	s.hashIndex[h] = key
}

// forgetHashLocked drops key's hashIndex entry. Caller holds s.mu
// (write). No-op when collision tracking is off.
func (c *Cache[K, V]) forgetHashLocked(s *shard[K, V], key K) {
	if s.hashIndex == nil {
		return
	}
	h := c.hasher(key)
	if prev, exists := s.hashIndex[h]; exists {
		if pk, ok := prev.(K); ok && pk == key {
			delete(s.hashIndex, h)
		}
	}
}

// recordHitObserve is recordHit + AdmissionPolicy.Observe. Used on
// Get hits so that frequency-tracking admission policies can build
// their picture of the workload from real read traffic rather than
// just the writes routed through upsertLocked.
func (c *Cache[K, V]) recordHitObserve(key K) {
	c.recordHit()
	if c.admission != nil {
		c.admission.Observe(key)
	}
}

func (c *Cache[K, V]) recordMiss() {
	if c.cfg.statsEnabled {
		c.counters.addMiss()
	}
}

func (c *Cache[K, V]) setLocked(key K, value V, ttl time.Duration, sliding bool, tags []string) error {
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
	s.mu.Lock()
	defer c.unlockShard(s)
	c.upsertLocked(s, key, value, weight, c.effectiveTTL(ttl), sliding, int64(ttl), tags)
	return nil
}

// checkKeySize enforces [WithMaxKeySize] for string and []byte keys.
// Other K types are not measured.
func (c *Cache[K, V]) checkKeySize(key K) error {
	if c.cfg.maxKeySize <= 0 {
		return nil
	}
	var n int
	switch k := any(key).(type) {
	case string:
		n = len(k)
	case []byte:
		n = len(k)
	default:
		return nil
	}
	if n <= c.cfg.maxKeySize {
		return nil
	}
	return &CapacityError{
		Key:        key,
		Reason:     "key exceeds MaxKeySize",
		LimitField: "MaxKeySize",
		Cause:      ErrKeyTooLarge,
	}
}

func (c *Cache[K, V]) checkTagLimits(key K, tags []string) error {
	err := c.validateTagLimits(tags)
	if err == nil {
		return nil
	}
	if ce, _ := errors.AsType[*CapacityError](err); ce != nil {
		ce.Key = key
		ce.Cause = ErrTooManyTags
	}
	return err
}

func (c *Cache[K, V]) computeWeight(key K, value V) (int64, error) {
	weight := c.weighOf(value)
	if c.cfg.maxValueWeight > 0 && weight > c.cfg.maxValueWeight {
		return 0, &CapacityError{
			Key:        key,
			Reason:     "value weight exceeds MaxValueWeight",
			LimitField: "MaxValueWeight",
			Cause:      ErrValueTooLarge,
		}
	}
	return weight, nil
}

func (c *Cache[K, V]) effectiveTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	return applyJitter(ttl, c.resolveJitter(ttl))
}

// resolveJitter picks the jitter window for ttl: 5% of ttl by default,
// or the explicit value from [WithTTLJitter] if configured.
func (c *Cache[K, V]) resolveJitter(ttl time.Duration) time.Duration {
	if c.cfg.ttlJitterExplicit {
		return c.cfg.ttlJitter
	}
	return ttl / 20
}

// upsertLocked is the shared insert-or-update routine. Caller MUST hold
// s.mu (write). effectiveTTL has jitter applied; rawTTL is the original
// duration persisted on the entry so [Touch] and sliding-TTL can re-derive.
func (c *Cache[K, V]) upsertLocked(
	s *shard[K, V], key K, value V, weight int64,
	effectiveTTL time.Duration, sliding bool, rawTTL int64, tags []string,
) {
	now := c.cfg.clock.Now().UnixNano()
	var expireAt int64
	if effectiveTTL > 0 {
		expireAt = now + int64(effectiveTTL)
	}
	existing, ok := s.storage.get(key)
	if !ok {
		// Pure insert: consult admission policy. Updates bypass the gate.
		if c.admission != nil {
			admit := c.admission.Admit(key)
			c.admission.Observe(key)
			if !admit {
				if c.cfg.statsEnabled {
					c.counters.admissionRejects.Add(1)
				}
				return
			}
		}
		c.recordHashCollisionLocked(s, key)
	}
	if ok {
		// retagLocked needs the full old set to untag.
		var oldTags []string
		if c.tags != nil && len(existing.tags) > 0 {
			oldTags = append([]string(nil), existing.tags...)
		}
		c.counters.bytes.Add(-existing.weight + weight)
		existing.storeValue(value)
		existing.weight = weight
		existing.expireAt.Store(expireAt)
		existing.lastAccess.Store(now)
		existing.hits.Store(0)
		existing.generation.Add(1)
		// Clear flagNegative when transitioning a tombstone to a real
		// value, otherwise the Set would be silently dropped.
		existing.flags &^= flagNegative
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
	e.storeValue(value)
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
	s.storage.set(key, e)
	s.markAmended()
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

func (c *Cache[K, V]) weighOf(v V) int64 {
	var w int64 = 1
	if c.weigher != nil {
		w = c.weigher(v)
	}
	return clampWeight(w)
}

// evictWhileOverBudgetLocked evicts policy-chosen victims until the shard
// is within its budget. Caller MUST hold s.mu.
func (c *Cache[K, V]) evictWhileOverBudgetLocked(s *shard[K, V]) {
	if s.budget <= 0 {
		return
	}
	for s.storage.length() > s.budget {
		victim := s.policy.Victim()
		if victim == nil {
			return
		}
		c.removeLocked(s, victim, EvictReasonCapacity)
		c.counters.evictions.Add(1)
	}
}

// entryExpiredLocked reports whether e is expired at now. Combines TTL
// check with optional [WithExpireFunc]; on predicate panic the entry is
// treated as fresh. Caller MUST hold the shard lock.
func (c *Cache[K, V]) entryExpiredLocked(e *entry[K, V], now int64) bool {
	expired, _ := c.entryExpiredLockedReason(e, now)
	return expired
}

func (c *Cache[K, V]) entryExpiredLockedReason(e *entry[K, V], now int64) (bool, EvictionReason) {
	if e.expired(now) {
		return true, EvictReasonExpired
	}
	if c.expireFunc == nil {
		return false, 0
	}
	expired, panicked := c.callExpireFunc(e)
	if panicked && c.cfg.logger != nil {
		c.cfg.logger.Warn("memcache: WithExpireFunc panicked; entry treated as fresh",
			"name", c.cfg.name)
	}
	if expired {
		return true, EvictReasonExpireFunc
	}
	return false, 0
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
	return c.expireFunc(e.key, e.loadValue(), e.metadata()), false
}

// removeLocked deletes e from the shard and notifies the policy. Caller
// MUST hold s.mu. Callbacks (OnEvict / OnExpire) and events are NOT
// fired inline; they are queued on s.pendingCallbacks and dispatched
// after the shard lock is released via [Cache.flushAndUnlock] /
// [Cache.unlockShard]. This prevents deadlock when a callback re-enters
// the cache for a key on the same shard.
func (c *Cache[K, V]) removeLocked(s *shard[K, V], e *entry[K, V], reason EvictionReason) {
	// Capture key/value/tags BEFORE returning the entry to the pool.
	key := e.key
	value := e.loadValue()
	var tagsCopy []string
	if len(e.tags) > 0 {
		tagsCopy = append([]string(nil), e.tags...)
	}
	negative := e.flags.has(flagNegative)

	s.storage.del(e.key)
	c.forgetHashLocked(s, e.key)
	s.expiryRemove(e)
	s.policy.OnRemove(e)
	if c.tags != nil && len(e.tags) > 0 {
		c.enqueueUntag(e.key, e.tags)
	}
	c.counters.entries.Add(-1)
	c.counters.bytes.Add(-e.weight)
	if c.cfg.statsEnabled {
		c.counters.evictionsByReason[reason].Add(1)
	}
	if c.cfg.lockFreeRead {
		// Mark invalidated so readers holding a stale snapshot observe
		// the deletion; do not return the entry to the pool because
		// concurrent readers may still be dereferencing it. Bump
		// readMisses so expire-only workloads still trigger promotions.
		e.markInvalidated()
		if s.readMisses.Add(1) >= s.readMissThreshold() {
			c.promoteReadMapLocked(s)
		}
	} else {
		s.pool.put(e)
	}

	// Negative tombstones are an internal artifact of the loader path;
	// suppress callbacks/events for them.
	if negative {
		return
	}

	// Defer user-visible callbacks until after the shard lock is
	// released; the closure captures key/value/tags by value because
	// the entry has been recycled by the time the callback fires.
	at := c.cfg.clock.Now()
	cb := c.removeCallbackFor(key, value, reason, tagsCopy, at)
	s.pendingCallbacks = append(s.pendingCallbacks, cb)
}

func (c *Cache[K, V]) removeCallbackFor(key K, value V, reason EvictionReason, tagsCopy []string, at time.Time) func() {
	return func() {
		switch reason {
		case EvictReasonExpired, EvictReasonExpireFunc:
			if c.onExpire != nil {
				c.runHook("OnExpire", func() { c.onExpire(key, value) })
			}
			c.publishEvent(Event[K, V]{
				Kind:   EventExpire,
				Key:    key,
				Value:  value,
				Reason: reason,
				At:     at,
				Tags:   tagsCopy,
			})
		default:
			if c.onEvict != nil {
				c.runHook("OnEvict", func() { c.onEvict(key, value, reason) })
			}
			c.publishEvent(Event[K, V]{
				Kind:   EventEvict,
				Key:    key,
				Value:  value,
				Reason: reason,
				At:     at,
				Tags:   tagsCopy,
			})
		}
		c.publishInvalidation(key, reason)
	}
}

// flushAndUnlock releases the shard lock and fires any callbacks deferred
// by removeLocked on this shard. Caller MUST hold the shard lock.
func (c *Cache[K, V]) flushAndUnlock(s *shard[K, V]) {
	cbs := s.pendingCallbacks
	s.pendingCallbacks = nil
	s.mu.Unlock()
	for _, cb := range cbs {
		cb()
	}
}

func (c *Cache[K, V]) unlockShard(s *shard[K, V]) {
	c.flushAndUnlock(s)
}

// Range calls fn for every entry in the cache. Iteration is shard by
// shard under each shard's read lock; expired and negative-cache entries
// are skipped. The visit set is NOT a consistent snapshot. fn returning
// false stops iteration immediately.
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

func (c *Cache[K, V]) rangeShard(s *shard[K, V], now int64, fn func(K, V) bool) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stopped := false
	s.storage.each(func(e *entry[K, V]) bool {
		if c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
			return true
		}
		if !fn(e.key, c.returnValue(e.loadValue())) {
			stopped = true
			return false
		}
		return true
	})
	return stopped
}

// Keys returns a freshly-allocated snapshot of every key in the cache.
// Use [Cache.Range] for non-allocating iteration.
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
// [EvictReasonClear]. When [WithPurgeVisitor] is configured, the visitor
// is invoked for every entry before its slot is recycled.
func (c *Cache[K, V]) Clear() {
	if c.closed.Load() {
		return
	}
	c.runPurgeVisitor()
	for _, s := range c.shards {
		s.mu.Lock()
		s.storage.each(func(e *entry[K, V]) bool {
			c.counters.entries.Add(-1)
			c.counters.bytes.Add(-e.weight)
			if c.cfg.statsEnabled {
				c.counters.evictionsByReason[EvictReasonClear].Add(1)
			}
			s.policy.OnRemove(e)
			c.recycleOrInvalidate(s, e)
			return true
		})
		s.storage.clearAll()
		s.policy.Reset()
		s.ttl.Reset()
		if c.cfg.lockFreeRead {
			s.read.Store(newEmptyReadMap[K, V]())
			s.readMisses.Store(0)
		}
		c.flushAndUnlock(s)
	}
	if c.tags != nil {
		c.tags.reset()
	}
}

// TTL returns the remaining time-to-live for the entry at key. (0, true)
// means the entry exists with no TTL; (0, false) means the entry is
// absent or expired.
func (c *Cache[K, V]) TTL(key K) (time.Duration, bool) {
	if c.closed.Load() {
		return 0, false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.storage.get(key)
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

// Expiry returns the absolute expiry time for the entry at key, or the
// zero time when the entry has no TTL. The bool is true when the entry
// exists and is fresh.
func (c *Cache[K, V]) Expiry(key K) (time.Time, bool) {
	if c.closed.Load() {
		return time.Time{}, false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.storage.get(key)
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
// value. The new TTL is the entry's sliding-TTL when set, otherwise
// [WithDefaultTTL]. Returns true when the entry was refreshed.
func (c *Cache[K, V]) Touch(key K) bool {
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
	ttl := c.cfg.defaultTTL
	if e.flags.has(flagSliding) && e.slidingTTL > 0 {
		ttl = time.Duration(e.slidingTTL)
	}
	c.refreshExpiryLocked(s, e, now, ttl)
	return true
}

// TouchWithTTL resets the TTL of the entry at key to ttl. A TTL of 0
// clears the expiry; a negative TTL is a no-op returning false.
func (c *Cache[K, V]) TouchWithTTL(key K, ttl time.Duration) bool {
	if c.closed.Load() || ttl < 0 {
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
	if err := c.checkKeySize(key); err != nil {
		return false, err
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return false, err
	}
	tags := deriveAutoTags(value)
	if err := c.checkTagLimits(key, tags); err != nil {
		return false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)
	if existing, ok := s.storage.get(key); ok && !c.entryExpiredLocked(existing, now) && !existing.flags.has(flagNegative) {
		return false, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), tags)
	return true, nil
}

// SetIfPresent updates the value under key only when an entry already
// exists (and is fresh). Returns updated=true when the entry was
// replaced.
func (c *Cache[K, V]) SetIfPresent(key K, value V) (bool, error) {
	if c.closed.Load() {
		return false, ErrClosed
	}
	if err := c.checkKeySize(key); err != nil {
		return false, err
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return false, err
	}
	tags := deriveAutoTags(value)
	if err := c.checkTagLimits(key, tags); err != nil {
		return false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)
	existing, ok := s.storage.get(key)
	if !ok || c.entryExpiredLocked(existing, now) || existing.flags.has(flagNegative) {
		return false, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), tags)
	return true, nil
}

// DeleteIf removes the entry for key only when pred returns true for the
// current value. pred is invoked under the shard write lock and MUST NOT
// call back into the cache for the same key.
func (c *Cache[K, V]) DeleteIf(key K, pred func(V) bool) bool {
	if c.closed.Load() || pred == nil {
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
	if !pred(c.returnValue(e.loadValue())) {
		return false
	}
	c.removeLocked(s, e, EvictReasonDeleted)
	c.counters.deletes.Add(1)
	return true
}

// GetOrSet returns the cached value for key when present and fresh, or
// stores value and returns it otherwise. The bool is true when the
// returned value came from the cache. Atomic with respect to concurrent
// Set on the same key. Does not invoke any configured [Loader]; for that
// see [Cache.GetOrLoad].
func (c *Cache[K, V]) GetOrSet(key K, value V) (V, bool, error) {
	var zero V
	if c.closed.Load() {
		return zero, false, ErrClosed
	}
	if err := c.checkKeySize(key); err != nil {
		return zero, false, err
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return zero, false, err
	}
	tags := deriveAutoTags(value)
	if err := c.checkTagLimits(key, tags); err != nil {
		return zero, false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)
	if existing, ok := s.storage.get(key); ok && !c.entryExpiredLocked(existing, now) && !existing.flags.has(flagNegative) {
		existing.hits.Add(1)
		if existing.touchAccess(now) {
			s.expiryFix(existing)
		}
		s.policy.OnAccess(existing)
		c.recordHit()
		return c.returnValue(existing.loadValue()), true, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), tags)
	return value, false, nil
}

// PeekOrAdd is [Cache.GetOrSet] that does not promote the existing entry
// in the eviction policy. New inserts follow normal policy placement.
func (c *Cache[K, V]) PeekOrAdd(key K, value V) (V, bool, error) {
	var zero V
	if c.closed.Load() {
		return zero, false, ErrClosed
	}
	if err := c.checkKeySize(key); err != nil {
		return zero, false, err
	}
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return zero, false, err
	}
	tags := deriveAutoTags(value)
	if err := c.checkTagLimits(key, tags); err != nil {
		return zero, false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer c.unlockShard(s)
	if existing, ok := s.storage.get(key); ok && !c.entryExpiredLocked(existing, now) && !existing.flags.has(flagNegative) {
		// Peek semantics: do NOT call OnAccess and do not bump hits.
		return c.returnValue(existing.loadValue()), true, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), tags)
	return value, false, nil
}

// Resize changes the cache's bound at runtime. newSize is interpreted as
// MaxEntries unless the cache was built with [WithMaxBytes], in which
// case it is the byte budget. Returns the number of entries evicted from
// shrinking; growing the cache or passing newSize <= 0 returns 0.
// Concurrent Resize calls produce last-writer-wins on the bound.
func (c *Cache[K, V]) Resize(newSize int64) int {
	if c.closed.Load() || newSize <= 0 {
		return 0
	}
	c.counters.resizes.Add(1)

	// Bytes-bounded only if maxBytes was explicitly configured.
	if c.cfg.maxBytes > 0 {
		c.cfg.maxBytes = newSize
	} else {
		c.cfg.maxEntries = int(newSize)
	}

	perShard := perShardBudget(c.cfg.maxEntries, len(c.shards))
	evicted := 0
	for _, s := range c.shards {
		s.mu.Lock()
		s.budget = perShard
		s.policy.SetBudget(perShard)
		evicted += c.shrinkShardLocked(s)
		c.flushAndUnlock(s)
	}
	c.publishEvent(Event[K, V]{
		Kind: EventResize,
		At:   c.cfg.clock.Now(),
	})
	return evicted
}

// shrinkShardLocked evicts victims until the shard is within budget,
// recording each eviction under [EvictReasonResize]. Caller MUST hold s.mu.
func (c *Cache[K, V]) shrinkShardLocked(s *shard[K, V]) int {
	if s.budget <= 0 {
		return 0
	}
	count := 0
	for s.storage.length() > s.budget {
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

// Sync drains pending background work (async tag cleanup, async writes)
// and returns when the cache is quiescent. Returns ctx.Err() if ctx
// cancels first. Useful before [Cache.Save] so the snapshot reflects
// every Set/Delete already returned.
func (c *Cache[K, V]) Sync(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // pass through ctx.Err verbatim
	}
	const pollInterval = 100 * time.Microsecond
	for {
		if c.tagCleanupBacklog() == 0 && c.asyncBacklog() == 0 {
			return nil
		}
		// Nudge the apply goroutine so a long-idle drain happens promptly.
		if c.async != nil && c.asyncBacklog() > 0 {
			c.signalAsyncApply()
		}
		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck // pass through ctx.Err verbatim
		case <-time.After(pollInterval):
		}
	}
}

// DeleteExpired sweeps every shard, removing entries whose TTL has
// elapsed, and returns the count removed.
func (c *Cache[K, V]) DeleteExpired() int {
	if c.closed.Load() {
		return 0
	}
	now := c.cfg.clock.Now().UnixNano()
	count := 0
	for _, s := range c.shards {
		s.mu.Lock()
		// Snapshot expired entries first; flatStore compactions can
		// rearrange storage during delete and would disturb the walk.
		var doomed []*entry[K, V]
		s.storage.each(func(e *entry[K, V]) bool {
			if e.expired(now) {
				doomed = append(doomed, e)
			}
			return true
		})
		for _, e := range doomed {
			c.removeLocked(s, e, EvictReasonExpired)
			c.counters.expirations.Add(1)
			count++
		}
		c.flushAndUnlock(s)
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
		var doomed []*entry[K, V]
		s.storage.each(func(e *entry[K, V]) bool {
			if matcher(e.key) {
				doomed = append(doomed, e)
			}
			return true
		})
		for _, e := range doomed {
			c.removeLocked(s, e, EvictReasonDeletedPrefix)
			count++
		}
		c.flushAndUnlock(s)
	}
	return count
}

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

// DeleteWhere removes every entry for which pred returns true. pred runs
// OUTSIDE the shard write lock and may call into the cache. An entry
// modified between snapshot and delete is still removed if its prior
// value satisfied pred (eventual semantics).
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
		s.storage.each(func(e *entry[K, V]) bool {
			if c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
				return true
			}
			pairs = append(pairs, candidate{key: e.key, value: c.returnValue(e.loadValue())})
			return true
		})
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
			if e, ok := s.storage.get(k); ok {
				c.removeLocked(s, e, EvictReasonDeletedWhere)
				count++
			}
		}
		c.flushAndUnlock(s)
	}
	return count
}

// GetOrLoad returns the cached value for key, or invokes the configured
// [Loader] when the key is absent or expired. Concurrent callers for the
// same missing key share a single Loader invocation (singleflight).
// Returns [ErrNoLoader], [ErrClosed], ctx.Err(), or any error from the
// Loader. [ErrNotFound] with [WithNegativeCache] active records a
// tombstone and returns the same error.
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

// GetOrLoadFn is [Cache.GetOrLoad] with a per-call loader function. fn
// is invoked at most once per concurrent miss (singleflight by key).
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

// Refresh asynchronously triggers a Loader call for key, replacing the
// cached value when the load completes. Joins an existing in-flight
// call. Returns [ErrNoLoader], [ErrClosed], or ctx.Err() if already
// canceled.
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

// RefreshAll triggers a Loader call for every key in the cache. Returns
// the number of flights queued, skipping keys with an in-flight load.
// Negative-cache tombstones are not refreshed.
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
		c.flushAndUnlock(s)
	}
	return count
}

func (c *Cache[K, V]) loadOrJoin(
	ctx context.Context, key K,
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) (V, error) {
	var zero V
	ctx, span := c.tracer.Start(ctx, "memcache.load", Attr{Key: "key", Value: any(key)})
	defer func() { span.End(nil) }()
	s := c.shardFor(key)
	s.mu.Lock()
	now := c.cfg.clock.Now().UnixNano()

	// Cache hit on a fresh, non-negative entry.
	if e, ok := s.storage.get(key); ok && !c.entryExpiredLocked(e, now) && !e.flags.has(flagNegative) {
		v := e.loadValue()
		e.hits.Add(1)
		if e.touchAccess(now) {
			s.expiryFix(e)
		}
		s.policy.OnAccess(e)
		c.flushAndUnlock(s)
		c.recordHit()
		return v, nil
	}

	// Negative-cache hit: short-circuit without invoking loader.
	if e, ok := s.storage.get(key); ok && e.flags.has(flagNegative) && !c.entryExpiredLocked(e, now) {
		c.flushAndUnlock(s)
		c.recordMiss()
		return zero, ErrNotFound
	}

	// Cached error: short-circuit and surface the same error.
	if c.cfg.errorTTL > 0 {
		if ce, ok := s.errors[key]; ok {
			if ce.expireAt > now {
				err := ce.err
				c.flushAndUnlock(s)
				c.counters.loadCachedError.Add(1)
				c.recordMiss()
				return zero, err
			}
			delete(s.errors, key)
		}
	}

	c.recordMiss()

	// Already-in-flight: join the existing call.
	if flight, ok := s.inflight[key]; ok {
		flight.join()
		c.flushAndUnlock(s)
		if c.cfg.statsEnabled {
			c.counters.loadCoalesced.Add(1)
		}
		return c.waitOrLeave(ctx, s, key, flight)
	}

	// Rate limit BEFORE registering a flight so a rejected leader does
	// not leave a phantom inflight entry. The shard lock is still held;
	// the limiter is a fast atomic check.
	if c.loaderLimiter != nil && !c.loaderLimiter.Allow() {
		c.flushAndUnlock(s)
		if c.cfg.statsEnabled {
			c.counters.loadErrors.Add(1)
			c.counters.loadRateLimited.Add(1)
		}
		c.publishEvent(Event[K, V]{Kind: EventLoadRateLimited, Key: key, At: c.cfg.clock.Now()})
		return zero, ErrLoaderRateLimited
	}

	// Leader path: create a new flight with a cancellable context. The
	// refcount starts at 1; followers join() to bump, ctx-cancel leave()
	// to decrement. At zero we cancel loaderCtx so the Loader aborts.
	loaderCtx, cancel := c.newLoaderCtx()
	flight := newFlightCall[V](cancel)
	s.inflight[key] = flight
	c.flushAndUnlock(s)

	go c.runLoader(loaderCtx, s, key, flight, fn) //nolint:contextcheck // detached by design
	return c.waitOrLeave(ctx, s, key, flight)
}

// waitOrLeave blocks on flight completion or ctx cancellation. On
// ctx-cancel it atomically decrements the flight refcount, cancels the
// loader ctx if all waiters left, and removes the flight from s.inflight
// so no new joiner attaches to an aborting flight. If the loader
// completes between ctx.Done and result read, the caller still gets
// ctx.Err() (gave-up semantics).
func (c *Cache[K, V]) waitOrLeave(ctx context.Context, s *shard[K, V], key K, flight *flightCall[V]) (V, error) {
	var zero V
	select {
	case <-flight.done:
		if flight.err != nil {
			return zero, flight.err
		}
		return flight.val, nil
	case <-ctx.Done():
		s.mu.Lock()
		if flight.refs.Add(-1) == 0 {
			if cur, ok := s.inflight[key]; ok && cur == flight {
				delete(s.inflight, key)
			}
			if flight.cancel != nil {
				flight.cancel()
			}
		}
		c.flushAndUnlock(s)
		return zero, ctx.Err() //nolint:wrapcheck // pass ctx.Err verbatim
	}
}

func (c *Cache[K, V]) newLoaderCtx() (context.Context, context.CancelFunc) {
	if c.cfg.loaderTimeout > 0 {
		return context.WithTimeout(context.Background(), c.cfg.loaderTimeout)
	}
	return context.WithCancel(context.Background())
}

// tagLoaderTimeout joins [ErrLoaderTimeout] onto a deadline error from
// [WithLoaderTimeout] so callers can distinguish a per-loader timeout
// from a generic ctx cancel. The original error is retained via
// errors.Join so errors.Is(err, context.DeadlineExceeded) keeps working.
func (c *Cache[K, V]) tagLoaderTimeout(err error) error {
	if err == nil || c.cfg.loaderTimeout <= 0 {
		return err
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.Join(ErrLoaderTimeout, err)
}

func (c *Cache[K, V]) runLoader(
	loaderCtx context.Context,
	s *shard[K, V], key K, flight *flightCall[V],
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) {
	defer close(flight.done)
	if flight.cancel != nil {
		defer flight.cancel()
	}

	// Concurrency cap: acquire a slot before invoking the loader. Wait
	// is bounded by the loader timeout above.
	if c.loadSlots != nil {
		select {
		case c.loadSlots <- struct{}{}:
			defer func() { <-c.loadSlots }()
		case <-loaderCtx.Done():
			flight.err = ErrLoaderTooManyInFlight
			s.mu.Lock()
			delete(s.inflight, key)
			if c.cfg.statsEnabled {
				c.counters.loadsTotal.Add(1)
				c.counters.loadErrors.Add(1)
			}
			c.flushAndUnlock(s)
			c.publishEvent(Event[K, V]{Kind: EventLoadError, Key: key, Err: flight.err, At: c.cfg.clock.Now()})
			return
		}
	}

	loadStart := c.cfg.clock.Now()
	val, ttl, err := fn(loaderCtx, key)
	c.counters.loadLatency.Record(c.cfg.clock.Now().Sub(loadStart))
	err = c.tagLoaderTimeout(err)
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
	case c.cfg.errorTTL > 0 && !errors.Is(err, ErrNotFound) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded):
		// WithErrorTTL skips ErrNotFound (handled by WithNegativeCache)
		// and ctx errors (which would leak one caller's cancel to
		// unrelated callers within errorTTL).
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
	c.flushAndUnlock(s)

	// OnLoad / EventLoad fire AFTER the shard lock so callbacks can
	// re-enter the cache without deadlocking.
	if c.onLoad != nil {
		c.runHook("OnLoad", func() { c.onLoad(key, val, ttl, err) })
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
			if c.cfg.statsEnabled {
				c.counters.loadTimeouts.Add(1)
			}
		}
		c.publishEvent(Event[K, V]{
			Kind: kind,
			Key:  key,
			Err:  err,
			At:   c.cfg.clock.Now(),
		})
	}
}

// storeLoadedLocked applies the Loader's result. ttl=0 inherits
// [WithDefaultTTL]. Caller MUST hold s.mu.
func (c *Cache[K, V]) storeLoadedLocked(s *shard[K, V], key K, val V, ttl time.Duration) {
	weight, err := c.computeWeight(key, val)
	if err != nil {
		// Value too big to cache; the loader's val is still returned.
		return
	}
	if ttl == 0 {
		ttl = c.cfg.defaultTTL
	}
	c.upsertLocked(s, key, val, weight,
		c.effectiveTTL(ttl),
		c.cfg.slidingTTL, int64(ttl), nil)
}

// insertNegativeTombstoneLocked records a negative-cache entry for key.
// Caller MUST hold s.mu.
func (c *Cache[K, V]) insertNegativeTombstoneLocked(s *shard[K, V], key K) {
	now := c.cfg.clock.Now().UnixNano()
	expireAt := now + int64(c.cfg.negativeTTL)
	if existing, ok := s.storage.get(key); ok {
		// Negative tombstones carry no tags; untag the prior set.
		if c.tags != nil && len(existing.tags) > 0 {
			c.enqueueUntag(key, existing.tags)
			existing.tags = existing.tags[:0]
		}
		var zero V
		existing.storeValue(zero)
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
	s.storage.set(key, e)
	s.markAmended()
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

func (c *Cache[K, V]) triggerAsyncRefresh(s *shard[K, V], key K) {
	if c.loader == nil {
		return
	}
	s.mu.Lock()
	c.triggerAsyncRefreshLocked(s, key)
	c.flushAndUnlock(s)
}

// triggerAsyncRefreshLocked is [Cache.triggerAsyncRefresh] under s.mu.
func (c *Cache[K, V]) triggerAsyncRefreshLocked(s *shard[K, V], key K) {
	if c.loader == nil {
		return
	}
	if _, busy := s.inflight[key]; busy {
		return
	}
	loaderCtx, cancel := c.newLoaderCtx()
	flight := newFlightCall[V](cancel)
	s.inflight[key] = flight
	go c.runLoader(loaderCtx, s, key, flight, c.loader.Load)
}

// GetMulti returns the cached value for each key. Missing or expired
// keys are absent from the returned map. Each lookup goes through
// [Cache.Get], so refresh-ahead, SWR, and sliding-TTL all apply.
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

// SetMulti stores every (key, value) in items. The first error aborts;
// successfully-stored entries are NOT rolled back.
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

// GetMultiOrLoad returns the cached value for each key. Hits are
// returned directly; misses are coalesced via [BulkLoader.LoadMulti] when
// [WithBulkLoader] is configured, otherwise via per-key [Cache.GetOrLoad].
// The map contains only successfully-resolved keys; the first transport
// error from LoadMulti aborts.
func (c *Cache[K, V]) GetMultiOrLoad(ctx context.Context, keys []K) (map[K]V, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	if len(keys) == 0 {
		return map[K]V{}, nil
	}

	out := make(map[K]V, len(keys))
	var missing []K
	for _, k := range keys {
		// GetCtx threads the caller's ctx through to any
		// configured Store read-through; without WithStore the ctx
		// is checked once and never blocks. A Store error during
		// the per-key read produces a miss for that key and lets
		// the bulk-load fallback take over.
		v, ok, err := c.GetCtx(ctx, k)
		if err != nil {
			ok = false
		}
		if ok {
			out[k] = v
		} else {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return out, nil
	}

	// Without a bulk loader, fall back to per-key loads. Loader errors
	// are NOT propagated; callers detect missing keys via len(out).
	if c.bulkLoader == nil {
		if c.loader == nil {
			return out, ErrNoLoader
		}
		for _, k := range missing {
			if v, err := c.GetOrLoad(ctx, k); err == nil {
				out[k] = v
			}
		}
		return out, nil
	}

	loaded, err := c.bulkLoader.LoadMulti(ctx, missing)
	if err != nil {
		return out, err
	}
	for k, res := range loaded {
		if res.Err != nil {
			continue
		}
		ttl := res.TTL
		if ttl == 0 {
			ttl = c.cfg.defaultTTL
		}
		// Caller sees the loaded value regardless of Set failure.
		//nolint:contextcheck // bulk-load Set persists past ctx cancellation by design
		if setErr := c.SetWithTTL(k, res.Value, ttl); setErr != nil && c.cfg.logger != nil {
			c.cfg.logger.Debug("memcache: GetMultiOrLoad set failed",
				"err", setErr)
		}
		out[k] = res.Value
	}
	return out, nil
}
