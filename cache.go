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

	// admission is the resolved [AdmissionPolicy]. Always
	// non-nil — defaults to [AdmitAlways] when no
	// [WithAdmissionPolicy] / [WithDoorkeeper] is configured.
	admission AdmissionPolicy[K]

	// invalidationPublisher fires on every removal; nil when
	// not configured.
	invalidationPublisher func(K, EvictionReason)

	// invalidationSubscriber drives the consumer goroutine that
	// turns remote-channel sends into local Deletes. nil when
	// not configured. invalidationSubscriberDone is closed by
	// Close to stop the goroutine.
	invalidationSubscriber     <-chan K
	invalidationSubscriberDone chan struct{}

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

	// async holds the [WithAsyncWrites] state — pending-op signal
	// channel and apply-goroutine lifecycle. nil when async writes
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

// build validates cfg and constructs a [Cache]. allowUnbounded
// permits a cache with no entry/byte limit; otherwise an unbounded
// configuration is rejected with [ErrUnbounded].
// initShards populates c.shards with freshly-constructed per-shard
// state. Factored out of [build] to keep that function under the
// project's funlen budget; the work itself is straightforward —
// allocate a policy, a TTL backend, and the shard wrapper for each
// slot.
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
	// Surface deferred constructor errors (e.g., bad AES key
	// length passed to WithEncryptedCodec) before doing any other
	// validation work — the rest of build assumes cfg.codec is
	// usable.
	if cfg.codecCtorErr != nil {
		return nil, &ConfigError{Field: "Codec", Message: cfg.codecCtorErr.Error()}
	}
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
	admission, err := resolveAdmissionPolicy[K](cfg, hasher)
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

// maybeStartAsync wires the [WithAsyncWrites] state and apply
// goroutine when the option is set. Factored out of [build] to
// keep that function under the project's funlen budget.
func (c *Cache[K, V]) maybeStartAsync(cfg *config) {
	if !cfg.asyncWrites {
		return
	}
	c.async = newAsyncWrites[K, V](c.shards)
	c.startAsyncApply()
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
	onHit        func(K, V)
	onMiss       func(K)
	onEvict      func(K, V, EvictionReason)
	onExpire     func(K, V)
	onLoad       func(K, V, time.Duration, error)
	purgeVisitor func(K, V) error
	copyOnGet    func(V) V
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

// defaultHasher returns a hasher specialized for common K types.
// String and []byte keys go through SipHash-2-4 (HashDoS-resistant).
// Numeric keys (int / uint families, plus boolean) use a single
// splitmix64 mix — orders of magnitude faster than going through
// SipHash and entirely sufficient since the key is already a fixed
// number of bits. Anything else falls through to a fmt-based
// stringification + SipHash; correct but slow.
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
	s.PolicyName = c.cfg.policy.String()
	if c.tags != nil {
		s.TagsTracked = c.tags.distinctTagCount()
	}
	s.TagCleanupBacklog = c.tagCleanupBacklog()
	// Compactions is summed across shards. mapStore's compactions()
	// always returns 0; flatStore returns its real counter, so the
	// sum is meaningful only when [WithFlatStorage] is set. Reads
	// without the shard lock — flatStore.compactions is atomic.
	for _, sh := range c.shards {
		s.Compactions += sh.storage.compactions()
	}
	// PolicyDetail samples shard 0's policy. All shards share the
	// concrete policy type, so the shape is stable; the values
	// describe a single shard's state and should be read as
	// representative rather than cache-wide.
	if len(c.shards) > 0 {
		sh := c.shards[0]
		sh.mu.RLock()
		s.PolicyDetail = sh.policy.Snapshot()
		sh.mu.RUnlock()
	}
	return s
}

// ResetStats zeros counters. Live entry/byte counts are preserved.
// Stamps Stats.LastResetAt with the cache's clock so callers can
// see when the measurement window opened.
func (c *Cache[K, V]) ResetStats() {
	c.counters.reset()
	c.counters.stampReset(c.cfg.clock.Now())
}

// Has reports whether the cache contains a fresh entry for key. It
// does NOT promote the entry in the eviction policy and never
// invokes the configured Loader.
//
// The RLock is held through every field read on the resolved entry
// — releasing it earlier would race with concurrent evictions that
// recycle the entry through the pool (see TestInvalidationSubscriber
// for the regression that surfaced the bug).
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
			// Fall through to the store check (if any) — an
			// expired in-memory copy doesn't reflect a fresh
			// store-side write, and a negative tombstone is an
			// in-memory artifact.
		} else {
			return true
		}
	} else {
		s.mu.RUnlock()
	}
	// Store fall-through. Use a background context — Has has no
	// caller-supplied ctx; users who need cancellation should call
	// the store directly.
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
//
// Implementation note: a read-lock fast path serves hits where the
// configured eviction policy doesn't need promotion (FIFO always,
// S3-FIFO at freq saturation), the entry has no sliding TTL, and
// no refresh-ahead window applies. The slow path takes the shard
// write lock so policy promotion and side-effects can proceed
// safely.
//
// When [WithStore] is configured, an in-memory miss falls through
// to the Store; a Store hit is promoted into the in-memory cache
// and returned.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	v, ok, err := c.getCtx(context.Background(), key)
	if err != nil && c.cfg.logger != nil {
		c.cfg.logger.Debug("memcache: store Get failed during read-through",
			"err", err)
	}
	return v, ok
}

// getCtx is the shared backend for [Cache.Get] and [Cache.GetCtx].
// The returned error is always nil when no Store is configured;
// otherwise it carries any [Store.Get] failure. Callers without a
// way to surface the error (Get) silently swallow it.
//
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
		// Pure read — `hits` is atomic so we can bump it under
		// RLock; the value copy lands while still locked so a
		// concurrent eviction can't pool the entry mid-read.
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
		s.mu.Unlock()
		// In-memory miss — fall through to the configured Store if
		// any. Doing this after the unlock keeps the (potentially
		// slow) Store call off the shard's hot path.
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
	if c.entryExpiredLocked(e, now) {
		// Stale-while-revalidate: serve stale + trigger refresh.
		if !e.flags.has(flagNegative) && c.shouldServeStale(e, now) {
			c.triggerAsyncRefreshLocked(s, key)
			c.counters.staleWhileRevalidate.Add(1)
			e.hits.Add(1)
			s.policy.OnAccess(e)
			val := e.loadValue()
			s.mu.Unlock()
			c.recordHitObserve(key)
			c.fireHit(key, val)
			return c.returnValue(val), true, nil
		}
		c.removeLocked(s, e, EvictReasonExpired)
		c.counters.expirations.Add(1)
		s.mu.Unlock()
		// Expired in-memory — try the Store; the user may have
		// re-written the key on the durable side without touching
		// the cache.
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
		s.mu.Unlock()
		c.recordMiss()
		c.fireMiss(key)
		return zero, false, nil
	}
	defer s.mu.Unlock()
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

// fireHit invokes the configured OnHit hook (if any) and publishes
// no event — there is no EventHit kind in the spec; subscribers
// derive hit info from Stats. The synchronous hook still runs
// since it gives callers raw observability.
func (c *Cache[K, V]) fireHit(key K, value V) {
	if c.onHit != nil {
		c.runHook("OnHit", func() { c.onHit(key, value) })
	}
}

// returnValue applies any [WithCopyOnGet] transform to value before
// returning it. Centralizes the hot-path branch so individual
// callsites stay legible.
func (c *Cache[K, V]) returnValue(value V) V {
	if c.copyOnGet != nil {
		return c.copyOnGet(value)
	}
	return value
}

// fireMiss invokes the configured OnMiss hook.
func (c *Cache[K, V]) fireMiss(key K) {
	if c.onMiss != nil {
		c.runHook("OnMiss", func() { c.onMiss(key) })
	}
}

// runHook executes fn under the optional [WithCallbackTimeout]
// watchdog. When the timeout is zero or negative, fn runs
// inline. When set, a goroutine measures fn's duration and logs
// a warning if it exceeds the budget; the warning is best-effort
// (Go cannot preempt the callback).
func (c *Cache[K, V]) runHook(name string, fn func()) {
	deadline := c.cfg.callbackTimeout
	if deadline <= 0 {
		fn()
		return
	}
	done := make(chan struct{})
	start := c.cfg.clock.Now()
	go func() {
		select {
		case <-done:
		case <-time.After(deadline):
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

	e, ok := s.storage.get(key)
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
		return e.loadValue(), time.Time{}, true
	}
	return e.loadValue(), time.Unix(0, expNanos), true
}

// Set stores value under key with the cache's default TTL.
//
// If V implements [CacheTTLer], the value's CacheTTL() overrides
// [WithDefaultTTL] (use [Cache.SetWithTTL] when you need to bypass
// the value's preference). If V implements [CacheTagger], its
// CacheTags() are merged into the entry's tag set.
//
// After insertion, any [WithGroup]-bounded group whose name is in
// the entry's tags is shrunk back to its configured capacity by
// evicting the oldest member if necessary.
func (c *Cache[K, V]) Set(key K, value V) error {
	return c.setCtx(context.Background(), key, value)
}

// setCtx is the shared backend for [Cache.Set] and [Cache.SetCtx].
// The ctx is threaded through to the configured [Store] write-
// through; without [WithStore] the context never reaches a
// blocking call.
func (c *Cache[K, V]) setCtx(ctx context.Context, key K, value V) error {
	ttl := extractCacheableTTL(value, c.cfg.defaultTTL)
	tags := extractCacheableTags(value)
	if tpl := extractTemplateTags(value); len(tpl) > 0 {
		tags = append(tags, tpl...)
	}
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

// SetWithTTL stores value under key with the given TTL. A TTL of 0
// means "no expiry"; a negative TTL returns [ErrInvalidTTL].
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	if ttl < 0 {
		return ErrInvalidTTL
	}
	if c.async != nil {
		return c.asyncSet(key, value, ttl, c.cfg.slidingTTL, nil)
	}
	if err := c.setLocked(key, value, ttl, c.cfg.slidingTTL, nil); err != nil {
		return err
	}
	return c.propagateSetToStore(context.Background(), key, value, ttl)
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
	s.mu.Unlock()
	// Mirror the write to the configured Store. For absolute-
	// expiry calls the TTL the Store sees is `expireAt - now`
	// (clamped to non-negative); for relative TTL calls it's the
	// per-call duration. Sliding TTLs surface as their initial
	// value — the Store has no Get-side hook.
	if c.store != nil {
		ttl := sc.ttl
		if sc.hasExpiry && !sc.expireAt.IsZero() {
			ttl = max(time.Until(sc.expireAt), 0)
		}
		if err := c.propagateSetToStore(context.Background(), key, value, ttl); err != nil {
			return err
		}
	}
	// Group enforcement runs OUTSIDE the shard lock so it can
	// take other shards' locks freely without inverting the
	// standard "shard then index" lock order.
	c.enforceGroupBudgets(sc.tags)
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

// Delete removes key from the cache. Returns true when an entry was
// removed. When [WithStore] is configured the delete is mirrored
// to the Store; Store errors are logged but do not affect the
// returned bool — callers that want the error should use
// [Cache.DeleteCtx]. When [WithAsyncWrites] is configured the
// delete is queued and the returned bool is "always true" except
// for closed caches; callers that need the post-delete state
// should call [Cache.Sync] first.
func (c *Cache[K, V]) Delete(key K) bool {
	if c.async != nil {
		return c.asyncDelete(key)
	}
	return c.deleteCtx(context.Background(), key, EvictReasonDeleted)
}

// deleteCtx is the shared backend for [Cache.Delete],
// [Cache.DeleteCtx], and the [WithInvalidationSubscriber] consumer
// goroutine. The returned bool is true when an entry was removed
// from the in-memory cache; the Store delete (if configured) is
// best-effort. The caller path that surfaces errors is DeleteCtx.
func (c *Cache[K, V]) deleteCtx(ctx context.Context, key K, reason EvictionReason) bool {
	removed := c.deleteWithReason(key, reason)
	if c.store != nil {
		// Errors are logged inside deleteThroughStore; this caller
		// has no way to surface them — DeleteCtx does, by re-
		// entering deleteThroughStore directly.
		_ = c.deleteThroughStore(ctx, key) //nolint:errcheck // see comment above
	}
	return removed
}

// deleteWithReason is the shared backend for [Cache.Delete] and the
// [WithInvalidationSubscriber] consumer goroutine, parameterized
// over the eviction reason so subscribers can be reported under
// [EvictReasonRemote] without duplicating the lookup logic.
func (c *Cache[K, V]) deleteWithReason(key K, reason EvictionReason) bool {
	if c.closed.Load() {
		return false
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
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
			s.pool.put(e)
			return true
		})
		s.storage.clearAll()
		s.policy.Reset()
		s.ttl.Reset()
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
	// Drain any pending async writes BEFORE the snapshot so the
	// final on-disk state reflects every Set/Delete the caller
	// returned from. The apply goroutine drains-and-exits when its
	// stop channel is closed.
	c.stopAsyncApply()
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
	if c.invalidationSubscriberDone != nil {
		close(c.invalidationSubscriberDone)
	}
	c.runPurgeVisitor()
	c.Reset()
	c.stopTagCleanup()
	if c.events != nil {
		c.events.close()
	}
	return nil
}

// runPurgeVisitor invokes the configured [WithPurgeVisitor] for
// every live (non-tombstone) entry, outside any shard lock. No-op
// when no visitor is configured. Caller is responsible for
// subsequent Reset/Clear.
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
				staged = append(staged, kv{k: e.key, v: e.loadValue()})
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

// recordHit increments the hit counter when stats are enabled.
func (c *Cache[K, V]) recordHit() {
	if c.cfg.statsEnabled {
		c.counters.addHit()
	}
}

// recordHashCollisionLocked checks whether key collides with a
// previously-inserted distinct key on the same shard, and if so
// bumps [Stats.HashCollisions]. No-op when [WithCollisionTracking]
// is off (s.hashIndex == nil). Caller holds s.mu (write).
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

// recordMiss increments the miss counter when stats are enabled.
func (c *Cache[K, V]) recordMiss() {
	if c.cfg.statsEnabled {
		c.counters.addMiss()
	}
}

// setLocked performs the shared write-path used by Set / SetWithTTL.
func (c *Cache[K, V]) setLocked(key K, value V, ttl time.Duration, sliding bool, tags []string) error {
	if c.closed.Load() {
		return ErrClosed
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
	defer s.mu.Unlock()
	c.upsertLocked(s, key, value, weight, c.effectiveTTL(ttl), sliding, int64(ttl), tags)
	return nil
}

// checkTagLimits decorates a [Cache.validateTagLimits] result
// with the offending key and the [ErrTooManyTags] sentinel cause
// before returning it to the caller.
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
func (c *Cache[K, V]) effectiveTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	return applyJitter(ttl, c.resolveJitter(ttl))
}

// resolveJitter picks the jitter window for a given ttl. When the
// caller didn't configure WithTTLJitter, the spec-default 5% of ttl
// applies; otherwise the configured absolute duration wins (a
// caller-supplied zero is honored as "no jitter").
func (c *Cache[K, V]) resolveJitter(ttl time.Duration) time.Duration {
	if c.cfg.ttlJitterExplicit {
		return c.cfg.ttlJitter
	}
	return ttl / 20
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
	existing, ok := s.storage.get(key)
	if !ok {
		// Pure insert — consult the admission policy. Updates
		// (existing != nil) bypass the gate so callers can
		// always overwrite values they previously stored.
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
		// Collision tracking: when WithCollisionTracking is on, a
		// new key whose hasher output collides with the most
		// recently-inserted distinct key on this shard bumps
		// Stats.HashCollisions.
		c.recordHashCollisionLocked(s, key)
	}
	if ok {
		// Capture the prior tag set before we overwrite the
		// slice; retagLocked needs the full old set to untag.
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
	for s.storage.length() > s.budget {
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
	return c.expireFunc(e.key, e.loadValue(), e.metadata()), false
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
		// Mark the entry as invalidated so any reader holding a
		// stale snapshot pointer observes the deletion. Skip
		// pooling — concurrent readers may still be dereferencing
		// the entry; let GC reclaim it after the next snapshot
		// promotion drops the last reference.
		e.markInvalidated()
	} else {
		s.pool.put(e)
	}

	// Negative tombstones are an internal artifact of the loader
	// path; suppress callbacks/events for them so subscribers
	// don't see "evictions" they never asked for.
	if negative {
		return
	}

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
			At:     c.cfg.clock.Now(),
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
			At:     c.cfg.clock.Now(),
			Tags:   tagsCopy,
		})
	}
	c.publishInvalidation(key, reason)
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
	stopped := false
	s.storage.each(func(e *entry[K, V]) bool {
		if c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
			return true
		}
		if !fn(e.key, e.loadValue()) {
			stopped = true
			return false
		}
		return true
	})
	return stopped
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
// [EvictReasonClear] in stats. When [WithPurgeVisitor] is
// configured, every entry is handed to the visitor (outside the
// shard lock) before its slot is recycled.
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
			s.pool.put(e)
			return true
		})
		s.storage.clearAll()
		s.policy.Reset()
		s.ttl.Reset()
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
	weight, err := c.computeWeight(key, value)
	if err != nil {
		return false, err
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.storage.get(key); ok && !existing.expired(now) && !existing.flags.has(flagNegative) {
		return false, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
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
	existing, ok := s.storage.get(key)
	if !ok || c.entryExpiredLocked(existing, now) || existing.flags.has(flagNegative) {
		return false, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
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
	e, ok := s.storage.get(key)
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		return false
	}
	if !pred(e.loadValue()) {
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
	if existing, ok := s.storage.get(key); ok && !existing.expired(now) && !existing.flags.has(flagNegative) {
		// Peek semantics: do NOT call OnAccess and do not bump hits.
		return c.returnValue(existing.loadValue()), true, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
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
	c.counters.resizes.Add(1)

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

// Sync drains pending background work — async tag cleanup, async
// loader refreshes — and returns when the cache is in a quiescent
// state. Returns the context's error if it cancels first. Useful
// in tests and immediately before [Cache.Save] so the snapshot
// reflects every Set/Delete that's already returned.
//
// Sync waits for the tag-cleanup backlog AND the [WithAsyncWrites]
// pending queue (when enabled) to reach zero. Future items
// (in-flight loader refresh-aheads) will join here without
// changing the surface.
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
		// Async writes wait on a signal — nudge the apply
		// goroutine so a long-idle drain still happens promptly.
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
		// Snapshot expired entries before deleting so a storage
		// implementation that may rearrange itself on delete
		// (e.g. [flatStore] compactions) doesn't disturb the walk.
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
		s.storage.each(func(e *entry[K, V]) bool {
			if c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
				return true
			}
			pairs = append(pairs, candidate{key: e.key, value: e.loadValue()})
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
		s.mu.Unlock()
		c.recordHit()
		return v, nil
	}

	// Negative-cache hit: short-circuit without invoking loader.
	if e, ok := s.storage.get(key); ok && e.flags.has(flagNegative) && !c.entryExpiredLocked(e, now) {
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
		s.mu.Unlock()
		if c.cfg.statsEnabled {
			c.counters.loadCoalesced.Add(1)
		}
		return waitForFlight(ctx, flight)
	}

	// Rate limit BEFORE registering a flight so a rejected leader
	// doesn't leave a phantom inflight entry. The shard lock is
	// still held — the limiter is a fast atomic-style check, so
	// holding briefly is fine.
	if c.loaderLimiter != nil && !c.loaderLimiter.Allow() {
		s.mu.Unlock()
		if c.cfg.statsEnabled {
			c.counters.loadErrors.Add(1)
			c.counters.loadRateLimited.Add(1)
		}
		c.publishEvent(Event[K, V]{Kind: EventLoadRateLimited, Key: key, At: c.cfg.clock.Now()})
		return zero, ErrLoaderRateLimited
	}

	// Leader path: create a new flight with a cancellable
	// context. The flight's refcount starts at 1 (the leader
	// itself). Followers `join()` to bump it; ctx-cancel exits
	// `leave()` to decrement. When the count reaches zero we
	// cancel loaderCtx — the Loader sees ctx.Done and can abort.
	loaderCtx, cancel := c.newLoaderCtx()
	flight := newFlightCall[V](cancel)
	s.inflight[key] = flight
	s.mu.Unlock()

	go c.runLoader(loaderCtx, s, key, flight, fn) //nolint:contextcheck // detached by design
	return waitForFlight(ctx, flight)
}

// newLoaderCtx builds the loader's context. Always cancellable so
// ctx-aggregation can fire; honors [WithLoaderTimeout] when
// configured.
func (c *Cache[K, V]) newLoaderCtx() (context.Context, context.CancelFunc) {
	if c.cfg.loaderTimeout > 0 {
		return context.WithTimeout(context.Background(), c.cfg.loaderTimeout)
	}
	return context.WithCancel(context.Background())
}

// runLoader executes fn in a fresh goroutine, persists the result
// (or error tombstone) in the cache, removes the flight from the
// inflight map, and signals waiters via close(flight.done).
//
// Concurrent waiters block on flight.done so they observe the
// stored val/err under happens-before guarantees from the channel
// close.
func (c *Cache[K, V]) runLoader(
	loaderCtx context.Context,
	s *shard[K, V], key K, flight *flightCall[V],
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) {
	defer close(flight.done)
	// Always release the cancellable context the leader prepared.
	if flight.cancel != nil {
		defer flight.cancel()
	}

	// Concurrency cap: acquire a slot before invoking the loader.
	// Releasing happens after the loader returns. When the cap is
	// configured but no slot is available, callers wait until one
	// frees — bounded by the loader timeout above.
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
			s.mu.Unlock()
			c.publishEvent(Event[K, V]{Kind: EventLoadError, Key: key, Err: flight.err, At: c.cfg.clock.Now()})
			return
		}
	}

	loadStart := c.cfg.clock.Now()
	val, ttl, err := fn(loaderCtx, key)
	c.counters.loadLatency.Record(c.cfg.clock.Now().Sub(loadStart))
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
		c.effectiveTTL(ttl),
		c.cfg.slidingTTL, int64(ttl), nil)
}

// insertNegativeTombstoneLocked records a negative-cache entry for
// key, reusing the existing entry slot when possible to keep the
// shard's bookkeeping consistent. Caller must hold s.mu.
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
	loaderCtx, cancel := c.newLoaderCtx()
	flight := newFlightCall[V](cancel)
	s.inflight[key] = flight
	go c.runLoader(loaderCtx, s, key, flight, c.loader.Load)
}

// waitForFlight blocks until the flight resolves or ctx is canceled.
// On ctx cancel the canceling waiter decrements the flight's
// refcount via [flightCall.leave]; the loader keeps running while
// any other waiter is still interested. When ALL waiters have
// canceled, leave's atomic decrement reaches zero and the flight's
// stored cancel function fires — the Loader's context goes Done
// and (if it respects ctx) the load aborts with ctx.Canceled.
func waitForFlight[V any](ctx context.Context, flight *flightCall[V]) (V, error) {
	var zero V
	select {
	case <-flight.done:
		if flight.err != nil {
			return zero, flight.err
		}
		return flight.val, nil
	case <-ctx.Done():
		flight.leave()
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

// GetMultiOrLoad returns the cached value for each key in keys.
// Cache hits are returned directly; misses are coalesced into a
// single [BulkLoader.LoadMulti] call when [WithBulkLoader] is
// configured. Without a bulk loader, the cache falls back to
// per-key [Cache.GetOrLoad] (each missing key triggers an
// individual Loader call, but singleflight still deduplicates
// concurrent callers for the same missing key).
//
// The returned map contains exactly the keys that resolved
// successfully — keys whose Loader returned an error or whose
// per-key LoadResult.Err was non-nil are absent. The first
// transport-level error from LoadMulti aborts the call.
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

	// Without a bulk loader, fall back to per-key loads. Loader
	// errors are NOT propagated — callers see the present subset
	// and can detect missing keys via len(out) < len(keys).
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
		// Caller always sees the loaded value; if SetWithTTL
		// fails (e.g., MaxValueWeight), the cache simply
		// doesn't retain it for the next call.
		if setErr := c.SetWithTTL(k, res.Value, ttl); setErr != nil && c.cfg.logger != nil {
			c.cfg.logger.Debug("memcache: GetMultiOrLoad set failed",
				"err", setErr)
		}
		out[k] = res.Value
	}
	return out, nil
}
