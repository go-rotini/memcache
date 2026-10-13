package memcache

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"strings"
	"sync"
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
	// ttlStore is store when it implements [TTLStore], else nil.
	ttlStore TTLStore[K, V]

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

	// traits records which value hooks V can have; see [valueTraitsOf].
	traits valueTraits

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

	// maxEntries and maxBytes are the live entry and byte bounds
	// (0 = none), read on the hot path and changed by Resize; resizeMu
	// serializes Resize calls so every shard ends up with the same
	// budget. shedding guards [Cache.shedOverBudget] against concurrent
	// or re-entrant sweeps; shedRetry asks the sweeping goroutine to
	// check the bounds again; shedNext rotates the shard a sweep starts
	// from so no shard is always drained first.
	maxEntries atomic.Int64
	maxBytes   atomic.Int64
	resizeMu   sync.Mutex
	shedding   atomic.Bool
	shedRetry  atomic.Bool
	shedNext   atomic.Uint64

	closed atomic.Bool

	// sealed is set by Close once its final auto-save is written; from
	// then on nothing is inserted and no janitor starts, so a write
	// racing Close can't repopulate the closed cache.
	sealed atomic.Bool
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
	c.maxBytes.Store(cfg.maxBytes)
	c.maxEntries.Store(int64(cfg.maxEntries))
	perShard := perShardBudget(cfg.maxEntries, len(c.shards))
	pcfg := policyConfig[K]{budget: perShard, hasher: hasher, lockFreeRead: cfg.lockFreeRead}
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

// checkBounds validates the size bounds and the expvar name.
func checkBounds(cfg *config, allowUnbounded bool) error {
	if cfg.maxEntries < 0 {
		return &ConfigError{Field: "MaxEntries", Message: "must be non-negative"}
	}
	if cfg.maxBytes < 0 {
		return &ConfigError{Field: configFieldMaxBytes, Message: "must be non-negative"}
	}
	if !allowUnbounded && cfg.maxEntries <= 0 && cfg.maxBytes <= 0 {
		return ErrUnbounded
	}
	return checkExpvarName(cfg.expvarName)
}

func build[K comparable, V any](cfg *config, allowUnbounded bool) (*Cache[K, V], error) {
	// Surface deferred constructor errors (e.g., bad AES key) before
	// other validation; the rest of build assumes cfg.codec is usable.
	if cfg.codecCtorErr != nil {
		return nil, &ConfigError{Field: "Codec", Message: cfg.codecCtorErr.Error()}
	}
	if err := checkBounds(cfg, allowUnbounded); err != nil {
		return nil, err
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
		ttlStore:                   asTTLStore(store),
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
		traits:                     valueTraitsOf[V](),
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
// configured. Missing files, and encrypted snapshots in an older format,
// are not treated as errors.
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
	if errors.Is(err, errEncryptedLegacy) {
		if c.cfg.logger != nil {
			c.cfg.logger.Warn("memcache: auto-load skipped an encrypted snapshot in an older format, continuing with empty cache",
				"path", c.cfg.autoLoadPath, "err", err)
		}
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
// maxEntries, with a 10% slop factor to accommodate hash skew. The
// global bound itself is enforced by [Cache.overBudget].
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
// for string and []byte keys, splitmix64 for integer, bool and float keys
// (with -0 hashed as +0), and [maphash.Comparable] for anything else, so
// keys that are == always hash alike.
//
// Each call draws a fresh random key from crypto/rand (the SipHash key,
// and a seed folded into integer keys before mixing), so key placement
// differs per cache and an attacker can't pick keys that all land in
// one shard or bucket. Hashes never leave the process (snapshots and
// stores persist keys, not hashes), and snapshot loads bound the total
// rather than each shard, so nothing needs them to be stable.
func defaultHasher[K comparable]() func(K) uint64 {
	var key [16]byte
	_, _ = cryptorand.Read(key[:])
	k0 := binary.LittleEndian.Uint64(key[:8])
	k1 := binary.LittleEndian.Uint64(key[8:])
	seed := maphash.MakeSeed()
	return func(k K) uint64 {
		switch v := any(k).(type) {
		case string:
			return sketch.HashString(k0, k1, v)
		case []byte:
			return sketch.SipHash24(k0, k1, v)
		case int:
			return sketch.MixUint64(uint64(v) ^ k0)
		case int8:
			return sketch.MixUint64(uint64(v) ^ k0)
		case int16:
			return sketch.MixUint64(uint64(v) ^ k0)
		case int32:
			return sketch.MixUint64(uint64(v) ^ k0)
		case int64:
			return sketch.MixUint64(uint64(v) ^ k0)
		case uint:
			return sketch.MixUint64(uint64(v) ^ k0)
		case uint8:
			return sketch.MixUint64(uint64(v) ^ k0)
		case uint16:
			return sketch.MixUint64(uint64(v) ^ k0)
		case uint32:
			return sketch.MixUint64(uint64(v) ^ k0)
		case uint64:
			return sketch.MixUint64(v ^ k0)
		case uintptr:
			return sketch.MixUint64(uint64(v) ^ k0)
		case bool:
			if v {
				return sketch.MixUint64(1 ^ k0)
			}
			return sketch.MixUint64(k0)
		case float64:
			if v == 0 {
				v = 0 // -0 == +0, so both hash alike
			}
			return sketch.MixUint64(math.Float64bits(v) ^ k0)
		case float32:
			if v == 0 {
				v = 0
			}
			return sketch.MixUint64(uint64(math.Float32bits(v)) ^ k0)
		default:
			return maphash.Comparable(seed, k)
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
	if n := c.maxEntries.Load(); n > 0 {
		return n
	}
	return c.maxBytes.Load()
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
// not promote the entry in the eviction policy and never invokes the
// configured [Loader]. With [WithStore], an in-memory miss is checked
// against the Store.
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
	if !ok || c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
		s.mu.RUnlock()
		return zero, false
	}
	// Read the value before unlocking: once unlocked, a removal can
	// recycle e for another key.
	val := e.loadValue()
	s.mu.RUnlock()
	return c.returnValue(val), true
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

func (c *Cache[K, V]) getCtx(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if c.closed.Load() {
		return zero, false, nil
	}
	s := c.shardFor(key)
	// The clock is read only when something needs the time: pending
	// async writes, the lock-free snapshot, an entry with an expiry, or
	// the slow path.
	var now int64
	haveNow := false
	if s.pending != nil || c.cfg.lockFreeRead {
		now, haveNow = c.cfg.clock.Now().UnixNano(), true
		if val, hit, terminal := c.tryServeFromAsyncPending(s, key, now); terminal {
			return val, hit, nil
		}
		if val, hit, terminal := c.tryServeFromReadSnapshot(s, key, now); terminal {
			return val, hit, nil
		}
	}

	// Fast path: try to serve under the shard read lock. An entry
	// without an expiry is never expired and never due a refresh, so
	// it needs no clock.
	s.mu.RLock()
	e, ok := s.storage.get(key)
	if ok && !haveNow && e.expireAt.Load() != 0 {
		now, haveNow = c.cfg.clock.Now().UnixNano(), true
	}
	if ok &&
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
	if !haveNow {
		now = c.cfg.clock.Now().UnixNano()
	}

	// Slow path: full Get under the shard write lock. Re-validates
	// every condition because the entry may have been evicted or
	// mutated between RUnlock and Lock.
	s.mu.Lock()
	e, ok = s.storage.get(key)
	if !ok {
		c.flushAndUnlock(s)
		// Fall through to the Store after the unlock to keep slow Store
		// calls off the shard's hot path.
		return c.missOrReadThrough(ctx, key)
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
		return c.missOrReadThrough(ctx, key)
	}
	if e.flags.has(flagNegative) {
		c.counters.negativeHits.Add(1)
		c.flushAndUnlock(s)
		c.recordMiss()
		c.fireMiss(key)
		return zero, false, nil
	}
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
	val := e.loadValue()
	// The hook runs after the unlock, as on every other path, so it
	// may call back into the cache.
	c.flushAndUnlock(s)
	c.recordHitObserve(key)
	c.fireHit(key, val)
	return c.returnValue(val), true, nil
}

// missOrReadThrough finishes a Get that missed in memory: a Store hit
// is promoted and returned, anything else is recorded as a miss. Called
// without the shard lock.
func (c *Cache[K, V]) missOrReadThrough(ctx context.Context, key K) (V, bool, error) {
	if v, found, err := c.readThroughStore(ctx, key); err != nil || found {
		if found {
			c.recordHitObserve(key)
			c.fireHit(key, v)
		}
		return v, found, err
	}
	c.recordMiss()
	c.fireMiss(key)
	var zero V
	return zero, false, nil
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

// runHook runs fn and, when it runs past [WithCallbackTimeout], logs a
// warning at the deadline. The watchdog is a runtime timer, which costs
// no goroutine unless it fires. A panic in fn is recovered and logged
// with its stack, so it can't crash a background goroutine or stop the
// hooks queued after it.
func (c *Cache[K, V]) runHook(name string, fn func()) {
	logger := c.cfg.logger
	defer func() {
		if r := recover(); r != nil && logger != nil {
			logger.Error("memcache: hook panicked",
				"hook", name, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	deadline := c.cfg.callbackTimeout
	if deadline <= 0 || logger == nil {
		fn()
		return
	}
	t := time.AfterFunc(deadline, func() {
		logger.Warn("memcache: callback exceeded WithCallbackTimeout",
			"hook", name, "deadline", deadline)
	})
	defer t.Stop()
	fn()
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

	c.applyPendingKey(s, key)
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
		return c.returnValue(e.loadValue()), time.Time{}, true
	}
	return c.returnValue(e.loadValue()), time.Unix(0, expNanos), true
}

// Set stores value under key with the cache's default TTL. If V
// implements [CacheTTLer] the value's TTL overrides [WithDefaultTTL]; the
// entry's tags come from [CacheTagger] or a cache struct-tag template.
// After insertion, [WithGroup]-bounded groups named by the entry's tags
// are shrunk back to capacity.
func (c *Cache[K, V]) Set(key K, value V) error {
	return c.setCtx(context.Background(), key, value, nil)
}

// setCtx is Set with a ctx for the Store write. When after is not nil,
// the hooks the write fires, and the work that may fire more, are added
// to it for the caller to run instead.
func (c *Cache[K, V]) setCtx(ctx context.Context, key K, value V, after *[]func()) error {
	return c.setTTLCtx(ctx, key, value, c.valueTTL(value, c.cfg.defaultTTL), after)
}

// setTTLCtx is setCtx with the given TTL.
func (c *Cache[K, V]) setTTLCtx(ctx context.Context, key K, value V, ttl time.Duration, after *[]func()) error {
	tags := c.autoTags(value)
	if c.async != nil {
		return c.asyncSet(key, value, ttl, c.cfg.slidingTTL, tags, nil)
	}
	if err := c.setAndStore(ctx, key, value, ttl, c.cfg.slidingTTL, tags, after); err != nil {
		return err
	}
	c.settleGroups(after, tags)
	return nil
}

// setAndStore writes key to memory and then the Store under the key's
// lock, rolling memory back if the Store write fails. Hooks fire after
// the lock is released, so a hook may write the same key.
func (c *Cache[K, V]) setAndStore(
	ctx context.Context, key K, value V, ttl time.Duration, sliding bool, tags []string, after *[]func(),
) error {
	var cbs []func()
	s := c.shardFor(key)
	defer func() { c.settle(after, cbs, s) }()
	defer c.lockKey(key)()
	if err := c.setLockedInto(s, key, value, ttl, sliding, tags, &cbs); err != nil {
		return err
	}
	return c.propagateSetToStore(ctx, key, value, ttl, &cbs)
}

// settle fires cbs with [Cache.runCallbacks] or, when after is not nil,
// adds that to after for a caller that fires it once its own lock is
// released.
func (c *Cache[K, V]) settle(after *[]func(), cbs []func(), s *shard[K, V]) {
	if after == nil {
		c.runCallbacks(cbs, s)
		return
	}
	*after = append(*after, func() { c.runCallbacks(cbs, s) })
}

// settleGroups is [Cache.enforceGroupBudgets], deferred to after as
// [Cache.settle] defers callbacks.
func (c *Cache[K, V]) settleGroups(after *[]func(), tags []string) {
	if len(c.cfg.groups) == 0 || len(tags) == 0 {
		return
	}
	if after == nil {
		c.enforceGroupBudgets(tags)
		return
	}
	*after = append(*after, func() { c.enforceGroupBudgets(tags) })
}

// SetWithTTL stores value under key with the given TTL. A TTL of 0 means
// no expiry; a negative TTL returns [ErrInvalidTTL]. Like every TTL, it
// is jittered by ±5% unless [WithTTLJitter] says otherwise.
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	return c.setWithTTL(key, value, ttl, nil)
}

// setWithTTL is SetWithTTL that adds the hooks to after, as setCtx does.
func (c *Cache[K, V]) setWithTTL(key K, value V, ttl time.Duration, after *[]func()) error {
	if ttl < 0 {
		return ErrInvalidTTL
	}
	return c.setTTLCtx(context.Background(), key, value, ttl, after)
}

// SetWithOptions stores value under key with per-call overrides supplied
// via [SetOption]. Cache defaults apply to fields no option touches; when
// they conflict, the per-call option wins.
func (c *Cache[K, V]) SetWithOptions(key K, value V, opts ...SetOption) error {
	return c.setWithOptions(key, value, nil, opts...)
}

// setWithOptions is SetWithOptions that adds the hooks to after, as
// setCtx does.
func (c *Cache[K, V]) setWithOptions(key K, value V, after *[]func(), opts ...SetOption) error {
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
	// auto-tag derivation. Explicit per-call options always win.
	if !sc.hasTags {
		sc.tags = c.autoTags(value)
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
		return c.asyncSet(key, value, sc.ttl, sc.sliding, sc.tags, sc.guard)
	}
	if err := c.setOptionsAndStore(key, value, weight, &sc, after); err != nil {
		return err
	}
	// Group enforcement runs OUTSIDE the shard lock to avoid inverting
	// the standard shard-then-index lock order.
	c.settleGroups(after, sc.tags)
	return nil
}

// setOptionsAndStore is setAndStore for SetWithOptions.
func (c *Cache[K, V]) setOptionsAndStore(key K, value V, weight int64, sc *setConfig, after *[]func()) (err error) {
	var cbs []func()
	s := c.shardFor(key)
	defer func() { c.settle(after, cbs, s) }()
	defer c.lockKey(key)()
	s.mu.Lock()
	if sc.guard != nil && !sc.guard() {
		c.releaseShardInto(s, &cbs)
		return errWriteSuperseded
	}
	if sc.hasExpiry {
		c.upsertWithAbsoluteExpiryLocked(s, key, value, weight, *sc)
	} else {
		c.upsertLocked(s, key, value, weight,
			c.effectiveTTL(sc.ttl),
			sc.sliding, int64(sc.ttl), sc.tags)
	}
	c.releaseShardInto(s, &cbs)
	if c.store == nil {
		return nil
	}
	if sc.hasExpiry && !sc.expireAt.IsZero() {
		if err = c.writeThroughStoreUntil(context.Background(), key, value, sc.expireAt); err != nil {
			c.rollbackInMemory(key, &cbs)
		}
		return err
	}
	return c.propagateSetToStore(context.Background(), key, value, sc.ttl, &cbs)
}

func (c *Cache[K, V]) upsertWithAbsoluteExpiryLocked(
	s *shard[K, V], key K, value V, weight int64, sc setConfig,
) {
	now := c.cfg.clock.Now().UnixNano()
	var expireAt int64
	if !sc.expireAt.IsZero() {
		expireAt = sc.expireAt.UnixNano()
	}
	s.supersedeFlight(key)
	if c.sealed.Load() {
		return
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
		// SetExpireAt always implies absolute, never sliding; also clear
		// any negative-cache tombstone (see upsertLocked).
		existing.flags.clear(flagSliding | flagNegative)
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
	return c.deleteInto(key, nil)
}

// deleteInto is Delete that adds the hooks to after, as setCtx does.
func (c *Cache[K, V]) deleteInto(key K, after *[]func()) bool {
	if c.async != nil {
		return c.asyncDelete(key)
	}
	removed, _ := c.deleteKey(context.Background(), key, EvictReasonDeleted, true, after) //nolint:errcheck // Store errors are logged; DeleteCtx returns them
	return removed
}

// deleteWithReason removes key from memory only.
func (c *Cache[K, V]) deleteWithReason(key K, reason EvictionReason) bool {
	removed, _ := c.deleteKey(context.Background(), key, reason, false, nil) //nolint:errcheck // no Store call, no error
	return removed
}

// deleteKey removes key under its lock: it applies any queued async op
// for key, then (with throughStore) deletes from the Store, then from
// memory. Store first: a Get between the two deletes still finds the
// in-memory entry rather than reading the Store's copy back in. Hooks
// fire after the lock is released, so a hook may write the same key;
// when after is not nil they are added to it instead.
func (c *Cache[K, V]) deleteKey(
	ctx context.Context, key K, reason EvictionReason, throughStore bool, after *[]func(),
) (removed bool, err error) {
	if c.closed.Load() {
		return false, nil
	}
	var cbs []func()
	s := c.shardFor(key)
	defer func() { c.settle(after, cbs, s) }()
	defer c.lockKey(key)()
	s.mu.Lock()
	op, queued := c.applyPendingKeyLocked(s, key)
	if !throughStore || c.store == nil {
		removed = c.removeKeyLocked(s, key, reason)
		c.releaseShardInto(s, &cbs)
		if queued {
			c.asyncStoreEffect(op)
		}
		return removed, nil
	}
	c.releaseShardInto(s, &cbs)
	if queued {
		c.asyncStoreEffect(op)
	}
	err = c.deleteThroughStore(ctx, key)
	s.mu.Lock()
	removed = c.removeKeyLocked(s, key, reason)
	c.releaseShardInto(s, &cbs)
	return removed, err
}

// removeKeyLocked removes key from memory under reason. Single-key
// deletes count in [Stats.Deletes]; bulk ones (DeleteWhere,
// DeletePrefix) don't. Caller MUST hold s.mu (write).
func (c *Cache[K, V]) removeKeyLocked(s *shard[K, V], key K, reason EvictionReason) bool {
	s.supersedeFlight(key)
	e, ok := s.storage.get(key)
	if !ok {
		return false
	}
	c.removeLocked(s, e, reason)
	if reason != EvictReasonDeletedWhere && reason != EvictReasonDeletedPrefix {
		c.counters.deletes.Add(1)
	}
	return true
}

// Reset removes all entries without firing eviction callbacks.
func (c *Cache[K, V]) Reset() {
	c.applyAllPending()
	if c.admission != nil {
		c.admission.Reset()
	}
	for _, s := range c.shards {
		s.mu.Lock()
		c.untagShardLocked(s)
		s.storage.each(func(e *entry[K, V]) bool {
			c.counters.entries.Add(-1)
			c.counters.bytes.Add(-e.weight)
			s.policy.OnRemove(e)
			c.recycleOrInvalidate(s, e)
			return true
		})
		s.storage.clearAll()
		s.supersedeAllFlights()
		s.policy.Reset()
		s.ttl.Reset()
		if c.cfg.lockFreeRead {
			s.read.Store(newEmptyReadMap[K, V]())
			s.readMisses.Store(0)
		}
		c.flushAndUnlock(s)
	}
}

func (c *Cache[K, V]) recycleOrInvalidate(s *shard[K, V], e *entry[K, V]) {
	if c.cfg.lockFreeRead {
		e.markInvalidated()
		return
	}
	s.pool.put(e)
}

// Close drains pending async writes, writes a final [WithAutoSave]
// snapshot, stops background goroutines, runs the [WithPurgeVisitor]
// visitor over the remaining entries, empties the cache, and closes
// every [Cache.Subscribe] channel. Close is idempotent and always
// returns nil; a final auto-save error is logged. After Close, writes
// return [ErrClosed] and reads report a miss.
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
	c.sealed.Store(true)
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
	unpublishExpvar(c)
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

// setLockedInto validates and writes key to memory on shard s, adding
// the callbacks the write defers to cbs. Caller runs them with
// [Cache.runCallbacks].
func (c *Cache[K, V]) setLockedInto(
	s *shard[K, V], key K, value V, ttl time.Duration, sliding bool, tags []string, cbs *[]func(),
) error {
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
	s.mu.Lock()
	c.upsertLocked(s, key, value, weight, c.effectiveTTL(ttl), sliding, int64(ttl), tags)
	c.releaseShardInto(s, cbs)
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
	s.supersedeFlight(key)
	if c.sealed.Load() {
		return
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
		// Clear flagNegative when transitioning a tombstone to a real
		// value, otherwise the Set would be silently dropped.
		existing.flags.clear(flagNegative)
		if sliding {
			existing.flags.set(flagSliding)
			existing.slidingTTL = rawTTL
		} else {
			existing.flags.clear(flagSliding)
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
		if c.events.active() {
			c.publishEvent(Event[K, V]{
				Kind:  EventUpdate,
				Key:   key,
				Value: value,
				At:    c.cfg.clock.Now(),
				Tags:  tags,
			})
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
	if sliding {
		e.flags.set(flagSliding)
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
	if c.events.active() {
		c.publishEvent(Event[K, V]{
			Kind:  EventInsert,
			Key:   key,
			Value: value,
			At:    c.cfg.clock.Now(),
			Tags:  tags,
		})
	}
}

func (c *Cache[K, V]) weighOf(v V) int64 {
	var w int64 = 1
	if c.weigher != nil {
		w = c.weigher(v)
	}
	return clampWeight(w)
}

// evictWhileOverBudgetLocked evicts policy-chosen victims until the shard
// is within its entry budget and the cache within its entry and byte
// bounds (or s is down to one entry; [Cache.shedOverBudget] covers the
// rest). Caller MUST hold s.mu.
func (c *Cache[K, V]) evictWhileOverBudgetLocked(s *shard[K, V]) {
	c.evictOverBudgetLocked(s, EvictReasonCapacity, 1)
}

// overBudget reports whether the cache holds more entries than
// [WithMaxEntries] or weighs more than [WithMaxBytes] allows.
func (c *Cache[K, V]) overBudget() bool {
	if me := c.maxEntries.Load(); me > 0 && c.counters.entries.Load() > me {
		return true
	}
	mb := c.maxBytes.Load()
	return mb > 0 && c.counters.bytes.Load() > mb
}

// evictOverBudgetLocked evicts victims from s under reason while s holds
// more than its entry budget, or while the cache is over its entry or
// byte bound and s holds more than keep entries; it returns how many it
// evicted. After a Set, keep is 1 so the bounds are met from older
// entries first ([Cache.shedOverBudget] takes the rest). Caller MUST
// hold s.mu.
func (c *Cache[K, V]) evictOverBudgetLocked(s *shard[K, V], reason EvictionReason, keep int) int {
	count := 0
	for {
		n := s.storage.length()
		overShard := s.budget > 0 && n > s.budget
		overTotal := n > keep && c.overBudget()
		var victim *entry[K, V]
		if overShard {
			victim = s.policy.Victim()
		}
		if victim == nil && overTotal {
			victim = shedVictim(s.policy, keep > 0)
		}
		if victim == nil {
			return count
		}
		c.removeLocked(s, victim, reason)
		c.counters.evictions.Add(1)
		count++
	}
}

// shedOverBudget evicts across shards, one shard lock at a time, until
// the cache is within its entry and byte bounds, and returns how many it
// evicted. It covers what a single shard can't: an update that grew an
// entry, or an insert into a shard holding too little to cover the
// overage. Shard last, if non-nil, is visited after the others, so the
// shard just written to gives up its newest entry only when nothing else
// is left.
//
// One goroutine sheds at a time. A caller that finds another shedding
// leaves the work to it through shedRetry, and the shedder sweeps again
// while the cache is over its bounds, so a write that lands during a
// sweep can't leave the cache over them.
func (c *Cache[K, V]) shedOverBudget(reason EvictionReason, last *shard[K, V]) int {
	count := 0
	for c.overBudget() {
		if !c.shedding.CompareAndSwap(false, true) {
			c.shedRetry.Store(true)
			if !c.shedding.CompareAndSwap(false, true) {
				// The shedder releases after this store, so sees it.
				return count
			}
		}
		c.shedRetry.Store(false)
		n := c.shedPass(reason, last)
		c.shedding.Store(false)
		count += n
		if n == 0 && !c.shedRetry.Load() {
			break
		}
	}
	return count
}

// shedPass is one sweep of shedOverBudget. Caller MUST hold c.shedding.
func (c *Cache[K, V]) shedPass(reason EvictionReason, last *shard[K, V]) int {
	count := 0
	shed := func(s *shard[K, V]) bool {
		if !c.overBudget() {
			return false
		}
		s.mu.Lock()
		count += c.evictOverBudgetLocked(s, reason, 0)
		c.releaseShard(s)
		return true
	}
	start := c.shedNext.Add(1)
	for i := range uint64(len(c.shards)) {
		s := c.shards[(start+i)&c.shardMask]
		if s != last && !shed(s) {
			return count
		}
	}
	if last != nil {
		shed(last)
	}
	return count
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
	// Negative tombstones are an internal artifact of the loader path;
	// they fire no callbacks or events. Nor does anything when nothing
	// would receive it.
	notify := !e.flags.has(flagNegative) && (c.onExpire != nil || c.onEvict != nil ||
		c.invalidationPublisher != nil || c.events.active())
	var tagsCopy []string
	if notify && len(e.tags) > 0 {
		tagsCopy = append([]string(nil), e.tags...)
	}

	if supersedesFlight(reason) {
		s.supersedeFlight(key)
	}
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

	if !notify {
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
		// A remote removal came from a peer; publishing it again
		// would echo it back.
		if reason != EvictReasonRemote {
			c.publishInvalidation(key, reason)
		}
	}
}

// flushAndUnlock releases the shard lock, fires any callbacks deferred
// by removeLocked on this shard, and then brings the cache back within
// its entry and byte bounds. Caller MUST hold the shard lock.
func (c *Cache[K, V]) flushAndUnlock(s *shard[K, V]) {
	c.releaseShard(s)
	c.shedOverBudget(EvictReasonCapacity, s)
}

// releaseShard releases the shard lock and fires any callbacks deferred
// by removeLocked on this shard. Caller MUST hold the shard lock.
func (c *Cache[K, V]) releaseShard(s *shard[K, V]) {
	cbs := s.pendingCallbacks
	s.pendingCallbacks = nil
	s.mu.Unlock()
	for _, cb := range cbs {
		cb()
	}
}

// releaseShardInto releases the shard lock and moves the callbacks it
// deferred into *cbs instead of firing them, so a caller holding a key
// lock can fire them once that is released ([Cache.runCallbacks]).
// Caller MUST hold the shard lock.
func (c *Cache[K, V]) releaseShardInto(s *shard[K, V], cbs *[]func()) {
	*cbs = append(*cbs, s.pendingCallbacks...)
	s.pendingCallbacks = nil
	s.mu.Unlock()
}

// runCallbacks fires callbacks collected by releaseShardInto and then
// brings the cache back within its bounds, as flushAndUnlock does.
func (c *Cache[K, V]) runCallbacks(cbs []func(), s *shard[K, V]) {
	for _, cb := range cbs {
		cb()
	}
	c.shedOverBudget(EvictReasonCapacity, s)
}

func (c *Cache[K, V]) unlockShard(s *shard[K, V]) {
	c.flushAndUnlock(s)
}

// Range calls fn for every entry in the cache. Each shard's live
// entries are copied under its read lock and fn is called after the lock
// is released, so fn may call into the cache, including to set or delete
// the key it is given. Expired and negative-cache entries are skipped.
// The visit set is NOT a consistent snapshot: fn may see a value that a
// concurrent write has since replaced or removed. fn returning false
// stops iteration immediately.
func (c *Cache[K, V]) Range(fn func(key K, value V) bool) {
	if c.closed.Load() || fn == nil {
		return
	}
	now := c.cfg.clock.Now().UnixNano()
	var buf []rangePair[K, V]
	for _, s := range c.shards {
		buf = c.copyShard(s, now, buf[:0])
		for i := range buf {
			if !fn(buf[i].key, c.returnValue(buf[i].value)) {
				return
			}
		}
	}
}

// rangePair is one entry copied out of a shard by [Cache.Range].
type rangePair[K comparable, V any] struct {
	key   K
	value V
}

// copyShard appends s's live entries to buf under s's read lock.
func (c *Cache[K, V]) copyShard(s *shard[K, V], now int64, buf []rangePair[K, V]) []rangePair[K, V] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.storage.each(func(e *entry[K, V]) bool {
		if !c.entryExpiredLocked(e, now) && !e.flags.has(flagNegative) {
			buf = append(buf, rangePair[K, V]{key: e.key, value: e.loadValue()})
		}
		return true
	})
	return buf
}

// Keys returns a freshly-allocated snapshot of every key in the cache.
func (c *Cache[K, V]) Keys() []K {
	if c.closed.Load() {
		return nil
	}
	keys := make([]K, 0, c.Len())
	now := c.cfg.clock.Now().UnixNano()
	for _, s := range c.shards {
		s.mu.RLock()
		s.storage.each(func(e *entry[K, V]) bool {
			if !c.entryExpiredLocked(e, now) && !e.flags.has(flagNegative) {
				keys = append(keys, e.key)
			}
			return true
		})
		s.mu.RUnlock()
	}
	return keys
}

// Clear removes every entry, recording each removal under
// [EvictReasonClear]. When [WithPurgeVisitor] is configured, the visitor
// is invoked for every entry before its slot is recycled.
func (c *Cache[K, V]) Clear() {
	if c.closed.Load() {
		return
	}
	c.applyAllPending()
	c.runPurgeVisitor()
	for _, s := range c.shards {
		s.mu.Lock()
		c.untagShardLocked(s)
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
		s.supersedeAllFlights()
		s.policy.Reset()
		s.ttl.Reset()
		if c.cfg.lockFreeRead {
			s.read.Store(newEmptyReadMap[K, V]())
			s.readMisses.Store(0)
		}
		c.flushAndUnlock(s)
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
	c.applyPendingKey(s, key)
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
	c.applyPendingKey(s, key)
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
	c.applyPendingKey(s, key)
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
	c.applyPendingKey(s, key)
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
		expireAt = now + int64(c.effectiveTTL(ttl))
	}
	e.expireAt.Store(expireAt)
	e.lastAccess.Store(now)
	s.expiryFix(e)
	if expireAt > 0 {
		c.startJanitorLocked(s)
	}
}

// SetIfAbsent stores value under key only when the key is absent (or
// expired). Returns stored=true when the value was inserted. With
// [WithStore], a key the Store holds is present.
func (c *Cache[K, V]) SetIfAbsent(key K, value V) (stored bool, err error) {
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
	tags := c.autoTags(value)
	if err := c.checkTagLimits(key, tags); err != nil {
		return false, err
	}
	s := c.shardFor(key)
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	var sm storeMirror[K, V]
	defer func() {
		if merr := c.mirrorToStore(key, &sm, &cbs); merr != nil && err == nil {
			err = merr
			stored = false
		}
	}()
	s.mu.Lock()
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	defer c.releaseShardInto(s, &cbs)
	if _, _, present, err := c.currentLocked(s, key, &sm, &cbs); err != nil || present {
		return false, err
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), tags)
	c.noteStoreSetLocked(&sm, s, key, value)
	return true, nil
}

// SetIfPresent updates the value under key only when an entry already
// exists (and is fresh). Returns updated=true when the entry was
// replaced. With [WithStore], a key the Store holds is present.
func (c *Cache[K, V]) SetIfPresent(key K, value V) (updated bool, err error) {
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
	tags := c.autoTags(value)
	if err := c.checkTagLimits(key, tags); err != nil {
		return false, err
	}
	s := c.shardFor(key)
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	var sm storeMirror[K, V]
	defer func() {
		if merr := c.mirrorToStore(key, &sm, &cbs); merr != nil && err == nil {
			err = merr
			updated = false
		}
	}()
	s.mu.Lock()
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	defer c.releaseShardInto(s, &cbs)
	if _, _, present, err := c.currentLocked(s, key, &sm, &cbs); err != nil || !present {
		return false, err
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), tags)
	c.noteStoreSetLocked(&sm, s, key, value)
	return true, nil
}

// DeleteIf removes the entry for key only when pred returns true for the
// current value. pred is invoked under the shard write lock and MUST NOT
// call back into the cache for the same key. With [WithStore], a key
// missing from memory is checked against the Store's value, and a
// removal is deleted from the Store first, as [Cache.Delete] does.
func (c *Cache[K, V]) DeleteIf(key K, pred func(V) bool) bool {
	if c.closed.Load() || pred == nil {
		return false
	}
	s := c.shardFor(key)
	if c.store != nil {
		return c.deleteIfThroughStore(s, key, pred)
	}
	now := c.cfg.clock.Now().UnixNano()
	c.applyPendingKey(s, key)
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

// deleteIfThroughStore is DeleteIf for a cache with a Store.
func (c *Cache[K, V]) deleteIfThroughStore(s *shard[K, V], key K, pred func(V) bool) bool {
	var cbs []func()
	defer func() { c.runCallbacks(cbs, s) }()
	defer c.lockKey(key)()
	var sm storeMirror[K, V]
	if !c.deleteIfMatches(s, key, pred, &sm, &cbs) {
		return false
	}
	_ = c.deleteThroughStore(context.Background(), key) //nolint:errcheck // logged; the in-memory delete completes regardless, as in Delete
	s.mu.Lock()
	if !c.removeKeyLocked(s, key, EvictReasonDeleted) {
		c.counters.deletes.Add(1)
	}
	c.releaseShardInto(s, &cbs)
	return true
}

// deleteIfMatches reports whether pred matches key's current value,
// read as currentLocked reads it. It takes and releases s.mu, adding the
// callbacks to cbs, and then writes any async op it applied to the
// Store. Caller MUST hold key's lock.
func (c *Cache[K, V]) deleteIfMatches(
	s *shard[K, V], key K, pred func(V) bool, sm *storeMirror[K, V], cbs *[]func(),
) bool {
	s.mu.Lock()
	defer func() {
		if sm.hasPending {
			c.asyncStoreEffect(sm.pending)
		}
	}()
	defer c.releaseShardInto(s, cbs)
	sm.pending, sm.hasPending = c.applyPendingKeyLocked(s, key)
	_, current, present, err := c.currentLocked(s, key, sm, cbs)
	return err == nil && present && pred(c.returnValue(current))
}

// GetOrSet returns the cached value for key when present and fresh, or
// stores value and returns it otherwise. The bool is true when the
// returned value came from the cache. Atomic with respect to concurrent
// Set on the same key. Does not invoke any configured [Loader]; for that
// see [Cache.GetOrLoad]. With [WithStore], a key missing from memory is
// read from the Store, and a Store hit is returned (and promoted)
// rather than overwritten.
func (c *Cache[K, V]) GetOrSet(key K, value V) (_ V, _ bool, err error) {
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
	tags := c.autoTags(value)
	if err := c.checkTagLimits(key, tags); err != nil {
		return zero, false, err
	}
	s := c.shardFor(key)
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
	existing, current, present, err := c.currentLocked(s, key, &sm, &cbs)
	if err != nil {
		return zero, false, err
	}
	if present {
		if existing != nil {
			existing.hits.Add(1)
			if existing.touchAccess(c.cfg.clock.Now().UnixNano()) {
				s.expiryFix(existing)
			}
			s.policy.OnAccess(existing)
		}
		c.recordHit()
		return c.returnValue(current), true, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), tags)
	c.noteStoreSetLocked(&sm, s, key, value)
	return value, false, nil
}

// PeekOrAdd is [Cache.GetOrSet] that does not promote the existing entry
// in the eviction policy. New inserts follow normal policy placement.
func (c *Cache[K, V]) PeekOrAdd(key K, value V) (_ V, _ bool, err error) {
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
	tags := c.autoTags(value)
	if err := c.checkTagLimits(key, tags); err != nil {
		return zero, false, err
	}
	s := c.shardFor(key)
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
		// Peek semantics: do NOT call OnAccess and do not bump hits.
		return c.returnValue(current), true, nil
	}
	c.upsertLocked(s, key, value, weight,
		c.effectiveTTL(c.cfg.defaultTTL),
		c.cfg.slidingTTL, int64(c.cfg.defaultTTL), tags)
	c.noteStoreSetLocked(&sm, s, key, value)
	return value, false, nil
}

// Resize changes the cache's bound at runtime. newSize is interpreted as
// MaxEntries unless the cache was built with [WithMaxBytes], in which
// case it is the byte budget. Returns the number of entries evicted from
// shrinking; growing the cache or passing newSize <= 0 returns 0.
// Concurrent Resize calls run one at a time; the last one's bound wins.
func (c *Cache[K, V]) Resize(newSize int64) int {
	if c.closed.Load() || newSize <= 0 {
		return 0
	}
	c.resizeMu.Lock()
	defer c.resizeMu.Unlock()
	c.counters.resizes.Add(1)

	// Bytes-bounded only if maxBytes was explicitly configured.
	if c.cfg.maxBytes > 0 {
		c.maxBytes.Store(newSize)
	} else {
		c.maxEntries.Store(newSize)
	}

	perShard := perShardBudget(int(c.maxEntries.Load()), len(c.shards))
	evicted := 0
	for _, s := range c.shards {
		s.mu.Lock()
		s.budget = perShard
		s.policy.SetBudget(perShard)
		// Shard budgets only; shedOverBudget spreads the rest below.
		evicted += c.evictOverBudgetLocked(s, EvictReasonResize, math.MaxInt)
		c.releaseShard(s)
	}
	evicted += c.shedOverBudget(EvictReasonResize, nil)
	c.publishEvent(Event[K, V]{
		Kind: EventResize,
		At:   c.cfg.clock.Now(),
	})
	return evicted
}

// Sync drains pending background work (async tag cleanup, async writes)
// and returns when the cache is quiescent. Returns ctx.Err() if ctx
// ends first, or [ErrClosed] on a closed cache. Useful before
// [Cache.Save] so the snapshot reflects every Set/Delete already
// returned. Called from a hook that applying a [WithAsyncWrites] write
// fired, Sync returns once every other write is applied: it can't wait
// for the writes whose hooks are still running.
func (c *Cache[K, V]) Sync(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // pass through ctx.Err verbatim
	}
	// From a hook of an applied op, Sync can't wait for that op, nor
	// for ops whose hooks wait in Sync too: none of them completes
	// until the waits end. On the apply goroutine, nothing else applies
	// queued ops either, so Sync applies them.
	fromHook, onApply := false, false
	if c.async != nil {
		gid := goroutineID()
		var release func()
		if release, fromHook = c.async.waitFromHook(gid); fromHook {
			defer release()
		}
		onApply = gid == c.async.applyGID.Load()
	}
	settled := func() bool {
		backlog := c.asyncBacklog()
		return backlog == 0 || (fromHook && backlog <= c.async.heldByWaiters())
	}
	const pollInterval = 100 * time.Microsecond
	for {
		if onApply {
			c.drainAllShards()
		}
		if c.tagCleanupBacklog() == 0 && settled() {
			return nil
		}
		// Nudge the apply goroutine so a long-idle drain happens promptly.
		if c.asyncBacklog() > 0 {
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

// DeletePrefix removes every entry whose key begins with prefix and
// returns the number removed. K must be string or implement [Prefixer];
// for other key types DeletePrefix is a no-op that returns 0.
//
// DeletePrefix scans every entry. [Cache.InvalidateTag] is cheaper when
// the entries share a tag. With [WithStore], each removed key is deleted
// from the Store first, as [Cache.Delete] does; keys only the Store
// holds are not visited.
func (c *Cache[K, V]) DeletePrefix(prefix string) int {
	if c.closed.Load() {
		return 0
	}
	c.applyAllPending()
	matcher := prefixMatcher[K](prefix)
	if matcher == nil {
		return 0
	}
	count := 0
	for _, s := range c.shards {
		n, keys := c.deletePrefixShard(s, matcher)
		count += n
		if len(keys) > 0 {
			count += c.deleteKeysThroughStore(keys, EvictReasonDeletedPrefix)
		}
	}
	return count
}

// deletePrefixShard removes s's entries whose keys match and returns how
// many it removed. With a Store it removes none and returns the matching
// keys instead, for [Cache.deleteKeysThroughStore].
func (c *Cache[K, V]) deletePrefixShard(s *shard[K, V], matcher func(K) bool) (int, []K) {
	s.mu.Lock()
	defer c.flushAndUnlock(s)
	for _, flights := range []map[K]*flightCall[V]{s.inflight, s.orphans} {
		for k, f := range flights {
			if matcher(k) {
				f.superseded = true
			}
		}
	}
	var doomed []*entry[K, V]
	s.storage.each(func(e *entry[K, V]) bool {
		if matcher(e.key) {
			doomed = append(doomed, e)
		}
		return true
	})
	if c.store != nil {
		keys := make([]K, len(doomed))
		for i, e := range doomed {
			keys[i] = e.key
		}
		return 0, keys
	}
	for _, e := range doomed {
		c.removeLocked(s, e, EvictReasonDeletedPrefix)
	}
	return len(doomed), nil
}

// deleteKeysThroughStore deletes each key from the Store and then from
// memory, as [Cache.Delete] does, and returns how many were removed from
// memory. Store errors are logged.
func (c *Cache[K, V]) deleteKeysThroughStore(keys []K, reason EvictionReason) int {
	n := 0
	for _, k := range keys {
		//nolint:errcheck // logged; the in-memory delete completes regardless, as in Delete
		if removed, _ := c.deleteKey(context.Background(), k, reason, true, nil); removed {
			n++
		}
	}
	return n
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
// value satisfied pred (eventual semantics). With [WithStore], each
// removed key is deleted from the Store first, as [Cache.Delete] does;
// keys only the Store holds are not visited.
func (c *Cache[K, V]) DeleteWhere(pred func(key K, value V) bool) int {
	if c.closed.Load() || pred == nil {
		return 0
	}
	c.applyAllPending()
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
		if c.store != nil {
			count += c.deleteKeysThroughStore(doomed, EvictReasonDeletedWhere)
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
// Returns [ErrNoLoader], [ErrClosed], ctx.Err(), or a [*LoadError]
// wrapping the Loader's error; match the underlying error with
// [errors.Is]. A Loader's [ErrNotFound] with [WithNegativeCache] active
// records a tombstone; later calls while it lives return [ErrNotFound]
// itself. The load runs on a context detached from ctx: a caller whose
// ctx ends gets ctx.Err(), and the load is canceled only once every
// waiting caller has given up; its result is still stored unless the
// key is written, deleted or invalidated first. A load that finishes
// after such a change returns its value to its callers but leaves the
// cache alone. With [WithRefreshAhead] and [WithStaleWhileRevalidate],
// GetOrLoad refreshes and serves stale entries as [Cache.Get] does. A
// panicking Loader returns a [*LoadError] wrapping [ErrLoaderPanic].
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
// cached value when the load completes. It does nothing when a load for
// key is already in flight. Returns [ErrNoLoader], [ErrClosed], or
// ctx.Err() if ctx has already ended.
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
) (_ V, err error) {
	var zero V
	ctx, span := c.tracer.Start(ctx, "memcache.load")
	defer func() { span.End(err) }()
	s := c.shardFor(key)
	c.applyPendingKey(s, key)
	s.mu.Lock()
	now := c.cfg.clock.Now().UnixNano()

	if v, ok := c.serveLoadHitLocked(s, key, now, fn); ok {
		c.flushAndUnlock(s)
		c.recordHit()
		return c.returnValue(v), nil
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
	s.startFlight(key, flight)
	c.flushAndUnlock(s)

	go c.runLoader(loaderCtx, s, key, flight, fn) //nolint:contextcheck // detached by design
	return c.waitOrLeave(ctx, s, key, flight)
}

// serveLoadHitLocked serves a [Cache.GetOrLoad] hit: a fresh,
// non-negative entry, or a stale one that stale-while-revalidate may
// still serve. Either may start a background refresh through fn, as
// Get does. Caller MUST hold s.mu (write).
func (c *Cache[K, V]) serveLoadHitLocked(
	s *shard[K, V], key K, now int64,
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) (V, bool) {
	e, ok := s.storage.get(key)
	if !ok || e.flags.has(flagNegative) {
		var zero V
		return zero, false
	}
	fresh := !c.entryExpiredLocked(e, now)
	switch {
	case fresh:
		if e.touchAccess(now) {
			s.expiryFix(e)
		}
		if c.shouldRefreshAhead(e, now) {
			c.counters.refreshAhead.Add(1)
			c.startRefreshLocked(s, key, fn)
		}
	case c.shouldServeStale(e, now):
		c.counters.staleWhileRevalidate.Add(1)
		c.startRefreshLocked(s, key, fn)
	default:
		var zero V
		return zero, false
	}
	e.hits.Add(1)
	s.policy.OnAccess(e)
	return e.loadValue(), true
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
		return c.returnValue(flight.val), nil
	case <-ctx.Done():
		s.mu.Lock()
		if flight.refs.Add(-1) == 0 {
			s.orphanFlight(key, flight)
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
			s.leaveInflight(key, flight)
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
	val, ttl, err := c.callLoader(loaderCtx, key, fn)
	c.counters.loadLatency.Record(c.cfg.clock.Now().Sub(loadStart))
	err = c.tagLoaderTimeout(err)
	flight.val = val
	flight.ttl = ttl
	if err != nil {
		flight.err = &LoadError{Key: key, Err: err}
	}

	var lv loadedValue
	if err == nil {
		lv = c.prepareLoaded(key, val)
	}
	s.mu.Lock()
	s.leaveInflight(key, flight)
	if c.cfg.statsEnabled {
		c.counters.loadsTotal.Add(1)
	}

	if c.cfg.statsEnabled {
		if err == nil {
			c.counters.loadHits.Add(1)
		} else {
			c.counters.loadErrors.Add(1)
		}
	}
	c.applyLoadResultLocked(s, key, flight, val, ttl, err, lv)
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

// applyLoadResultLocked records a finished load in the cache: the value,
// a negative-cache tombstone, or a cached error. A load overtaken by a
// write, delete or invalidation of the key (or by Close) hands its
// result to its waiters but stores nothing. Caller MUST hold s.mu.
func (c *Cache[K, V]) applyLoadResultLocked(
	s *shard[K, V], key K, flight *flightCall[V], val V, ttl time.Duration, err error, lv loadedValue,
) {
	switch {
	case flight.superseded || c.closed.Load():
	case err == nil:
		c.storeLoadedLocked(s, key, val, ttl, lv)
	case errors.Is(err, ErrNotFound):
		if c.cfg.negativeTTL > 0 {
			c.insertNegativeTombstoneLocked(s, key)
		}
	case c.cfg.errorTTL > 0 && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded):
		// WithErrorTTL skips ErrNotFound (handled by WithNegativeCache)
		// and ctx errors (which would leak one caller's cancel to
		// unrelated callers within errorTTL).
		c.cacheErrorLocked(s, key, flight.err)
	}
}

// errorCacheMinPerShard is the least number of [WithErrorTTL] errors a
// shard keeps, however small its entry budget.
const errorCacheMinPerShard = 64

// cacheErrorLocked records err for key under [WithErrorTTL]. A shard
// keeps at most max(budget, errorCacheMinPerShard) errors: when full it
// drops expired errors, then arbitrary ones. Caller MUST hold s.mu.
func (c *Cache[K, V]) cacheErrorLocked(s *shard[K, V], key K, err error) {
	now := c.cfg.clock.Now().UnixNano()
	limit := max(s.budget, errorCacheMinPerShard)
	if _, ok := s.errors[key]; !ok && len(s.errors) >= limit {
		for k, ce := range s.errors {
			if ce.expireAt <= now {
				delete(s.errors, k)
			}
		}
		for k := range s.errors {
			if len(s.errors) < limit {
				break
			}
			delete(s.errors, k)
		}
	}
	s.errors[key] = &cachedError{err: err, expireAt: now + int64(c.cfg.errorTTL)}
}

// callLoader runs fn and turns a panic into an error wrapping
// [ErrLoaderPanic], so the flight completes and every waiter gets the
// error instead of the process crashing. The stack is logged.
func (c *Cache[K, V]) callLoader(
	ctx context.Context, key K,
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) (val V, ttl time.Duration, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero V
			val, ttl = zero, 0
			err = fmt.Errorf("%w: %v", ErrLoaderPanic, r)
			if c.cfg.logger != nil {
				c.cfg.logger.Error("memcache: loader panicked",
					"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			}
		}
	}()
	return fn(ctx, key)
}

// loadedValue is a loaded value's weight and tags, worked out before
// the shard lock is taken since both may run user code. ok is false when
// the value is too large to cache.
type loadedValue struct {
	weight int64
	tags   []string
	ok     bool
}

// prepareLoaded weighs and tags a loaded value for storeLoadedLocked.
func (c *Cache[K, V]) prepareLoaded(key K, val V) loadedValue {
	weight, err := c.computeWeight(key, val)
	if err != nil {
		// Value too big to cache; the loader's val is still returned.
		return loadedValue{}
	}
	return loadedValue{weight: weight, tags: c.autoTags(val), ok: true}
}

// storeLoadedLocked applies the Loader's result, prepared by
// prepareLoaded. ttl=0 inherits [WithDefaultTTL]. Caller MUST hold s.mu.
func (c *Cache[K, V]) storeLoadedLocked(s *shard[K, V], key K, val V, ttl time.Duration, lv loadedValue) {
	if !lv.ok {
		return
	}
	if ttl == 0 {
		ttl = c.cfg.defaultTTL
	}
	// A refresh keeps the tags the entry already carries, unless the
	// value derives its own.
	tags := lv.tags
	if e, ok := s.storage.get(key); ok && len(tags) == 0 && len(e.tags) > 0 {
		tags = append([]string(nil), e.tags...)
	}
	c.upsertLocked(s, key, val, lv.weight,
		c.effectiveTTL(ttl),
		c.cfg.slidingTTL, int64(ttl), tags)
}

// insertNegativeTombstoneLocked records a negative-cache entry for key.
// Caller MUST hold s.mu.
func (c *Cache[K, V]) insertNegativeTombstoneLocked(s *shard[K, V], key K) {
	if c.sealed.Load() {
		return
	}
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
		existing.flags.store(existing.flags.load()&^flagSliding | flagNegative)
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
	e.flags.set(flagNegative)
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
	c.startRefreshLocked(s, key, c.loader.Load)
}

// startRefreshLocked starts a background load of key through fn unless
// one is already in flight. Caller MUST hold s.mu (write).
func (c *Cache[K, V]) startRefreshLocked(
	s *shard[K, V], key K,
	fn func(ctx context.Context, key K) (V, time.Duration, error),
) {
	if _, busy := s.inflight[key]; busy {
		return
	}
	loaderCtx, cancel := c.newLoaderCtx()
	flight := newFlightCall[V](cancel)
	s.startFlight(key, flight)
	go c.runLoader(loaderCtx, s, key, flight, fn)
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
// error from LoadMulti aborts. A bulk-loaded value is cached as a
// [Cache.Set] with the result's TTL would cache it, and so with
// [WithStore] is written to the Store too, unless its key was written,
// deleted or invalidated while LoadMulti ran.
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

	// With a bulk loader, watch every key from before its lookup, so a
	// result that a write, delete or invalidation of its key overtook
	// is not stored.
	var watches map[K]*watchToken
	if c.bulkLoader != nil {
		watches = make(map[K]*watchToken, len(keys))
		for _, k := range keys {
			if _, ok := watches[k]; !ok {
				watches[k] = c.watchKey(k)
			}
		}
		defer func() {
			for k, w := range watches {
				c.unwatchKey(k, w)
			}
		}()
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
	for k := range out {
		if w, ok := watches[k]; ok {
			c.unwatchKey(k, w)
			delete(watches, k)
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
		// The caller sees every loaded value; only the keys it asked
		// for are cached.
		if w, ok := watches[k]; ok && res.TTL >= 0 {
			c.storeBulkLoaded(k, res.Value, res.TTL, w)
		}
		out[k] = c.returnValue(res.Value)
	}
	return out, nil
}

// storeBulkLoaded caches a [BulkLoader] result, unless w says the key
// changed since its lookup began. ttl=0 inherits [WithDefaultTTL]. With
// a Store, it is written through as [Cache.SetWithOptions] writes, with
// w checked under the key's lock.
func (c *Cache[K, V]) storeBulkLoaded(key K, value V, ttl time.Duration, w *watchToken) {
	if c.store != nil {
		opts := []SetOption{setGuard(func() bool { return !w.superseded && !c.closed.Load() })}
		if ttl > 0 {
			opts = append(opts, SetTTL(ttl))
		}
		err := c.SetWithOptions(key, value, opts...)
		if err != nil && !errors.Is(err, errWriteSuperseded) && !errors.Is(err, ErrClosed) && c.cfg.logger != nil {
			c.cfg.logger.Warn("memcache: caching a bulk-loaded value failed", "err", err)
		}
		return
	}
	lv := c.prepareLoaded(key, value)
	s := c.shardFor(key)
	s.mu.Lock()
	defer c.flushAndUnlock(s)
	if !w.superseded && !c.closed.Load() {
		c.storeLoadedLocked(s, key, value, ttl, lv)
	}
}
