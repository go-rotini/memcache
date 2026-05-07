package memcache

import (
	"log/slog"
	"maps"
	"runtime"
	"time"
)

// Option configures a [Cache] at construction time. Options are applied
// in order; later options override earlier ones for the same setting.
type Option func(*config)

// config is the internal, mutable representation of cache options.
// Generic fields (loader, weigher, hasher) are stored as any and
// type-asserted at construction time so that Option can stay
// non-generic while still composing with WithLoader[K, V] et al.
type config struct {
	// Capacity bounds. At least one must be > 0 for [New]; both are
	// optional for [NewUnbounded].
	maxEntries int
	maxBytes   int64

	// Per-entry limits.
	maxKeySize     int
	maxValueWeight int64

	// Default TTL applied by Set when no per-call TTL is supplied. Zero
	// means "no TTL".
	defaultTTL time.Duration

	// Sliding-TTL default. When true, every Get refreshes the entry's
	// expiry via touchAccess.
	slidingTTL bool

	// TTL jitter range, in nanoseconds. The actual expiry of a TTL'd
	// entry is `ttl + uniform(-jitter, +jitter)`. Default 5% of the
	// applied TTL when ttlJitterExplicit is false; otherwise the
	// caller-supplied absolute window.
	ttlJitter time.Duration

	// ttlJitterExplicit is set by [WithTTLJitter] so the cache can
	// distinguish "user opted out" (explicit 0) from "user didn't
	// configure" (use the 5%-of-TTL default).
	ttlJitterExplicit bool

	// Janitor sweep interval. Zero disables the janitor (lazy expiry
	// only).
	janitorInterval time.Duration

	// negativeTTL is the TTL applied to negative-cache tombstones
	// produced when a Loader returns [ErrNotFound]. Zero (the
	// default) disables negative caching.
	negativeTTL time.Duration

	// errorTTL is the TTL applied to cached Loader errors (other
	// than [ErrNotFound]). Zero disables error caching.
	errorTTL time.Duration

	// loaderTimeout is the per-call deadline applied to Loader
	// invocations when the caller does not supply one. Zero leaves
	// loader contexts deadline-free.
	loaderTimeout time.Duration

	// refreshAheadAt is the fraction of an entry's TTL after which
	// a Get triggers an asynchronous Loader call to refresh the
	// entry. Must be in (0, 1) to be active; zero disables.
	refreshAheadAt float64

	// swrStaleFor is the stale-while-revalidate window: an entry
	// whose expireAt was less than swrStaleFor ago is served stale
	// while a background Loader refreshes it.
	swrStaleFor time.Duration

	// eventsBuffer is the per-subscriber default buffer size used
	// by [Cache.Subscribe] when the caller passes buf <= 0.
	eventsBuffer int

	// maxSnapshotBytes caps Load input. 0 lets snapshot.go's
	// defaultMaxSnapshotBytes apply (256 MiB).
	maxSnapshotBytes int64

	// autoSavePath, when non-empty, enables a per-cache goroutine
	// that periodically writes a snapshot to disk.
	autoSavePath     string
	autoSaveInterval time.Duration

	// autoLoadPath, when non-empty, attempts to populate the
	// cache from a snapshot at construction time. autoLoadIgnore
	// suppresses I/O errors so a missing-or-corrupt snapshot does
	// not block New.
	autoLoadPath   string
	autoLoadIgnore bool

	// snapshotMetadata is persisted alongside the snapshot header for
	// auditing (e.g. embedding the binary's git SHA).
	snapshotMetadata map[string]string

	// tracer is the optional [Tracer] for span emission on
	// cache operations. When nil, the cache uses a no-op tracer.
	tracer Tracer

	// expvarName, when non-empty, registers the cache's Stats
	// snapshot under that name in stdlib `expvar`.
	expvarName string

	// Hook callbacks. Each is type-erased into the config and
	// type-asserted into the typed shape at cache construction.
	onHit    any // func(K, V)
	onMiss   any // func(K)
	onEvict  any // func(K, V, EvictionReason)
	onExpire any // func(K, V)
	onLoad   any // func(K, V, time.Duration, error)

	// Eviction and admission.
	policy Policy

	// Shard count; must be a power of two when set explicitly.
	shards int

	// Identification, observability.
	name         string
	clock        Clock
	codec        Codec
	logger       *slog.Logger
	statsEnabled bool

	// Type-erased fields. Each is type-asserted into its typed shape by
	// the cache constructor.
	weigher    any
	hasher     any
	loader     any
	bulkLoader any
	expireFunc any

	// loaderRatePerSecond bounds the number of Loader invocations
	// per second across the cache. 0 disables rate limiting.
	loaderRatePerSecond int

	// maxConcurrentLoads caps the number of in-flight Loader calls
	// across the cache. 0 disables the cap.
	maxConcurrentLoads int

	// maxTagsPerEntry caps the number of tags an individual entry
	// can carry. 0 disables the per-entry cap.
	maxTagsPerEntry int

	// maxTagsTotal caps the number of distinct tags the
	// cache-level [tagIndex] may carry. 0 disables the cap.
	maxTagsTotal int

	// groups is `name → capacity` for capacity-bounded tag
	// groups. After a Set whose tags include `name`, the cache
	// enforces `member count <= capacity` for that group by
	// evicting the oldest member (by inserted time).
	groups map[string]int

	// safeKeys, when true, runs a construction-time check that
	// logs a warning if K is a pointer-y type that compares by
	// identity rather than contents.
	safeKeys bool

	// callbackTimeout bounds synchronous hook duration before a
	// warning is logged. Zero disables the watchdog.
	callbackTimeout time.Duration

	// purgeVisitor is type-erased into the config and asserted
	// into func(K, V) error at cache construction. Called for
	// every entry during Clear and Close.
	purgeVisitor any

	// copyOnGet is type-erased into the config and asserted into
	// func(V) V at cache construction. Applied to every Get /
	// Peek return value before handoff to the caller.
	copyOnGet any

	// admissionPolicy is type-erased; resolveAdmissionPolicy
	// asserts it into AdmissionPolicy[K] at construction.
	admissionPolicy any

	// doorkeeperEnabled requests a default-sized Doorkeeper when
	// no explicit AdmissionPolicy is supplied.
	doorkeeperEnabled bool

	// invalidationPublisher is a type-erased callback invoked on
	// every eviction; resolveInvalidationPublisher asserts it
	// into func(K, EvictionReason) at construction.
	invalidationPublisher any

	// invalidationSubscriber is a type-erased <-chan K;
	// resolveInvalidationSubscriber asserts it at construction.
	invalidationSubscriber any

	// shardedStats requests per-CPU sharded counters for the
	// hot-path Hits/Misses fields. Reduces contention on >32-core
	// machines at the cost of slightly more memory and a fan-in
	// cost on Stats(). Default off.
	shardedStats bool

	// collisionTracking, when true, asks each shard to maintain a
	// hash→last-key map and bump Stats.HashCollisions on conflicts.
	// Diagnostics-only; not the hot path under normal load.
	collisionTracking bool

	// codecCtorErr is set by options that construct codecs (e.g.
	// WithEncryptedCodec) when their input is invalid. New
	// surfaces it as a *ConfigError before any further validation.
	codecCtorErr error

	// ttlBuckets / ttlBucketsTickPerBucket capture the requested
	// hashed-wheel parameters. When ttlBuckets > 0, [newTTLBackend]
	// selects the wheel-backed TTL backend for every shard; the
	// per-shard min-heap remains the default otherwise.
	ttlBuckets              int
	ttlBucketsTickPerBucket int

	// flatStorage opts each shard into [flatStore] (a flat hash-probed
	// table with linear probing and tombstone-driven compactions)
	// instead of the default map[K]*entry[K, V]. Toggled by
	// [WithFlatStorage].
	flatStorage bool

	// store is a type-erased [Store] supplied by [WithStore]. When
	// non-nil the cache treats it as the source of truth: in-memory
	// state is a write-through cache of the Store, hot-bounded by
	// the configured eviction policy. Resolved to its typed form
	// in [build].
	store any

	// asyncWrites is set by [WithAsyncWrites]. When true the cache
	// decouples Set/Delete from the storage update: callers see an
	// immediate return after a brief enqueue, while a per-cache
	// apply goroutine drains the per-shard pending maps in the
	// background. Reads check pending before storage so the
	// visibility contract (a Set followed by a Get returns the new
	// value) holds.
	asyncWrites bool

	// lockFreeRead is set by [WithLockFreeRead]. When true, each shard
	// publishes an atomic map snapshot consulted without lock; entries
	// ever published into a snapshot remain GC-managed instead of
	// returning to sync.Pool.
	lockFreeRead bool
}

func defaultConfig() *config {
	return &config{
		policy:          PolicyS3FIFO,
		shards:          defaultShardCount(),
		clock:           RealClock{},
		codec:           GobCodec{},
		logger:          slog.Default(),
		statsEnabled:    true,
		ttlJitter:       0,
		callbackTimeout: 100 * time.Millisecond,
		janitorInterval: 30 * time.Second,
	}
}

func defaultShardCount() int {
	n := max(runtime.GOMAXPROCS(0)*4, 1)
	const maxShards = 1024
	if n > maxShards {
		return maxShards
	}
	return nextPowerOfTwo(n)
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// WithMaxEntries sets the maximum number of entries (across all
// shards). Must be > 0.
func WithMaxEntries(n int) Option {
	return func(c *config) { c.maxEntries = n }
}

// WithMaxBytes sets the maximum total weight of entries across the
// cache. Effective only when the cache has a configured Weigher.
func WithMaxBytes(n int64) Option {
	return func(c *config) { c.maxBytes = n }
}

// WithDefaultTTL sets the TTL applied by Set when no per-call TTL is
// supplied. A zero duration means "no TTL by default".
func WithDefaultTTL(d time.Duration) Option {
	return func(c *config) { c.defaultTTL = d }
}

// WithSlidingTTL toggles sliding-TTL behavior on the cache's default TTL.
// When true, every Get refreshes the entry's expiry to now+slidingTTL.
func WithSlidingTTL(b bool) Option {
	return func(c *config) { c.slidingTTL = b }
}

// WithTTLJitter sets the jitter window applied to TTL expirations: actual
// expiry is ttl + uniform(-j, +j). Default is 5% of the resolved TTL; an
// explicit zero disables jitter entirely.
func WithTTLJitter(j time.Duration) Option {
	return func(c *config) {
		c.ttlJitter = j
		c.ttlJitterExplicit = true
	}
}

// WithJanitorInterval sets how often each shard's background expiry
// sweep runs. A non-positive duration disables the janitor entirely;
// expired entries are still removed lazily on Get.
func WithJanitorInterval(d time.Duration) Option {
	return func(c *config) { c.janitorInterval = d }
}

// WithPolicy selects the eviction policy. The default is
// [PolicyS3FIFO].
func WithPolicy(p Policy) Option {
	return func(c *config) { c.policy = p }
}

// WithShards sets the shard count. Must be a power of two; values that
// are not are rounded up at construction time.
func WithShards(n int) Option {
	return func(c *config) {
		if n < 1 {
			n = 1
		}
		c.shards = nextPowerOfTwo(n)
	}
}

// WithMaxKeySize rejects keys whose serialized representation exceeds
// n bytes. Default 0 (no limit).
func WithMaxKeySize(n int) Option {
	return func(c *config) { c.maxKeySize = n }
}

// WithMaxValueWeight rejects values whose Weigher result exceeds n.
// Default 0 (no limit).
func WithMaxValueWeight(n int64) Option {
	return func(c *config) { c.maxValueWeight = n }
}

// WithName attaches a human-readable name to the cache. Used in stats,
// events, and error messages.
func WithName(name string) Option {
	return func(c *config) { c.name = name }
}

// WithClock overrides the clock used for all TTL and timing decisions.
// The default is [RealClock]; tests may pass [FakeClock].
func WithClock(clk Clock) Option {
	return func(c *config) {
		if clk != nil {
			c.clock = clk
		}
	}
}

// WithCodec selects the codec used by [Cache.Save] and [Cache.Load].
// The default is [GobCodec].
func WithCodec(codec Codec) Option {
	return func(c *config) {
		if codec != nil {
			c.codec = codec
		}
	}
}

// WithLogger attaches a [slog.Logger] for internal warnings (snapshot
// corruption, dropped events, slow callbacks). Default
// [slog.Default].
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithStatsEnabled toggles statistics collection. Default on; disabling
// removes a small amount of atomic-add overhead from the hot path.
func WithStatsEnabled(b bool) Option {
	return func(c *config) { c.statsEnabled = b }
}

// WithCollisionTracking enables hash-collision counting in
// [Stats.HashCollisions]. Off by default; turn on only when diagnosing
// hot keys or weak custom hashers, since it adds map lookup+write per
// insert.
func WithCollisionTracking(b bool) Option {
	return func(c *config) { c.collisionTracking = b }
}

// WithTTLBuckets enables a hashed timing wheel as the per-shard TTL
// backend, replacing the default min-heap. The wheel has slots buckets
// with per-tick duration ~janitorInterval/tickPerBucket (one tick per
// janitor interval when tickPerBucket <= 0). Useful for very large
// numbers of TTL'd entries where the heap's O(log n) cost dominates.
func WithTTLBuckets(slots, tickPerBucket int) Option {
	return func(c *config) {
		c.ttlBuckets = slots
		c.ttlBucketsTickPerBucket = tickPerBucket
	}
}

// WithLockFreeRead enables a sync.Map-style read fast path: each shard
// publishes an atomic snapshot consulted without lock. Misses fall
// through to the locked path; entries published into a snapshot are NOT
// returned to the sync.Pool. Gated to S3-FIFO with no [WithExpireFunc].
// EXPERIMENTAL.
func WithLockFreeRead() Option {
	return func(c *config) { c.lockFreeRead = true }
}

// WithAsyncWrites decouples [Cache.Set] / [Cache.Delete] from the storage
// update; ops enqueue into a per-shard pending map drained by a per-cache
// apply goroutine. Visibility: a Set followed by Get returns the new
// value (read paths consult pending before storage). Eviction state,
// hit counters, refresh-ahead, and Store write-through are deferred to
// apply time; Store.Set errors during apply are logged not returned.
// [Cache.Compute] stays synchronous. [Cache.Sync] blocks until pending
// is empty; [Cache.Close] drains pending. EXPERIMENTAL.
func WithAsyncWrites() Option {
	return func(c *config) { c.asyncWrites = true }
}

// WithStore wires a user-supplied [Store] behind the cache as the source
// of truth. Reads check in-memory first then fall through to Store with
// promotion; writes go through to Store and roll the in-memory entry
// back on Store error; deletes go through and the in-memory delete
// completes regardless of Store error. The cache does NOT close the
// Store on [Cache.Close]. Iteration helpers ([Cache.Range],
// [Cache.Keys]) walk only the in-memory portion. A Store whose K/V do
// not match the cache parameters is a [ConfigError] at [New].
func WithStore[K comparable, V any](store Store[K, V]) Option {
	return func(c *config) { c.store = store }
}

// WithFlatStorage opts each shard into a flat hash-probed storage layout
// (linear probing + tombstone-driven compactions) instead of the default
// Go map. [Stats.Compactions] reports total rebuilds across shards.
// EXPERIMENTAL.
func WithFlatStorage() Option {
	return func(c *config) { c.flatStorage = true }
}

// WithShardedStats enables per-CPU sharded counters for Hits/Misses,
// reducing cache-line contention on >32-core machines at the cost of
// slight memory and a fan-in cost on every [Cache.Stats] read. Default off.
func WithShardedStats(b bool) Option {
	return func(c *config) { c.shardedStats = b }
}

// WithWeigher attaches a [Weigher] used together with [WithMaxBytes] for
// byte-bounded caches. A weigher whose type does not match V is rejected
// by [New] as a [*ConfigError].
func WithWeigher[V any](fn Weigher[V]) Option {
	return func(c *config) {
		if fn != nil {
			c.weigher = fn
		}
	}
}

// WithHasher overrides the default key hasher. Useful when the caller
// can supply a faster hasher for their key type than the package's
// default reflect-based fallback.
func WithHasher[K comparable](fn func(K) uint64) Option {
	return func(c *config) {
		if fn != nil {
			c.hasher = fn
		}
	}
}

// WithLoader attaches a [Loader] used by [Cache.GetOrLoad] and
// related methods.
func WithLoader[K comparable, V any](l Loader[K, V]) Option {
	return func(c *config) {
		if l != nil {
			c.loader = l
		}
	}
}

// WithBulkLoader attaches a [BulkLoader] used by
// [Cache.GetMultiOrLoad]. When configured, missing keys in a
// bulk-get are coalesced into a single LoadMulti call rather than
// firing one [Loader] invocation per key.
func WithBulkLoader[K comparable, V any](l BulkLoader[K, V]) Option {
	return func(c *config) {
		if l != nil {
			c.bulkLoader = l
		}
	}
}

// WithLoaderRateLimit bounds Loader invocations per second across the
// cache. Excess callers receive [ErrLoaderRateLimited] (no blocking).
// A non-positive value disables rate limiting.
func WithLoaderRateLimit(perSecond int) Option {
	return func(c *config) { c.loaderRatePerSecond = perSecond }
}

// WithGroup defines a capacity-bounded tag group. After a Set whose tags
// include name, the cache evicts oldest-first under [EvictReasonTag] if
// more than capacity entries carry that tag. Multiple WithGroup calls
// register independent groups; last call wins per name. Non-positive
// capacity removes a prior registration.
func WithGroup(name string, capacity int) Option {
	return func(c *config) {
		if c.groups == nil {
			c.groups = make(map[string]int)
		}
		if capacity <= 0 {
			delete(c.groups, name)
			return
		}
		c.groups[name] = capacity
	}
}

// WithMaxTagsPerEntry caps the number of tags on any single entry.
// Inputs exceeding the cap return [*CapacityError] wrapping
// [ErrTooManyTags] without inserting. 0 disables the cap.
func WithMaxTagsPerEntry(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.maxTagsPerEntry = n
		}
	}
}

// WithMaxTagsTotal caps the number of distinct tags the cache's inverted
// index may carry. A Set introducing a new tag past the cap returns
// [*CapacityError] wrapping [ErrTooManyTags] without inserting. 0
// disables the cap.
func WithMaxTagsTotal(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.maxTagsTotal = n
		}
	}
}

// WithMaxConcurrentLoads caps in-flight Loader calls across the cache.
// Leaders blocked on a slot return [ErrLoaderTooManyInFlight] on timeout
// (bounded by [WithLoaderTimeout]). When all waiters cancel, the
// leader's ctx is canceled. A non-positive value disables the cap.
func WithMaxConcurrentLoads(n int) Option {
	return func(c *config) { c.maxConcurrentLoads = n }
}

// WithNegativeCache caches "not found" results. When a Loader returns
// [ErrNotFound], the cache stores a tombstone with the supplied TTL and
// subsequent calls return [ErrNotFound] without re-invoking the Loader.
// negativeTTL <= 0 disables negative caching.
func WithNegativeCache(negativeTTL time.Duration) Option {
	return func(c *config) { c.negativeTTL = negativeTTL }
}

// WithErrorTTL caches Loader errors (other than [ErrNotFound]) for d.
// Subsequent [Cache.GetOrLoad] calls within the window return the cached
// error without re-invoking the Loader.
func WithErrorTTL(d time.Duration) Option {
	return func(c *config) { c.errorTTL = d }
}

// WithLoaderTimeout sets the per-call deadline applied to Loader
// contexts when the caller does not supply one.
func WithLoaderTimeout(d time.Duration) Option {
	return func(c *config) { c.loaderTimeout = d }
}

// WithRefreshAhead enables refresh-ahead: when an entry's age exceeds
// refreshAt*TTL, the next [Cache.Get] triggers an asynchronous Loader
// call. refreshAt must be in (0, 1); other values disable.
func WithRefreshAhead(refreshAt float64) Option {
	return func(c *config) { c.refreshAheadAt = refreshAt }
}

// WithStaleWhileRevalidate enables stale-while-revalidate: when an entry
// has expired but its expireAt was less than staleFor ago, the next
// [Cache.Get] returns the stale value and triggers a background refresh.
// staleFor <= 0 disables SWR.
func WithStaleWhileRevalidate(staleFor time.Duration) Option {
	return func(c *config) { c.swrStaleFor = staleFor }
}

// WithExpvar registers the cache's Stats under name in stdlib expvar as
// a JSON object keyed on snake-case stat names. A duplicate registration
// is silently a no-op.
func WithExpvar(name string) Option {
	return func(c *config) {
		if name != "" {
			c.expvarName = name
		}
	}
}

// WithTracer attaches a [Tracer] for span emission. See [Tracer] for the
// catalog of span names.
func WithTracer(t Tracer) Option {
	return func(c *config) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithSnapshotMetadata embeds a key/value map in the snapshot header,
// exposed by [InspectSnapshot]. Up to 65535 keys; the map is cloned on
// each option call. Calling this option more than once replaces the
// metadata wholesale.
func WithSnapshotMetadata(meta map[string]string) Option {
	return func(c *config) {
		if meta == nil {
			c.snapshotMetadata = nil
			return
		}
		c.snapshotMetadata = maps.Clone(meta)
	}
}

// WithMaxSnapshotBytes caps snapshots accepted by [Cache.Load] /
// [Cache.LoadFile]. A non-positive value falls back to the default
// (256 MiB).
func WithMaxSnapshotBytes(n int64) Option {
	return func(c *config) { c.maxSnapshotBytes = n }
}

// WithAutoSave persists a snapshot to path every interval via a per-cache
// goroutine. [Cache.Close] writes a final snapshot before tearing it
// down. Errors are logged. A non-positive interval disables the periodic
// save.
func WithAutoSave(path string, interval time.Duration) Option {
	return func(c *config) {
		c.autoSavePath = path
		c.autoSaveInterval = interval
	}
}

// WithAutoLoad populates the cache from a snapshot at path during [New].
// A missing file is not an error; any other load error fails [New]
// unless [WithAutoLoadIgnoreErrors] is also set.
func WithAutoLoad(path string) Option {
	return func(c *config) { c.autoLoadPath = path }
}

// WithAutoLoadIgnoreErrors makes [WithAutoLoad] log and swallow load
// errors instead of aborting [New].
func WithAutoLoadIgnoreErrors(b bool) Option {
	return func(c *config) { c.autoLoadIgnore = b }
}

// WithEventsBuffer sets the default buffer size used by
// [Cache.Subscribe] when the caller does not supply an explicit
// size. Default 64.
func WithEventsBuffer(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.eventsBuffer = n
		}
	}
}

// WithOnHit registers a synchronous callback invoked on every Get hit.
// The callback runs under the shard lock; slow callbacks block the Get
// path and re-entering the cache for a same-shard key deadlocks. Use
// [Cache.Subscribe] for asynchronous notification.
func WithOnHit[K comparable, V any](fn func(key K, value V)) Option {
	return func(c *config) {
		if fn != nil {
			c.onHit = fn
		}
	}
}

// WithOnMiss registers a synchronous callback invoked on every Get miss
// (including expired and negative-tombstone hits). Same caveats as
// [WithOnHit] apply.
func WithOnMiss[K comparable](fn func(key K)) Option {
	return func(c *config) {
		if fn != nil {
			c.onMiss = fn
		}
	}
}

// WithOnEvict registers a callback invoked when an entry is removed for
// any reason OTHER than TTL expiry. The callback fires AFTER the shard
// lock is released, so re-entering the cache from the callback is safe.
// Multiple removals from one locked section batch their callbacks.
func WithOnEvict[K comparable, V any](fn func(key K, value V, reason EvictionReason)) Option {
	return func(c *config) {
		if fn != nil {
			c.onEvict = fn
		}
	}
}

// WithOnExpire registers a callback invoked when an entry is removed by
// TTL expiry or because [WithExpireFunc] returned true. The callback
// fires AFTER the shard lock is released, so re-entry is safe.
func WithOnExpire[K comparable, V any](fn func(key K, value V)) Option {
	return func(c *config) {
		if fn != nil {
			c.onExpire = fn
		}
	}
}

// WithOnLoad registers a synchronous callback invoked after every Loader
// completion (success or failure).
func WithOnLoad[K comparable, V any](fn func(key K, value V, ttl time.Duration, err error)) Option {
	return func(c *config) {
		if fn != nil {
			c.onLoad = fn
		}
	}
}

// WithExpireFunc registers a per-entry expiry predicate. fn is called
// on every Get and janitor sweep with the entry's metadata; true marks
// it expired regardless of TTL. Removals via fn are recorded under
// [EvictReasonExpireFunc]. fn MUST be fast and side-effect-free; it is
// called under shard locks and is NOT routed through
// [WithCallbackTimeout]. A panicking fn is recovered and the entry is
// treated as fresh.
func WithExpireFunc[K comparable, V any](fn func(key K, value V, meta Metadata) bool) Option {
	return func(c *config) {
		if fn != nil {
			c.expireFunc = fn
		}
	}
}
