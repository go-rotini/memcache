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

	// snapshotMetadata is an arbitrary key/value bag persisted
	// alongside the snapshot header (spec §9.10). Useful for
	// auditing — e.g. embedding the binary's git SHA so callers
	// can detect cross-version snapshots before loading.
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
	// hashed-wheel parameters. The wheel implementation lives in
	// internal/wheel; the option is recognized but not yet wired
	// into the cache's TTL backend. New emits a one-shot info log
	// when the option is set so users know the option is parsed
	// but not yet active.
	ttlBuckets              int
	ttlBucketsTickPerBucket int

	// flatStorage opts each shard into [flatStore] — a flat
	// hash-probed table with linear probing and tombstone-driven
	// compactions — instead of the default `map[K]*entry[K, V]`.
	// Toggled by [WithFlatStorage].
	flatStorage bool
}

// defaultConfig returns the package's baseline configuration. It is
// applied before any user options.
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

// defaultShardCount returns the default shard count for the host:
// next-power-of-two(GOMAXPROCS * 4), capped at 1024.
func defaultShardCount() int {
	n := max(runtime.GOMAXPROCS(0)*4, 1)
	const maxShards = 1024
	if n > maxShards {
		return maxShards
	}
	return nextPowerOfTwo(n)
}

// nextPowerOfTwo returns the smallest power of two >= n. For n <= 1 it
// returns 1.
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

// WithSlidingTTL toggles sliding-TTL behavior on the cache's default
// TTL. When true, every Get refreshes the entry's expiry to
// `now + slidingTTL`.
func WithSlidingTTL(b bool) Option {
	return func(c *config) { c.slidingTTL = b }
}

// WithTTLJitter sets the jitter window applied to TTL expirations.
// The actual expiry of a TTL'd entry is `ttl + uniform(-j, +j)`. Use
// to break up cohort expirations and avoid stampedes.
//
// When this option is not configured, the cache applies a default
// jitter equal to 5% of the resolved TTL on each insert. Pass a
// non-positive duration to disable jitter entirely (the explicit
// zero overrides the 5% default).
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

// WithStatsEnabled toggles statistics collection. Default on.
// Disabling removes a small amount of atomic-add overhead from the hot
// path.
func WithStatsEnabled(b bool) Option {
	return func(c *config) { c.statsEnabled = b }
}

// WithCollisionTracking enables hash-collision counting in
// [Stats.HashCollisions]. When two distinct keys produce the same
// hasher output (shard-routing hash), the second one's insert
// increments the counter. Off by default — useful when diagnosing
// hot keys, weak custom hashers passed via [WithHasher], or
// pathological key distributions.
//
// The check is per-shard and adds one map lookup + one map write
// per insert; turn off in production unless you're actively
// investigating a distribution problem.
func WithCollisionTracking(b bool) Option {
	return func(c *config) { c.collisionTracking = b }
}

// WithTTLBuckets requests time-bucketed expiration via the hashed
// timing wheel implemented in internal/wheel. The wheel uses
// `slots × tickPerBucket` slots with a per-tick wall duration of
// roughly the configured janitor interval / tickPerBucket.
//
// EXPERIMENTAL in v0: the wheel package ships and is fully tested,
// but cache hot-path wiring is deferred to v0.2 pending benchmark-
// driven validation (the per-shard expiry heap is correct and
// fast for typical CLI workloads). When this option is supplied
// the cache logs an info message and continues to use the heap;
// no behavior change yet. The option is recognized so user code
// targeting v1 compiles unchanged.
func WithTTLBuckets(slots, tickPerBucket int) Option {
	return func(c *config) {
		c.ttlBuckets = slots
		c.ttlBucketsTickPerBucket = tickPerBucket
	}
}

// WithFlatStorage opts each shard into a flat hash-probed storage
// layout instead of the default Go map. The flat layout uses linear
// probing with tombstone-driven compactions; per-entry overhead is
// lower than the map's bucket-and-overflow structure and probes
// keep cache lines hot, which can improve throughput on workloads
// with small-to-mid sized values and modest churn.
//
// Trade-offs:
//   - Lookups, inserts, and deletes are O(1) amortized but pay a
//     slot-scan cost when clusters are dense.
//   - Deletes leave tombstones until the next compaction; sustained
//     churn-heavy workloads will see periodic rebuilds.
//   - The number of compactions performed across all shards is
//     reported as [Stats.Compactions]; a persistently-rising
//     counter under steady state indicates the workload is
//     compaction-dominated and the default map storage may serve
//     it better.
//
// EXPERIMENTAL in v0; enable only when benchmarks for your workload
// show a win.
func WithFlatStorage() Option {
	return func(c *config) { c.flatStorage = true }
}

// WithShardedStats requests per-CPU sharded counters for the
// hot-path Hits/Misses fields. Reduces cache-line contention on
// >32-core machines at the cost of slightly more memory and a
// fan-in cost on every [Cache.Stats] read. The remaining stats
// counters stay as plain atomics — they update on insert/evict
// rather than every Get and so don't pay the same penalty.
// Default: off.
func WithShardedStats(b bool) Option {
	return func(c *config) { c.shardedStats = b }
}

// WithWeigher attaches a function that returns the "weight" of a
// value. Used together with [WithMaxBytes] for byte-bounded caches.
// The function is type-asserted at cache construction time; passing a
// weigher whose value type does not match V results in
// [ErrPolicyConfig].
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

// WithLoaderRateLimit bounds the number of Loader invocations per
// second across the cache. Excess callers receive
// [ErrLoaderRateLimited] rather than blocking; their context is
// not consulted (the limiter rejects synchronously).
//
// A non-positive value disables rate limiting. The limiter is a
// simple steady-rate token bucket sized at perSecond tokens with
// refill rate perSecond/second; it does not allow bursts above its
// capacity.
func WithLoaderRateLimit(perSecond int) Option {
	return func(c *config) { c.loaderRatePerSecond = perSecond }
}

// WithGroup defines a capacity-bounded tag group. After a Set
// whose tags include `name`, the cache enforces that no more than
// `capacity` entries simultaneously carry that tag — surplus
// entries are evicted oldest-first (by insertion time) under
// [EvictReasonTag].
//
// Multiple WithGroup options may be supplied (one per group); the
// last call for a given name wins.
//
// A non-positive capacity removes any prior registration for that
// name.
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

// WithMaxTagsPerEntry caps the number of tags carried by any
// single entry. [Cache.SetWithTags] / [SetTags] / [CacheTagger]
// inputs that exceed the limit cause the affected Set call to
// return [*CapacityError] wrapping [ErrTooManyTags] without
// inserting the entry.
//
// 0 (the default) disables the per-entry cap.
func WithMaxTagsPerEntry(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.maxTagsPerEntry = n
		}
	}
}

// WithMaxTagsTotal caps the number of distinct tags the cache's
// inverted index may carry across all entries. When a Set would
// introduce a new tag past the limit, the call returns
// [*CapacityError] wrapping [ErrTooManyTags] without inserting.
// Existing entries on already-known tags are unaffected.
//
// 0 disables the cache-wide cap.
func WithMaxTagsTotal(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.maxTagsTotal = n
		}
	}
}

// WithMaxConcurrentLoads caps the number of in-flight Loader calls
// across the cache. When the cap is reached, further [Cache.GetOrLoad]
// callers wait until a slot opens, returning their context's error
// (or [ErrLoaderTooManyInFlight] when the wait times out via
// [WithLoaderTimeout]).
//
// A non-positive value disables the cap.
func WithMaxConcurrentLoads(n int) Option {
	return func(c *config) { c.maxConcurrentLoads = n }
}

// WithNegativeCache enables caching of "not found" results. When a
// Loader returns [ErrNotFound], the cache stores a tombstone with
// the supplied TTL; subsequent Get/GetOrLoad calls return
// [ErrNotFound] without re-invoking the Loader.
//
// negativeTTL <= 0 disables negative caching entirely; the cache
// behaves as if the option had not been set.
func WithNegativeCache(negativeTTL time.Duration) Option {
	return func(c *config) { c.negativeTTL = negativeTTL }
}

// WithErrorTTL caches Loader errors (other than [ErrNotFound]) for
// the given duration. Subsequent [Cache.GetOrLoad] calls within
// the window return the cached error without re-invoking the
// Loader. Distinct from [WithNegativeCache]: that option caches
// "key does not exist"; this one caches "the loader broke".
func WithErrorTTL(d time.Duration) Option {
	return func(c *config) { c.errorTTL = d }
}

// WithLoaderTimeout sets the per-call deadline applied to Loader
// contexts when the caller does not supply one. Recommended in
// production to prevent a runaway Loader from monopolizing
// singleflight slots.
func WithLoaderTimeout(d time.Duration) Option {
	return func(c *config) { c.loaderTimeout = d }
}

// WithRefreshAhead enables refresh-ahead: when an entry's age
// exceeds refreshAt × its TTL, the next [Cache.Get] triggers an
// asynchronous Loader call to refresh the entry. The cached value
// continues to be served until the reload completes. refreshAt
// must be in (0, 1); values outside the range disable refresh-ahead.
func WithRefreshAhead(refreshAt float64) Option {
	return func(c *config) { c.refreshAheadAt = refreshAt }
}

// WithStaleWhileRevalidate enables stale-while-revalidate: when an
// entry has expired but its expireAt was less than staleFor ago,
// the next [Cache.Get] returns the stale value AND triggers a
// background Loader call to refresh it. Inspired by RFC 5861.
//
// staleFor <= 0 disables SWR.
func WithStaleWhileRevalidate(staleFor time.Duration) Option {
	return func(c *config) { c.swrStaleFor = staleFor }
}

// WithExpvar registers the cache's Stats under the given name in
// stdlib `expvar`. The published variable is a JSON-shaped object
// keyed on the snake-case stat names (`hits`, `misses`, …,
// `entries`, `bytes`, `capacity`, `hit_rate_permille`).
//
// Useful for `/debug/vars` integrations: drop the option in,
// expose `expvar.Handler()` from your HTTP server, and the
// cache's counters are visible immediately.
//
// If a variable with the same name has already been published in
// this process the call is silently a no-op (avoiding the panic
// stdlib's `expvar.Publish` would otherwise raise on duplicate
// registration).
func WithExpvar(name string) Option {
	return func(c *config) {
		if name != "" {
			c.expvarName = name
		}
	}
}

// WithTracer attaches a [Tracer] for span emission on cache
// operations. Spans are emitted around `Get`, `Set`, the loader,
// eviction, and snapshot save/load paths. See the [Tracer] doc
// for the catalog of span names.
func WithTracer(t Tracer) Option {
	return func(c *config) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithSnapshotMetadata embeds a key/value map in the snapshot
// header. The metadata is exposed by [InspectSnapshot] so callers
// can audit a snapshot's environment of origin (e.g., embed
// `app_version` and refuse to load snapshots from incompatible
// builds). Up to 65535 keys; each key and value are length-
// prefixed `uint32` strings. The map is copied on the option call
// so subsequent mutation by the caller does not affect future
// saves.
//
// Calling this option more than once replaces the metadata wholesale.
func WithSnapshotMetadata(meta map[string]string) Option {
	return func(c *config) {
		if meta == nil {
			c.snapshotMetadata = nil
			return
		}
		c.snapshotMetadata = maps.Clone(meta)
	}
}

// WithMaxSnapshotBytes caps the size of snapshots accepted by
// [Cache.Load] and [Cache.LoadFile]. A non-positive value falls
// back to the package default (256 MiB). Use this to harden the
// cache against corrupt or hostile snapshot files that would
// otherwise OOM the process during Load.
func WithMaxSnapshotBytes(n int64) Option {
	return func(c *config) { c.maxSnapshotBytes = n }
}

// WithAutoSave instructs the cache to persist a snapshot to path
// every interval. Saving is performed by a per-cache goroutine
// started during construction; [Cache.Close] writes a final
// snapshot before tearing the goroutine down. Errors during
// auto-save are logged via the configured slog.Logger and do not
// fail subsequent saves.
//
// A non-positive interval disables the periodic save (useful when
// pairing only WithAutoLoad with a manual SaveFile on shutdown).
func WithAutoSave(path string, interval time.Duration) Option {
	return func(c *config) {
		c.autoSavePath = path
		c.autoSaveInterval = interval
	}
}

// WithAutoLoad attempts to populate the cache from a snapshot at
// path during [New]. A missing file is treated as "no snapshot to
// load" (not an error). Any other error during load fails
// construction unless [WithAutoLoadIgnoreErrors] is also set.
func WithAutoLoad(path string) Option {
	return func(c *config) { c.autoLoadPath = path }
}

// WithAutoLoadIgnoreErrors makes [WithAutoLoad] swallow load
// errors and log them through the configured slog.Logger instead
// of aborting [New]. Recommended for CLIs where a corrupt or
// version-mismatched snapshot should fall back to an empty cache
// rather than refuse to start.
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

// WithOnHit registers a synchronous callback invoked on every Get
// hit. The callback runs under the shard write lock; slow
// callbacks block the Get path. For asynchronous notification use
// [Cache.Subscribe] instead.
func WithOnHit[K comparable, V any](fn func(key K, value V)) Option {
	return func(c *config) {
		if fn != nil {
			c.onHit = fn
		}
	}
}

// WithOnMiss registers a synchronous callback invoked on every Get
// miss (including expired and negative-tombstone hits). Runs under
// the shard write lock.
func WithOnMiss[K comparable](fn func(key K)) Option {
	return func(c *config) {
		if fn != nil {
			c.onMiss = fn
		}
	}
}

// WithOnEvict registers a synchronous callback invoked when an
// entry is removed for any reason OTHER than TTL expiry. The
// callback receives the key, the (now-pool-bound) value, and the
// reason. Runs under the shard write lock.
func WithOnEvict[K comparable, V any](fn func(key K, value V, reason EvictionReason)) Option {
	return func(c *config) {
		if fn != nil {
			c.onEvict = fn
		}
	}
}

// WithOnExpire registers a synchronous callback invoked when an
// entry is removed because its TTL has elapsed (lazy or janitor
// path) or because [WithExpireFunc] returned true. Distinct from
// [WithOnEvict] so callers can react differently to natural
// expiration vs capacity-driven eviction.
func WithOnExpire[K comparable, V any](fn func(key K, value V)) Option {
	return func(c *config) {
		if fn != nil {
			c.onExpire = fn
		}
	}
}

// WithOnLoad registers a synchronous callback invoked after every
// Loader completion (success or failure). The callback receives
// the key, the loaded value (or zero V on error), the TTL the
// Loader returned (or 0), and the error.
func WithOnLoad[K comparable, V any](fn func(key K, value V, ttl time.Duration, err error)) Option {
	return func(c *config) {
		if fn != nil {
			c.onLoad = fn
		}
	}
}

// WithExpireFunc registers a per-entry expiry predicate. On each
// Get and during each janitor sweep, fn is called with the entry's
// metadata; returning true causes the cache to treat the entry as
// expired regardless of its TTL.
//
// Use cases include "expire when an external resource changes" —
// e.g., file mtime checks, schema-version comparison, etag mismatch.
// fn must be fast and side-effect-free; it is called under the
// shard's read lock on the Get path and the write lock during
// sweeps. A panicking fn is recovered: the entry is treated as
// fresh (defensive default — better to keep stale data than lose it)
// and a warning is logged through the configured slog.Logger.
//
// fn is type-asserted at cache construction time; passing a
// predicate whose type parameters do not match the cache's K/V
// types results in a [*ConfigError].
func WithExpireFunc[K comparable, V any](fn func(key K, value V, meta Metadata) bool) Option {
	return func(c *config) {
		if fn != nil {
			c.expireFunc = fn
		}
	}
}
