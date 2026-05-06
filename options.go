package memcache

import (
	"log/slog"
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
	// applied TTL.
	ttlJitter time.Duration

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
	expireFunc any
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
func WithTTLJitter(j time.Duration) Option {
	return func(c *config) { c.ttlJitter = j }
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
