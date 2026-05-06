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
	weigher any
	hasher  any
	loader  any
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
