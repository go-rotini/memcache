package memcache

import (
	"context"
	"time"
)

// Loader is the interface invoked on a cache miss when a [Cache] has
// been configured with [WithLoader]. The returned TTL is applied to
// the resulting entry; a zero TTL falls back to the cache's
// configured default.
type Loader[K comparable, V any] interface {
	// Load fetches the value for key. Implementations should respect
	// ctx cancellation and return promptly when ctx is done.
	Load(ctx context.Context, key K) (value V, ttl time.Duration, err error)
}

// LoaderFunc adapts a plain function to the [Loader] interface.
type LoaderFunc[K comparable, V any] func(ctx context.Context, key K) (V, time.Duration, error)

// Load satisfies [Loader].
func (f LoaderFunc[K, V]) Load(ctx context.Context, key K) (V, time.Duration, error) {
	return f(ctx, key)
}

// BulkLoader loads multiple keys at once, e.g. via a database batch
// query. Used by [Cache.GetMultiOrLoad] when configured with
// [WithBulkLoader].
type BulkLoader[K comparable, V any] interface {
	// LoadMulti fetches values for the given keys. Implementations
	// should populate the returned map with one entry per requested
	// key; missing keys may be omitted (treated as load-failures with
	// [ErrNotFound]) or present with a per-key error in
	// LoadResult.Err.
	LoadMulti(ctx context.Context, keys []K) (map[K]LoadResult[V], error)
}

// flightCall represents a single in-flight Loader invocation. The
// first caller for a missing key creates the flight; subsequent
// callers join by reading flight.done. When the loader goroutine
// finishes, it stores val/ttl/err and closes done; every waiter
// then reads its outcome under happens-before guarantees from the
// channel close.
type flightCall[V any] struct {
	// done is closed when val/ttl/err have been finalized and the
	// shard's inflight entry has been removed.
	done chan struct{}

	val V
	ttl time.Duration
	err error
}

// newFlightCall returns a flightCall ready for waiters.
func newFlightCall[V any]() *flightCall[V] {
	return &flightCall[V]{done: make(chan struct{})}
}

// cachedError is a per-shard "the loader broke" tombstone enabled
// by [WithErrorTTL]. Distinct from the negative-cache tombstone
// (which lives on the entries map with [flagNegative] set) because
// the cached error needs to carry an actual error value back to
// callers; the entry struct cannot, since its value field is V.
type cachedError struct {
	err      error
	expireAt int64 // unix nanos
}
