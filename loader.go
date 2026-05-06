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
