package memcache

import (
	"context"
	"sync/atomic"
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
	// LoadMulti fetches values for the given keys. Keys that could
	// not be loaded may be omitted or carry a per-key error in
	// LoadResult.Err; either way they are left out of the result and
	// not cached. A non-nil error aborts the whole call.
	LoadMulti(ctx context.Context, keys []K) (map[K]LoadResult[V], error)
}

// flightCall is a single in-flight Loader invocation. The first caller
// creates it; followers join by reading flight.done. Each waiter holds
// a refcount; a ctx-cancellation by all waiters cancels the loader ctx.
// Successful completion via flight.done does not decrement.
type flightCall[V any] struct {
	// done is closed when val/ttl/err have been finalized and the
	// shard's inflight entry has been removed.
	done chan struct{}

	val V
	ttl time.Duration
	err error

	// refs is the count of waiters who haven't given up. Starts
	// at 1 (the leader) and grows as followers join via
	// [flightCall.join]. A ctx-canceling waiter decrements it in
	// [Cache.waitOrLeave]; when the count reaches zero the
	// loader's context is canceled.
	refs atomic.Int32

	// cancel is the cancel func of the loader's context.
	cancel func()
}

// newFlightCall returns a flightCall with refs=1 (the leader). cancel
// is the loader's context-cancel function.
func newFlightCall[V any](cancel func()) *flightCall[V] {
	f := &flightCall[V]{
		done:   make(chan struct{}),
		cancel: cancel,
	}
	f.refs.Store(1)
	return f
}

// join increments the waiter refcount by 1.
func (f *flightCall[V]) join() {
	f.refs.Add(1)
}

// cachedError is a per-shard tombstone enabled by [WithErrorTTL]. Lives
// off the entries map because it must carry an error value, which the
// entry struct's V-typed value field cannot represent.
type cachedError struct {
	err      error
	expireAt int64 // unix nanos
}
