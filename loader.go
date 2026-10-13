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

	// superseded is set when the key is written, deleted or
	// invalidated while the load runs, or when a newer load for the key
	// starts after every waiter of this one gave up. The loader then
	// hands its result to its waiters but does not store it. Guarded
	// by the shard lock.
	superseded bool
}

// supersedeFlight marks the in-flight or abandoned load for key, and
// any watch on it, as superseded. Caller MUST hold s.mu (write).
func (s *shard[K, V]) supersedeFlight(key K) {
	if len(s.inflight) > 0 {
		if f, ok := s.inflight[key]; ok {
			f.superseded = true
		}
	}
	if len(s.orphans) > 0 {
		if f, ok := s.orphans[key]; ok {
			f.superseded = true
			delete(s.orphans, key)
		}
	}
	if len(s.watches) > 0 {
		if w, ok := s.watches[key]; ok {
			w.superseded = true
			delete(s.watches, key)
		}
	}
}

// supersedeAllFlights marks every in-flight and abandoned load on s as
// superseded. Caller MUST hold s.mu (write).
func (s *shard[K, V]) supersedeAllFlights() {
	for _, f := range s.inflight {
		f.superseded = true
	}
	for _, f := range s.orphans {
		f.superseded = true
	}
	clear(s.orphans)
	for _, w := range s.watches {
		w.superseded = true
	}
	clear(s.watches)
}

// orphanFlight moves flight, whose waiters have all given up, from
// s.inflight to s.orphans: it still stores its result when it
// finishes, unless a write or a newer load for the key supersedes it
// first. Caller MUST hold s.mu (write).
func (s *shard[K, V]) orphanFlight(key K, flight *flightCall[V]) {
	if cur, ok := s.inflight[key]; !ok || cur != flight {
		return
	}
	delete(s.inflight, key)
	if s.orphans == nil {
		s.orphans = make(map[K]*flightCall[V])
	}
	if prev, ok := s.orphans[key]; ok {
		prev.superseded = true
	}
	s.orphans[key] = flight
}

// startFlight registers flight as the load for key. An abandoned load
// for the key still running is superseded by it. Caller MUST hold s.mu
// (write).
func (s *shard[K, V]) startFlight(key K, flight *flightCall[V]) {
	if len(s.orphans) > 0 {
		if prev, ok := s.orphans[key]; ok {
			prev.superseded = true
			delete(s.orphans, key)
		}
	}
	s.inflight[key] = flight
}

// leaveInflight removes flight from s.inflight or s.orphans if it is
// still registered there for key; a newer flight is left alone. Caller
// MUST hold s.mu (write).
func (s *shard[K, V]) leaveInflight(key K, flight *flightCall[V]) {
	if cur, ok := s.inflight[key]; ok && cur == flight {
		delete(s.inflight, key)
	}
	if cur, ok := s.orphans[key]; ok && cur == flight {
		delete(s.orphans, key)
	}
}

// supersedesFlight reports whether a removal for reason replaces the
// key's data, so an in-flight load for the key must not store its
// result. Capacity, expiry and resize removals don't: a load finishing
// after them still brings back current data.
func supersedesFlight(reason EvictionReason) bool {
	switch reason {
	case EvictReasonCapacity, EvictReasonExpired, EvictReasonExpireFunc, EvictReasonResize:
		return false
	}
	return true
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
