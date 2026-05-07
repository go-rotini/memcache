package memcache

import (
	"context"
	"sync"
	"time"
)

// Store is the pluggable backend interface. The package ships
// [MemoryStore] as the default; disk-backed (BoltDB-style),
// Redis-backed, and other adapters are intended to be third-party
// implementations layered behind this contract.
//
// Every method takes a [context.Context] so backends can support
// per-call deadlines, cancellation, and tracing without
// retrofitting them later. Implementations of in-process stores
// (like [MemoryStore]) can choose to honor only ctx.Err() at entry.
//
// Store implementations MUST be safe for concurrent use.
//
// A Cache built with [WithStore] treats the Store as its source of
// truth (read-through with promotion, write-through, delete-through).
// Store also serves as L2 for [Tiered].
type Store[K comparable, V any] interface {
	// Get returns the value stored for key. Missing keys produce
	// (zero V, false, nil); only I/O failures populate err.
	Get(ctx context.Context, key K) (V, bool, error)

	// Set stores value under key with the given TTL. ttl <= 0
	// means "no expiry"; the implementation decides how to record
	// that internally.
	Set(ctx context.Context, key K, value V, ttl time.Duration) error

	// Delete removes the entry for key. Returns whether an entry
	// was actually removed.
	Delete(ctx context.Context, key K) (bool, error)

	// Iterate calls fn for each currently-stored entry. Returning
	// false from fn stops iteration. Order is unspecified;
	// callers cannot rely on it.
	Iterate(ctx context.Context, fn func(key K, value V) bool) error

	// Len returns the number of entries currently in the store.
	Len(ctx context.Context) (int, error)

	// Close releases backend resources. Stores not backed by an
	// external resource may return nil. Close is idempotent.
	Close() error
}

// MemoryStore is the default in-process [Store]. TTLs are enforced
// lazily on Get. Safe for concurrent use; zero value is invalid (use
// [NewMemoryStore]).
type MemoryStore[K comparable, V any] struct {
	mu      sync.RWMutex
	entries map[K]storedEntry[V]
	clock   Clock
	closed  bool
}

// storedEntry is the per-key bundle MemoryStore keeps internally.
type storedEntry[V any] struct {
	value    V
	expireAt int64 // unix nanos; 0 = no TTL
}

// NewMemoryStore constructs an empty in-memory [Store]. clock
// controls TTL evaluation; passing nil falls back to [RealClock]
// so callers can inline `NewMemoryStore[K, V](nil)` for typical
// production use.
func NewMemoryStore[K comparable, V any](clock Clock) *MemoryStore[K, V] {
	if clock == nil {
		clock = RealClock{}
	}
	return &MemoryStore[K, V]{
		entries: make(map[K]storedEntry[V]),
		clock:   clock,
	}
}

// Get returns the value at key, honoring TTL. ctx cancellation is
// checked once at entry; the rest of the call is a single map
// lookup under read lock.
func (m *MemoryStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, false, err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return zero, false, ErrClosed
	}
	e, ok := m.entries[key]
	if !ok {
		return zero, false, nil
	}
	if e.expireAt != 0 && m.clock.Now().UnixNano() >= e.expireAt {
		return zero, false, nil
	}
	return e.value, true, nil
}

// Set stores value with the given TTL. ttl <= 0 stores with no
// expiry.
func (m *MemoryStore[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	var expireAt int64
	if ttl > 0 {
		expireAt = m.clock.Now().UnixNano() + int64(ttl)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	m.entries[key] = storedEntry[V]{value: value, expireAt: expireAt}
	return nil
}

// Delete removes the entry for key.
func (m *MemoryStore[K, V]) Delete(ctx context.Context, key K) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, ErrClosed
	}
	if _, ok := m.entries[key]; !ok {
		return false, nil
	}
	delete(m.entries, key)
	return true, nil
}

// Iterate calls fn for each non-expired entry. fn returns false to
// stop iteration. Iteration order is map-iteration order
// (unspecified); callers must not rely on it.
//
// fn runs under the store's read lock; mutations from fn would
// deadlock. Callers who need to mutate should snapshot first.
func (m *MemoryStore[K, V]) Iterate(ctx context.Context, fn func(key K, value V) bool) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	now := m.clock.Now().UnixNano()
	for k, e := range m.entries {
		if e.expireAt != 0 && now >= e.expireAt {
			continue
		}
		if !fn(k, e.value) {
			return nil
		}
	}
	return nil
}

// Len returns the number of non-expired entries. Pays the cost of
// scanning every entry to filter expired ones; callers who need
// O(1) length on an unbounded store should consider an external
// counter.
func (m *MemoryStore[K, V]) Len(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err //nolint:wrapcheck // pass ctx.Err verbatim
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return 0, ErrClosed
	}
	now := m.clock.Now().UnixNano()
	count := 0
	for _, e := range m.entries {
		if e.expireAt == 0 || now < e.expireAt {
			count++
		}
	}
	return count, nil
}

// Close empties the store and marks it closed. Subsequent calls
// return [ErrClosed]. Idempotent.
func (m *MemoryStore[K, V]) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	m.entries = nil
	return nil
}
