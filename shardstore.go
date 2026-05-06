package memcache

// shardStore is the storage abstraction owned by a [shard]. Each shard
// holds exactly one shardStore, accessed only while the shard's
// `sync.RWMutex` is held (RLock for read methods, Lock for write
// methods). Implementations are NOT independently concurrency-safe.
//
// Two implementations ship in v0:
//   - [mapStore] is the default; a near-zero-cost wrapper over
//     `map[K]*entry[K, V]`.
//   - [flatStore], opt-in via [WithFlatStorage], is a flat
//     hash-probed table with linear probing and tombstone-driven
//     compactions.
//
// The interface is positioned for two follow-on items: a Store-backed
// implementation that wires the [Store] interface into the cache's
// hot path, and a sync.Map-style read/dirty split for a lock-free
// Get fast path. Both reuse the same call-site migration this
// abstraction performs.
type shardStore[K comparable, V any] interface {
	// get returns the entry stored under key, or (nil, false) if
	// absent.
	get(key K) (*entry[K, V], bool)

	// set inserts or replaces the entry for key.
	set(key K, e *entry[K, V])

	// del removes the entry for key. Returns whether anything was
	// removed.
	del(key K) bool

	// length returns the live entry count.
	length() int

	// clearAll removes every entry. The implementation may or may
	// not release backing memory; callers should treat the store as
	// "size 0" without further assumption.
	clearAll()

	// each invokes fn for every live entry. Returning false from fn
	// stops iteration. Iteration order is unspecified across
	// implementations.
	each(fn func(*entry[K, V]) bool)

	// compactions returns the running count of compaction operations
	// performed by this store. Default implementations that do not
	// compact return 0.
	compactions() uint64
}

// newShardStore constructs the configured shardStore implementation.
// The hasher is passed through so flat-storage shards share the
// cache's key-hash function (avoiding a second hasher dispatch).
func newShardStore[K comparable, V any](cfg *config, hasher func(K) uint64) shardStore[K, V] {
	if cfg.flatStorage {
		return newFlatStore[K, V](hasher, flatStoreInitialCap)
	}
	return newMapStore[K, V]()
}

// mapStore is the default [shardStore] implementation. The wrapper
// methods are simple enough that the Go compiler can inline them on
// the hot path.
type mapStore[K comparable, V any] struct {
	m map[K]*entry[K, V]
}

func newMapStore[K comparable, V any]() *mapStore[K, V] {
	return &mapStore[K, V]{m: make(map[K]*entry[K, V])}
}

func (s *mapStore[K, V]) get(key K) (*entry[K, V], bool) {
	e, ok := s.m[key]
	return e, ok
}

func (s *mapStore[K, V]) set(key K, e *entry[K, V]) { s.m[key] = e }

func (s *mapStore[K, V]) del(key K) bool {
	if _, ok := s.m[key]; !ok {
		return false
	}
	delete(s.m, key)
	return true
}

func (s *mapStore[K, V]) length() int { return len(s.m) }

func (s *mapStore[K, V]) clearAll() { clear(s.m) }

func (s *mapStore[K, V]) each(fn func(*entry[K, V]) bool) {
	for _, e := range s.m {
		if !fn(e) {
			return
		}
	}
}

func (s *mapStore[K, V]) compactions() uint64 { return 0 }
