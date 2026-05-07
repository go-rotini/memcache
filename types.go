package memcache

import (
	"time"
)

// unknownString is the human-readable label returned by every enum
// String method when the underlying value is outside the defined range.
const unknownString = "unknown"

// Policy selects an eviction policy.
type Policy uint8

// Eviction policies. PolicyS3FIFO is the default.
const (
	// PolicyS3FIFO is the default eviction policy: small/main/ghost FIFO
	// queues with a 2-bit saturating frequency counter. Achieves
	// hit-rate competitive with TinyLFU at lower implementation cost.
	PolicyS3FIFO Policy = iota

	// PolicyLRU evicts the least-recently-used entry.
	PolicyLRU

	// PolicyLFU evicts the least-frequently-used entry. Frequency is
	// tracked with a doubly-linked list of frequency buckets for O(1)
	// access and eviction.
	PolicyLFU

	// PolicyTinyLFU is W-TinyLFU: a small window LRU plus an LFU main
	// region admitted by a 4-bit count-min sketch.
	PolicyTinyLFU

	// PolicyFIFO evicts in insertion order. Cheapest policy.
	PolicyFIFO

	// PolicyARC is the Adaptive Replacement Cache. Splits cache into
	// recency and frequency sub-caches with adaptive sizing.
	PolicyARC

	// Policy2Q is the 2Q algorithm: A1in (FIFO) + Am (LRU) + A1out
	// (ghost). Lighter than ARC, similar workload coverage.
	Policy2Q
)

// String returns a human-readable name for the policy.
func (p Policy) String() string {
	switch p {
	case PolicyS3FIFO:
		return "s3fifo"
	case PolicyLRU:
		return "lru"
	case PolicyLFU:
		return "lfu"
	case PolicyTinyLFU:
		return "tinylfu"
	case PolicyFIFO:
		return "fifo"
	case PolicyARC:
		return "arc"
	case Policy2Q:
		return "2q"
	default:
		return unknownString
	}
}

// EvictionReason describes why an entry was removed from the cache.
type EvictionReason uint8

// Eviction reasons. The order is fixed for stable indexing into
// Stats.EvictionsByReason.
const (
	EvictReasonCapacity      EvictionReason = iota // policy chose to evict
	EvictReasonExpired                             // TTL hit
	EvictReasonExpireFunc                          // WithExpireFunc returned true
	EvictReasonReplaced                            // overwritten by Set
	EvictReasonDeleted                             // explicit Delete
	EvictReasonDeletedPrefix                       // DeletePrefix
	EvictReasonDeletedWhere                        // DeleteWhere
	EvictReasonTag                                 // InvalidateTag
	EvictReasonRemote                              // WithInvalidationSubscriber
	EvictReasonResize                              // Resize shrunk the cache
	EvictReasonComputed                            // Compute returned ComputeDelete
	EvictReasonClear                               // Clear()
	EvictReasonStoreRollback                       // Store write-through failed; in-memory rolled back
)

// numEvictionReasons is the count of distinct reasons. Update if reasons
// are added.
const numEvictionReasons = 13

// String returns a human-readable name for the reason.
func (r EvictionReason) String() string {
	switch r {
	case EvictReasonCapacity:
		return "capacity"
	case EvictReasonExpired:
		return "expired"
	case EvictReasonExpireFunc:
		return "expire-func"
	case EvictReasonReplaced:
		return "replaced"
	case EvictReasonDeleted:
		return "deleted"
	case EvictReasonDeletedPrefix:
		return "deleted-prefix"
	case EvictReasonDeletedWhere:
		return "deleted-where"
	case EvictReasonTag:
		return "tag"
	case EvictReasonRemote:
		return "remote"
	case EvictReasonResize:
		return "resize"
	case EvictReasonComputed:
		return "computed"
	case EvictReasonClear:
		return "clear"
	case EvictReasonStoreRollback:
		return "store-rollback"
	default:
		return unknownString
	}
}

// EventKind identifies an event kind for Subscribe.
type EventKind uint8

// Event kinds.
const (
	EventInsert EventKind = iota
	EventUpdate
	EventEvict
	EventExpire
	EventLoad
	EventLoadError
	EventLoadTimeout
	EventLoadRateLimited
	EventInvalidateTag
	EventResize
	EventSnapshot // Save/Load completion
)

// String returns a human-readable name for the event kind.
func (k EventKind) String() string {
	switch k {
	case EventInsert:
		return "insert"
	case EventUpdate:
		return "update"
	case EventEvict:
		return "evict"
	case EventExpire:
		return "expire"
	case EventLoad:
		return "load"
	case EventLoadError:
		return "load-error"
	case EventLoadTimeout:
		return "load-timeout"
	case EventLoadRateLimited:
		return "load-rate-limited"
	case EventInvalidateTag:
		return "invalidate-tag"
	case EventResize:
		return "resize"
	case EventSnapshot:
		return "snapshot"
	default:
		return unknownString
	}
}

// Event is an observable cache event delivered to Subscribe channels.
type Event[K comparable, V any] struct {
	Kind   EventKind
	Key    K
	Value  V
	Reason EvictionReason // populated for EventEvict
	Err    error          // populated for EventLoadError
	At     time.Time
	Tags   []string
}

// ComputeAction is the return discriminator for Compute callbacks.
type ComputeAction uint8

// Compute actions.
const (
	ComputeStore  ComputeAction = iota // store the returned value
	ComputeDelete                      // remove the entry
	ComputeNoOp                        // leave the entry unchanged
)

// String returns a human-readable name for the action.
func (a ComputeAction) String() string {
	switch a {
	case ComputeStore:
		return "store"
	case ComputeDelete:
		return "delete"
	case ComputeNoOp:
		return "no-op"
	default:
		return unknownString
	}
}

// Item is the value-with-metadata view used by Range, snapshots, and
// events.
type Item[V any] struct {
	Value      V
	Expiry     time.Time // zero if no TTL
	LastAccess time.Time
	Inserted   time.Time
	Hits       uint32 // hit count since insertion
	Weight     int64
	Tags       []string
	Sliding    bool
}

// KeyedItem pairs a key with its Item view.
type KeyedItem[K comparable, V any] struct {
	Item[V]

	Key K
}

// Metadata is the value-less view used by ItemMetadata.
type Metadata struct {
	Expiry     time.Time
	Inserted   time.Time
	LastAccess time.Time
	Hits       uint32
	Weight     int64
	Tags       []string
	Sliding    bool
}

// LoadResult is one entry in a BulkLoader response.
type LoadResult[V any] struct {
	Value V
	TTL   time.Duration
	Err   error // per-key error; cache treats these as load failures, not catastrophic
}

// Histogram is a coarse multi-dimensional summary of cache contents.
type Histogram struct {
	AgeBuckets    [8]int64 // bucket bounds: 1s, 10s, 1m, 10m, 1h, 1d, 7d, +inf
	WeightBuckets [8]int64 // bucket bounds: 1, 16, 256, 4Ki, 64Ki, 1Mi, 16Mi, +inf
	HitsBuckets   [8]int64 // bucket bounds: 0, 1, 2, 4, 16, 64, 256, +inf
	TotalEntries  int
	TotalWeight   int64
}

// SnapshotInfo summarizes a snapshot file's header without loading it.
type SnapshotInfo struct {
	Version   uint8
	Codec     string
	Name      string
	SaveTime  time.Time
	Count     int64
	SizeBytes int64
	Metadata  map[string]string
}

// Number is the type constraint for numeric atomic operations
// (Increment, Decrement, IncrementBy).
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// Prefixer is implemented by key types that have a meaningful "prefix"
// concept. [Cache.DeletePrefix] first checks if K is string, then
// falls back to this interface; for K types that satisfy neither,
// DeletePrefix is a silent no-op (returns 0).
type Prefixer interface {
	HasPrefix(prefix string) bool
}
