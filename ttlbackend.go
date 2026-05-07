package memcache

import "container/heap"

// ttlBackend is the per-shard interface that tracks pending entry
// expirations. Two implementations:
//
//   - expiryHeapBackend is a min-heap keyed on expireAt (the
//     default; precise to the nanosecond).
//   - wheelBackend wraps internal/wheel.Wheel for high-volume TTL
//     workloads (opt-in via [WithTTLBuckets]; precise to one tick).
//
// Implementations are NOT safe for concurrent use; the owning
// shard's mutex serializes every call.
type ttlBackend[K comparable, V any] interface {
	// Add tracks e under its current expireAt. No-op when e has
	// no TTL (expireAt == 0). Caller must hold s.mu (write).
	Add(e *entry[K, V])

	// Remove drops e from tracking. Idempotent.
	Remove(e *entry[K, V])

	// Fix re-positions e after its expireAt changed; handles every
	// transition (insert, remove, in-place reposition).
	Fix(e *entry[K, V])

	// Sweep returns every entry whose expireAt <= now and untracks them.
	// The caller drives the actual cache eviction; subsequent Remove
	// from the eviction path is a no-op.
	Sweep(now int64) []*entry[K, V]

	// Len reports the number of currently-tracked entries.
	Len() int

	// Reset drops every tracked entry without producing
	// expirations. Used by [Cache.Reset] / [Cache.Clear].
	Reset()
}

// newTTLBackend selects the per-shard TTL backend per cfg. The
// wheel backend is used when [WithTTLBuckets] is set; otherwise
// the heap backend is the default.
func newTTLBackend[K comparable, V any](cfg *config) ttlBackend[K, V] {
	if cfg.ttlBuckets > 0 {
		slots := cfg.ttlBuckets
		// tickPerBucket controls precision: per-tick wall
		// duration = janitorInterval / tickPerBucket. Default
		// to 1 (one tick per janitor interval) when not set.
		ticks := cfg.ttlBucketsTickPerBucket
		if ticks <= 0 {
			ticks = 1
		}
		base := cfg.janitorInterval
		if base <= 0 {
			// Wheel still needs a tick scale even when janitor is off.
			base = 30_000_000_000 // 30s in nanoseconds
		}
		tickNs := max(int64(base)/int64(ticks), 1)
		var origin int64
		if cfg.clock != nil {
			origin = cfg.clock.Now().UnixNano()
		}
		return newWheelBackend[K, V](slots, tickNs, origin)
	}
	return newExpiryHeapBackend[K, V]()
}

// expiryHeapBackend is the default ttlBackend: a min-heap keyed on
// expireAt with a per-entry heap-index back-pointer for O(log n)
// arbitrary remove. Implementation moves between this struct and
// the embedded `expiryHeap` slice; the slice is exposed via the
// Heap method for tests that want to inspect ordering.
type expiryHeapBackend[K comparable, V any] struct {
	heap expiryHeap[K, V]
}

// newExpiryHeapBackend returns an empty heap-backed TTL tracker.
func newExpiryHeapBackend[K comparable, V any]() *expiryHeapBackend[K, V] {
	return &expiryHeapBackend[K, V]{}
}

// Add inserts e into the heap iff e has a non-zero expireAt.
func (b *expiryHeapBackend[K, V]) Add(e *entry[K, V]) {
	if e.expireAt.Load() == 0 {
		e.heapIndex = -1
		return
	}
	heap.Push(&b.heap, e)
}

// Remove pulls e from the heap if currently tracked.
func (b *expiryHeapBackend[K, V]) Remove(e *entry[K, V]) {
	if e.heapIndex < 0 || e.heapIndex >= len(b.heap) {
		return
	}
	heap.Remove(&b.heap, e.heapIndex)
}

// Fix handles every transition: insert if newly TTL'd, remove if
// TTL cleared, in-place fix if TTL changed.
func (b *expiryHeapBackend[K, V]) Fix(e *entry[K, V]) {
	exp := e.expireAt.Load()
	switch {
	case e.heapIndex < 0 && exp > 0:
		heap.Push(&b.heap, e)
	case e.heapIndex >= 0 && exp == 0:
		heap.Remove(&b.heap, e.heapIndex)
	case e.heapIndex >= 0:
		heap.Fix(&b.heap, e.heapIndex)
	}
}

// Sweep pops every expired entry in expireAt order.
func (b *expiryHeapBackend[K, V]) Sweep(now int64) []*entry[K, V] {
	var out []*entry[K, V]
	for b.heap.Len() > 0 && b.heap[0].expireAt.Load() <= now {
		e, _ := heap.Pop(&b.heap).(*entry[K, V])
		out = append(out, e)
	}
	return out
}

// Len reports the heap size.
func (b *expiryHeapBackend[K, V]) Len() int { return b.heap.Len() }

// Reset drops every entry. Each entry's heapIndex is left as the
// sentinel (-1) so a subsequent Add behaves correctly.
func (b *expiryHeapBackend[K, V]) Reset() {
	for i := range b.heap {
		if b.heap[i] != nil {
			b.heap[i].heapIndex = -1
		}
		b.heap[i] = nil
	}
	b.heap = b.heap[:0]
}
