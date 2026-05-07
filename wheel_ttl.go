package memcache

import "github.com/go-rotini/memcache/internal/wheel"

// wheelBackend is the [WithTTLBuckets]-driven ttlBackend. It wraps
// internal/wheel.Wheel; per-entry back-pointers live in the
// `wheelHandle` field on entry so Remove is O(1).
//
// Precision: an entry expires within one wheel tick of its
// configured TTL. The cache's per-shard tick is the configured
// `slotTickNs`, defaulting to JanitorInterval / tickPerBucket.
//
// Capacity: slotCount × slotTickNs ≥ longest expected TTL keeps
// `revolutions` typically zero on the hot path; see internal/wheel.
type wheelBackend[K comparable, V any] struct {
	wheel *wheel.Wheel[*entry[K, V]]
}

// newWheelBackend constructs a wheel-backed TTL tracker. originNs
// aligns the wheel's slot 0; subsequent Add calls compute slots
// relative to it.
func newWheelBackend[K comparable, V any](slotCount int, slotTickNs, originNs int64) *wheelBackend[K, V] {
	return &wheelBackend[K, V]{
		wheel: wheel.New[*entry[K, V]](slotCount, slotTickNs, originNs),
	}
}

// Add inserts e iff e has a non-zero expireAt. The wheel.Entry
// pointer is stashed on e.wheelHandle for O(1) Remove.
func (b *wheelBackend[K, V]) Add(e *entry[K, V]) {
	exp := e.expireAt.Load()
	if exp == 0 {
		e.wheelHandle = nil
		return
	}
	we := &wheel.Entry[*entry[K, V]]{Payload: e, ExpireAtNs: exp}
	b.wheel.Add(we)
	e.wheelHandle = we
}

// Remove unlinks e's wheel entry. Idempotent.
func (b *wheelBackend[K, V]) Remove(e *entry[K, V]) {
	if e.wheelHandle == nil {
		return
	}
	w, _ := e.wheelHandle.(*wheel.Entry[*entry[K, V]])
	if w == nil {
		return
	}
	b.wheel.Remove(w)
	e.wheelHandle = nil
}

// Fix re-tracks e after its expireAt changes: drop the old wheel
// entry (if any), then Add against the new expireAt.
func (b *wheelBackend[K, V]) Fix(e *entry[K, V]) {
	b.Remove(e)
	b.Add(e)
}

// Sweep advances the wheel to now and returns every entry whose
// revolutions reached zero this pass. The wheel has already
// untracked them; the caller's removeLocked → Remove will see
// e.wheelHandle == nil and no-op.
func (b *wheelBackend[K, V]) Sweep(now int64) []*entry[K, V] {
	expired := b.wheel.AdvanceTo(now)
	if len(expired) == 0 {
		return nil
	}
	out := make([]*entry[K, V], 0, len(expired))
	for _, we := range expired {
		if we.Payload != nil {
			we.Payload.wheelHandle = nil
			out = append(out, we.Payload)
		}
	}
	return out
}

// Len reports the wheel's currently-tracked entry count.
func (b *wheelBackend[K, V]) Len() int { return b.wheel.Len() }

// Reset clears every slot; the cache's Reset path nils per-entry
// wheelHandle pointers when entries are pooled.
func (b *wheelBackend[K, V]) Reset() { b.wheel.Reset() }
