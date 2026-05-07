package memcache

import "sync/atomic"

// flatStoreInitialCap is the starting slot count of a freshly-
// constructed flatStore. Power of two, sized to keep small caches
// out of the first grow.
const flatStoreInitialCap = 16

// flatStore growth/compaction thresholds. The combined-load trigger
// (occupied + tombstones) drives the rebuild decision; the
// tombstone-fraction threshold decides whether the rebuild grows or
// stays in place.
const (
	flatStoreLoadFactorNum   = 3 // numerator
	flatStoreLoadFactorDen   = 4 // denominator -> 0.75
	flatStoreTombFractionNum = 1 // numerator
	flatStoreTombFractionDen = 2 // denominator -> 0.5
)

// flatSlotState tags each slot in a flatStore as empty, occupied, or
// a tombstone (a deleted entry whose place must remain visible to
// linear probes that overflowed past it).
type flatSlotState uint8

const (
	flatSlotEmpty     flatSlotState = 0
	flatSlotOccupied  flatSlotState = 1
	flatSlotTombstone flatSlotState = 2
)

// flatSlot is one cell of the flatStore's slots array.
type flatSlot[K comparable, V any] struct {
	key   K
	value *entry[K, V]
	state flatSlotState
}

// flatStore is a flat open-addressing/linear-probing shard storage with
// tombstone-aware compaction. Compaction grows by 2x unless tombstones
// dominate (>=50% of in-use slots), in which case it rebuilds at the
// same capacity. compactCount is atomic so [Cache.Stats] can read it
// without the shard lock; otherwise the owning shard's mutex serializes
// all method calls.
type flatStore[K comparable, V any] struct {
	hasher       func(K) uint64
	slots        []flatSlot[K, V]
	occupied     int
	tombstones   int
	compactCount atomic.Uint64
}

// newFlatStore constructs a flatStore with hasher and a power-of-two
// slot count (minimum [flatStoreInitialCap]) so probes use bit-mask.
func newFlatStore[K comparable, V any](hasher func(K) uint64, initialCap int) *flatStore[K, V] {
	if initialCap < flatStoreInitialCap {
		initialCap = flatStoreInitialCap
	}
	slotCount := nextPowerOfTwo(initialCap)
	return &flatStore[K, V]{
		hasher: hasher,
		slots:  make([]flatSlot[K, V], slotCount),
	}
}

// probe linear-scans for key. On hit returns (idx, true); on miss
// returns (insertIdx, false) where insertIdx is the first
// empty-or-tombstone slot. Probe terminates at the first empty (not
// tombstone) slot per the linear-probing invariant.
func (s *flatStore[K, V]) probe(key K) (idx int, found bool) {
	mask := uint64(len(s.slots) - 1)
	start := s.hasher(key) & mask
	insertIdx := -1
	i := start
	for {
		slot := &s.slots[i]
		switch slot.state {
		case flatSlotEmpty:
			if insertIdx == -1 {
				insertIdx = int(i)
			}
			return insertIdx, false
		case flatSlotTombstone:
			if insertIdx == -1 {
				insertIdx = int(i)
			}
		case flatSlotOccupied:
			if slot.key == key {
				return int(i), true
			}
		}
		i = (i + 1) & mask
		if i == start {
			// Full sweep without an empty slot. Caller should have
			// triggered a resize before reaching this state.
			return insertIdx, false
		}
	}
}

func (s *flatStore[K, V]) get(key K) (*entry[K, V], bool) {
	idx, found := s.probe(key)
	if !found {
		return nil, false
	}
	return s.slots[idx].value, true
}

func (s *flatStore[K, V]) set(key K, e *entry[K, V]) {
	// Probe first; pure updates on existing keys don't trigger rebuild.
	idx, found := s.probe(key)
	if found {
		s.slots[idx].value = e
		return
	}
	// Rebuild before inserting if the load factor crossed the grow
	// threshold or probe wrapped without an empty slot (defensive).
	if idx < 0 || s.shouldGrowBeforeInsert() {
		s.rebuild(s.nextRebuildCap())
		idx, _ = s.probe(key)
	}
	slot := &s.slots[idx]
	switch slot.state {
	case flatSlotEmpty:
		slot.key = key
		slot.value = e
		slot.state = flatSlotOccupied
		s.occupied++
	case flatSlotTombstone:
		slot.key = key
		slot.value = e
		slot.state = flatSlotOccupied
		s.occupied++
		s.tombstones--
	case flatSlotOccupied:
		// Defensive: shouldn't reach here after rebuild, but if a
		// concurrent update slipped in, just overwrite.
		slot.value = e
	}
}

func (s *flatStore[K, V]) del(key K) bool {
	idx, found := s.probe(key)
	if !found {
		return false
	}
	slot := &s.slots[idx]
	var zeroK K
	slot.key = zeroK
	slot.value = nil
	slot.state = flatSlotTombstone
	s.occupied--
	s.tombstones++
	if s.shouldCompactAfterDelete() {
		s.rebuild(s.nextRebuildCap())
	}
	return true
}

func (s *flatStore[K, V]) length() int { return s.occupied }

func (s *flatStore[K, V]) clearAll() {
	for i := range s.slots {
		s.slots[i] = flatSlot[K, V]{}
	}
	s.occupied = 0
	s.tombstones = 0
}

func (s *flatStore[K, V]) each(fn func(*entry[K, V]) bool) {
	for i := range s.slots {
		if s.slots[i].state != flatSlotOccupied {
			continue
		}
		if !fn(s.slots[i].value) {
			return
		}
	}
}

func (s *flatStore[K, V]) compactions() uint64 { return s.compactCount.Load() }

// shouldGrowBeforeInsert reports whether the next insert would cross
// the load factor; rebuild while at least one empty slot remains.
func (s *flatStore[K, V]) shouldGrowBeforeInsert() bool {
	inUse := s.occupied + s.tombstones
	return inUse*flatStoreLoadFactorDen >= flatStoreLoadFactorNum*len(s.slots)
}

// shouldCompactAfterDelete reports whether the tombstone fraction has
// crossed the rebuild threshold (for tombstone-only steady-state churn).
func (s *flatStore[K, V]) shouldCompactAfterDelete() bool {
	if s.tombstones == 0 {
		return false
	}
	return s.tombstones*flatStoreTombFractionDen >= flatStoreTombFractionNum*len(s.slots)
}

// nextRebuildCap returns the next rebuild slot count: doubled when
// occupied alone is above the load factor, same otherwise. Power of two.
func (s *flatStore[K, V]) nextRebuildCap() int {
	cur := len(s.slots)
	if s.occupied*flatStoreLoadFactorDen >= flatStoreLoadFactorNum*cur {
		return cur * 2
	}
	return cur
}

// rebuild allocates a fresh power-of-two slots array of newCap and
// re-inserts every occupied entry. compactCount is incremented.
func (s *flatStore[K, V]) rebuild(newCap int) {
	old := s.slots
	s.slots = make([]flatSlot[K, V], newCap)
	s.occupied = 0
	s.tombstones = 0
	for i := range old {
		if old[i].state != flatSlotOccupied {
			continue
		}
		idx, _ := s.probe(old[i].key)
		s.slots[idx].key = old[i].key
		s.slots[idx].value = old[i].value
		s.slots[idx].state = flatSlotOccupied
		s.occupied++
	}
	s.compactCount.Add(1)
}
