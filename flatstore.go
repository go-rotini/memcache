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

// flatStore is a flat hash-probed shard storage layout. It uses open
// addressing with linear probing; deletion creates a tombstone so
// in-cluster probes still find later-inserted keys.
//
// Compaction triggers when (occupied + tombstones) / cap exceeds the
// load factor. If tombstones make up at least half of the in-use
// slots the rebuild stays at the current capacity (just collects the
// tombstones); otherwise the table grows by 2x. The compactions
// counter is incremented on every rebuild and surfaces as
// [Stats.Compactions].
//
// Concurrency: flatStore is NOT independently safe — the owning
// shard's RWMutex guards every method. The compactions counter is
// atomic only so [Cache.Stats] can read it without grabbing the
// shard lock.
type flatStore[K comparable, V any] struct {
	hasher       func(K) uint64
	slots        []flatSlot[K, V]
	occupied     int
	tombstones   int
	compactCount atomic.Uint64
}

// newFlatStore constructs a flatStore with the given hasher and an
// initial slot count rounded up to the next power of two (minimum
// [flatStoreInitialCap]). Power-of-two capacity is required so the
// hot path can reduce hashes via bit-mask instead of modulo.
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

// probe scans the slot table starting at the bucket implied by key.
// On a hit it returns (idx, true) for the occupied slot.
// On a miss it returns (insertIdx, false) where insertIdx is the
// first empty-or-tombstone slot encountered along the probe — the
// position a subsequent set should write to. The caller is
// responsible for maintaining the occupied / tombstones counts.
//
// The probe terminates at the first empty (not tombstone) slot
// because keys past that point must have been written before the
// empty was created (linear probing invariant).
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
			// Full sweep without an empty slot — the table is
			// pathologically full. Caller must have triggered a
			// resize before reaching this state; if not, returning
			// (insertIdx, false) gives the caller something to act
			// on (insertIdx will be >=0 unless every slot is
			// occupied with a non-matching key, which is impossible
			// when the load-factor invariant holds).
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
	if s.shouldGrowBeforeInsert() {
		s.rebuild(s.nextRebuildCap())
	}
	idx, found := s.probe(key)
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
		// Update in place; counters unchanged.
		_ = found
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

// shouldGrowBeforeInsert reports whether the next insert would push
// the in-use fraction past the configured load factor. Triggered
// before insert because we want to rebuild while we still have at
// least one empty slot to terminate probes.
func (s *flatStore[K, V]) shouldGrowBeforeInsert() bool {
	inUse := s.occupied + s.tombstones
	return inUse*flatStoreLoadFactorDen >= flatStoreLoadFactorNum*len(s.slots)
}

// shouldCompactAfterDelete reports whether the tombstone fraction
// has crossed the threshold that justifies a rebuild on its own.
// Without this check tombstone-only churn (steady-state insert/
// delete on a saturated table) would never trigger compaction.
func (s *flatStore[K, V]) shouldCompactAfterDelete() bool {
	if s.tombstones == 0 {
		return false
	}
	return s.tombstones*flatStoreTombFractionDen >= flatStoreTombFractionNum*len(s.slots)
}

// nextRebuildCap returns the slot count for the next rebuild. The
// rebuild grows the table when the live (occupied) population is
// itself above the load factor; otherwise it stays at the same cap
// (collects tombstones in place). Always returns a power of two.
func (s *flatStore[K, V]) nextRebuildCap() int {
	cur := len(s.slots)
	if s.occupied*flatStoreLoadFactorDen >= flatStoreLoadFactorNum*cur {
		return cur * 2
	}
	return cur
}

// rebuild allocates a new slots array sized to newCap (must be a
// power of two), re-inserts every occupied entry, and replaces the
// store's slots in place. The compactions counter is incremented
// regardless of whether the rebuild grew the table.
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
