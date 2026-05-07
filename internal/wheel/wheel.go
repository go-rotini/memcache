// Package wheel implements a hashed timing wheel suitable for
// scheduling many timers with O(1) amortized add / cancel / expire.
// It is the alternative TTL backend for the cache when
// `memcache.WithTTLBuckets(...)` is enabled — high-volume TTL
// workloads (millions of entries with TTLs) trade the per-shard
// heap's exact-time ordering for the wheel's amortized constant-
// time work per tick.
//
// The wheel hosts a fixed number of `slots` arranged in a ring;
// each tick advances the cursor by one slot. An entry whose expiry
// lies in tick T mod slots lands on slot (T mod slots) with a
// "revolutions remaining" counter for ticks beyond the first
// rotation. AdvanceTo flushes every slot whose tick passes the
// supplied now and returns the expired entries.
//
// Precision: an entry with TTL d expires at the next tick after d,
// so worst-case error is one tick. Choose tick = JanitorInterval
// (typically 30s) for diagnostics and TTL workloads where precision
// finer than a tick doesn't matter.
//
// Concurrency: the wheel is NOT safe for concurrent use. Callers
// (the cache shard) must hold the appropriate lock while calling
// Add / Remove / AdvanceTo. The matching mutex is the natural
// shard.mu.
package wheel

// Entry is the user-supplied opaque payload tracked by the wheel.
// The wheel does not interpret it — Add returns a Handle the user
// stores so subsequent Remove calls can find their slot in O(1)
// without walking the wheel.
type Entry[T any] struct {
	Payload     T
	ExpireAtNs  int64
	revolutions int       // remaining full rotations before expiry
	prev, next  *Entry[T] // intrusive doubly-linked list per slot
	slot        int       // owning slot index
}

// Wheel is the timing wheel itself.
type Wheel[T any] struct {
	slots        []*Entry[T] // each is the head of a doubly-linked list
	slotCount    int
	tickNs       int64 // duration of one slot in nanoseconds
	cursor       int   // index of the slot whose tick is currently "now"
	cursorAtNs   int64 // wall-clock time aligned to slot cursor
	originNs     int64 // time at which the wheel was last reset / created
	totalEntries int
}

// New constructs a wheel with `slotCount` slots, each covering
// `tickNs` nanoseconds of wall time. originNs is the reference
// time aligned to slot 0. slotCount must be > 0; tickNs must be > 0.
//
// Pick slotCount × tickNs ≥ longest expected TTL to keep
// `revolutions` typically zero on the hot path.
func New[T any](slotCount int, tickNs, originNs int64) *Wheel[T] {
	if slotCount <= 0 {
		slotCount = 1
	}
	if tickNs <= 0 {
		tickNs = 1
	}
	return &Wheel[T]{
		slots:      make([]*Entry[T], slotCount),
		slotCount:  slotCount,
		tickNs:     tickNs,
		cursorAtNs: originNs,
		originNs:   originNs,
	}
}

// Len returns the number of live entries across every slot.
func (w *Wheel[T]) Len() int { return w.totalEntries }

// SlotCount returns the configured slot count.
func (w *Wheel[T]) SlotCount() int { return w.slotCount }

// TickNs returns the configured tick duration.
func (w *Wheel[T]) TickNs() int64 { return w.tickNs }

// Add schedules e to expire at e.ExpireAtNs. The same Entry pointer
// is what callers pass to Remove later — the wheel writes the
// owning slot and revolutions back into the entry so removal is
// O(1). Expired entries (ExpireAtNs ≤ current cursor time) land on
// the next-to-be-processed slot with revolutions=0 so the next
// AdvanceTo surfaces them — placing them on the cursor's current
// slot would mean they wait a full rotation before being noticed
// (the cursor's slot was already processed by whichever AdvanceTo
// landed there).
func (w *Wheel[T]) Add(e *Entry[T]) {
	if e == nil {
		return
	}
	delta := e.ExpireAtNs - w.cursorAtNs
	var ticksAhead int64
	if delta > 0 {
		ticksAhead = delta / w.tickNs
		if delta%w.tickNs != 0 {
			ticksAhead++ // round up — entry expires NO LATER than the tick
		}
	}
	if ticksAhead < 1 {
		ticksAhead = 1 // expire on the next AdvanceTo, not after a full rotation
	}
	revolutions := int((ticksAhead - 1) / int64(w.slotCount))
	slot := (w.cursor + int(ticksAhead%int64(w.slotCount))) % w.slotCount
	e.revolutions = revolutions
	e.slot = slot
	w.linkHead(slot, e)
	w.totalEntries++
}

// Remove unlinks e from its slot. Safe to call on entries whose
// `slot` was never set (no-op).
func (w *Wheel[T]) Remove(e *Entry[T]) {
	if e == nil || e.slot < 0 {
		return
	}
	w.unlink(e)
	w.totalEntries--
}

// AdvanceTo flushes every slot whose tick lies at or before nowNs,
// decrementing revolutions on entries that survive and returning
// the entries whose revolutions reached zero (i.e., expired). The
// wheel's cursor is left at the slot that nowNs falls within so a
// subsequent Add lands on the correct slot.
//
// Caller is responsible for actually evicting the returned entries
// from whatever upstream storage holds them; the wheel only manages
// scheduling state.
func (w *Wheel[T]) AdvanceTo(nowNs int64) []*Entry[T] {
	if nowNs <= w.cursorAtNs {
		return nil
	}
	stepsTotal := (nowNs - w.cursorAtNs) / w.tickNs
	if stepsTotal <= 0 {
		return nil
	}
	var expired []*Entry[T]
	// Split the advance into "full revolutions" plus a "partial"
	// trailing scan. Full revolutions are handled by decrementing
	// every entry's revolution counter (each full rev = one
	// virtual crossing of every slot). The partial covers the
	// fractional revolution at the end and is processed slot-by-
	// slot so entries in slots the cursor lands on get the extra
	// crossing while entries in other slots do not.
	slotN := int64(w.slotCount)
	fullRevs := stepsTotal / slotN
	partial := stepsTotal % slotN
	if fullRevs > 0 {
		expired = append(expired, w.decrementAllRevolutions(int(fullRevs))...)
	}
	for range partial {
		w.cursor = (w.cursor + 1) % w.slotCount
		expired = append(expired, w.processSlot(w.cursor)...)
	}
	w.cursorAtNs += stepsTotal * w.tickNs
	return expired
}

// processSlot walks the cursor's slot. Entries with revolutions=0
// expire (and are unlinked); entries with revolutions>0 have
// revolutions decremented and stay in place.
func (w *Wheel[T]) processSlot(slot int) []*Entry[T] {
	var expired []*Entry[T]
	e := w.slots[slot]
	for e != nil {
		next := e.next
		if e.revolutions == 0 {
			w.unlink(e)
			w.totalEntries--
			expired = append(expired, e)
		} else {
			e.revolutions--
		}
		e = next
	}
	return expired
}

// decrementAllRevolutions subtracts n from every entry's
// revolutions counter; entries whose counter would go below zero
// expire on the spot. Used when AdvanceTo skips more than one full
// revolution.
func (w *Wheel[T]) decrementAllRevolutions(n int) []*Entry[T] {
	var expired []*Entry[T]
	for slot := range w.slots {
		e := w.slots[slot]
		for e != nil {
			next := e.next
			if e.revolutions >= n {
				e.revolutions -= n
			} else {
				w.unlink(e)
				w.totalEntries--
				expired = append(expired, e)
			}
			e = next
		}
	}
	return expired
}

// linkHead inserts e at the head of slot's list.
func (w *Wheel[T]) linkHead(slot int, e *Entry[T]) {
	head := w.slots[slot]
	e.prev = nil
	e.next = head
	if head != nil {
		head.prev = e
	}
	w.slots[slot] = e
}

// unlink removes e from its slot's list and clears its pointers.
func (w *Wheel[T]) unlink(e *Entry[T]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if e.slot >= 0 && e.slot < len(w.slots) && w.slots[e.slot] == e {
		w.slots[e.slot] = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	}
	e.prev, e.next = nil, nil
	e.slot = -1
}

// Reset clears every slot. Entries are leaked from the wheel's
// perspective; callers are responsible for releasing the
// underlying storage.
func (w *Wheel[T]) Reset() {
	for i := range w.slots {
		w.slots[i] = nil
	}
	w.totalEntries = 0
	w.cursor = 0
	w.cursorAtNs = w.originNs
}
