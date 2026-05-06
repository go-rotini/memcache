package wheel

import (
	"testing"
	"time"
)

const (
	tick   = int64(time.Second)
	origin = int64(0)
)

func TestNewWheelEmpty(t *testing.T) {
	w := New[string](16, tick, origin)
	if w.Len() != 0 {
		t.Errorf("Len = %d, want 0", w.Len())
	}
	if w.SlotCount() != 16 {
		t.Errorf("SlotCount = %d, want 16", w.SlotCount())
	}
}

func TestAddAndAdvanceExpires(t *testing.T) {
	w := New[string](16, tick, origin)
	e := &Entry[string]{Payload: "k", ExpireAtNs: 5 * tick}
	w.Add(e)
	if got := w.Len(); got != 1 {
		t.Fatalf("Len after Add = %d, want 1", got)
	}
	// Advance to 4s — entry not yet expired.
	if exp := w.AdvanceTo(4 * tick); len(exp) != 0 {
		t.Errorf("AdvanceTo(4s) returned %d entries, want 0", len(exp))
	}
	if got := w.Len(); got != 1 {
		t.Errorf("Len mid-flight = %d, want 1", got)
	}
	// Advance to 5s — entry expires.
	exp := w.AdvanceTo(5 * tick)
	if len(exp) != 1 || exp[0].Payload != "k" {
		t.Errorf("AdvanceTo(5s) returned %v, want [k]", exp)
	}
	if got := w.Len(); got != 0 {
		t.Errorf("Len after expire = %d, want 0", got)
	}
}

func TestRemoveBeforeExpire(t *testing.T) {
	w := New[string](16, tick, origin)
	e := &Entry[string]{Payload: "k", ExpireAtNs: 10 * tick}
	w.Add(e)
	w.Remove(e)
	if got := w.Len(); got != 0 {
		t.Fatalf("Len after Remove = %d, want 0", got)
	}
	exp := w.AdvanceTo(10 * tick)
	if len(exp) != 0 {
		t.Errorf("AdvanceTo after Remove returned %d entries, want 0", len(exp))
	}
}

func TestMultiRevolution(t *testing.T) {
	// 4-slot wheel; entry expires 10 ticks out → 2 full revolutions.
	w := New[string](4, tick, origin)
	e := &Entry[string]{Payload: "k", ExpireAtNs: 10 * tick}
	w.Add(e)
	if e.revolutions != 2 {
		t.Errorf("revolutions = %d, want 2", e.revolutions)
	}
	// Advance 4 ticks (one full rev) — entry survives, revolutions decremented.
	if exp := w.AdvanceTo(4 * tick); len(exp) != 0 {
		t.Errorf("after 1 rev expected no expirations, got %d", len(exp))
	}
	if e.revolutions != 1 {
		t.Errorf("after 1 rev revolutions = %d, want 1", e.revolutions)
	}
	// Advance 8 more ticks — should expire.
	exp := w.AdvanceTo(12 * tick)
	if len(exp) != 1 {
		t.Errorf("AdvanceTo(12s) = %d entries, want 1", len(exp))
	}
}

func TestAdvancePastFullRevolution(t *testing.T) {
	w := New[string](4, tick, origin)
	// Two entries: one expires in this rev, one in 3 revs.
	e1 := &Entry[string]{Payload: "soon", ExpireAtNs: 2 * tick}
	e2 := &Entry[string]{Payload: "later", ExpireAtNs: 14 * tick}
	w.Add(e1)
	w.Add(e2)
	// Skip 14 ticks — three full revs + 2 more — both expire.
	exp := w.AdvanceTo(14 * tick)
	if len(exp) != 2 {
		t.Errorf("expected 2 expirations, got %d", len(exp))
	}
}

func TestAddInThePast(t *testing.T) {
	w := New[string](4, tick, origin)
	w.AdvanceTo(5 * tick) // cursor at 5s
	e := &Entry[string]{Payload: "k", ExpireAtNs: 1 * tick}
	w.Add(e)
	// Past-expiry entries land on the current slot with revolutions=0;
	// the next AdvanceTo flushes them.
	exp := w.AdvanceTo(6 * tick)
	if len(exp) != 1 {
		t.Errorf("past-expiry Add: AdvanceTo returned %d, want 1", len(exp))
	}
}

func TestNilEntrySafe(t *testing.T) {
	w := New[string](4, tick, origin)
	w.Add(nil)    // must not panic
	w.Remove(nil) // must not panic
	if w.Len() != 0 {
		t.Errorf("nil Add changed Len: %d", w.Len())
	}
}

func TestResetClears(t *testing.T) {
	w := New[string](4, tick, origin)
	w.Add(&Entry[string]{Payload: "a", ExpireAtNs: 2 * tick})
	w.Add(&Entry[string]{Payload: "b", ExpireAtNs: 3 * tick})
	w.Reset()
	if w.Len() != 0 {
		t.Errorf("after Reset Len = %d, want 0", w.Len())
	}
	if exp := w.AdvanceTo(10 * tick); len(exp) != 0 {
		t.Errorf("after Reset, AdvanceTo returned %d", len(exp))
	}
}

func TestExpirationOrderWithinSlot(t *testing.T) {
	// Entries that hash to the same slot expire together.
	w := New[string](4, tick, origin)
	w.Add(&Entry[string]{Payload: "first", ExpireAtNs: 2 * tick})
	w.Add(&Entry[string]{Payload: "second", ExpireAtNs: 2 * tick})
	exp := w.AdvanceTo(2 * tick)
	if len(exp) != 2 {
		t.Errorf("expected both entries, got %d", len(exp))
	}
}

func TestRevolutionRoundsUp(t *testing.T) {
	// An entry with TTL of 1.5 ticks should fall on the slot
	// covering 2 ticks ahead, not 1 (rounding up keeps the
	// expiration NO LATER than the requested time).
	w := New[string](4, tick, origin)
	e := &Entry[string]{Payload: "k", ExpireAtNs: tick + tick/2}
	w.Add(e)
	// Advance one tick — entry should NOT have expired (it lives
	// in slot 2, cursor is at 1).
	if exp := w.AdvanceTo(tick); len(exp) != 0 {
		t.Errorf("AdvanceTo(1 tick) = %d, want 0 (rounding)", len(exp))
	}
	if exp := w.AdvanceTo(2 * tick); len(exp) != 1 {
		t.Errorf("AdvanceTo(2 ticks) = %d, want 1", len(exp))
	}
}
