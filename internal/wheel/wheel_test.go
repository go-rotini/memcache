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
	// Advance to 4s: entry not yet expired.
	if exp := w.AdvanceTo(4 * tick); len(exp) != 0 {
		t.Errorf("AdvanceTo(4s) returned %d entries, want 0", len(exp))
	}
	if got := w.Len(); got != 1 {
		t.Errorf("Len mid-flight = %d, want 1", got)
	}
	// Advance to 5s: entry expires.
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
	// Advance 4 ticks (one full rev): entry survives, revolutions decremented.
	if exp := w.AdvanceTo(4 * tick); len(exp) != 0 {
		t.Errorf("after 1 rev expected no expirations, got %d", len(exp))
	}
	if e.revolutions != 1 {
		t.Errorf("after 1 rev revolutions = %d, want 1", e.revolutions)
	}
	// Advance 8 more ticks: should expire.
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
	// Skip 14 ticks (three full revs + 2 more); both expire.
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

// TestAdvanceSlotAwareDecrement is a regression test for AdvanceTo
// over-decrementing entries in slots the partial sweep didn't cross.
func TestAdvanceSlotAwareDecrement(t *testing.T) {
	// 4-slot wheel. Two entries: e1 in a slot the partial sweep
	// will cross, e2 in a slot it will NOT cross. Both have the
	// same revolutions count.
	w := New[string](4, tick, origin)
	// ExpireAtNs=10 → ticksAhead=10, slot=(0+10)%4=2, rev=(10-1)/4=2.
	e1 := &Entry[string]{Payload: "in-partial", ExpireAtNs: 10 * tick}
	// ExpireAtNs=11 → slot=(0+11)%4=3, rev=(11-1)/4=2.
	e2 := &Entry[string]{Payload: "outside-partial", ExpireAtNs: 11 * tick}
	w.Add(e1)
	w.Add(e2)
	if e1.revolutions != 2 || e2.revolutions != 2 {
		t.Fatalf("setup: e1.rev=%d e2.rev=%d, want 2 each",
			e1.revolutions, e2.revolutions)
	}
	// Advance 10 ticks: 2 full revs + 2-tick partial. The partial
	// crosses slots 1 and 2, so e1 in slot 2 sees an extra
	// crossing (3 total) and expires; e2 in slot 3 sees only the
	// 2 full-rev crossings and survives with rev=0.
	exp := w.AdvanceTo(10 * tick)
	if len(exp) != 1 {
		t.Errorf("AdvanceTo(10) expirations = %d, want 1 (only e1)", len(exp))
	}
	if e2.revolutions != 0 {
		t.Errorf("e2.revolutions = %d, want 0 (full revs decremented; partial didn't cross)",
			e2.revolutions)
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

func TestNewClampsInvalidArgs(t *testing.T) {
	w := New[string](0, 0, origin)
	if w.SlotCount() != 1 {
		t.Errorf("SlotCount with slotCount=0 fallback = %d, want 1", w.SlotCount())
	}
	if w.TickNs() != 1 {
		t.Errorf("TickNs with tickNs=0 fallback = %d, want 1", w.TickNs())
	}
	w2 := New[string](-5, -10, origin)
	if w2.SlotCount() != 1 || w2.TickNs() != 1 {
		t.Errorf("negative inputs not clamped: SlotCount=%d TickNs=%d",
			w2.SlotCount(), w2.TickNs())
	}
}

func TestTickNsReturnsConfigured(t *testing.T) {
	w := New[string](8, 250, origin)
	if got := w.TickNs(); got != 250 {
		t.Errorf("TickNs = %d, want 250", got)
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
	// Advance one tick: entry should NOT have expired (slot 2, cursor at 1).
	if exp := w.AdvanceTo(tick); len(exp) != 0 {
		t.Errorf("AdvanceTo(1 tick) = %d, want 0 (rounding)", len(exp))
	}
	if exp := w.AdvanceTo(2 * tick); len(exp) != 1 {
		t.Errorf("AdvanceTo(2 ticks) = %d, want 1", len(exp))
	}
}
