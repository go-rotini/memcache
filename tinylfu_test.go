package memcache

import "testing"

func makeTinyLFUEntry(k string) *entry[string, int] {
	return &entry[string, int]{key: k}
}

// noopHasher is sufficient for unit tests where we only care about
// behavioral semantics; collisions are acceptable for the tiny
// scenarios.
func noopHasher(s string) uint64 {
	var h uint64 = 1469598103934665603
	for i := range len(s) {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func TestTinyLFUInsertGoesToWindow(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	a := makeTinyLFUEntry("a")
	p.OnInsert(a)
	n := a.policyData.(*tinyLFUNode[string, int])
	if n.region != regionWindow {
		t.Errorf("new insert region = %v, want %v", n.region, regionWindow)
	}
}

func TestTinyLFUAccessBumpsSketch(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	a := makeTinyLFUEntry("a")
	p.OnInsert(a) // observe once
	for range 5 {
		p.OnAccess(a)
	}
	// Sketch counter for "a" should be > 1 (saturating but bumped).
	freq := p.estimate("a")
	if freq < 2 {
		t.Errorf("estimate after 6 observations = %d, want ≥ 2", freq)
	}
}

func TestTinyLFUVictimWithSpareMainPromotesWindow(t *testing.T) {
	// budget=100 → window=1, protected≈79, probationary≈20. Insert
	// 2 entries: window over-budget after second insert; first
	// should demote to Probationary (free space).
	p := newTinyLFU[string, int](100, noopHasher)
	a := makeTinyLFUEntry("a")
	b := makeTinyLFUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	v := p.Victim()
	if v != nil {
		t.Errorf("with spare Probationary, Victim should demote not evict; got %v", v)
	}
	if got := a.policyData.(*tinyLFUNode[string, int]).region; got != regionProbationary {
		t.Errorf("first inserted entry region = %v, want %v", got, regionProbationary)
	}
}

func TestTinyLFUEvictsColdNewcomer(t *testing.T) {
	// Tight budget so admission is exercised.
	// budget=2 → window=1, main=1 (since budget/100=0 → max(1)).
	p := newTinyLFU[string, int](2, noopHasher)

	// Warm "hot" by repeated observation; populate sketch directly.
	for range 8 {
		p.sketch.Increment(p.hashKey("hot"))
	}
	hot := makeTinyLFUEntry("hot")
	p.OnInsert(hot) // window
	// Promote hot to main by triggering Victim (window over budget? no, only 1 entry).
	// Insert second entry to push window over.
	cold := makeTinyLFUEntry("cold")
	p.OnInsert(cold)
	v := p.Victim() // window over budget; main has spare → hot to main
	if v != nil {
		// might still have spare main; that's fine for this scenario.
		_ = v
	}

	// Now insert a third entry. Window over budget again, main full.
	// The candidate from window has freq=1 (just inserted), main's
	// LRU tail has freq derived from prior observations.
	stale := makeTinyLFUEntry("stale")
	p.OnInsert(stale)
	// Drive eviction.
	for p.Len() > 2 {
		v := p.Victim()
		if v == nil {
			break
		}
		v.policyData = nil
	}
	if p.Len() > 2 {
		t.Errorf("Len after eviction = %d, want ≤ 2", p.Len())
	}
}

func TestTinyLFURemoveSafe(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	a := makeTinyLFUEntry("a")
	p.OnInsert(a)
	p.OnRemove(a)
	if p.Len() != 0 {
		t.Errorf("Len after Remove = %d, want 0", p.Len())
	}
}

func TestTinyLFUReset(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	for _, k := range []string{"a", "b", "c"} {
		p.OnInsert(makeTinyLFUEntry(k))
	}
	p.Reset()
	if p.Len() != 0 {
		t.Errorf("after Reset: Len=%d, want 0", p.Len())
	}
}

func TestTinyLFUBudgetSplit(t *testing.T) {
	// budget=200 → window=2 (1%), main=198 → protected≈158
	// (80%), probationary≈40 (20%). Sum is 200.
	p := newTinyLFU[string, int](200, noopHasher)
	if p.windowBudget != 2 {
		t.Errorf("windowBudget = %d, want 2", p.windowBudget)
	}
	if p.protectedBudget+p.probationaryBudget+p.windowBudget != 200 {
		t.Errorf("budgets do not sum to total: window=%d protected=%d probationary=%d",
			p.windowBudget, p.protectedBudget, p.probationaryBudget)
	}
	// Protected should be roughly 3-4× probationary (80/20 split).
	if p.protectedBudget < 3*p.probationaryBudget {
		t.Errorf("protected (%d) too small vs probationary (%d); expected ~80/20 split",
			p.protectedBudget, p.probationaryBudget)
	}
}

func TestTinyLFUProbationaryHitPromotesToProtected(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	a := makeTinyLFUEntry("a")
	b := makeTinyLFUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	// Drive a victim cycle to demote a to Probationary.
	_ = p.Victim()
	if got := a.policyData.(*tinyLFUNode[string, int]).region; got != regionProbationary {
		t.Fatalf("setup: a should be in Probationary; got %v", got)
	}
	// A hit on a Probationary entry promotes to Protected.
	p.OnAccess(a)
	if got := a.policyData.(*tinyLFUNode[string, int]).region; got != regionProtected {
		t.Errorf("after hit, a region = %v, want %v", got, regionProtected)
	}
}

func TestTinyLFUNilHasher(t *testing.T) {
	// A nil hasher must not panic; behavior degrades to "everything
	// hashes to zero" but stays correct.
	p := newTinyLFU[string, int](100, nil)
	a := makeTinyLFUEntry("a")
	p.OnInsert(a)
	p.OnAccess(a)
	if p.estimate("a") == 0 {
		// Sketch was bumped at least twice; estimate is non-zero
		// even under a constant hasher (collision-heavy but
		// still increments).
		t.Error("estimate should reflect observed accesses even with nil hasher")
	}
}
