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

func TestTinyLFUOnUpdate(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	a := makeTinyLFUEntry("a")
	p.OnInsert(a)
	freqBefore := p.estimate("a")
	p.OnUpdate(a) // delegates to OnAccess
	if p.estimate("a") <= freqBefore {
		t.Errorf("estimate after OnUpdate = %d, want > %d",
			p.estimate("a"), freqBefore)
	}
}

func TestTinyLFUSnapshot(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	for _, k := range []string{"a", "b", "c"} {
		p.OnInsert(makeTinyLFUEntry(k))
	}
	d, ok := p.Snapshot().(PolicyDetailTinyLFU)
	if !ok {
		t.Fatalf("Snapshot type %T, want PolicyDetailTinyLFU", p.Snapshot())
	}
	// All three should be in window initially since we did no Victim.
	if d.WindowSize+d.MainSize != 3 {
		t.Errorf("WindowSize+MainSize = %d, want 3", d.WindowSize+d.MainSize)
	}
	if d.SketchOps == 0 {
		t.Error("SketchOps should reflect observe calls")
	}
}

func TestTinyLFUPromotionNeededAlwaysTrue(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	if !p.PromotionNeeded(nil) {
		t.Error("TinyLFU PromotionNeeded should always return true")
	}
}

func TestTinyLFUSetBudget(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	p.SetBudget(200)
	if p.windowBudget+p.protectedBudget+p.probationaryBudget != 200 {
		t.Errorf("after SetBudget(200), budgets do not sum: w=%d p=%d pr=%d",
			p.windowBudget, p.protectedBudget, p.probationaryBudget)
	}
	if p.ageThreshold != tinyLFUAgeThreshold(200) {
		t.Errorf("ageThreshold = %d, want %d", p.ageThreshold, tinyLFUAgeThreshold(200))
	}
	// Zero/negative budget produces zeroed splits but a default age threshold.
	p.SetBudget(0)
	if p.windowBudget != 0 || p.protectedBudget != 0 || p.probationaryBudget != 0 {
		t.Errorf("SetBudget(0): budgets should all be zero, got w=%d p=%d pr=%d",
			p.windowBudget, p.protectedBudget, p.probationaryBudget)
	}
	if p.ageThreshold == 0 {
		t.Error("ageThreshold should fall back to a non-zero default for budget=0")
	}
}

func TestTinyLFUSplitBudgetEdgeCases(t *testing.T) {
	// budget=0 → zeros.
	w, pr, prob := tinyLFUSplitBudget(0)
	if w != 0 || pr != 0 || prob != 0 {
		t.Errorf("budget=0: got %d/%d/%d, want 0/0/0", w, pr, prob)
	}
	// budget=1 → window=1; with window>=budget the function snaps window=1
	// (still degenerate). Verify no negative outputs.
	w, pr, prob = tinyLFUSplitBudget(1)
	if w < 0 || pr < 0 || prob < 0 {
		t.Errorf("budget=1: got %d/%d/%d, want non-negative", w, pr, prob)
	}
	// budget=2 → also covers the window snap path.
	w, pr, prob = tinyLFUSplitBudget(2)
	if w+pr+prob < 2 {
		t.Errorf("budget=2 split sums = %d, want >= 2", w+pr+prob)
	}
}

func TestTinyLFUAgeThresholdZeroFallsBackToDefault(t *testing.T) {
	if got := tinyLFUAgeThreshold(0); got != 128 {
		t.Errorf("ageThreshold(0) = %d, want 128", got)
	}
	if got := tinyLFUAgeThreshold(50); got != 100 {
		t.Errorf("ageThreshold(50) = %d, want 100", got)
	}
}

func TestTinyLFUEstimateNilSketch(t *testing.T) {
	p := &tinyLFUPolicy[string, int]{}
	if got := p.estimate("k"); got != 0 {
		t.Errorf("estimate with nil sketch = %d, want 0", got)
	}
}

// TestTinyLFUObserveNilSketchIsNoop covers the "sketch == nil" early
// return branch in observe().
func TestTinyLFUObserveNilSketchIsNoop(t *testing.T) {
	p := &tinyLFUPolicy[string, int]{}
	p.observe("anything") // must not panic, must not increment ops
	if p.ops != 0 {
		t.Errorf("ops after nil-sketch observe = %d, want 0", p.ops)
	}
}

// TestTinyLFUObserveAgingResetsSketch exercises the ops>=ageThreshold
// path in observe(): once the operation counter reaches the threshold,
// the sketch is reset and ops zeroed.
func TestTinyLFUObserveAgingResetsSketch(t *testing.T) {
	// budget=1 → ageThreshold=2.
	p := newTinyLFU[string, int](1, noopHasher)
	if p.ageThreshold != 2 {
		t.Fatalf("ageThreshold = %d, want 2", p.ageThreshold)
	}
	a := makeTinyLFUEntry("a")
	p.OnInsert(a) // observe #1, ops=1
	p.OnAccess(a) // observe #2, ops=2 >= threshold → reset, ops=0
	if p.ops != 0 {
		t.Errorf("ops after aging = %d, want 0", p.ops)
	}
}

// TestTinyLFUVictimDemotesProtectedOverBudget exercises the
// "protected over budget → demote tail" branch of Victim by building
// the segment state explicitly via the public API and a hand-tuned
// budget shrink.
func TestTinyLFUVictimDemotesProtectedOverBudget(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	entries := make(map[string]*entry[string, int], len(keys))
	for _, k := range keys {
		e := makeTinyLFUEntry(k)
		p.OnInsert(e)
		entries[k] = e
	}
	// Drain Victim to push entries from window into probationary.
	for {
		v := p.Victim()
		if v == nil {
			break
		}
		v.policyData = nil
	}
	// Promote surviving entries from probationary to protected via OnAccess.
	for _, e := range entries {
		if e.policyData != nil {
			p.OnAccess(e)
		}
	}
	if p.protectedSize == 0 {
		t.Fatalf("setup failed: no entries in protected")
	}
	// Now shrink the budget so protected becomes over-budget and the
	// "demote tail to probationary" branch in Victim fires.
	p.SetBudget(2)
	for range 30 {
		v := p.Victim()
		if v == nil {
			break
		}
		v.policyData = nil
	}
}

// TestTinyLFUWindowDemotionEmptyProbationary covers the
// "probationaryTail == nil" branch of handleWindowDemotion.
// Hit by setting all budgets to 0 so probationary is "at budget"
// (0>=0) and the tail is nil.
func TestTinyLFUWindowDemotionEmptyProbationary(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	a := makeTinyLFUEntry("a")
	b := makeTinyLFUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	// Force window-over-budget while probationary is empty AND budget=0.
	p.SetBudget(0) // all budgets zero
	v := p.Victim()
	// With budget=0 and 2 entries in window: handleWindowDemotion runs
	// for the window tail; probationary is at-budget (0>=0), tail nil
	// → unlink(candidate) and return candidate.entry as victim.
	if v == nil {
		t.Fatal("expected a victim under budget=0 with window-only entries")
	}
}

// TestTinyLFUVictimEvictsProbationaryOverBudget exercises the
// "probationary over budget → evict tail" branch in Victim by setting
// up a probationary-over-budget state via SetBudget shrink.
func TestTinyLFUVictimEvictsProbationaryOverBudget(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		p.OnInsert(makeTinyLFUEntry(k))
	}
	// Force entries into probationary by draining Victim once.
	for {
		v := p.Victim()
		if v == nil {
			break
		}
		v.policyData = nil
	}
	// Shrink so probationary is over budget; window may also be over
	// but each Victim iteration handles whichever applies first.
	p.SetBudget(1)
	for range 20 {
		v := p.Victim()
		if v == nil {
			break
		}
		v.policyData = nil
	}
}

// TestTinyLFUVictimEmptyReturnsNil covers the early-return path.
func TestTinyLFUVictimEmptyReturnsNil(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	if v := p.Victim(); v != nil {
		t.Errorf("Victim on empty = %v, want nil", v)
	}
}

// TestTinyLFUVictimNoSpareMainEvictsCandidate exercises the
// "probationary at budget AND tail's freq >= candidate's" branch.
func TestTinyLFUVictimNoSpareMainEvictsCandidate(t *testing.T) {
	p := newTinyLFU[string, int](2, noopHasher) // window=1, main=1, probationary=1
	// Saturate probationary tail's frequency in the sketch.
	for range 10 {
		p.sketch.Increment(p.hashKey("hot"))
	}
	hot := makeTinyLFUEntry("hot")
	p.OnInsert(hot) // window
	// Insert a second to push window over budget, allowing demotion to probationary.
	bridge := makeTinyLFUEntry("bridge")
	p.OnInsert(bridge) // windowSize=2 over budget
	_ = p.Victim()     // window over budget, prob has spare → demote 'hot' in
	// Now probationary holds "hot" (frequent). Push another into window
	// to put it over budget again.
	cold := makeTinyLFUEntry("cold")
	p.OnInsert(cold)
	v := p.Victim() // window over budget, prob full, candidate freq < tail freq → evict candidate
	if v == nil {
		t.Fatal("expected a victim")
	}
	// Candidate is the window tail (oldest), which is "bridge" here.
	if v.key != "bridge" {
		t.Errorf("victim = %v, want bridge (low-freq window tail evicted)", v.key)
	}
}

func TestTinyLFURemoveBadPolicyDataIsNoop(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	bogus := makeTinyLFUEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnRemove(bogus)
}

func TestTinyLFUAccessBadPolicyDataIsNoop(t *testing.T) {
	p := newTinyLFU[string, int](100, noopHasher)
	bogus := makeTinyLFUEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnAccess(bogus)
}
