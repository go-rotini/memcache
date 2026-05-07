package memcache

import "testing"

func makeTwoQEntry(k string) *entry[string, int] {
	return &entry[string, int]{key: k}
}

func TestTwoQInsertNewToA1in(t *testing.T) {
	p := newTwoQ[string, int](8)
	a := makeTwoQEntry("a")
	p.OnInsert(a)
	n := a.policyData.(*twoQNode[string, int])
	if n.inAm {
		t.Error("new key should land in A1in, not Am")
	}
}

func TestTwoQGhostHitPromotesToAm(t *testing.T) {
	p := newTwoQ[string, int](2) // inBudget=1, amBudget=1, outBudget=1
	a := makeTwoQEntry("a")
	b := makeTwoQEntry("b")
	p.OnInsert(a)
	p.OnInsert(b) // a should now be over budget in A1in

	v := p.Victim()
	if v == nil || v.key != "a" {
		t.Fatalf("expected a evicted to A1out, got %v", v)
	}
	if _, ok := p.outSet["a"]; !ok {
		t.Error("evicted A1in entry should land in A1out ghost")
	}
	v.policyData = nil

	// Re-insert "a"; should go to Am, not A1in.
	a2 := makeTwoQEntry("a")
	p.OnInsert(a2)
	n := a2.policyData.(*twoQNode[string, int])
	if !n.inAm {
		t.Error("ghost-hit re-insert should land in Am")
	}
}

func TestTwoQA1inHitDoesNotPromote(t *testing.T) {
	p := newTwoQ[string, int](8)
	a := makeTwoQEntry("a")
	p.OnInsert(a)
	p.OnAccess(a) // a hit while in A1in
	n := a.policyData.(*twoQNode[string, int])
	if n.inAm {
		t.Error("hit on A1in entry should not promote to Am")
	}
}

func TestTwoQAmHitMovesToMRU(t *testing.T) {
	p := newTwoQ[string, int](4)
	// Push two entries directly to Am via ghost path.
	a := makeTwoQEntry("a")
	b := makeTwoQEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	// Force "a" through ghost.
	p.recordGhost("a")
	p.recordGhost("b")
	a2 := makeTwoQEntry("a")
	b2 := makeTwoQEntry("b")
	p.OnInsert(a2) // → Am
	p.OnInsert(b2) // → Am, becomes MRU. amHead=b2, amTail=a2.

	if p.amHead.entry.key != "b" || p.amTail.entry.key != "a" {
		t.Fatalf("Am after dual ghost insert: head=%v tail=%v",
			p.amHead.entry.key, p.amTail.entry.key)
	}
	p.OnAccess(a2) // a should move to MRU
	if p.amHead.entry.key != "a" {
		t.Errorf("after access, amHead = %s, want a", p.amHead.entry.key)
	}
}

func TestTwoQRemove(t *testing.T) {
	p := newTwoQ[string, int](4)
	a := makeTwoQEntry("a")
	p.OnInsert(a)
	p.OnRemove(a)
	if p.Len() != 0 {
		t.Errorf("Len after Remove = %d, want 0", p.Len())
	}
}

func TestTwoQReset(t *testing.T) {
	p := newTwoQ[string, int](4)
	for _, k := range []string{"a", "b"} {
		p.OnInsert(makeTwoQEntry(k))
	}
	p.recordGhost("ghost")
	p.Reset()
	if p.Len() != 0 || len(p.outSet) != 0 {
		t.Errorf("after Reset: Len=%d ghosts=%d", p.Len(), len(p.outSet))
	}
}

func TestTwoQBudgetSplit(t *testing.T) {
	// budget=8 → in≈2 (25%), am=6, out=4 (50%).
	p := newTwoQ[string, int](8)
	if p.inBudget != 2 || p.amBudget != 6 || p.outBudget != 4 {
		t.Errorf("budget split: in=%d am=%d out=%d (want 2,6,4)",
			p.inBudget, p.amBudget, p.outBudget)
	}
}

func TestTwoQRecordGhostDuplicateNoop(t *testing.T) {
	p := newTwoQ[string, int](4)
	p.recordGhost("k")
	p.recordGhost("k") // duplicate; should early-return
	if p.outSize != 1 {
		t.Errorf("outSize after dup recordGhost = %d, want 1", p.outSize)
	}
}

func TestTwoQGhostBoundedByBudget(t *testing.T) {
	p := newTwoQ[string, int](2) // outBudget=1
	p.recordGhost("a")
	p.recordGhost("b")
	if p.outSize != 1 {
		t.Errorf("ghost size after 2 records (budget=1) = %d, want 1", p.outSize)
	}
	if _, ok := p.outSet["a"]; ok {
		t.Error("oldest ghost should be evicted")
	}
}

func TestTwoQUpdateBehavesLikeAccess(t *testing.T) {
	// Place two entries in Am via the ghost path so that OnUpdate/OnAccess
	// promote within Am.
	p := newTwoQ[string, int](4)
	p.recordGhost("a")
	p.recordGhost("b")
	a := makeTwoQEntry("a")
	b := makeTwoQEntry("b")
	p.OnInsert(a) // → Am
	p.OnInsert(b) // → Am, MRU
	p.OnUpdate(a) // delegates to OnAccess
	if p.amHead.entry.key != "a" {
		t.Errorf("after update on a, amHead = %s, want a", p.amHead.entry.key)
	}
}

func TestTwoQSnapshot(t *testing.T) {
	p := newTwoQ[string, int](4)
	a := makeTwoQEntry("a")
	p.OnInsert(a)
	p.recordGhost("ghost")
	d, ok := p.Snapshot().(PolicyDetail2Q)
	if !ok {
		t.Fatalf("Snapshot type %T, want PolicyDetail2Q", p.Snapshot())
	}
	if d.A1inSize != 1 {
		t.Errorf("A1inSize = %d, want 1", d.A1inSize)
	}
	if d.A1outSize != 1 {
		t.Errorf("A1outSize = %d, want 1", d.A1outSize)
	}
}

func TestTwoQPromotionNeededAlwaysTrue(t *testing.T) {
	p := newTwoQ[string, int](4)
	if !p.PromotionNeeded(nil) {
		t.Error("2Q PromotionNeeded should always return true")
	}
}

func TestTwoQSetBudget(t *testing.T) {
	p := newTwoQ[string, int](4)
	for _, k := range []string{"a", "b", "c", "d"} {
		p.recordGhost(k)
	}
	p.SetBudget(2) // outBudget shrinks; outSize must trim
	if p.outSize > p.outBudget {
		t.Errorf("after SetBudget, outSize=%d > outBudget=%d", p.outSize, p.outBudget)
	}
}

func TestTwoQSplitBudgetEdgeCases(t *testing.T) {
	in, am, out := twoQSplitBudget(0)
	if in != 0 || am != 0 || out != 0 {
		t.Errorf("budget=0: got %d/%d/%d, want 0/0/0", in, am, out)
	}
	in, am, out = twoQSplitBudget(1)
	if in < 1 || am < 1 || out < 1 {
		t.Errorf("budget=1: got %d/%d/%d, want >=1 each", in, am, out)
	}
}

func TestTwoQVictimEmptyReturnsNil(t *testing.T) {
	p := newTwoQ[string, int](4)
	if v := p.Victim(); v != nil {
		t.Errorf("Victim on empty = %v, want nil", v)
	}
}

func TestTwoQVictimFromAmWhenA1inWithinBudget(t *testing.T) {
	// budget=2 (in=1, am=1). Push one to Am via ghost, one to A1in,
	// then make am over-budget.
	p := newTwoQ[string, int](2)
	p.recordGhost("a")
	a := makeTwoQEntry("a")
	p.OnInsert(a) // → Am (ghost rebirth), amSize=1 (== budget, not over)
	// Add a second Am entry by ghost-promoting another key.
	p.recordGhost("b")
	b := makeTwoQEntry("b")
	p.OnInsert(b) // → Am, amSize=2 > amBudget=1
	v := p.Victim()
	if v == nil {
		t.Fatal("Victim returned nil")
	}
	// Am tail is "a" (since b is MRU).
	if v.key != "a" {
		t.Errorf("Victim = %s, want a", v.key)
	}
}

// TestTwoQVictimFallbackInPath drives the third branch in Victim:
// neither A1in nor Am is alone over budget but inHead exists.
func TestTwoQVictimFallbackInPath(t *testing.T) {
	p := newTwoQ[string, int](8)
	a := makeTwoQEntry("a")
	p.OnInsert(a) // → A1in, inSize=1, inBudget=2
	// Neither queue is over budget; fallback path picks A1in's head.
	v := p.Victim()
	if v == nil || v.key != "a" {
		t.Errorf("fallback Victim = %v, want a", v)
	}
}

// TestTwoQVictimFallbackAmPath drives the fourth branch in Victim:
// A1in empty, Am within budget, am tail is the only remaining entry.
func TestTwoQVictimFallbackAmPath(t *testing.T) {
	p := newTwoQ[string, int](8)
	p.recordGhost("a")
	a := makeTwoQEntry("a")
	p.OnInsert(a) // → Am
	v := p.Victim()
	if v == nil || v.key != "a" {
		t.Errorf("fallback Am victim = %v, want a", v)
	}
}

func TestTwoQResetClearsGhost(t *testing.T) {
	p := newTwoQ[string, int](4)
	a := makeTwoQEntry("a")
	p.OnInsert(a)
	p.recordGhost("ghost1")
	p.recordGhost("ghost2")
	p.Reset()
	if len(p.outSet) != 0 || p.outSize != 0 {
		t.Errorf("after Reset: outSet=%d outSize=%d", len(p.outSet), p.outSize)
	}
	if p.outHead != nil || p.outTail != nil {
		t.Error("after Reset, ghost head/tail should be nil")
	}
}

// TestTwoQResetClearsBothQueues iterates the Am list during Reset by
// putting entries into both A1in and Am beforehand.
func TestTwoQResetClearsBothQueues(t *testing.T) {
	p := newTwoQ[string, int](4)
	a := makeTwoQEntry("a")
	p.OnInsert(a) // A1in
	p.recordGhost("b")
	b := makeTwoQEntry("b")
	p.OnInsert(b) // Am via ghost rebirth
	p.recordGhost("c")
	c := makeTwoQEntry("c")
	p.OnInsert(c) // Am via ghost rebirth (multi-element Am list)
	p.Reset()
	if p.Len() != 0 {
		t.Errorf("after Reset: Len=%d, want 0", p.Len())
	}
	if a.policyData != nil || b.policyData != nil || c.policyData != nil {
		t.Error("after Reset, entries' policyData must be cleared")
	}
}

func TestTwoQRemoveBadPolicyDataIsNoop(t *testing.T) {
	p := newTwoQ[string, int](4)
	bogus := makeTwoQEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnRemove(bogus)
}

func TestTwoQAccessBadPolicyDataIsNoop(t *testing.T) {
	p := newTwoQ[string, int](4)
	bogus := makeTwoQEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnAccess(bogus)
}

// TestTwoQAccessOnAmHeadIsNoop covers the "n == amHead" early return.
// TestTwoQRemoveMiddleA1in covers the unlinkIn middle-of-list path
// (n.prev != nil AND n.next != nil).
func TestTwoQRemoveMiddleA1in(t *testing.T) {
	p := newTwoQ[string, int](16) // generous so nothing evicts
	a := makeTwoQEntry("a")
	b := makeTwoQEntry("b")
	c := makeTwoQEntry("c")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnInsert(c)
	p.OnRemove(b) // b in middle of A1in
	if p.Len() != 2 {
		t.Errorf("Len after middle remove = %d, want 2", p.Len())
	}
}

// TestTwoQUnlinkInAlreadyDetached covers the default branch in unlinkIn
// when the caller passes a node already detached from any list.
func TestTwoQUnlinkInAlreadyDetached(t *testing.T) {
	p := newTwoQ[string, int](4)
	// Hand-build a detached node and call unlinkIn directly.
	stray := &twoQNode[string, int]{}
	p.unlinkIn(stray) // must be a no-op, must not panic, must not touch inSize
	if p.inSize != 0 {
		t.Errorf("inSize after stray unlink = %d, want 0", p.inSize)
	}
}

func TestTwoQAccessOnAmHeadIsNoop(t *testing.T) {
	p := newTwoQ[string, int](4)
	p.recordGhost("a")
	a := makeTwoQEntry("a")
	p.OnInsert(a) // → Am, becomes amHead
	p.OnAccess(a) // already at head
	if p.amHead.entry.key != "a" {
		t.Errorf("amHead = %s, want a", p.amHead.entry.key)
	}
}
