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

	// Re-insert "a" — should go to Am, not A1in.
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
