package memcache

import "testing"

func makeARCEntry(k string) *entry[string, int] {
	return &entry[string, int]{key: k}
}

func TestARCInsertNewToT1(t *testing.T) {
	p := newARC[string, int](8)
	a := makeARCEntry("a")
	p.OnInsert(a)
	n := a.policyData.(*arcNode[string, int])
	if n.inT2 {
		t.Error("first-time insert should land in T1, not T2")
	}
}

func TestARCAccessMovesT1ToT2(t *testing.T) {
	p := newARC[string, int](8)
	a := makeARCEntry("a")
	p.OnInsert(a)
	p.OnAccess(a)
	n := a.policyData.(*arcNode[string, int])
	if !n.inT2 {
		t.Error("access on T1 entry should move it to T2")
	}
	if p.t2Size != 1 || p.t1Size != 0 {
		t.Errorf("after access: t1=%d t2=%d, want 0,1", p.t1Size, p.t2Size)
	}
}

func TestARCAccessOnT2PromotesToHead(t *testing.T) {
	p := newARC[string, int](8)
	a := makeARCEntry("a")
	b := makeARCEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnAccess(a)
	p.OnAccess(b) // both in T2, b is MRU
	if p.t2Head.entry.key != "b" {
		t.Fatalf("t2Head = %s, want b", p.t2Head.entry.key)
	}
	p.OnAccess(a) // a should move to MRU
	if p.t2Head.entry.key != "a" {
		t.Errorf("after access on a, t2Head = %s, want a", p.t2Head.entry.key)
	}
}

func TestARCVictimFromT1WhenLargerThanP(t *testing.T) {
	p := newARC[string, int](4)
	a := makeARCEntry("a")
	b := makeARCEntry("b")
	p.OnInsert(a)
	p.OnInsert(b) // both in T1, p=0
	v := p.Victim()
	if v == nil {
		t.Fatal("expected victim")
	}
	// p=0 and t2 is empty → evict from T1; oldest first.
	if v.key != "a" {
		t.Errorf("victim = %s, want a", v.key)
	}
	if !p.inB1("a") {
		t.Error("evicted T1 entry should be in B1")
	}
}

func TestARCGhostHitInB1IncreasesP(t *testing.T) {
	p := newARC[string, int](4)
	a := makeARCEntry("a")
	b := makeARCEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	v := p.Victim() // a → B1
	v.policyData = nil
	pBefore := p.p

	a2 := makeARCEntry("a")
	p.OnInsert(a2) // ghost hit on B1
	if p.p <= pBefore {
		t.Errorf("p should increase on B1 ghost hit; before=%d after=%d",
			pBefore, p.p)
	}
	// Re-inserted entry should be in T2 (per ARC paper case II).
	if !a2.policyData.(*arcNode[string, int]).inT2 {
		t.Error("B1 ghost hit re-insert should land in T2")
	}
}

func TestARCGhostHitInB2DecreasesP(t *testing.T) {
	p := newARC[string, int](4)
	p.p = 4 // start at maximum so we observe a decrement
	// Force-record a key in B2.
	p.recordB2("k")
	pBefore := p.p
	k := makeARCEntry("k")
	p.OnInsert(k)
	if p.p >= pBefore {
		t.Errorf("p should decrease on B2 ghost hit; before=%d after=%d",
			pBefore, p.p)
	}
	if !k.policyData.(*arcNode[string, int]).inT2 {
		t.Error("B2 ghost hit re-insert should land in T2")
	}
}

func TestARCRemove(t *testing.T) {
	p := newARC[string, int](4)
	a := makeARCEntry("a")
	p.OnInsert(a)
	p.OnRemove(a)
	if p.Len() != 0 {
		t.Errorf("Len after Remove = %d, want 0", p.Len())
	}
}

func TestARCReset(t *testing.T) {
	p := newARC[string, int](4)
	for _, k := range []string{"a", "b"} {
		p.OnInsert(makeARCEntry(k))
	}
	p.recordB1("ghost1")
	p.recordB2("ghost2")
	p.Reset()
	if p.Len() != 0 || len(p.b1Set) != 0 || len(p.b2Set) != 0 || p.p != 0 {
		t.Errorf("after Reset: Len=%d b1=%d b2=%d p=%d",
			p.Len(), len(p.b1Set), len(p.b2Set), p.p)
	}
}

func TestARCB1BoundedByCMinusT1(t *testing.T) {
	// |T1|+|B1| ≤ c. With c=2 and |T1|=0, |B1| ≤ 2.
	p := newARC[string, int](2)
	for _, k := range []string{"a", "b", "c", "d"} {
		p.recordB1(k)
	}
	if p.b1Size > 2 {
		t.Errorf("|B1| = %d, want ≤ 2 when |T1|=0 and c=2", p.b1Size)
	}
}

func TestARCUpdateBehavesLikeAccess(t *testing.T) {
	p := newARC[string, int](4)
	a := makeARCEntry("a")
	p.OnInsert(a)
	p.OnUpdate(a) // delegates to OnAccess; T1 -> T2
	n := a.policyData.(*arcNode[string, int])
	if !n.inT2 {
		t.Error("OnUpdate on T1 entry should promote to T2")
	}
}

func TestARCSnapshot(t *testing.T) {
	p := newARC[string, int](4)
	a := makeARCEntry("a")
	b := makeARCEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnAccess(a) // a -> T2
	p.recordB1("ghost-1")
	p.recordB2("ghost-2")
	p.p = 3
	d, ok := p.Snapshot().(PolicyDetailARC)
	if !ok {
		t.Fatalf("Snapshot type %T, want PolicyDetailARC", p.Snapshot())
	}
	if d.T1Size != 1 || d.T2Size != 1 {
		t.Errorf("T1=%d T2=%d, want 1/1", d.T1Size, d.T2Size)
	}
	if d.B1Size != 1 || d.B2Size != 1 {
		t.Errorf("B1=%d B2=%d, want 1/1", d.B1Size, d.B2Size)
	}
	if d.P != 3 {
		t.Errorf("P=%d, want 3", d.P)
	}
}

func TestARCPromotionNeededAlwaysTrue(t *testing.T) {
	p := newARC[string, int](4)
	if !p.PromotionNeeded(nil) {
		t.Error("ARC PromotionNeeded should always return true")
	}
}

func TestARCSetBudget(t *testing.T) {
	p := newARC[string, int](8)
	// Pile up ghosts so SetBudget has work to trim.
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		p.recordB1(k)
	}
	for _, k := range []string{"x", "y", "z", "u", "v", "w"} {
		p.recordB2(k)
	}
	p.p = 6
	// Reduce budget; expect b1Size <= c-t1Size, b2Size <= 2c-t2Size, p <= c.
	p.SetBudget(2)
	if p.c != 2 {
		t.Errorf("c = %d, want 2", p.c)
	}
	if p.p > p.c {
		t.Errorf("p = %d, want <= c (%d)", p.p, p.c)
	}
	if p.b1Size > p.c {
		t.Errorf("b1Size = %d, want <= c (%d)", p.b1Size, p.c)
	}
	if p.b2Size > 2*p.c {
		t.Errorf("b2Size = %d, want <= 2c (%d)", p.b2Size, 2*p.c)
	}
	// Negative or zero budget should clamp to 1.
	p.SetBudget(0)
	if p.c < 1 {
		t.Errorf("c after SetBudget(0) = %d, want >= 1", p.c)
	}
}

func TestARCVictimEmpty(t *testing.T) {
	p := newARC[string, int](4)
	if v := p.Victim(); v != nil {
		t.Errorf("Victim on empty = %v, want nil", v)
	}
}

func TestARCVictimT2WhenT1WithinP(t *testing.T) {
	// Promote both entries to T2 so eviction must come from T2.
	p := newARC[string, int](4)
	a := makeARCEntry("a")
	b := makeARCEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnAccess(a) // a -> T2
	p.OnAccess(b) // b -> T2; T1 empty, T2={b,a}
	p.p = 4       // |T1|=0 not > p; pick from T2
	v := p.Victim()
	if v == nil {
		t.Fatal("Victim returned nil")
	}
	// T2 tail is 'a' (b was promoted second).
	if v.key != "a" {
		t.Errorf("Victim = %v, want a", v.key)
	}
	if !p.inB2("a") {
		t.Error("evicted T2 entry should be in B2")
	}
}

func TestARCRemoveOnT2Entry(t *testing.T) {
	p := newARC[string, int](4)
	a := makeARCEntry("a")
	p.OnInsert(a)
	p.OnAccess(a) // a -> T2
	p.OnRemove(a)
	if p.Len() != 0 {
		t.Errorf("Len after T2 remove = %d, want 0", p.Len())
	}
}

func TestARCRemoveBadPolicyDataIsNoop(t *testing.T) {
	p := newARC[string, int](4)
	bogus := makeARCEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnRemove(bogus)
}

func TestARCRemoveFromB1AbsentKey(t *testing.T) {
	p := newARC[string, int](4)
	p.removeFromB1("absent") // no-op
	p.removeFromB2("absent") // no-op
	if p.b1Size != 0 || p.b2Size != 0 {
		t.Error("removeFromB1/B2 on absent key should not affect size")
	}
}

func TestARCRecordB1B2DuplicateNoop(t *testing.T) {
	p := newARC[string, int](4)
	p.recordB1("k")
	p.recordB1("k") // duplicate; early return
	if p.b1Size != 1 {
		t.Errorf("b1Size after dup record = %d, want 1", p.b1Size)
	}
	p.recordB2("k")
	p.recordB2("k") // duplicate; early return
	if p.b2Size != 1 {
		t.Errorf("b2Size after dup record = %d, want 1", p.b2Size)
	}
}

func TestARCAccessBadPolicyDataIsNoop(t *testing.T) {
	p := newARC[string, int](4)
	bogus := makeARCEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnAccess(bogus)
}

func TestARCResetClearsBudgetState(t *testing.T) {
	p := newARC[string, int](4)
	a := makeARCEntry("a")
	b := makeARCEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnAccess(a) // T1->T2
	p.recordB1("ghost1")
	p.recordB2("ghost2")
	p.p = 2
	p.Reset()
	if p.t1Size != 0 || p.t2Size != 0 || p.b1Size != 0 || p.b2Size != 0 {
		t.Errorf("after Reset: t1=%d t2=%d b1=%d b2=%d, want all 0",
			p.t1Size, p.t2Size, p.b1Size, p.b2Size)
	}
	if p.p != 0 {
		t.Errorf("after Reset p=%d, want 0", p.p)
	}
}
