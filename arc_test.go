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
