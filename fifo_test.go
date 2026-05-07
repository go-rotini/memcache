package memcache

import "testing"

func makeFIFOEntry(k string) *entry[string, int] {
	return &entry[string, int]{key: k}
}

func TestFIFOInsertAppendsTail(t *testing.T) {
	p := newFIFO[string, int]()
	a := makeFIFOEntry("a")
	b := makeFIFOEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	if p.head.entry.key != "a" {
		t.Errorf("head (oldest) = %s, want a", p.head.entry.key)
	}
	if p.tail.entry.key != "b" {
		t.Errorf("tail (newest) = %s, want b", p.tail.entry.key)
	}
}

func TestFIFOAccessIsNoop(t *testing.T) {
	p := newFIFO[string, int]()
	a := makeFIFOEntry("a")
	b := makeFIFOEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnAccess(a) // FIFO does NOT promote
	if p.head.entry.key != "a" {
		t.Error("FIFO access should not change ordering")
	}
}

func TestFIFOVictimReturnsOldest(t *testing.T) {
	p := newFIFO[string, int]()
	a := makeFIFOEntry("a")
	b := makeFIFOEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	v := p.Victim()
	if v == nil || v.key != "a" {
		t.Errorf("Victim = %v, want a", v)
	}
}

func TestFIFORemoveSafe(t *testing.T) {
	p := newFIFO[string, int]()
	a := makeFIFOEntry("a")
	p.OnInsert(a)
	p.OnRemove(a)
	if p.Len() != 0 {
		t.Errorf("Len after Remove = %d, want 0", p.Len())
	}
}

func TestFIFOReset(t *testing.T) {
	p := newFIFO[string, int]()
	for _, k := range []string{"a", "b", "c"} {
		p.OnInsert(makeFIFOEntry(k))
	}
	p.Reset()
	if p.Len() != 0 {
		t.Errorf("after Reset: Len=%d, want 0", p.Len())
	}
}

func TestFIFOOnAccessAndOnUpdateAreNoops(t *testing.T) {
	p := newFIFO[string, int]()
	a := makeFIFOEntry("a")
	b := makeFIFOEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	headBefore := p.head.entry.key
	tailBefore := p.tail.entry.key
	p.OnAccess(a)
	p.OnUpdate(a)
	p.OnAccess(b)
	p.OnUpdate(b)
	if p.head.entry.key != headBefore || p.tail.entry.key != tailBefore {
		t.Errorf("OnAccess/OnUpdate must not change ordering: head=%q tail=%q, want %q/%q",
			p.head.entry.key, p.tail.entry.key, headBefore, tailBefore)
	}
}

func TestFIFOSetBudgetIsNoop(t *testing.T) {
	p := newFIFO[string, int]()
	a := makeFIFOEntry("a")
	p.OnInsert(a)
	p.SetBudget(0)
	p.SetBudget(100)
	p.SetBudget(-1)
	if p.Len() != 1 {
		t.Errorf("Len after SetBudget = %d, want 1", p.Len())
	}
}

func TestFIFOVictimOnEmptyReturnsNil(t *testing.T) {
	p := newFIFO[string, int]()
	if v := p.Victim(); v != nil {
		t.Errorf("Victim on empty policy = %v, want nil", v)
	}
}

func TestFIFORemoveMiddleEntry(t *testing.T) {
	p := newFIFO[string, int]()
	a := makeFIFOEntry("a")
	b := makeFIFOEntry("b")
	c := makeFIFOEntry("c")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnInsert(c) // a-b-c in FIFO order
	p.OnRemove(b) // remove middle
	if p.Len() != 2 {
		t.Errorf("Len = %d, want 2", p.Len())
	}
	// Head should still be 'a', tail still 'c'.
	if p.head.entry.key != "a" || p.tail.entry.key != "c" {
		t.Errorf("after middle remove: head=%q tail=%q, want a/c",
			p.head.entry.key, p.tail.entry.key)
	}
}

func TestFIFORemoveBadPolicyDataIsNoop(t *testing.T) {
	p := newFIFO[string, int]()
	bogus := makeFIFOEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnRemove(bogus)
	if p.Len() != 0 {
		t.Errorf("Len = %d, want 0", p.Len())
	}
}
