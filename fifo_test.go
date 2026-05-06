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
