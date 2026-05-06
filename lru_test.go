package memcache

import "testing"

func makeLRUEntry(k string) *entry[string, int] {
	return &entry[string, int]{key: k}
}

func TestLRUInsertOrdersHeadFirst(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	b := makeLRUEntry("b")
	p.OnInsert(a) // head & tail
	p.OnInsert(b) // new head
	if p.head.entry.key != "b" {
		t.Errorf("head = %s, want b", p.head.entry.key)
	}
	if p.tail.entry.key != "a" {
		t.Errorf("tail = %s, want a", p.tail.entry.key)
	}
}

func TestLRUVictimReturnsTail(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	b := makeLRUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	v := p.Victim()
	if v == nil || v.key != "a" {
		t.Errorf("Victim = %v, want a", v)
	}
}

func TestLRUAccessMovesToHead(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	b := makeLRUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnAccess(a) // a should move to head
	if p.head.entry.key != "a" {
		t.Errorf("after access, head = %s, want a", p.head.entry.key)
	}
	if p.tail.entry.key != "b" {
		t.Errorf("after access, tail = %s, want b", p.tail.entry.key)
	}
}

func TestLRUAccessOnHeadIsNoop(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	p.OnInsert(a)
	p.OnAccess(a)
	if p.head != p.tail || p.head.entry.key != "a" {
		t.Error("single-entry LRU should still have the entry as head and tail")
	}
}

func TestLRURemoveDetaches(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	p.OnInsert(a)
	p.OnRemove(a)
	if p.Len() != 0 {
		t.Errorf("Len after Remove = %d, want 0", p.Len())
	}
	if a.policyData != nil {
		t.Error("entry policyData should be cleared on Remove")
	}
}

func TestLRUReset(t *testing.T) {
	p := newLRU[string, int]()
	for _, k := range []string{"a", "b", "c"} {
		p.OnInsert(makeLRUEntry(k))
	}
	p.Reset()
	if p.Len() != 0 {
		t.Errorf("after Reset: Len=%d, want 0", p.Len())
	}
	if v := p.Victim(); v != nil {
		t.Errorf("Victim on reset policy = %v, want nil", v)
	}
}
