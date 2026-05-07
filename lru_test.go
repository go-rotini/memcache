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

func TestLRUUpdateBehavesLikeAccess(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	b := makeLRUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnUpdate(a) // OnUpdate delegates to OnAccess
	if p.head.entry.key != "a" {
		t.Errorf("after update, head = %s, want a", p.head.entry.key)
	}
}

func TestLRUSetBudgetIsNoop(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	p.OnInsert(a)
	// SetBudget is a no-op; it must not affect tracking.
	p.SetBudget(0)
	p.SetBudget(100)
	p.SetBudget(-1)
	if p.Len() != 1 {
		t.Errorf("Len after SetBudget = %d, want 1", p.Len())
	}
}

func TestLRUPromotionNeededAlwaysTrue(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	p.OnInsert(a)
	if !p.PromotionNeeded(a) {
		t.Error("LRU PromotionNeeded should always return true")
	}
	if !p.PromotionNeeded(nil) {
		t.Error("LRU PromotionNeeded(nil) should still return true")
	}
}

func TestLRUAccessOnEntryWithBadPolicyDataIsNoop(t *testing.T) {
	p := newLRU[string, int]()
	bogus := makeLRUEntry("bogus")
	bogus.policyData = "not-a-node"
	// Should not panic; nothing to promote.
	p.OnAccess(bogus)
	p.OnRemove(bogus)
	if p.Len() != 0 {
		t.Errorf("Len = %d, want 0", p.Len())
	}
}

func TestLRUAccessMidListMovesToHead(t *testing.T) {
	p := newLRU[string, int]()
	a := makeLRUEntry("a")
	b := makeLRUEntry("b")
	c := makeLRUEntry("c")
	p.OnInsert(a) // head=a, tail=a
	p.OnInsert(b) // head=b, mid=a, tail=a
	p.OnInsert(c) // head=c, mid=b, tail=a
	p.OnAccess(b) // b in middle; should become head
	if p.head.entry.key != "b" {
		t.Errorf("after access on b, head = %s, want b", p.head.entry.key)
	}
	if p.tail.entry.key != "a" {
		t.Errorf("after access on b, tail = %s, want a", p.tail.entry.key)
	}
}
