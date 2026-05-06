package memcache

import "testing"

func makeLFUEntry(k string) *entry[string, int] {
	return &entry[string, int]{key: k}
}

func TestLFUInsertStartsAtFreq1(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	n := a.policyData.(*lfuNode[string, int])
	if n.freq != 1 {
		t.Errorf("new entry freq = %d, want 1", n.freq)
	}
	if p.minFreq != 1 {
		t.Errorf("minFreq = %d, want 1", p.minFreq)
	}
}

func TestLFUAccessIncrementsFreq(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	for i := 1; i <= 5; i++ {
		p.OnAccess(a)
		n := a.policyData.(*lfuNode[string, int])
		want := uint64(i + 1)
		if n.freq != want {
			t.Errorf("after %d accesses, freq=%d want %d", i, n.freq, want)
		}
	}
}

func TestLFUVictimPicksLowestFreq(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	b := makeLFUEntry("b")
	c := makeLFUEntry("c")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnInsert(c)

	// Bump b and c so a stays lowest.
	p.OnAccess(b)
	p.OnAccess(c)
	p.OnAccess(c)

	v := p.Victim()
	if v == nil || v.key != "a" {
		t.Fatalf("Victim = %v, want %q", v, "a")
	}
}

func TestLFUTieBreakerIsFIFOWithinBucket(t *testing.T) {
	// Two entries with the same freq → the older one (inserted first)
	// is the next victim.
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	b := makeLFUEntry("b")
	p.OnInsert(a) // bucket head
	p.OnInsert(b) // bucket tail
	v := p.Victim()
	if v == nil || v.key != "a" {
		t.Errorf("tie victim = %v, want %q", v, "a")
	}
}

func TestLFUMinFreqAdvancesOnPromotion(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	// Only entry: bucket{1} = [a]. Access a → bucket{2} = [a],
	// bucket{1} empty. minFreq should advance to 2.
	p.OnAccess(a)
	if p.minFreq != 2 {
		t.Errorf("minFreq after sole-entry promotion = %d, want 2", p.minFreq)
	}
}

func TestLFURemove(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	p.OnRemove(a)
	if p.Len() != 0 {
		t.Errorf("Len after Remove = %d, want 0", p.Len())
	}
	if v := p.Victim(); v != nil {
		t.Error("Victim on empty policy should be nil")
	}
}

func TestLFUReset(t *testing.T) {
	p := newLFU[string, int]()
	for _, k := range []string{"a", "b", "c"} {
		p.OnInsert(makeLFUEntry(k))
	}
	p.Reset()
	if p.Len() != 0 || len(p.buckets) != 0 || p.minFreq != 0 {
		t.Errorf("after Reset: Len=%d buckets=%d minFreq=%d",
			p.Len(), len(p.buckets), p.minFreq)
	}
}

func TestLFUVictimAfterEmptyBucket(t *testing.T) {
	// Force advance through several frequency levels with promotions.
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	b := makeLFUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	// a goes to freq=3, b stays at freq=1.
	p.OnAccess(a)
	p.OnAccess(a)
	v := p.Victim()
	if v == nil || v.key != "b" {
		t.Errorf("Victim = %v, want b (lowest freq)", v)
	}
}
