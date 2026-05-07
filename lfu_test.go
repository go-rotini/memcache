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

func TestLFUUpdateBehavesLikeAccess(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	p.OnUpdate(a)
	n := a.policyData.(*lfuNode[string, int])
	if n.freq != 2 {
		t.Errorf("after OnUpdate, freq=%d want 2", n.freq)
	}
}

func TestLFUSetBudgetIsNoop(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	p.SetBudget(0)
	p.SetBudget(100)
	p.SetBudget(-1)
	if p.Len() != 1 {
		t.Errorf("Len after SetBudget = %d, want 1", p.Len())
	}
}

func TestLFUSnapshot(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	b := makeLFUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnAccess(a) // a now in bucket 2
	d, ok := p.Snapshot().(PolicyDetailLFU)
	if !ok {
		t.Fatalf("Snapshot type %T, want PolicyDetailLFU", p.Snapshot())
	}
	if d.Size != 2 {
		t.Errorf("Size = %d, want 2", d.Size)
	}
	if d.DistinctBuckets != 2 {
		t.Errorf("DistinctBuckets = %d, want 2", d.DistinctBuckets)
	}
}

func TestLFUPromotionNeededAlwaysTrue(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	if !p.PromotionNeeded(a) {
		t.Error("LFU PromotionNeeded should always return true")
	}
}

// TestLFUVictimWalksToNextNonEmpty drives nextNonEmpty by manually
// putting the policy into a state where minFreq points at a stale,
// missing bucket but a non-empty bucket exists at higher freq. The
// Victim loop must scan forward and find it.
func TestLFUVictimWalksToNextNonEmpty(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	// Promote 'a' a few times so it sits in bucket 5.
	p.OnAccess(a)
	p.OnAccess(a)
	p.OnAccess(a)
	p.OnAccess(a) // now in bucket 5, minFreq=5
	// Force minFreq stale: simulate a state where minFreq points at
	// a missing bucket so Victim must walk via nextNonEmpty.
	p.minFreq = 1
	v := p.Victim()
	if v == nil || v.key != "a" {
		t.Errorf("Victim with stale minFreq = %v, want a", v)
	}
}

func TestLFUNextNonEmptyEmptyMap(t *testing.T) {
	p := newLFU[string, int]()
	if _, ok := p.nextNonEmpty(0); ok {
		t.Error("nextNonEmpty on empty buckets should return false")
	}
}

// TestLFUNextNonEmptySkipsEmptyBuckets exercises the "list.head == nil"
// continue branch by injecting an empty bucket directly.
func TestLFUNextNonEmptySkipsEmptyBuckets(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	p.OnInsert(a)
	p.OnAccess(a) // bucket{1} empties, bucket{2}={a}
	// Manually inject an empty bucket at freq 3 to force the head==nil
	// continue branch.
	p.buckets[3] = &lfuList[string, int]{}
	got, ok := p.nextNonEmpty(0)
	if !ok || got != 2 {
		t.Errorf("nextNonEmpty = (%d, %v), want (2, true)", got, ok)
	}
}

// TestLFUNextNonEmptyFromSkipsLowerBuckets exercises the
// "f <= from continue" branch by passing a from value that exceeds
// some populated bucket frequencies.
func TestLFUNextNonEmptyFromSkipsLowerBuckets(t *testing.T) {
	p := newLFU[string, int]()
	a := makeLFUEntry("a")
	b := makeLFUEntry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	p.OnAccess(a)
	p.OnAccess(a) // a in bucket 3
	// Now buckets has 1 (b) and 3 (a). Asking for nextNonEmpty(2)
	// must skip bucket 1 (f <= from) and pick bucket 3.
	got, ok := p.nextNonEmpty(2)
	if !ok || got != 3 {
		t.Errorf("nextNonEmpty(2) = (%d, %v), want (3, true)", got, ok)
	}
}

func TestLFUVictimEmptyReturnsNil(t *testing.T) {
	p := newLFU[string, int]()
	if v := p.Victim(); v != nil {
		t.Errorf("Victim on empty policy = %v, want nil", v)
	}
}

func TestLFURemoveBadPolicyDataIsNoop(t *testing.T) {
	p := newLFU[string, int]()
	bogus := makeLFUEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnRemove(bogus)
	if p.Len() != 0 {
		t.Errorf("Len = %d, want 0", p.Len())
	}
}

func TestLFUAccessBadPolicyDataIsNoop(t *testing.T) {
	p := newLFU[string, int]()
	bogus := makeLFUEntry("bogus")
	bogus.policyData = "not-a-node"
	p.OnAccess(bogus)
}

// TestLFUListRemoveOnDetachedNodeIsNoop exercises the default-return
// branch in lfuList.remove when the node has already been unlinked.
func TestLFUListRemoveOnDetachedNodeIsNoop(t *testing.T) {
	l := &lfuList[string, int]{}
	stray := &lfuNode[string, int]{}
	l.remove(stray) // n.prev nil and l.head != n → default return; no panic
}
