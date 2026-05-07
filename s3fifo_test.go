package memcache

import "testing"

// makeEntry constructs an entry with a given key, suitable as policy input.
func makeS3Entry(k string) *entry[string, int] {
	return &entry[string, int]{key: k}
}

func TestS3FIFOInsertAccessVictim(t *testing.T) {
	p := newS3FIFO[string, int](10)
	a := makeS3Entry("a")
	b := makeS3Entry("b")
	p.OnInsert(a)
	p.OnInsert(b)

	if p.Len() != 2 {
		t.Fatalf("Len = %d, want 2", p.Len())
	}
	// New entries land in Small. Both have freq=0.
	p.OnAccess(a) // bump a's freq
	if n := a.policyData.(*s3Node[string, int]); n.freq.Load() != 1 {
		t.Errorf("a.freq after access = %d, want 1", n.freq.Load())
	}
	// Saturate.
	for range 10 {
		p.OnAccess(a)
	}
	if n := a.policyData.(*s3Node[string, int]); n.freq.Load() != s3FreqMax {
		t.Errorf("a.freq saturated = %d, want %d", n.freq.Load(), s3FreqMax)
	}
}

func TestS3FIFOSmallToGhostOnEvict(t *testing.T) {
	// budget=2 → smallBudget=1, mainBudget=1.
	p := newS3FIFO[string, int](2)
	a := makeS3Entry("a") // small, freq=0
	b := makeS3Entry("b") // small, freq=0; puts smallSize over budget
	p.OnInsert(a)
	p.OnInsert(b)

	v := p.Victim()
	if v == nil {
		t.Fatal("Victim returned nil")
	}
	// "a" is the older Small entry with freq=0 → evicted, key recorded in Ghost.
	if v.key != "a" {
		t.Errorf("first victim = %q, want %q", v.key, "a")
	}
	if _, ok := p.ghostSet["a"]; !ok {
		t.Error("evicted Small entry should be recorded in Ghost")
	}
}

func TestS3FIFOPromoteSmallToMainOnFreq(t *testing.T) {
	// Smaller config so we control queue sizes precisely.
	p := newS3FIFO[string, int](10)
	a := makeS3Entry("a")
	p.OnInsert(a)
	p.OnAccess(a) // freq=1

	// Force a victim cycle while Small is over budget. Insert
	// many to push Small over its budget.
	for i := range 5 {
		p.OnInsert(makeS3Entry(string(rune('b' + i))))
	}
	// Now exercise Victim repeatedly to drain over-budget queues.
	for p.Len() > 5 {
		v := p.Victim()
		if v == nil {
			break
		}
		// Simulate cache removing the evicted entry.
		v.policyData = nil
	}
	// "a" should have been promoted to Main rather than evicted to
	// Ghost (because it had freq ≥ 1 when Small considered it).
	if _, inGhost := p.ghostSet["a"]; inGhost {
		t.Error("entry with freq ≥ 1 should promote to Main, not Ghost")
	}
}

func TestS3FIFOReinsertFromGhostGoesToMain(t *testing.T) {
	p := newS3FIFO[string, int](2)
	a := makeS3Entry("a")
	b := makeS3Entry("b")
	p.OnInsert(a)
	p.OnInsert(b)
	v := p.Victim() // evicts "a", places it in Ghost
	if v == nil {
		t.Fatal("expected victim")
	}
	v.policyData = nil

	// Re-insert "a"; it was in Ghost, so should land in Main.
	a2 := makeS3Entry("a")
	p.OnInsert(a2)
	n := a2.policyData.(*s3Node[string, int])
	if !n.inMain {
		t.Error("re-insert from Ghost should place entry into Main")
	}
}

func TestS3FIFORemoveSafe(t *testing.T) {
	p := newS3FIFO[string, int](4)
	a := makeS3Entry("a")
	p.OnInsert(a)
	p.OnRemove(a)
	if p.Len() != 0 {
		t.Errorf("Len after remove = %d, want 0", p.Len())
	}
	// Double-remove must not panic or corrupt state.
	p.OnRemove(a)
}

func TestS3FIFOReset(t *testing.T) {
	p := newS3FIFO[string, int](4)
	for _, k := range []string{"a", "b", "c"} {
		p.OnInsert(makeS3Entry(k))
	}
	p.Reset()
	if p.Len() != 0 || len(p.ghostSet) != 0 {
		t.Errorf("after Reset: Len=%d ghost=%d", p.Len(), len(p.ghostSet))
	}
	if v := p.Victim(); v != nil {
		t.Error("Victim on reset policy should return nil")
	}
}

func TestS3FIFOBudgetSplit(t *testing.T) {
	// budget=10 → smallBudget=1 (10%), mainBudget=9.
	p := newS3FIFO[string, int](10)
	if p.smallBudget != 1 || p.mainBudget != 9 {
		t.Errorf("budget split: small=%d main=%d, want 1, 9",
			p.smallBudget, p.mainBudget)
	}
	// Ghost budget mirrors main per spec.
	if p.ghostBudget != 9 {
		t.Errorf("ghostBudget = %d, want 9", p.ghostBudget)
	}
}

func TestS3SplitBudgetEdgeCases(t *testing.T) {
	if s, m := s3SplitBudget(0); s != 0 || m != 0 {
		t.Errorf("s3SplitBudget(0) = %d/%d, want 0/0", s, m)
	}
	if s, m := s3SplitBudget(-5); s != 0 || m != 0 {
		t.Errorf("s3SplitBudget(-5) = %d/%d, want 0/0", s, m)
	}
	if s, m := s3SplitBudget(1); s != 1 || m != 0 {
		t.Errorf("s3SplitBudget(1) = %d/%d, want 1/0", s, m)
	}
	// Default-100/9 path; just ensure positivity.
	if s, m := s3SplitBudget(100); s+m != 100 || s == 0 {
		t.Errorf("s3SplitBudget(100): s=%d m=%d", s, m)
	}
}

func TestS3FIFOSetBudgetTrimsGhost(t *testing.T) {
	p := newS3FIFO[string, int](10)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		p.recordGhost(k)
	}
	// Shrink budget; ghosts should be trimmed.
	p.SetBudget(1) // small=1, main=0, ghostBudget=0
	if p.ghostSize != 0 {
		t.Errorf("after SetBudget(1) ghostSize=%d, want 0", p.ghostSize)
	}
	// Negative budget: zero everything.
	p.SetBudget(-1)
	if p.smallBudget != 0 || p.mainBudget != 0 {
		t.Errorf("SetBudget(-1) budgets: small=%d main=%d, want 0/0",
			p.smallBudget, p.mainBudget)
	}
}

// TestS3FIFOPromotionNeededAfterSaturation covers the freq-saturated branch
// where PromotionNeeded returns false.
func TestS3FIFOPromotionNeededAfterSaturation(t *testing.T) {
	p := newS3FIFO[string, int](10)
	a := makeS3Entry("a")
	p.OnInsert(a)
	if !p.PromotionNeeded(a) {
		t.Error("fresh entry should need promotion")
	}
	for range int(s3FreqMax) {
		p.OnAccess(a)
	}
	if p.PromotionNeeded(a) {
		t.Error("saturated entry should NOT need promotion")
	}
	// Bad policyData should err on the side of "promotion needed".
	bogus := makeS3Entry("bogus")
	bogus.policyData = "not-a-node"
	if !p.PromotionNeeded(bogus) {
		t.Error("entry with non-s3Node policyData should need promotion")
	}
}

func TestS3FIFOAccessBadPolicyDataIsNoop(t *testing.T) {
	p := newS3FIFO[string, int](10)
	bogus := makeS3Entry("bogus")
	bogus.policyData = "not-a-node"
	p.OnAccess(bogus) // must not panic
}

func TestS3FIFORecordGhostDuplicateNoop(t *testing.T) {
	p := newS3FIFO[string, int](4)
	p.recordGhost("k")
	p.recordGhost("k") // duplicate; should early-return
	if p.ghostSize != 1 {
		t.Errorf("ghostSize after dup recordGhost = %d, want 1", p.ghostSize)
	}
}

func TestS3FIFOResetClearsGhost(t *testing.T) {
	p := newS3FIFO[string, int](4)
	a := makeS3Entry("a")
	p.OnInsert(a)
	p.recordGhost("ghost1")
	p.recordGhost("ghost2")
	p.Reset()
	if p.ghostSize != 0 || len(p.ghostSet) != 0 {
		t.Errorf("after Reset: ghostSize=%d ghostSet=%d",
			p.ghostSize, len(p.ghostSet))
	}
	if p.ghostHead != nil || p.ghostTail != nil {
		t.Error("after Reset, ghost head/tail should be nil")
	}
}

// TestS3FIFOResetClearsAllQueues exercises the small, main, and ghost
// loops in Reset() by populating each region first.
func TestS3FIFOResetClearsAllQueues(t *testing.T) {
	p := newS3FIFO[string, int](4)
	// Put one entry into Small.
	a := makeS3Entry("a")
	p.OnInsert(a)
	// Put one entry into Main directly via ghost rebirth.
	p.recordGhost("b")
	b := makeS3Entry("b")
	p.OnInsert(b)
	// Add additional ghost entries.
	p.recordGhost("ghost-x")
	p.recordGhost("ghost-y")
	if p.smallSize == 0 || p.mainSize == 0 || p.ghostSize == 0 {
		t.Fatalf("setup failed: small=%d main=%d ghost=%d",
			p.smallSize, p.mainSize, p.ghostSize)
	}
	p.Reset()
	if p.smallSize != 0 || p.mainSize != 0 || p.ghostSize != 0 {
		t.Errorf("after Reset: small=%d main=%d ghost=%d, want all 0",
			p.smallSize, p.mainSize, p.ghostSize)
	}
	if a.policyData != nil || b.policyData != nil {
		t.Error("after Reset, entries' policyData must be cleared")
	}
}

func TestS3FIFOVictimEmptyReturnsNil(t *testing.T) {
	p := newS3FIFO[string, int](4)
	if v := p.Victim(); v != nil {
		t.Errorf("Victim on empty = %v, want nil", v)
	}
}

// TestS3FIFOVictimMainSecondChance exercises the main second-chance path
// where main entry has freq>=1, gets demoted to tail with freq decremented.
func TestS3FIFOVictimMainSecondChance(t *testing.T) {
	p := newS3FIFO[string, int](2) // small=1, main=1
	// Promote 'a' to main with high freq.
	a := makeS3Entry("a")
	p.OnInsert(a)
	p.OnAccess(a) // freq=1
	// Push small over budget so 'a' gets promoted to main.
	b := makeS3Entry("b")
	p.OnInsert(b) // smallSize=2 > 1
	for {
		v := p.Victim()
		if v == nil {
			break
		}
		v.policyData = nil
	}
	// Now insert another entry that goes to small; trigger eviction
	// to push main over budget too.
	for _, k := range []string{"c", "d", "e"} {
		p.OnInsert(makeS3Entry(k))
	}
	// Drive Victim repeatedly.
	for range 10 {
		v := p.Victim()
		if v == nil {
			break
		}
		v.policyData = nil
	}
}

// TestS3FIFO_SetMustNotEvictItself is a minimized regression for the
// fuzz failure originally surfaced by FuzzCacheOps seed
// testdata/fuzz/FuzzCacheOps/a822e908b2cd2d00:
//
//	fuzz_test.go:45: Set(48, 90) -> Get = (0, false)
//
// With WithMaxEntries(8) + WithShards(1) the per-shard budget is 9
// (8 plus the 10% slop in perShardBudget). S3-FIFO splits that into
// smallBudget=1 / mainBudget=8. After the trace below the Main queue
// holds 9 entries with mostly-saturated frequencies; the final Set
// lands the new key in Small. evictWhileOverBudgetLocked then calls
// Victim, whose rotation loop must give Main enough decrement-and-
// rotate passes to drive a head's freq to zero. Pre-fix the loop
// bound (smallSize+mainSize+1) ran out first and the defensive
// fallback evicted smallHead - i.e. the entry Set had just inserted.
//
// Encoding mirrors fuzz_test.go: 0=Set+Get, 1=Get, 6=SetIfAbsent,
// 7=DeleteIf(true). val=byte(2*step).
func TestS3FIFO_SetMustNotEvictItself(t *testing.T) {
	c, err := New[byte, byte](WithMaxEntries(8), WithShards(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type op struct{ kind, key, val byte }
	ops := []op{
		{0, 49, 2}, {0, 50, 4}, {0, 55, 6}, {0, 56, 8}, {0, 57, 10},
		{0, 65, 12}, {6, 66, 14}, {0, 67, 16}, {0, 89, 18}, {6, 90, 20},
		{0, 57, 22}, {0, 65, 24}, {0, 66, 26}, {0, 48, 28}, {0, 49, 30},
		{0, 50, 32}, {6, 55, 34}, {1, 48, 36}, {0, 50, 40}, {6, 88, 42},
		{0, 56, 44}, {0, 97, 46}, {0, 66, 54}, {0, 49, 56}, {0, 55, 58},
		{0, 67, 60}, {6, 56, 62}, {0, 90, 64}, {0, 55, 68}, {0, 57, 70},
		{0, 67, 72}, {0, 49, 74}, {0, 88, 78}, {0, 48, 82}, {7, 48, 84},
		{0, 56, 86}, {0, 50, 88}, {0, 48, 90},
	}
	for i, o := range ops {
		switch o.kind {
		case 0: // Set then immediate Get round-trip
			if err := c.Set(o.key, o.val); err != nil {
				t.Fatalf("step %d Set(%d,%d): %v", i, o.key, o.val, err)
			}
			if got, ok := c.Get(o.key); !ok || got != o.val {
				t.Fatalf("step %d: Set(%d, %d) -> Get = (%d, %v)",
					i, o.key, o.val, got, ok)
			}
		case 1:
			_, _ = c.Get(o.key)
		case 6:
			_, _ = c.SetIfAbsent(o.key, o.val)
		case 7:
			_ = c.DeleteIf(o.key, func(byte) bool { return true })
		}
	}
}
