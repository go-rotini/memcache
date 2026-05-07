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
