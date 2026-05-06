package memcache

import (
	"testing"
	"time"
)

func TestViewSharesStorage(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("k", 42)
	v := c.View()

	got, ok := v.Get("k")
	if !ok || got != 42 {
		t.Errorf("View.Get = (%d, %v), want (42, true)", got, ok)
	}

	// Mutations to parent are visible through view.
	_ = c.Set("k", 99)
	got, _ = v.Get("k")
	if got != 99 {
		t.Errorf("View.Get after parent Set = %d, want 99", got)
	}
}

func TestViewLenAndKeys(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(k, 0)
	}
	v := c.View()
	if v.Len() != 3 {
		t.Errorf("View.Len = %d, want 3", v.Len())
	}
	if got := v.Keys(); len(got) != 3 {
		t.Errorf("View.Keys returned %d entries, want 3", len(got))
	}
}

func TestViewRange(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for _, k := range []string{"a", "b"} {
		_ = c.Set(k, 1)
	}
	v := c.View()
	count := 0
	v.Range(func(string, int) bool { count++; return true })
	if count != 2 {
		t.Errorf("View.Range visited %d, want 2", count)
	}
}

func TestViewPeekAndHas(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	v := c.View()
	if !v.Has("k") {
		t.Error("View.Has(present) = false")
	}
	if got, ok := v.Peek("k"); !ok || got != 1 {
		t.Errorf("View.Peek = (%d, %v)", got, ok)
	}
}

func TestViewTTLAndExpiry(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(4), WithClock(clk), WithTTLJitter(0))
	defer c.Close()
	_ = c.SetWithTTL("k", 1, 5*time.Second)
	v := c.View()
	if d, _ := v.TTL("k"); d != 5*time.Second {
		t.Errorf("View.TTL = %v, want 5s", d)
	}
	if exp, ok := v.Expiry("k"); !ok || exp.IsZero() {
		t.Error("View.Expiry should report a non-zero time")
	}
}

func TestViewStatsForwarded(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	_, _ = c.Get("k")
	v := c.View()
	st := v.Stats()
	if st.Hits != 1 {
		t.Errorf("View.Stats.Hits = %d, want 1", st.Hits)
	}
}

func TestViewNilSafe(t *testing.T) {
	var v *CacheView[string, int]
	if got, ok := v.Get("k"); ok || got != 0 {
		t.Error("nil view Get should be safe and return zero")
	}
	if v.Len() != 0 {
		t.Error("nil view Len should be 0")
	}
}

func TestCloneIndependent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for i := range 5 {
		_ = c.Set(itoaSimple(i), i*10)
	}
	cl, err := c.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	defer cl.Close()
	if cl.Len() != c.Len() {
		t.Errorf("Clone.Len = %d, want %d", cl.Len(), c.Len())
	}
	// Mutate parent; clone must not change.
	_ = c.Set("0", 999)
	got, _ := cl.Get("0")
	if got != 0 {
		t.Errorf("Clone should be independent: cloned[0] = %d after parent mutation, want 0", got)
	}
	// Mutate clone; parent must not change.
	_ = cl.Set("100", 100)
	if c.Has("100") {
		t.Error("parent should not see clone's mutations")
	}
}

func TestClonePreservesTTL(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("k", 1, 30*time.Second)
	cl, err := c.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	// Both caches share the same clock instance (cfg copy is
	// shallow), so TTLs evaluate identically.
	defer cl.Close()
	d, ok := cl.TTL("k")
	// 5% default jitter on a 30s TTL means the actual remaining
	// duration falls in [28.5s, 31.5s]; assert the band rather than
	// exact equality.
	if !ok || d < 28*time.Second || d > 32*time.Second {
		t.Errorf("clone TTL = (%v, %v), want ~30s ± 5%%", d, ok)
	}
}

func TestCloneSkipsExpired(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("alive", 1, time.Hour)
	_ = c.SetWithTTL("dead", 2, time.Second)
	clk.Advance(2 * time.Second)
	cl, err := c.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	defer cl.Close()
	if cl.Has("dead") {
		t.Error("Clone should skip expired entries")
	}
	if !cl.Has("alive") {
		t.Error("Clone should retain non-expired entries")
	}
}

func TestCloneClosedReturnsError(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	_ = c.Close()
	if _, err := c.Clone(); err == nil {
		t.Error("Clone on closed cache should error")
	}
}
