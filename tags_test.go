package memcache

import (
	"errors"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetWithTagsIndexesEntry(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	if err := c.SetWithTags("k1", 1, "user-42", "team-a"); err != nil {
		t.Fatal(err)
	}
	got := c.Tags("k1")
	sort.Strings(got)
	want := []string{"team-a", "user-42"}
	if !slices.Equal(got, want) {
		t.Errorf("Tags(k1) = %v, want %v", got, want)
	}
}

func TestTagsOnAbsentReturnsNil(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	if got := c.Tags("missing"); got != nil {
		t.Errorf("Tags on absent = %v, want nil", got)
	}
}

func TestTagsOnUntaggedEntry(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	if got := c.Tags("k"); got != nil {
		t.Errorf("Tags on untagged = %v, want nil", got)
	}
}

func TestInvalidateTagRemovesAllTagged(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(16), WithShards(4))
	defer c.Close()
	_ = c.SetWithTags("a", 1, "user-42")
	_ = c.SetWithTags("b", 2, "user-42")
	_ = c.SetWithTags("c", 3, "user-42", "team-a")
	_ = c.SetWithTags("d", 4, "user-7") // unrelated

	n := c.InvalidateTag("user-42")
	if n != 3 {
		t.Errorf("InvalidateTag removed %d, want 3", n)
	}
	for _, k := range []string{"a", "b", "c"} {
		if c.Has(k) {
			t.Errorf("entry %q should be gone after InvalidateTag", k)
		}
	}
	if !c.Has("d") {
		t.Error("untagged-by-other-tag entry should survive")
	}
	st := c.Stats()
	if got := st.EvictionsByReason[EvictReasonTag]; got != 3 {
		t.Errorf("EvictionsByReason[Tag] = %d, want 3", got)
	}
}

func TestInvalidateTagOnUnknownTagIsZero(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.SetWithTags("k", 1, "x")
	if n := c.InvalidateTag("unknown"); n != 0 {
		t.Errorf("InvalidateTag(unknown) = %d, want 0", n)
	}
	if !c.Has("k") {
		t.Error("entry should remain after invalidating an unknown tag")
	}
}

func TestInvalidateTagsUnion(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.SetWithTags("a", 1, "x")
	_ = c.SetWithTags("b", 2, "y")
	_ = c.SetWithTags("c", 3, "x", "y") // tagged by both
	_ = c.SetWithTags("d", 4, "z")

	// Invalidate union of x and y. "c" should be counted once.
	n := c.InvalidateTags("x", "y")
	if n != 3 {
		t.Errorf("InvalidateTags removed %d, want 3 (a,b,c counted once)", n)
	}
	if !c.Has("d") {
		t.Error("entry tagged 'z' should survive")
	}
}

func TestEntryEvictionUntags(t *testing.T) {
	// Single-shard with budget 2 + slop=1 → after 4 inserts the
	// LRU policy will evict. Confirm tag index forgets the evicted
	// key so InvalidateTag doesn't try to remove it.
	c, _ := New[string, int](WithMaxEntries(2), WithShards(1))
	defer c.Close()
	for i, k := range []string{"a", "b", "c", "d", "e"} {
		_ = c.SetWithTags(k, i, "shared")
	}
	// Some entries got evicted. The tag index should now hold only
	// the surviving keys. InvalidateTag should not touch evicted ones.
	survived := c.Len()
	n := c.InvalidateTag("shared")
	if n != survived {
		t.Errorf("InvalidateTag removed %d, want %d (= surviving entries)", n, survived)
	}
}

func TestUpdateChangesTagSet(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.SetWithTags("k", 1, "old")
	// Re-set with different tags.
	_ = c.SetWithTags("k", 2, "new")
	if n := c.InvalidateTag("old"); n != 0 {
		t.Errorf("after re-tag, InvalidateTag(old) removed %d; want 0", n)
	}
	if n := c.InvalidateTag("new"); n != 1 {
		t.Errorf("after re-tag, InvalidateTag(new) removed %d; want 1", n)
	}
}

func TestUpdateClearsTagsWhenNoneSupplied(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.SetWithTags("k", 1, "x")
	// Plain Set replaces with no tags; old tag should drop out of
	// the index.
	_ = c.Set("k", 2)
	if n := c.InvalidateTag("x"); n != 0 {
		t.Errorf("after plain Set, InvalidateTag(x) removed %d; want 0", n)
	}
}

func TestDeleteUntags(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.SetWithTags("k", 1, "shared")
	c.Delete("k")
	if n := c.InvalidateTag("shared"); n != 0 {
		t.Errorf("after Delete, InvalidateTag removed %d; want 0", n)
	}
}

func TestResetClearsTagIndex(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.SetWithTags("k", 1, "shared")
	c.Reset()
	if n := c.InvalidateTag("shared"); n != 0 {
		t.Errorf("after Reset, InvalidateTag removed %d; want 0", n)
	}
}

func TestClearClearsTagIndex(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.SetWithTags("k", 1, "shared")
	c.Clear()
	if n := c.InvalidateTag("shared"); n != 0 {
		t.Errorf("after Clear, InvalidateTag removed %d; want 0", n)
	}
}

func TestGetMulti(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("a", 1)
	_ = c.Set("b", 2)
	got := c.GetMulti([]string{"a", "b", "missing"})
	if len(got) != 2 {
		t.Fatalf("GetMulti returned %d, want 2", len(got))
	}
	if got["a"] != 1 || got["b"] != 2 {
		t.Errorf("GetMulti = %v", got)
	}
	if _, exists := got["missing"]; exists {
		t.Error("missing keys should not be in result map")
	}
}

func TestGetMultiEmptyReturnsEmptyMap(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	got := c.GetMulti(nil)
	if got == nil {
		t.Error("GetMulti(nil) should return empty map, not nil")
	}
}

func TestSetMulti(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	if err := c.SetMulti(map[string]int{"a": 1, "b": 2, "c": 3}); err != nil {
		t.Fatalf("SetMulti: %v", err)
	}
	if c.Len() != 3 {
		t.Errorf("Len after SetMulti = %d, want 3", c.Len())
	}
}

func TestDeleteMulti(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(k, 0)
	}
	n := c.DeleteMulti([]string{"a", "b", "missing"})
	if n != 2 {
		t.Errorf("DeleteMulti removed %d, want 2", n)
	}
	if c.Has("a") || c.Has("b") {
		t.Error("targeted keys should be gone")
	}
	if !c.Has("c") {
		t.Error("untouched key should remain")
	}
}

func TestWithMaxTagsPerEntryRejects(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithMaxTagsPerEntry(2),
	)
	defer c.Close()
	if err := c.SetWithTags("k", 1, "a", "b"); err != nil {
		t.Fatalf("at-cap should succeed: %v", err)
	}
	err := c.SetWithTags("k2", 2, "a", "b", "c")
	if err == nil {
		t.Fatal("over-cap should error")
	}
	var ce *CapacityError
	if !errors.As(err, &ce) || ce.LimitField != "MaxTagsPerEntry" {
		t.Errorf("expected CapacityError(MaxTagsPerEntry); got %v", err)
	}
	if !errors.Is(err, ErrTooManyTags) {
		t.Errorf("error should chain to ErrTooManyTags; got %v", err)
	}
}

func TestWithMaxTagsTotalRejects(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithMaxTagsTotal(3),
	)
	defer c.Close()
	for _, k := range []string{"a", "b", "c"} {
		if err := c.SetWithTags("k-"+k, 0, k); err != nil {
			t.Fatalf("Set with tag %q: %v", k, err)
		}
	}
	// Adding a fourth distinct tag should bust the cap.
	err := c.SetWithTags("k-d", 0, "d")
	var ce *CapacityError
	if !errors.As(err, &ce) || ce.LimitField != "MaxTagsTotal" {
		t.Errorf("expected CapacityError(MaxTagsTotal); got %v", err)
	}
}

func TestWithGroupCapsMembers(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithGroup("session", 3),
	)
	defer c.Close()

	// Add 5 entries with the "session" tag — capacity 3, so the
	// oldest 2 should be evicted as we exceed the cap.
	clk := NewFakeClock(time.Unix(0, 0))
	c.cfg.clock = clk // Use fake clock for deterministic ordering.
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		clk.Advance(time.Second)
		if err := c.SetWithTags(k, 1, "session"); err != nil {
			t.Fatal(err)
		}
	}
	// "session" should now have at most 3 members.
	members := c.tags.snapshot("session")
	if len(members) != 3 {
		t.Errorf("session group members = %d, want 3", len(members))
	}
	// "a" and "b" should be evicted (oldest by inserted).
	if c.Has("a") || c.Has("b") {
		t.Error("oldest members of bounded group should be evicted")
	}
	// "c", "d", "e" should survive.
	for _, k := range []string{"c", "d", "e"} {
		if !c.Has(k) {
			t.Errorf("recent member %q should survive", k)
		}
	}
}

func TestWithGroupOnlyAffectsMatchingTags(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(32),
		WithGroup("bounded", 2),
	)
	defer c.Close()

	for _, k := range []string{"a", "b", "c"} {
		_ = c.SetWithTags(k, 1, "bounded")
	}
	// Tags that aren't groups should be unaffected by the cap.
	for _, k := range []string{"x", "y", "z"} {
		_ = c.SetWithTags(k, 1, "unbounded")
	}
	if got := len(c.tags.snapshot("bounded")); got != 2 {
		t.Errorf("bounded group size = %d, want 2", got)
	}
	if got := len(c.tags.snapshot("unbounded")); got != 3 {
		t.Errorf("unbounded tag size = %d, want 3", got)
	}
}

func TestWithGroupZeroDisables(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(32),
		WithGroup("g", 5),
		WithGroup("g", 0), // disable
	)
	defer c.Close()
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		_ = c.SetWithTags(k, 1, "g")
	}
	if got := len(c.tags.snapshot("g")); got != 6 {
		t.Errorf("disabled group cap should leave all 6; got %d", got)
	}
}

func TestSetWithOptionsTagsIndexesEntry(t *testing.T) {
	// SetWithOptions(SetTags(...)) should produce the same tag
	// indexing as SetWithTags.
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.SetWithOptions("k", 1, SetTags("a", "b"))
	if n := c.InvalidateTag("a"); n != 1 {
		t.Errorf("InvalidateTag after SetWithOptions(SetTags) = %d, want 1", n)
	}
}

func TestSetWithOptionsExpireAtAlsoIndexesTags(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	// SetExpireAt path (upsertWithAbsoluteExpiryLocked) should
	// also wire tags into the index.
	if err := c.SetWithOptions("k", 1, SetTags("future"), SetTTL(0)); err != nil {
		t.Fatalf("SetWithOptions: %v", err)
	}
	if n := c.InvalidateTag("future"); n != 1 {
		t.Errorf("InvalidateTag after ExpireAt path = %d, want 1", n)
	}
}

// TestTagsConcurrencyStress exercises SetWithTags + InvalidateTag
// under many concurrent goroutines. Race detector confirms there
// are no data races and the snapshot-then-delete InvalidateTag
// pattern stays deadlock-free even when intermixed with writes.
func TestTagsConcurrencyStress(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(1000), WithShards(8))
	defer c.Close()

	const writers = 16
	const invalidators = 4
	stop := atomic.Bool{}

	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			i := 0
			for !stop.Load() {
				k := keyFor(w*1000 + i%1000)
				_ = c.SetWithTags(k, 1, "global", keyFor(w))
				i++
			}
		})
	}
	for range invalidators {
		wg.Go(func() {
			for !stop.Load() {
				_ = c.InvalidateTag("global")
			}
		})
	}

	// Run for a brief period.
	done := make(chan struct{})
	go func() {
		for range 50 {
			_ = c.Len()
		}
		close(done)
	}()
	<-done
	stop.Store(true)
	wg.Wait()
}
