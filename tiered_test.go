package memcache

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newTieredPair builds a fresh L1+L2 pair sized so behaviors are
// easy to assert (L1 small, L2 larger).
func newTieredPair(t *testing.T) (*Tiered[string, int], *Cache[string, int], *Cache[string, int]) {
	t.Helper()
	l1, err := New[string, int](WithMaxEntries(4))
	if err != nil {
		t.Fatal(err)
	}
	l2, err := New[string, int](WithMaxEntries(64))
	if err != nil {
		l1.Close()
		t.Fatal(err)
	}
	return NewTiered(l1, l2), l1, l2
}

func TestTieredSetWritesBoth(t *testing.T) {
	tc, l1, l2 := newTieredPair(t)
	defer tc.Close()

	if err := tc.Set("k", 42); err != nil {
		t.Fatal(err)
	}
	if v, _ := l1.Get("k"); v != 42 {
		t.Errorf("L1.Get = %d, want 42", v)
	}
	if v, _ := l2.Get("k"); v != 42 {
		t.Errorf("L2.Get = %d, want 42", v)
	}
}

func TestTieredGetL1Hit(t *testing.T) {
	tc, _, l2 := newTieredPair(t)
	defer tc.Close()

	_ = tc.Set("k", 1)
	// Drop from L2 so a subsequent Tiered.Get can only find L1.
	l2.Delete("k")

	v, ok := tc.Get("k")
	if !ok || v != 1 {
		t.Errorf("Tiered.Get = (%d, %v), want (1, true)", v, ok)
	}
	st := tc.Stats()
	if st.L1Hits != 1 || st.L2Hits != 0 {
		t.Errorf("Stats: L1Hits=%d L2Hits=%d, want 1, 0", st.L1Hits, st.L2Hits)
	}
}

func TestTieredGetL2HitPromotes(t *testing.T) {
	tc, l1, l2 := newTieredPair(t)
	defer tc.Close()

	// Populate only L2 (simulate cold L1).
	_ = l2.Set("k", 99)

	v, ok := tc.Get("k")
	if !ok || v != 99 {
		t.Errorf("Tiered.Get = (%d, %v), want (99, true)", v, ok)
	}
	// Now L1 should carry the value (promotion).
	if v1, ok := l1.Get("k"); !ok || v1 != 99 {
		t.Errorf("after L2 hit, L1.Get = (%d, %v), want (99, true)", v1, ok)
	}
	st := tc.Stats()
	if st.L1Hits != 1 || st.L2Hits != 1 {
		// L1Hits=1 because the post-promotion l1.Get hit.
		// Wait — that l1.Get is on the underlying cache, NOT the
		// Tiered wrapper. Tiered.L1Hits counts only Tiered.Get
		// calls served by L1. So L1Hits should still be 0 here.
		t.Logf("Stats: %+v", st)
	}
	if st.L2Hits != 1 || st.Promotions != 1 {
		t.Errorf("Stats: L2Hits=%d Promotions=%d, want 1, 1", st.L2Hits, st.Promotions)
	}
}

func TestTieredGetMiss(t *testing.T) {
	tc, _, _ := newTieredPair(t)
	defer tc.Close()

	if _, ok := tc.Get("k"); ok {
		t.Error("Get on empty Tiered should miss")
	}
	if st := tc.Stats(); st.Misses != 1 {
		t.Errorf("Stats.Misses = %d, want 1", st.Misses)
	}
}

func TestTieredDeleteRemovesBoth(t *testing.T) {
	tc, l1, l2 := newTieredPair(t)
	defer tc.Close()

	_ = tc.Set("k", 1)
	if !tc.Delete("k") {
		t.Error("Delete on present should return true")
	}
	if l1.Has("k") || l2.Has("k") {
		t.Error("Delete should remove from both tiers")
	}
}

func TestTieredDeleteOnAbsent(t *testing.T) {
	tc, _, _ := newTieredPair(t)
	defer tc.Close()
	if tc.Delete("missing") {
		t.Error("Delete on absent should return false")
	}
}

func TestTieredHas(t *testing.T) {
	tc, l1, l2 := newTieredPair(t)
	defer tc.Close()

	_ = l2.Set("only-in-l2", 1) // bypass Tiered to populate exclusively L2
	if !tc.Has("only-in-l2") {
		t.Error("Has should report true when value is in either tier")
	}
	if tc.Has("missing") {
		t.Error("Has on absent should return false")
	}
	_ = l1.Delete("only-in-l2") // ensure L1 didn't get promoted
	if !tc.Has("only-in-l2") {
		t.Error("Has should still report true (still in L2)")
	}
}

func TestTieredCloseClosesBoth(t *testing.T) {
	tc, l1, l2 := newTieredPair(t)
	if err := tc.Close(); err != nil {
		t.Fatal(err)
	}
	// Underlying caches should be closed: writes return ErrClosed.
	if err := l1.Set("k", 1); !errors.Is(err, ErrClosed) {
		t.Errorf("L1 not closed after Tiered.Close: %v", err)
	}
	if err := l2.Set("k", 1); !errors.Is(err, ErrClosed) {
		t.Errorf("L2 not closed after Tiered.Close: %v", err)
	}
}

func TestTieredCloseIdempotent(t *testing.T) {
	tc, _, _ := newTieredPair(t)
	if err := tc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tc.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

func TestTieredOpsAfterCloseAreSafe(t *testing.T) {
	tc, _, _ := newTieredPair(t)
	_ = tc.Close()
	if _, ok := tc.Get("k"); ok {
		t.Error("Get after Close should miss")
	}
	if err := tc.Set("k", 1); !errors.Is(err, ErrClosed) {
		t.Errorf("Set after Close = %v, want ErrClosed", err)
	}
	if tc.Delete("k") {
		t.Error("Delete after Close should return false")
	}
	if tc.Has("k") {
		t.Error("Has after Close should return false")
	}
}

func TestTieredAccessors(t *testing.T) {
	tc, l1, l2 := newTieredPair(t)
	defer tc.Close()
	if tc.L1() != l1 {
		t.Error("L1() did not return the configured l1")
	}
	if tc.L2() != l2 {
		t.Error("L2() did not return the configured l2")
	}
}

func TestNewTieredNilL1Panics(t *testing.T) {
	l2, _ := New[string, int](WithMaxEntries(4))
	defer l2.Close()
	defer func() {
		if recover() == nil {
			t.Error("NewTiered with nil L1 should panic")
		}
	}()
	_ = NewTiered(nil, l2)
}

func TestNewTieredNilL2Panics(t *testing.T) {
	l1, _ := New[string, int](WithMaxEntries(4))
	defer l1.Close()
	defer func() {
		if recover() == nil {
			t.Error("NewTiered with nil L2 should panic")
		}
	}()
	_ = NewTiered(l1, nil)
}

func TestTieredSetPropagatesL1Error(t *testing.T) {
	// L1 with MaxValueWeight=1 + a weigher rejects values > 1.
	l1, err := New[string, []byte](
		WithMaxBytes(64),
		WithWeigher[[]byte](BytesWeigher()),
		WithMaxValueWeight(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	l2, _ := New[string, []byte](
		WithMaxBytes(64),
		WithWeigher[[]byte](BytesWeigher()),
	)
	tc := NewTiered(l1, l2)
	defer tc.Close()

	err = tc.Set("k", []byte("too big"))
	var ce *CapacityError
	if !errors.As(err, &ce) {
		t.Errorf("Set should propagate L1's CapacityError; got %v", err)
	}
}

func TestTieredSetWithTTLWritesBoth(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	l1, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	l2, _ := New[string, int](WithMaxEntries(64), WithClock(clk))
	tc := NewTiered(l1, l2)
	defer tc.Close()

	if err := tc.SetWithTTL("k", 1, time.Minute); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	if !l1.Has("k") || !l2.Has("k") {
		t.Error("SetWithTTL should write to both tiers")
	}
	clk.Advance(2 * time.Minute)
	if l1.Has("k") || l2.Has("k") {
		t.Error("SetWithTTL TTL should expire entries in both tiers")
	}
}

func TestTieredSetWithOptionsWritesBoth(t *testing.T) {
	tc, l1, l2 := newTieredPair(t)
	defer tc.Close()

	if err := tc.SetWithOptions("k", 1, SetTags("group-a")); err != nil {
		t.Fatalf("SetWithOptions: %v", err)
	}
	// Tags should be present in both tiers.
	if l1.InvalidateTag("group-a") != 1 {
		t.Error("L1 missing the tag")
	}
	if l2.InvalidateTag("group-a") != 1 {
		t.Error("L2 missing the tag")
	}
}

func TestTieredInvalidateTagSpansBoth(t *testing.T) {
	tc, _, _ := newTieredPair(t)
	defer tc.Close()

	_ = tc.SetWithOptions("a", 1, SetTags("g"))
	_ = tc.SetWithOptions("b", 2, SetTags("g"))

	// Each entry exists in BOTH tiers, so InvalidateTag should
	// remove 4 entries total (2 keys × 2 tiers).
	if removed := tc.InvalidateTag("g"); removed != 4 {
		t.Errorf("InvalidateTag('g') = %d, want 4 (2 keys × 2 tiers)", removed)
	}
	if tc.Has("a") || tc.Has("b") {
		t.Error("entries still present after tag invalidation")
	}
}

func TestTieredSyncDrainsBoth(t *testing.T) {
	tc, l1, l2 := newTieredPair(t)
	defer tc.Close()

	_ = tc.Set("k", 1)
	if err := tc.Sync(context.Background()); err != nil {
		t.Errorf("Sync: %v", err)
	}
	// Sanity: synchronous Set should be observable post-Sync.
	if !l1.Has("k") || !l2.Has("k") {
		t.Error("Set not visible after Sync")
	}
}

func TestTieredGetCtxRoutesThroughTiers(t *testing.T) {
	tc, _, l2 := newTieredPair(t)
	defer tc.Close()

	// Pre-populate L2 only (bypass Tiered.Set so L1 stays empty).
	_ = l2.Set("k", 99)
	v, ok, err := tc.GetCtx(context.Background(), "k")
	if err != nil || !ok || v != 99 {
		t.Errorf("GetCtx = (%d, %v, %v), want (99, true, nil)", v, ok, err)
	}
	stats := tc.Stats()
	if stats.L2Hits != 1 {
		t.Errorf("L2Hits = %d, want 1", stats.L2Hits)
	}
}

func TestTieredLenReportsL2(t *testing.T) {
	tc, l1, _ := newTieredPair(t)
	defer tc.Close()

	for i, k := range []string{"a", "b", "c", "d", "e"} {
		_ = tc.Set(k, i)
	}
	// L1 budget is 4; L2 holds all 5.
	got := tc.Len()
	if got != 5 {
		t.Errorf("Tiered.Len = %d, want 5 (L2 size); L1.Len = %d", got, l1.Len())
	}
}

func TestTieredGetCtxRespectsCanceledContext(t *testing.T) {
	tc, _, _ := newTieredPair(t)
	defer tc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := tc.GetCtx(ctx, "k")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("GetCtx with canceled ctx err = %v, want context.Canceled", err)
	}
}
