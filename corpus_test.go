// corpus_test.go is the regression-scenario suite. Every test in
// this file replays a specific operation sequence that exposed a bug
// during development. They are the "narrow but deep" complement to
// fuzz_test.go's "wide but random" coverage: a fuzz failure that
// reduces to a one-line assertion belongs here, encoded with full
// context, so the regression can never re-emerge silently.
//
// New entries should:
//   - reference the fix commit / PR in the comment block,
//   - assert the exact behavior that was wrong, and
//   - keep the operation sequence as small as possible while still
//     reproducing the failure.

package memcache

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCorpus_S3FIFOGhostRebirthSurvivesPostInsertEviction guards the
// S3-FIFO Ghost-rebirth path against a regression where a freshly
// re-inserted key (returning from Ghost into Main) was selected as
// the Main victim by the SAME upsert's post-insert eviction loop.
//
// Reproduction recipe (from FuzzCacheOps/812f7169a63de001):
//   - Capacity budget = 9 (8 max-entries + ~10% slop, single shard).
//   - Insert key K once, get it evicted to Ghost.
//   - Insert 8 other distinct keys, Get each once so freq=1.
//   - Re-insert K. The Ghost-hit routes K to Main with the spec's
//     freq=1 boost. Without the boost, the same Victim() call
//     promotes 8 Small entries into Main, Main overflows, and K
//     (sitting at mainHead with freq=0) is the chosen victim.
//
// The user-visible symptom of the bug was that c.Set(K) returned nil
// but the very next c.Get(K) returned ok=false.
func TestCorpus_S3FIFOGhostRebirthSurvivesPostInsertEviction(t *testing.T) {
	c, err := New[byte, byte](WithMaxEntries(8), WithShards(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Initial insert that will get evicted to Ghost when overflowed.
	if _, err := c.SetIfAbsent(250, 0); err != nil {
		t.Fatal(err)
	}

	// Saturate Small: 8 distinct keys, each Set then Get to bump freq.
	keys := []byte{48, 49, 56, 55, 57, 66, 50, 65, 67}
	for i, k := range keys {
		val := byte((i + 1) * 2)
		if err := c.Set(k, val); err != nil {
			t.Fatalf("Set(%d): %v", k, err)
		}
		if got, ok := c.Get(k); !ok || got != val {
			t.Fatalf("Set(%d, %d) → Get = (%d, %v)", k, val, got, ok)
		}
	}

	// Re-insert the original key — the regression case.
	if err := c.Set(250, 99); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Get(250)
	if !ok || got != 99 {
		t.Fatalf("after Ghost-rebirth Set(250, 99) → Get = (%d, %v); want (99, true)", got, ok)
	}
}

// TestCorpus_StampedeOnlyInvokesLoaderOnce locks down the
// singleflight contract: 100 concurrent GetOrLoad on the same missing
// key must result in exactly one loader invocation. Spec §6 lists
// this as the package's flagship guarantee.
func TestCorpus_StampedeOnlyInvokesLoaderOnce(t *testing.T) {
	hold := make(chan struct{})
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		<-hold
		return 7, 0, nil
	})
	c, _ := New[string, int](WithMaxEntries(8), WithLoader(loader))
	defer c.Close()

	const G = 100
	var wg sync.WaitGroup
	results := make([]int, G)
	for i := range G {
		wg.Go(func() {
			v, _ := c.GetOrLoad(context.Background(), "k")
			results[i] = v
		})
	}
	time.Sleep(5 * time.Millisecond)
	close(hold)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("loader fired %d times; want 1", calls.Load())
	}
	for i, v := range results {
		if v != 7 {
			t.Fatalf("goroutine %d: got %d, want 7", i, v)
		}
	}
}

// TestCorpus_NegativeCacheTombstoneSticks guards against a
// regression where ErrNotFound from the loader was not converted
// into a tombstone, causing every subsequent GetOrLoad to invoke
// the loader again. The negative-cache TTL must hold the tombstone
// for the configured duration.
func TestCorpus_NegativeCacheTombstoneSticks(t *testing.T) {
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		return 0, 0, ErrNotFound
	})
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader(loader),
		WithNegativeCache(time.Hour),
	)
	defer c.Close()

	for range 50 {
		_, err := c.GetOrLoad(context.Background(), "x")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader fired %d times under negative cache; want 1", calls.Load())
	}
}

// TestCorpus_SnapshotRoundTripPreservesNonExpiredEntries guards the
// Save/Load contract: an entry's value, TTL, and tags survive a
// round-trip. The CRC trailer catches truncation but not silent
// payload mismatches, so we assert the rebuilt cache matches the
// source's per-entry state.
func TestCorpus_SnapshotRoundTripPreservesNonExpiredEntries(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(64))
	defer src.Close()
	_ = src.SetWithTags("a", 1, "tag-x")
	_ = src.SetWithTags("b", 2, "tag-y")
	_ = src.SetWithTTL("c", 3, time.Hour)

	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := New[string, int](WithMaxEntries(64))
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"a", "b", "c"} {
		got, ok := dst.Get(k)
		if !ok {
			t.Errorf("after round-trip: Get(%q) miss", k)
			continue
		}
		want := map[string]int{"a": 1, "b": 2, "c": 3}[k]
		if got != want {
			t.Errorf("after round-trip: Get(%q) = %d, want %d", k, got, want)
		}
	}
	// Tag-based invalidation must still work after a load —
	// proves tag indexes were rebuilt, not silently dropped.
	if dropped := dst.InvalidateTag("tag-x"); dropped != 1 {
		t.Errorf("InvalidateTag after Load dropped %d, want 1", dropped)
	}
}

// TestCorpus_CloseIsIdempotent guards against double-close panics.
// Real REPL code can call Close from both a defer and an explicit
// shutdown handler; the second call must be a no-op error, not a
// panic on closed channels.
func TestCorpus_CloseIsIdempotent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestCorpus_DeleteDuringIterationDoesNotDeadlock guards against
// the lock-ordering trap where a callback inside Items() or Range()
// tries to mutate the same shard. The cache must either snapshot
// before iterating (safe re-entry) or reject mutation with a clear
// error — never deadlock.
func TestCorpus_DeleteDuringIterationDoesNotDeadlock(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64))
	defer c.Close()
	for i := range 10 {
		_ = c.Set(itoaSimple(i), i)
	}
	done := make(chan struct{})
	go func() {
		for _, ki := range c.Items() {
			c.Delete(ki.Key)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Delete-during-iteration deadlocked")
	}
	if c.Len() != 0 {
		t.Errorf("after delete-all: Len=%d, want 0", c.Len())
	}
}
