// corpus_test.go is the regression-scenario suite. Each test
// replays a specific operation sequence that exposed a bug.

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

// TestCorpus_S3FIFOGhostRebirthSurvivesPostInsertEviction is a
// regression for FuzzCacheOps/812f7169a63de001: a Ghost-rebirth
// re-insert was selected as Main victim by the same upsert's
// post-insert eviction loop. Symptom: c.Set(K) returned nil but
// the next c.Get(K) returned ok=false.
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
			t.Fatalf("Set(%d, %d) -> Get = (%d, %v)", k, val, got, ok)
		}
	}

	// Re-insert the original key (the regression case).
	if err := c.Set(250, 99); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Get(250)
	if !ok || got != 99 {
		t.Fatalf("after Ghost-rebirth Set(250, 99) -> Get = (%d, %v); want (99, true)", got, ok)
	}
}

// TestCorpus_StampedeOnlyInvokesLoaderOnce: 100 concurrent
// GetOrLoad on the same missing key MUST yield exactly one loader
// invocation.
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

// TestCorpus_NegativeCacheTombstoneSticks is a regression for
// ErrNotFound not being converted into a tombstone.
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
	// Tag-based invalidation must still work after a load
	// (proves tag indexes were rebuilt).
	if dropped := dst.InvalidateTag("tag-x"); dropped != 1 {
		t.Errorf("InvalidateTag after Load dropped %d, want 1", dropped)
	}
}

// TestCorpus_CloseIsIdempotent guards against double-close panics.
func TestCorpus_CloseIsIdempotent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestCorpus_DeleteDuringIterationDoesNotDeadlock: mutating the
// cache from within Items()/Range() must not deadlock.
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
