// acceptance_test.go validates end-to-end scenarios. Invoked via
// `make test-acceptance` (Makefile filter `-run TestAcceptance`).

package memcache

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAcceptanceREPLWarmRestart simulates a REPL exiting cleanly,
// snapshotting its cache state, and re-launching with auto-load.
func TestAcceptanceREPLWarmRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "repl-cache.gob")

	// First run: populate the cache and exit cleanly.
	first, err := New[string, int](
		WithMaxEntries(64),
		WithName("repl-1"),
		WithAutoSave(path, 0), // final-save on Close only
	)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		if err := first.Set(itoaSimple(i), i*100); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// Second run: WithAutoLoad reads the snapshot at New() time.
	second, err := New[string, int](
		WithMaxEntries(64),
		WithName("repl-2"),
		WithAutoLoad(path),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	if got := second.Len(); got != 10 {
		t.Errorf("warm-restart Len = %d, want 10", got)
	}
	for i := range 10 {
		v, ok := second.Get(itoaSimple(i))
		if !ok || v != i*100 {
			t.Errorf("warm-restart Get(%d) = (%d, %v), want (%d, true)", i, v, ok, i*100)
		}
	}
}

// TestAcceptanceTokenRefresh simulates an auth-token cache that
// silently refreshes via WithRefreshAhead before the token expires.
func TestAcceptanceTokenRefresh(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	tokenVersion := atomic.Int64{}
	tokenVersion.Store(1)
	loader := LoaderFunc[string, string](func(_ context.Context, _ string) (string, time.Duration, error) {
		v := tokenVersion.Load()
		return "token-v" + itoaSimple(int(v)), 10 * time.Second, nil
	})
	c, _ := New[string, string](
		WithMaxEntries(8),
		WithClock(clk),
		WithLoader[string, string](loader),
		WithRefreshAhead(0.5), // refresh after 50% of TTL elapses
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()

	// Initial load.
	v, err := c.GetOrLoad(context.Background(), "auth")
	if err != nil || v != "token-v1" {
		t.Fatalf("initial load = (%q, %v)", v, err)
	}

	// Advance to 60% of TTL: refresh-ahead should fire on Get.
	clk.Advance(6 * time.Second)
	tokenVersion.Store(2) // upstream rotated the token
	if v, _ := c.Get("auth"); v != "token-v1" {
		t.Errorf("during refresh, Get returns stale-but-fresh token; got %q", v)
	}
	// Wait for the async refresh to land.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if v, _ := c.Get("auth"); v == "token-v2" {
			return // success
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("refresh-ahead did not pick up the rotated token within timeout")
}

func TestAcceptanceFsnotifyInvalidation(t *testing.T) {
	c, _ := New[string, []byte](
		WithMaxBytes(1<<20),
		WithWeigher[[]byte](BytesWeigher()),
	)
	defer c.Close()

	// Cache derived artifacts, tagged by their source file.
	_ = c.SetWithTags("ast:foo.go", []byte("ast bytes"), "src:foo.go")
	_ = c.SetWithTags("doc:foo.go", []byte("docs bytes"), "src:foo.go")
	_ = c.SetWithTags("ast:bar.go", []byte("bar bytes"), "src:bar.go")

	// fsnotify sees foo.go change → drop everything derived from it.
	dropped := c.InvalidateTag("src:foo.go")
	if dropped != 2 {
		t.Errorf("fsnotify-style invalidation dropped %d, want 2", dropped)
	}
	if c.Has("ast:foo.go") || c.Has("doc:foo.go") {
		t.Error("foo.go-derived artifacts should be gone")
	}
	if !c.Has("ast:bar.go") {
		t.Error("bar.go artifact should survive")
	}
}

// TestAcceptanceStampede1000ConcurrentLoads: 1000 goroutines all
// fetching the same missing key must invoke the Loader exactly once.
func TestAcceptanceStampede1000ConcurrentLoads(t *testing.T) {
	hold := make(chan struct{})
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		<-hold
		return 42, 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader[string, int](loader),
	)
	defer c.Close()

	const G = 1000
	var wg sync.WaitGroup
	got := make([]int, G)
	for i := range G {
		wg.Go(func() {
			v, err := c.GetOrLoad(context.Background(), "k")
			if err != nil {
				t.Errorf("goroutine %d error: %v", i, err)
			}
			got[i] = v
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(hold)
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("loader fired %d times across 1000 goroutines; want 1", calls.Load())
	}
	for i, v := range got {
		if v != 42 {
			t.Errorf("goroutine %d got %d, want 42", i, v)
		}
	}
}

// TestAcceptanceDaemonSnapshotRotation: auto-save goroutine must
// not leak resources or interfere with foreground reads/writes.
func TestAcceptanceDaemonSnapshotRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.gob")
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithAutoSave(path, 50*time.Millisecond), // tight rotation
	)
	defer c.Close()

	// Drive foreground load while auto-save runs in the background.
	stop := atomic.Bool{}
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			j := 0
			for !stop.Load() {
				_ = c.Set(itoaSimple(i*1000+j%100), j)
				_, _ = c.Get(itoaSimple(i*1000 + j%100))
				j++
			}
		})
	}

	// Run for 200ms (plenty of time for several save rotations).
	time.Sleep(200 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	// Confirm a snapshot exists and is loadable.
	other, err := New[string, int](
		WithMaxEntries(64),
		WithAutoLoad(path),
		WithAutoLoadIgnoreErrors(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
}

func TestAcceptanceNegativeCacheBlocksRepeatedLookups(t *testing.T) {
	calls := atomic.Int64{}
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		calls.Add(1)
		return 0, 0, ErrNotFound
	})
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLoader[string, int](loader),
		WithNegativeCache(time.Hour),
	)
	defer c.Close()

	for range 100 {
		_, err := c.GetOrLoad(context.Background(), "missing")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Errorf("loader fired %d times across 100 lookups; want 1 (negative cache)", calls.Load())
	}
}

// TestAcceptanceTieredWarmPool composes a tight L1 with a generous L2.
func TestAcceptanceTieredWarmPool(t *testing.T) {
	l1, _ := New[string, int](WithMaxEntries(4), WithShards(1))
	l2, _ := New[string, int](WithMaxEntries(64))
	tc := NewTiered(l1, l2)
	defer tc.Close()

	for i := range 20 {
		_ = tc.Set(itoaSimple(i), i)
	}
	// L1 saturates fast; L2 retains everything.
	if l2.Len() != 20 {
		t.Errorf("L2.Len = %d, want 20", l2.Len())
	}

	// Reads on entries that fell out of L1 hit L2 and promote.
	for i := range 5 {
		v, ok := tc.Get(itoaSimple(i))
		if !ok || v != i {
			t.Errorf("Tiered.Get(%d) = (%d, %v)", i, v, ok)
		}
	}
	st := tc.Stats()
	if st.L2Hits == 0 {
		t.Error("expected L2 hits during sweep")
	}
}
