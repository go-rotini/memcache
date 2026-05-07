// get_fastpath_test.go covers the read-lock fast path. The
// behavioral contract is identical to the slow-path Get.

package memcache

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestFastPathFIFOServesUnderRLock(t *testing.T) {
	// FIFO never promotes, so every hit takes the fast path.
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithPolicy(PolicyFIFO),
	)
	defer c.Close()
	_ = c.Set("hot", 42)

	const G = 16
	const N = 1000
	var wg sync.WaitGroup
	wrong := make(chan int, G*N)
	for range G {
		wg.Go(func() {
			for range N {
				v, ok := c.Get("hot")
				if !ok || v != 42 {
					wrong <- v
				}
			}
		})
	}
	wg.Wait()
	close(wrong)
	if got := len(wrong); got != 0 {
		t.Errorf("%d torn reads on fast path", got)
	}
}

func TestFastPathS3FIFOAtSaturation(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithPolicy(PolicyS3FIFO),
		WithShards(1),
	)
	defer c.Close()
	_ = c.Set("k", 1)

	// Drive freq to saturation. Each Get bumps freq up to 3.
	for range 5 {
		v, ok := c.Get("k")
		if !ok || v != 1 {
			t.Fatalf("Get during ramp-up = (%d, %v)", v, ok)
		}
	}
	// Subsequent Gets should now hit the fast path. Behavioral
	// check: the answer is unchanged.
	for range 100 {
		v, ok := c.Get("k")
		if !ok || v != 1 {
			t.Fatalf("Get post-saturation = (%d, %v)", v, ok)
		}
	}
}

func TestFastPathRespectsExpiry(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithPolicy(PolicyFIFO),
		WithClock(clk),
		WithTTLJitter(0),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Second)
	clk.Advance(2 * time.Second)
	if _, ok := c.Get("k"); ok {
		t.Error("expired entry must miss even on fast path")
	}
}

func TestFastPathRespectsNegativeTombstone(t *testing.T) {
	loader := LoaderFunc[string, int](func(context.Context, string) (int, time.Duration, error) {
		return 0, 0, ErrNotFound
	})
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithPolicy(PolicyFIFO),
		WithLoader(loader),
		WithNegativeCache(time.Hour),
	)
	defer c.Close()
	_, _ = c.GetOrLoad(context.Background(), "missing")
	if _, ok := c.Get("missing"); ok {
		t.Error("negative tombstone must miss even on fast path")
	}
}

func TestFastPathSlidingTTLTakesSlowPath(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithPolicy(PolicyFIFO),
		WithClock(clk),
		WithSlidingTTL(true),
		WithTTLJitter(0),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Hour)
	clk.Advance(30 * time.Minute)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("Get should hit before half-TTL")
	}
	// Advance past the original expiry; if sliding worked, the
	// entry is still alive.
	clk.Advance(45 * time.Minute) // total = 1h15m
	if _, ok := c.Get("k"); !ok {
		t.Error("sliding TTL should have moved expiry past wall-clock advance")
	}
}

func TestFastPathBumpsHits(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithPolicy(PolicyFIFO),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	for range 10 {
		_, _ = c.Get("k")
	}
	if got := c.Stats().Hits; got != 10 {
		t.Errorf("Stats.Hits = %d, want 10 (fast path must record hits)", got)
	}
}

func TestFastPathFiresOnHitHook(t *testing.T) {
	hits := 0
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithPolicy(PolicyFIFO),
		WithOnHit[string, int](func(string, int) { hits++ }),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	for range 5 {
		_, _ = c.Get("k")
	}
	if hits != 5 {
		t.Errorf("OnHit fired %d times, want 5", hits)
	}
}
