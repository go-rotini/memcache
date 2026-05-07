package memcache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInvalidationPublisher_FiresOnDelete(t *testing.T) {
	var mu sync.Mutex
	var got []EvictionReason
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithInvalidationPublisher(func(_ string, r EvictionReason) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, r)
		}),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	c.Delete("k")
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != EvictReasonDeleted {
		t.Errorf("publisher saw %v, want [Deleted]", got)
	}
}

func TestInvalidationPublisher_FiresOnEvict(t *testing.T) {
	var mu sync.Mutex
	count := 0
	c, _ := New[string, int](
		WithMaxEntries(2),
		WithShards(1),
		WithInvalidationPublisher(func(string, EvictionReason) {
			mu.Lock()
			defer mu.Unlock()
			count++
		}),
	)
	defer c.Close()
	for i := range 10 {
		_ = c.Set("k"+itoaSimple(i), i)
	}
	mu.Lock()
	defer mu.Unlock()
	if count == 0 {
		t.Error("publisher should fire on capacity-driven eviction")
	}
}

func TestInvalidationSubscriber_ReceivesAndDeletes(t *testing.T) {
	ch := make(chan string, 4)
	var mu sync.Mutex
	publisherSeen := []EvictionReason{}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithInvalidationSubscriber(ch),
		WithInvalidationPublisher(func(_ string, r EvictionReason) {
			mu.Lock()
			defer mu.Unlock()
			publisherSeen = append(publisherSeen, r)
		}),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	ch <- "k"

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !c.Has("k") {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if c.Has("k") {
		t.Fatal("subscriber did not delete the key within timeout")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(publisherSeen) != 1 || publisherSeen[0] != EvictReasonRemote {
		t.Errorf("publisher saw %v, want [Remote]", publisherSeen)
	}
}

func TestInvalidationSubscriber_GoroutineExitsOnClose(t *testing.T) {
	ch := make(chan string)
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithInvalidationSubscriber(ch),
	)
	// Close must cause the subscriber goroutine to return; verify
	// by confirming Close returns within a reasonable budget.
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked; subscriber goroutine likely leaked")
	}
}

func TestWithShardedStats_HitsAndMissesAccurate(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShardedStats(true),
	)
	defer c.Close()

	_ = c.Set("a", 1)
	_ = c.Set("b", 2)

	// Drive a known mix of hits and misses across many goroutines
	// to actually exercise the per-CPU shadow.
	var wg sync.WaitGroup
	hits := atomic.Int64{}
	misses := atomic.Int64{}
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				if _, ok := c.Get("a"); ok {
					hits.Add(1)
				} else {
					misses.Add(1)
				}
				if _, ok := c.Get("missing"); ok {
					hits.Add(1)
				} else {
					misses.Add(1)
				}
			}
		})
	}
	wg.Wait()

	st := c.Stats()
	if int64(st.Hits) != hits.Load() {
		t.Errorf("Stats.Hits = %d, expected %d", st.Hits, hits.Load())
	}
	if int64(st.Misses) != misses.Load() {
		t.Errorf("Stats.Misses = %d, expected %d", st.Misses, misses.Load())
	}
}
