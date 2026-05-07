package memcache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// expectFlatStorage reports whether every shard of c uses [flatStore].
// Tests rely on it to confirm WithFlatStorage actually propagated to
// shard construction.
func expectFlatStorage[K comparable, V any](t *testing.T, c *Cache[K, V]) {
	t.Helper()
	for i, sh := range c.shards {
		if _, ok := sh.storage.(*flatStore[K, V]); !ok {
			t.Errorf("shard %d storage type = %T, want *flatStore", i, sh.storage)
		}
	}
}

func TestWithFlatStorageProducesFlatShards(t *testing.T) {
	c, err := New[string, int](
		WithMaxEntries(64),
		WithShards(2),
		WithFlatStorage(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	expectFlatStorage(t, c)
}

func TestWithoutFlatStorageProducesMapShards(t *testing.T) {
	c, err := New[string, int](
		WithMaxEntries(64),
		WithShards(2),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	for i, sh := range c.shards {
		if _, ok := sh.storage.(*mapStore[string, int]); !ok {
			t.Errorf("shard %d storage type = %T, want *mapStore (default)", i, sh.storage)
		}
	}
}

func TestFlatStorageBasicSetGetDelete(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(2),
		WithFlatStorage(),
	)
	defer c.Close()

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, ok := c.Get("a"); !ok || got != 1 {
		t.Errorf("Get(a) = (%d, %v), want (1, true)", got, ok)
	}
	if !c.Delete("a") {
		t.Error("Delete(a) reported nothing removed")
	}
	if _, ok := c.Get("a"); ok {
		t.Error("Get(a) succeeded after Delete")
	}
}

func TestFlatStorageRangeAndKeys(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(1),
		WithFlatStorage(),
	)
	defer c.Close()

	want := map[string]int{"a": 1, "b": 2, "c": 3}
	for k, v := range want {
		_ = c.Set(k, v)
	}

	got := map[string]int{}
	c.Range(func(k string, v int) bool {
		got[k] = v
		return true
	})
	if len(got) != len(want) {
		t.Fatalf("Range visited %d entries, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Range: key %q value = %d, want %d", k, got[k], v)
		}
	}

	keys := c.Keys()
	if len(keys) != len(want) {
		t.Errorf("Keys() returned %d, want %d", len(keys), len(want))
	}
}

func TestFlatStorageStatsCompactionsRisesAfterChurn(t *testing.T) {
	// Force a single shard so we can predict pressure on a single
	// flatStore.
	c, _ := New[string, int](
		WithMaxEntries(2048),
		WithShards(1),
		WithFlatStorage(),
	)
	defer c.Close()

	if got := c.Stats().Compactions; got != 0 {
		t.Errorf("baseline Compactions = %d, want 0", got)
	}

	// Insert enough to trigger at least one grow.
	for i := range 256 {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	if got := c.Stats().Compactions; got == 0 {
		t.Error("Compactions did not advance after insert burst")
	}

	// Re-insert/delete pattern to trigger a tombstone-driven
	// compaction.
	prev := c.Stats().Compactions
	for i := range 256 {
		_ = c.Delete(fmt.Sprintf("k%d", i))
	}
	if got := c.Stats().Compactions; got <= prev {
		t.Errorf("Compactions = %d after deletes, want > %d", got, prev)
	}
}

func TestFlatStorageStatsCompactionsZeroForMapStorage(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(2048),
		WithShards(1),
		// no WithFlatStorage
	)
	defer c.Close()

	for i := range 256 {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	for i := range 256 {
		_ = c.Delete(fmt.Sprintf("k%d", i))
	}
	if got := c.Stats().Compactions; got != 0 {
		t.Errorf("Compactions = %d for map-backed cache, want 0", got)
	}
}

func TestFlatStorageRespectsMaxEntries(t *testing.T) {
	// Bound the cache and confirm the eviction policy still bites.
	c, _ := New[string, int](
		WithMaxEntries(32),
		WithShards(1),
		WithPolicy(PolicyLRU),
		WithFlatStorage(),
	)
	defer c.Close()

	for i := range 256 {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	// LRU with a 32-entry budget should stay near the budget; the
	// eviction overflow tolerance lets it sit slightly above.
	if got := c.Len(); got > 64 {
		t.Errorf("Len = %d, expected near 32 with LRU+budget", got)
	}
}

func TestFlatStorageSnapshotRoundTrip(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(2),
		WithFlatStorage(),
	)
	defer c.Close()

	for i := range 16 {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}

	var buf bytes.Buffer
	if err := c.Save(&buf); err != nil {
		t.Fatalf("Save: %v", err)
	}

	c2, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(2),
		WithFlatStorage(),
	)
	defer c2.Close()
	if _, err := c2.Load(&buf); err != nil {
		t.Fatalf("Load: %v", err)
	}
	for i := range 16 {
		k := fmt.Sprintf("k%d", i)
		if got, ok := c2.Get(k); !ok || got != i {
			t.Errorf("after Load: Get(%s) = (%d, %v), want (%d, true)", k, got, ok, i)
		}
	}
}

func TestFlatStorageConcurrentSetGet(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(2048),
		WithShards(4),
		WithFlatStorage(),
	)
	defer c.Close()

	const goroutines = 8
	const opsPerG = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func() {
			defer wg.Done()
			for i := range opsPerG {
				k := fmt.Sprintf("g%d-k%d", g, i)
				_ = c.Set(k, g*1000+i)
				if got, ok := c.Get(k); !ok || got != g*1000+i {
					t.Errorf("goroutine %d: Get(%s) = (%d, %v), want (%d, true)",
						g, k, got, ok, g*1000+i)
				}
			}
		}()
	}
	wg.Wait()
}
