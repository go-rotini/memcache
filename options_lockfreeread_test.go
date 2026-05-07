package memcache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLockFreeReadGatedByPolicy(t *testing.T) {
	// LRU policy is not lock-free-safe in v0; the option must
	// silently disable.
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithPolicy(PolicyLRU),
		WithLockFreeRead(),
	)
	defer c.Close()
	for _, sh := range c.shards {
		if sh.read.Load() != nil {
			t.Error("LockFreeRead should have been gated off under LRU")
		}
	}
}

func TestLockFreeReadGatedByExpireFunc(t *testing.T) {
	// WithExpireFunc reads e.tags (non-atomic); the lock-free
	// path must be disabled.
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithExpireFunc[string, int](func(string, int, Metadata) bool { return false }),
		WithLockFreeRead(),
	)
	defer c.Close()
	for _, sh := range c.shards {
		if sh.read.Load() != nil {
			t.Error("LockFreeRead should have been gated off when ExpireFunc is configured")
		}
	}
}

func TestLockFreeReadResetReplacesSnapshot(t *testing.T) {
	// Reset must replace the snapshot AND not pool any entry the
	// snapshot still references; otherwise concurrent readers may
	// dereference recycled entries.
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(1),
		WithLockFreeRead(),
	)
	defer c.Close()

	for i := 0; i < 8; i++ {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	c.promoteReadMap(c.shards[0])
	pre := c.shards[0].read.Load()
	if pre == nil || len(pre.m) == 0 {
		t.Fatal("setup: expected populated snapshot")
	}

	c.Reset()

	post := c.shards[0].read.Load()
	if post == nil {
		t.Fatal("Reset cleared snapshot pointer; expected fresh empty snapshot")
	}
	if len(post.m) != 0 {
		t.Errorf("post-Reset snapshot has %d entries, want 0", len(post.m))
	}
	// Confirm the previous snapshot's entries are marked
	// invalidated so a stale-pointer reader sees a miss.
	for _, e := range pre.m {
		if !e.invalidated() {
			t.Error("Reset did not mark previously-snapshotted entry invalidated")
			break
		}
	}
}

func TestLockFreeReadClearReplacesSnapshot(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(1),
		WithLockFreeRead(),
	)
	defer c.Close()

	for i := 0; i < 8; i++ {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	c.promoteReadMap(c.shards[0])
	pre := c.shards[0].read.Load()

	c.Clear()

	post := c.shards[0].read.Load()
	if post == nil || len(post.m) != 0 {
		t.Errorf("post-Clear snapshot = %+v, want empty", post)
	}
	for _, e := range pre.m {
		if !e.invalidated() {
			t.Error("Clear did not mark previously-snapshotted entry invalidated")
			break
		}
	}
}

func TestLockFreeReadDefaultIsOff(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for _, sh := range c.shards {
		if sh.read.Load() != nil {
			t.Error("shard.read is non-nil without WithLockFreeRead")
		}
	}
}

func TestLockFreeReadInitializesEmptySnapshot(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLockFreeRead(),
	)
	defer c.Close()
	for i, sh := range c.shards {
		rm := sh.read.Load()
		if rm == nil {
			t.Errorf("shard %d snapshot is nil after construction", i)
			continue
		}
		if rm.amended {
			t.Errorf("shard %d initial snapshot has amended=true", i)
		}
		if len(rm.m) != 0 {
			t.Errorf("shard %d initial snapshot is non-empty", i)
		}
	}
}

func TestLockFreeReadSetMarksAmended(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(1),
		WithLockFreeRead(),
	)
	defer c.Close()

	if err := c.Set("k", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	rm := c.shards[0].read.Load()
	if rm == nil {
		t.Fatal("snapshot is nil after Set")
	}
	if !rm.amended {
		t.Error("snapshot is not amended after Set on a fresh shard")
	}
}

func TestLockFreeReadGetMissOnAmendedFallsThrough(t *testing.T) {
	// Snapshot is empty + amended after the first Set, so the
	// follow-up Get should fall through to dirty and find the key.
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(1),
		WithLockFreeRead(),
	)
	defer c.Close()

	_ = c.Set("k", 99)
	if got, ok := c.Get("k"); !ok || got != 99 {
		t.Errorf("Get(k) = (%d, %v), want (99, true)", got, ok)
	}
}

func TestLockFreeReadGetServesFromSnapshotAfterPromotion(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(1),
		WithLockFreeRead(),
	)
	defer c.Close()

	_ = c.Set("k", 99)
	// Force promotion so subsequent Gets serve from the snapshot
	// without ever taking the lock.
	c.promoteReadMap(c.shards[0])

	rm := c.shards[0].read.Load()
	if _, ok := rm.m["k"]; !ok {
		t.Fatal("snapshot does not contain k after promotion")
	}
	if rm.amended {
		t.Error("snapshot is still amended after promotion")
	}

	if got, ok := c.Get("k"); !ok || got != 99 {
		t.Errorf("Get(k) post-promotion = (%d, %v), want (99, true)", got, ok)
	}
}

func TestLockFreeReadDeleteMarksEntryInvalidated(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(1),
		WithLockFreeRead(),
	)
	defer c.Close()

	_ = c.Set("k", 1)
	c.promoteReadMap(c.shards[0])

	rm := c.shards[0].read.Load()
	e := rm.m["k"]
	if e == nil {
		t.Fatal("entry not in snapshot post-promotion")
	}

	c.Delete("k")
	if !e.invalidated() {
		t.Error("entry not marked invalidated after Delete")
	}
	if _, ok := c.Get("k"); ok {
		t.Error("Get(k) returned true after Delete")
	}
}

func TestLockFreeReadPromotesAfterEnoughMisses(t *testing.T) {
	// Initial miss threshold is 8 (the floor in readMissThreshold).
	// Setting more than 8 keys without promoting should eventually
	// trigger automatic promotion via the under-the-snapshot miss
	// counter.
	c, _ := New[string, int](
		WithMaxEntries(1024),
		WithShards(1),
		WithLockFreeRead(),
	)
	defer c.Close()

	for i := 0; i < 16; i++ {
		_ = c.Set(fmt.Sprintf("k%d", i), i)
	}
	// Snapshot is currently amended-empty (from initialization);
	// each Get is a snapshot miss + dirty hit. After enough misses,
	// the slow path triggers promoteReadMap.
	for i := 0; i < 16; i++ {
		k := fmt.Sprintf("k%d", i)
		if got, ok := c.Get(k); !ok || got != i {
			t.Errorf("Get(%s) = (%d, %v), want (%d, true)", k, got, ok, i)
		}
	}

	rm := c.shards[0].read.Load()
	if rm == nil || rm.amended || len(rm.m) == 0 {
		t.Errorf("snapshot did not auto-promote after >=8 misses; got %+v", rm)
	}
}

func TestLockFreeReadConcurrentGetSetUnderRace(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(2048),
		WithShards(4),
		WithLockFreeRead(),
	)
	defer c.Close()

	const writers = 4
	const readers = 8
	const opsPerG = 500

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	// Pre-seed so reads have something to find.
	for i := 0; i < 100; i++ {
		_ = c.Set(fmt.Sprintf("seed-%d", i), i)
	}

	stop := make(chan struct{})

	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				k := fmt.Sprintf("w%d-%d", id, i)
				_ = c.Set(k, id*1000+i)
			}
		}(w)
	}

	hits := atomic.Int64{}
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				k := fmt.Sprintf("seed-%d", i%100)
				if _, ok := c.Get(k); ok {
					hits.Add(1)
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	if hits.Load() == 0 {
		t.Error("no reader hits — something is very wrong")
	}
}

func TestLockFreeReadHandlesUpdateWithoutInvalidating(t *testing.T) {
	// An in-place update should overwrite the entry's value without
	// marking it invalidated; the snapshot's pointer is still valid
	// and the new value is visible atomically.
	c, _ := New[string, int](
		WithMaxEntries(64),
		WithShards(1),
		WithLockFreeRead(),
	)
	defer c.Close()

	_ = c.Set("k", 1)
	c.promoteReadMap(c.shards[0])

	rm := c.shards[0].read.Load()
	e := rm.m["k"]

	_ = c.Set("k", 42)

	if e.invalidated() {
		t.Error("entry marked invalidated after update (should be in-place)")
	}
	if got, ok := c.Get("k"); !ok || got != 42 {
		t.Errorf("Get(k) post-update = (%d, %v), want (42, true)", got, ok)
	}
}

func BenchmarkGetLockFreeReadHit(b *testing.B) {
	c, _ := New[string, int](
		WithMaxEntries(1024),
		WithShards(8),
		WithLockFreeRead(),
	)
	defer c.Close()

	keys := make([]string, 256)
	for i := 0; i < 256; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		_ = c.Set(keys[i], i)
	}
	// Force promotion so all reads hit the snapshot fast path.
	for _, sh := range c.shards {
		c.promoteReadMap(sh)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get(keys[i&255])
			i++
		}
	})
}

func BenchmarkGetDefaultHit(b *testing.B) {
	// Comparison baseline: no WithLockFreeRead.
	c, _ := New[string, int](
		WithMaxEntries(1024),
		WithShards(8),
	)
	defer c.Close()

	keys := make([]string, 256)
	for i := 0; i < 256; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		_ = c.Set(keys[i], i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get(keys[i&255])
			i++
		}
	})
}

func BenchmarkGetLockFreeReadHitNoStats(b *testing.B) {
	// Stats counters add atomic contention on the hot path; this
	// bench measures the lock-free path with stats off, isolating
	// the snapshot-and-entry overhead.
	c, _ := New[string, int](
		WithMaxEntries(1024),
		WithShards(8),
		WithLockFreeRead(),
		WithStatsEnabled(false),
	)
	defer c.Close()

	keys := make([]string, 256)
	for i := 0; i < 256; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		_ = c.Set(keys[i], i)
	}
	for _, sh := range c.shards {
		c.promoteReadMap(sh)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get(keys[i&255])
			i++
		}
	})
}

func BenchmarkSyncMapLoad(b *testing.B) {
	// Reference for the spec's "throughput vs sync.Map" perf
	// target row.
	var m sync.Map
	keys := make([]string, 256)
	for i := 0; i < 256; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		m.Store(keys[i], i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(keys[i&255])
			i++
		}
	})
}
