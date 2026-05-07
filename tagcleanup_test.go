package memcache

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTagCleanupBacklogDrainsAfterEvictions(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64))
	defer c.Close()

	for i := range 50 {
		_ = c.SetWithTags(itoaSimple(i), i, "tag-shared")
	}
	// Delete every entry; removeLocked enqueues untag ops. After
	// Sync the tag index must be empty.
	for i := range 50 {
		c.Delete(itoaSimple(i))
	}
	if err := c.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := c.Stats().TagCleanupBacklog; got != 0 {
		t.Errorf("TagCleanupBacklog after Sync = %d, want 0", got)
	}
	if got := c.Stats().TagsTracked; got != 0 {
		t.Errorf("TagsTracked after Sync = %d, want 0 (every entry was untagged)", got)
	}
}

func TestTagCleanupBacklogReportsDuringHighChurn(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64))
	defer c.Close()

	// Fire enough evictions to put items in flight; we don't
	// rely on the queue being any particular depth, only on the
	// counter being non-negative.
	for i := range 1000 {
		_ = c.SetWithTags(itoaSimple(i), i, "tag")
		c.Delete(itoaSimple(i))
	}
	if got := c.Stats().TagCleanupBacklog; got < 0 {
		t.Errorf("TagCleanupBacklog = %d (negative)", got)
	}
	_ = c.Sync(context.Background())
}

func TestTagCleanupCloseDrainsRemaining(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64))
	for i := range 50 {
		_ = c.SetWithTags(itoaSimple(i), i, "tag")
		c.Delete(itoaSimple(i))
	}
	// Close without Sync; the drainer must catch up before
	// returning so post-Close state is settled.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTagCleanupInvalidateTagAfterEvictionDoesNotDoubleFree(t *testing.T) {
	// Eviction enqueues untag; InvalidateTag walks the (lagging)
	// snapshot and tries to delete keys that may already be gone.
	// This must not panic or deadlock.
	c, _ := New[string, int](WithMaxEntries(64))
	defer c.Close()

	for i := range 30 {
		_ = c.SetWithTags(itoaSimple(i), i, "shared")
	}
	// Delete half; these enqueue untags.
	for i := range 15 {
		c.Delete(itoaSimple(i))
	}
	// Invalidate the tag; walks the (still-stale) index.
	dropped := c.InvalidateTag("shared")
	if dropped < 0 {
		t.Errorf("InvalidateTag returned %d", dropped)
	}
	_ = c.Sync(context.Background())
}

func TestTagCleanupConcurrentChurn(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(256))
	defer c.Close()

	const G, N = 16, 200
	var wg sync.WaitGroup
	for g := range G {
		wg.Go(func() {
			for i := range N {
				k := itoaSimple(g*1000 + i)
				_ = c.SetWithTags(k, i, "shared")
				c.Delete(k)
			}
		})
	}
	wg.Wait()
	// Sync must converge in a reasonable time bound.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Sync(ctx); err != nil {
		t.Fatalf("Sync timed out / failed: %v", err)
	}
	if got := c.Stats().TagsTracked; got != 0 {
		t.Errorf("TagsTracked after concurrent churn + Sync = %d, want 0", got)
	}
}

func TestTagCleanupSyncRespectsCanceledCtx(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64))
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sync(ctx); err == nil {
		t.Error("Sync with canceled ctx must return non-nil error")
	}
}
