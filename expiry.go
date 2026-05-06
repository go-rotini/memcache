package memcache

import (
	"container/heap"
	"sync"
	"sync/atomic"
)

// expiryHeap is a min-heap of *entry[K, V] ordered by expireAt
// (unix nanos). It implements [container/heap.Interface]; the
// container/heap package's free functions drive sift-up/sift-down.
//
// The heap is per-shard and protected by the shard's mutex. Entries
// without a TTL (expireAt == 0) are NOT in the heap; their
// heapIndex stays -1.
type expiryHeap[K comparable, V any] []*entry[K, V]

// Len reports the heap size. Required by [heap.Interface].
func (h expiryHeap[K, V]) Len() int { return len(h) }

// Less reports whether item i expires before item j. Required by
// [heap.Interface].
func (h expiryHeap[K, V]) Less(i, j int) bool {
	return h[i].expireAt.Load() < h[j].expireAt.Load()
}

// Swap exchanges items i and j and updates their cached heap
// positions. Required by [heap.Interface].
func (h expiryHeap[K, V]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

// Push appends x at the end. Required by [heap.Interface]; callers
// should use [heap.Push] (which handles sift-up) rather than calling
// this directly.
func (h *expiryHeap[K, V]) Push(x any) {
	e, _ := x.(*entry[K, V])
	e.heapIndex = len(*h)
	*h = append(*h, e)
}

// Pop removes and returns the last element. Required by
// [heap.Interface]; callers should use [heap.Pop] (which handles
// sift-down) rather than calling this directly.
func (h *expiryHeap[K, V]) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	e.heapIndex = -1
	return e
}

// expiryAdd inserts e into the shard's heap iff e has a non-zero
// expireAt. Caller must hold s.mu.
func (s *shard[K, V]) expiryAdd(e *entry[K, V]) {
	if e.expireAt.Load() == 0 {
		e.heapIndex = -1
		return
	}
	heap.Push(&s.expHeap, e)
}

// expiryRemove drops e from the heap if it is currently tracked.
// Caller must hold s.mu.
func (s *shard[K, V]) expiryRemove(e *entry[K, V]) {
	if e.heapIndex < 0 {
		return
	}
	heap.Remove(&s.expHeap, e.heapIndex)
}

// expiryFix re-orders the heap after the entry's expireAt has
// changed. Handles three cases:
//   - entry was not in heap, now has TTL → push
//   - entry was in heap, TTL cleared (expireAt=0) → remove
//   - entry was in heap, TTL changed → fix in place (O(log n))
//
// Caller must hold s.mu.
func (s *shard[K, V]) expiryFix(e *entry[K, V]) {
	exp := e.expireAt.Load()
	switch {
	case e.heapIndex < 0 && exp > 0:
		heap.Push(&s.expHeap, e)
	case e.heapIndex >= 0 && exp == 0:
		heap.Remove(&s.expHeap, e.heapIndex)
	case e.heapIndex >= 0:
		heap.Fix(&s.expHeap, e.heapIndex)
	}
}

// sweepExpiredLocked pops expired entries from the heap and removes
// them from the shard's map until the heap top is in the future or
// empty. Returns the number of entries removed. Caller must hold
// s.mu (write lock).
func (c *Cache[K, V]) sweepExpiredLocked(s *shard[K, V], now int64) int {
	count := 0
	for s.expHeap.Len() > 0 {
		e := s.expHeap[0]
		if e.expireAt.Load() > now {
			return count
		}
		c.removeLocked(s, e, EvictReasonExpired)
		c.counters.expirations.Add(1)
		count++
	}
	return count
}

// janitorState holds the per-shard janitor coordination. A nil-or-
// stopped janitor means no goroutine is running for this shard.
type janitorState struct {
	running atomic.Bool
	stop    chan struct{}
	tick    chan struct{}
	timer   Timer
}

// startJanitorLocked launches a janitor goroutine for s if none is
// currently running. The first sweep timer is registered with the
// cache's clock SYNCHRONOUSLY, before the goroutine is spawned, so
// callers (including tests using FakeClock) can advance time
// immediately and observe the janitor firing.
//
// Caller must hold s.mu so the start/stop transition is observed
// atomically with whichever insert demanded the janitor.
func (c *Cache[K, V]) startJanitorLocked(s *shard[K, V]) {
	if c.cfg.janitorInterval <= 0 {
		return
	}
	if !s.janitor.running.CompareAndSwap(false, true) {
		return
	}
	s.janitor.stop = make(chan struct{})
	s.janitor.tick = make(chan struct{}, 1)
	fire := func() {
		select {
		case s.janitor.tick <- struct{}{}:
		default:
		}
	}
	s.janitor.timer = c.cfg.clock.AfterFunc(c.cfg.janitorInterval, fire)
	go c.runJanitor(s)
}

// stopJanitor signals the janitor to exit. Idempotent — calling on
// a stopped janitor is a no-op.
func (c *Cache[K, V]) stopJanitor(s *shard[K, V]) {
	if !s.janitor.running.CompareAndSwap(true, false) {
		return
	}
	if s.janitor.timer != nil {
		s.janitor.timer.Stop()
	}
	close(s.janitor.stop)
}

// runJanitor is the per-shard sweep loop. It wakes every configured
// interval, sweeps expired entries from the heap, and re-arms.
// Exits cleanly when stop is closed.
func (c *Cache[K, V]) runJanitor(s *shard[K, V]) {
	stop := s.janitor.stop
	tick := s.janitor.tick
	for {
		select {
		case <-stop:
			return
		case <-tick:
			s.mu.Lock()
			now := c.cfg.clock.Now().UnixNano()
			c.sweepExpiredLocked(s, now)
			s.mu.Unlock()
			if s.janitor.timer != nil {
				s.janitor.timer.Reset(c.cfg.janitorInterval)
			}
		}
	}
}

// stopAllJanitors stops every shard's janitor and is called by
// [Cache.Close]. Safe to call once; subsequent calls are no-ops.
func (c *Cache[K, V]) stopAllJanitors() {
	var wg sync.WaitGroup
	wg.Add(len(c.shards))
	for _, s := range c.shards {
		go func(s *shard[K, V]) {
			defer wg.Done()
			c.stopJanitor(s)
		}(s)
	}
	wg.Wait()
}
