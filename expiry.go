package memcache

import (
	"sync"
	"sync/atomic"
)

// expiryHeap is the min-heap slice underlying [expiryHeapBackend].
// Kept as a named type so it can implement [container/heap.Interface]
// directly, which means heap.Push/Pop/Fix/Remove can drive it
// without an additional wrapper. The slice is exposed publicly to
// the heap-backed `expiryHeapBackend`; tests that need to inspect
// ordering go through that backend, not the slice directly.
type expiryHeap[K comparable, V any] []*entry[K, V]

// Len reports the heap size.
func (h expiryHeap[K, V]) Len() int { return len(h) }

// Less reports whether item i expires before item j.
func (h expiryHeap[K, V]) Less(i, j int) bool {
	return h[i].expireAt.Load() < h[j].expireAt.Load()
}

// Swap exchanges items i and j and updates their cached heap
// positions.
func (h expiryHeap[K, V]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

// Push appends x at the end.
func (h *expiryHeap[K, V]) Push(x any) {
	e, _ := x.(*entry[K, V])
	e.heapIndex = len(*h)
	*h = append(*h, e)
}

// Pop removes and returns the last element.
func (h *expiryHeap[K, V]) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	e.heapIndex = -1
	return e
}

// expiryAdd delegates to the shard's TTL backend. Caller must hold s.mu.
func (s *shard[K, V]) expiryAdd(e *entry[K, V]) { s.ttl.Add(e) }

// expiryRemove delegates to the shard's TTL backend.
func (s *shard[K, V]) expiryRemove(e *entry[K, V]) { s.ttl.Remove(e) }

// expiryFix delegates to the shard's TTL backend.
func (s *shard[K, V]) expiryFix(e *entry[K, V]) { s.ttl.Fix(e) }

// sweepExpiredLocked removes every entry whose expireAt ≤ now from
// the shard. Returns the count. Caller must hold s.mu (write).
func (c *Cache[K, V]) sweepExpiredLocked(s *shard[K, V], now int64) int {
	expired := s.ttl.Sweep(now)
	for _, e := range expired {
		c.removeLocked(s, e, EvictReasonExpired)
		c.counters.expirations.Add(1)
	}
	return len(expired)
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
	stop := make(chan struct{})
	tick := make(chan struct{}, 1)
	s.janitor.stop = stop
	s.janitor.tick = tick
	fire := func() {
		select {
		case tick <- struct{}{}:
		default:
		}
	}
	s.janitor.timer = c.cfg.clock.AfterFunc(c.cfg.janitorInterval, fire)
	// Pass channels by value so runJanitor doesn't read s.janitor
	// fields concurrently with a future startJanitorLocked that
	// might rewrite them on restart.
	go c.runJanitor(s, stop, tick)
}

// stopJanitor signals the janitor to exit. Idempotent. Acquires s.mu
// so reads of janitor.stop/timer are serialized with startJanitorLocked.
func (c *Cache[K, V]) stopJanitor(s *shard[K, V]) {
	if !s.janitor.running.CompareAndSwap(true, false) {
		return
	}
	s.mu.Lock()
	timer := s.janitor.timer
	stop := s.janitor.stop
	s.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if stop != nil {
		close(stop)
	}
}

// janitorIdleShutdownTicks is how many consecutive idle ticks the
// janitor must observe before exiting. Per spec §7.3 the
// recommendation is 5; matches the "no TTL entries for 5 ×
// JanitorInterval" formulation.
const janitorIdleShutdownTicks = 5

// runJanitor is the per-shard sweep loop. It wakes every configured
// interval, sweeps expired entries from the heap, and re-arms. After
// `janitorIdleShutdownTicks` consecutive idle ticks (no entries
// removed AND the heap is empty) the goroutine exits and the
// shard's `janitor.running` flag is cleared so the next TTL'd
// insert can launch a fresh janitor via [Cache.startJanitorLocked].
//
// Exits early when stop is closed (Cache.Close).
func (c *Cache[K, V]) runJanitor(s *shard[K, V], stop, tick <-chan struct{}) {
	idle := 0
	for {
		select {
		case <-stop:
			return
		case <-tick:
			s.mu.Lock()
			now := c.cfg.clock.Now().UnixNano()
			removed := c.sweepExpiredLocked(s, now)
			heapEmpty := s.ttl.Len() == 0

			if removed == 0 && heapEmpty {
				idle++
				if idle >= janitorIdleShutdownTicks {
					// Confirm still idle under the lock; on race,
					// stay alive and re-arm.
					if s.ttl.Len() == 0 {
						s.janitor.running.Store(false)
						if s.janitor.timer != nil {
							s.janitor.timer.Stop()
						}
						c.flushAndUnlock(s)
						return
					}
					idle = 0
				}
			} else {
				idle = 0
			}
			// Re-arm the timer UNDER the shard lock so a
			// concurrent stopJanitor can't race the Stop() →
			// Reset() ordering. flushAndUnlock releases
			// the lock as a side effect AFTER the Reset has
			// landed.
			if s.janitor.timer != nil {
				s.janitor.timer.Reset(c.cfg.janitorInterval)
			}
			c.flushAndUnlock(s)
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
