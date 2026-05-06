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
func (c *Cache[K, V]) runJanitor(s *shard[K, V]) {
	stop := s.janitor.stop
	tick := s.janitor.tick
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
			s.mu.Unlock()

			if removed == 0 && heapEmpty {
				idle++
				if idle >= janitorIdleShutdownTicks {
					// Transition to "not running" UNDER the
					// shard lock so a concurrent insert that
					// observes running=true still sees a live
					// goroutine. The shard lock is the same
					// one [Cache.startJanitorLocked] takes
					// when it CAS-es running false→true.
					s.mu.Lock()
					if s.ttl.Len() == 0 {
						// Confirm still idle; if the lock
						// race introduced new TTL entries,
						// stay alive.
						s.janitor.running.Store(false)
						if s.janitor.timer != nil {
							s.janitor.timer.Stop()
						}
						s.mu.Unlock()
						return
					}
					// Lost the race; reset idle counter and
					// continue.
					s.mu.Unlock()
					idle = 0
				}
			} else {
				idle = 0
			}
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
