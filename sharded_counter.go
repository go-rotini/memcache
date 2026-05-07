package memcache

import (
	"math/rand/v2"
	"sync/atomic"
)

// shardedCounter is a many-slot atomic counter designed to reduce
// cache-line contention on hot-path increments. Add picks a random
// slot via the per-goroutine PCG RNG (contention-free); Load sums
// all slots. The total operation count is the sum of every slot.
//
// The slot count is the next power of two ≥ requested; a slot count
// of 1 degenerates the structure to a single atomic.Uint64 wrapped
// in two extra pointer hops, so callers needing single-counter
// behavior should branch on cfg.shardedStats and bypass this type.
type shardedCounter struct {
	slots []atomic.Uint64
	mask  uint64
}

// newShardedCounter constructs a counter sized to the next power
// of two ≥ slots (minimum 1). Use [shardedStatsSize] to derive a
// reasonable slot count from runtime.GOMAXPROCS.
func newShardedCounter(slots int) *shardedCounter {
	if slots < 1 {
		slots = 1
	}
	n := nextPowerOfTwo(slots)
	return &shardedCounter{
		slots: make([]atomic.Uint64, n),
		mask:  uint64(n - 1),
	}
}

// Add increments a randomly-chosen slot by delta. The randomness
// distributes contention across slots; every slot is equally
// likely.
func (c *shardedCounter) Add(delta uint64) {
	c.slots[rand.Uint64()&c.mask].Add(delta)
}

// Load returns the sum across every slot.
func (c *shardedCounter) Load() uint64 {
	var sum uint64
	for i := range c.slots {
		sum += c.slots[i].Load()
	}
	return sum
}

// Store overwrites every slot's value. Used by [statsCounters.reset]
// to zero the counter; the value is split evenly across slots so
// the post-reset Load is exact.
func (c *shardedCounter) Store(v uint64) {
	if len(c.slots) == 1 {
		c.slots[0].Store(v)
		return
	}
	share := v / uint64(len(c.slots))
	rem := v - share*uint64(len(c.slots))
	for i := range c.slots {
		val := share
		if uint64(i) < rem {
			val++
		}
		c.slots[i].Store(val)
	}
}

// shardedStatsSize returns the slot count for a sharded stats counter:
// 4*GOMAXPROCS capped at 256.
func shardedStatsSize(p int) int {
	if p <= 0 {
		p = 1
	}
	n := p * 4
	const maxSlots = 256
	if n > maxSlots {
		return maxSlots
	}
	return n
}
