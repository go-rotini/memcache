package memcache

import (
	"sync"
	"testing"
)

func TestShardedCounterBasicAddLoad(t *testing.T) {
	c := newShardedCounter(8)
	for range 100 {
		c.Add(1)
	}
	if got := c.Load(); got != 100 {
		t.Errorf("Load = %d, want 100", got)
	}
}

func TestShardedCounterStoreSingleSlot(t *testing.T) {
	c := newShardedCounter(1)
	c.Store(42)
	if got := c.Load(); got != 42 {
		t.Errorf("single-slot Store/Load = %d, want 42", got)
	}
	// Re-store overwrites the slot.
	c.Store(0)
	if got := c.Load(); got != 0 {
		t.Errorf("single-slot Store(0) Load = %d, want 0", got)
	}
}

func TestShardedCounterStoreMultiSlotEvenSplit(t *testing.T) {
	// 8 slots, value=64 → each slot stores 8 exactly, Load returns 64.
	c := newShardedCounter(8)
	c.Store(64)
	if got := c.Load(); got != 64 {
		t.Errorf("multi-slot Store(64) Load = %d, want 64", got)
	}
}

func TestShardedCounterStoreMultiSlotWithRemainder(t *testing.T) {
	// 4 slots, value=10 → share=2, rem=2 → first two slots get +1.
	c := newShardedCounter(4)
	c.Store(10)
	if got := c.Load(); got != 10 {
		t.Errorf("Store(10) Load = %d, want 10", got)
	}
	// Drive the rem=0 path with an exact multiple.
	c.Store(8)
	if got := c.Load(); got != 8 {
		t.Errorf("Store(8) Load = %d, want 8", got)
	}
}

func TestShardedCounterAddConcurrent(t *testing.T) {
	c := newShardedCounter(16)
	var wg sync.WaitGroup
	const goroutines = 16
	const perG = 1024
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perG {
				c.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := c.Load(); got != goroutines*perG {
		t.Errorf("concurrent Load = %d, want %d", got, goroutines*perG)
	}
}

func TestNewShardedCounterClampsAndRoundsUpToPowerOfTwo(t *testing.T) {
	cases := []struct {
		in       int
		wantLen  int // expected internal slot count (power of two)
		wantMask uint64
	}{
		{in: 0, wantLen: 1, wantMask: 0},
		{in: -5, wantLen: 1, wantMask: 0},
		{in: 1, wantLen: 1, wantMask: 0},
		{in: 3, wantLen: 4, wantMask: 3},
		{in: 8, wantLen: 8, wantMask: 7},
		{in: 9, wantLen: 16, wantMask: 15},
	}
	for _, tc := range cases {
		c := newShardedCounter(tc.in)
		if len(c.slots) != tc.wantLen {
			t.Errorf("newShardedCounter(%d) slots=%d, want %d",
				tc.in, len(c.slots), tc.wantLen)
		}
		if c.mask != tc.wantMask {
			t.Errorf("newShardedCounter(%d) mask=%d, want %d",
				tc.in, c.mask, tc.wantMask)
		}
	}
}

func TestShardedStatsSize(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{in: 0, want: 4},     // p<=0 path → p=1 → 4
		{in: -3, want: 4},    // negative → p=1 → 4
		{in: 1, want: 4},     // 1 GOMAXPROC → 4 slots
		{in: 8, want: 32},    // 8 → 32
		{in: 64, want: 256},  // 64 → 256, hits the cap
		{in: 100, want: 256}, // > cap clamps to 256
	}
	for _, tc := range cases {
		if got := shardedStatsSize(tc.in); got != tc.want {
			t.Errorf("shardedStatsSize(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
