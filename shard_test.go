package memcache

import (
	"slices"
	"testing"
)

func TestShardCountIsPowerOfTwo(t *testing.T) {
	cases := []int{1, 2, 4, 8, 16, 32, 64}
	for _, want := range cases {
		c, err := New[string, int](WithMaxEntries(64), WithShards(want))
		if err != nil {
			t.Fatalf("New shards=%d: %v", want, err)
		}
		got := len(c.shards)
		_ = c.Close()
		if got != want {
			t.Errorf("len(shards) = %d, want %d", got, want)
		}
	}
}

func TestShardCountRoundsUp(t *testing.T) {
	c, err := New[string, int](WithMaxEntries(64), WithShards(5))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	if len(c.shards) != 8 {
		t.Errorf("WithShards(5) → len=%d, want 8 (next power of 2)", len(c.shards))
	}
}

func TestShardMaskMatchesCount(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64), WithShards(8))
	defer c.Close()
	if c.shardMask != uint64(8-1) {
		t.Errorf("shardMask = %d, want %d", c.shardMask, 7)
	}
}

func TestShardForRouting(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64), WithShards(8))
	defer c.Close()
	// shardFor must be deterministic and stay in range.
	for _, k := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		s := c.shardFor(k)
		if s == nil {
			t.Errorf("shardFor(%q) = nil", k)
			continue
		}
		// Must be one of the slice's shards.
		if !slices.Contains(c.shards, s) {
			t.Errorf("shardFor(%q) returned a shard not in c.shards", k)
		}
		// Repeated lookups must hash to the same shard.
		if c.shardFor(k) != s {
			t.Errorf("shardFor(%q) is non-deterministic", k)
		}
	}
}

func TestShardKeysDistributeAcrossShards(t *testing.T) {
	const (
		shards = 8
		n      = 1000
	)
	c, _ := New[string, int](WithMaxEntries(2*n), WithShards(shards))
	defer c.Close()

	hits := make([]int, shards)
	for i := range n {
		k := keyFor(i)
		idx := c.hasher(k) & c.shardMask
		hits[idx]++
	}
	// No shard should claim more than ~3× its fair share. Loose
	// bound — we're checking the hasher isn't pathologically biased.
	expected := n / shards
	for i, h := range hits {
		if h > expected*3 || h < expected/3 {
			t.Errorf("shard %d got %d keys (fair share %d)", i, h, expected)
		}
	}
}

func TestShardKeepsEntriesIsolated(t *testing.T) {
	// Keys mapped to different shards should not affect each other.
	c, _ := New[string, int](WithMaxEntries(64), WithShards(4))
	defer c.Close()
	for i := range 20 {
		_ = c.Set(keyFor(i), i)
	}
	// Delete a few keys; remaining entries should still be retrievable.
	for i := range 5 {
		c.Delete(keyFor(i))
	}
	for i := 5; i < 20; i++ {
		if _, ok := c.Get(keyFor(i)); !ok {
			t.Errorf("after cross-shard deletes, key %d was lost", i)
		}
	}
}

func TestNewShardConstructor(t *testing.T) {
	pol := newLRU[string, int]()
	s := newShard[string, int](pol, 16, false, newExpiryHeapBackend[string, int]())
	if s.policy == nil {
		t.Error("newShard returned shard with nil policy")
	}
	if s.budget != 16 {
		t.Errorf("budget = %d, want 16", s.budget)
	}
	if s.entries == nil || len(s.entries) != 0 {
		t.Error("newShard should produce an empty map")
	}
}

func TestPerShardBudget(t *testing.T) {
	cases := []struct {
		max, shards, want int
	}{
		{0, 4, 0},    // unbounded
		{4, 1, 5},    // ceil(4/1)=4 + slop=1 → 5
		{16, 4, 5},   // ceil(16/4)=4 + slop=1 → 5
		{100, 8, 14}, // ceil(100/8)=13 + slop=1 → 14
	}
	for _, tc := range cases {
		got := perShardBudget(tc.max, tc.shards)
		if got != tc.want {
			t.Errorf("perShardBudget(%d, %d) = %d, want %d",
				tc.max, tc.shards, got, tc.want)
		}
	}
}

// keyFor turns an int into a stable string key for routing tests.
func keyFor(i int) string {
	return "shard-key-" + itoaSimple(i)
}

func itoaSimple(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	buf := make([]byte, 0, 12)
	for i > 0 {
		buf = append(buf, byte('0'+i%10))
		i /= 10
	}
	if neg {
		buf = append(buf, '-')
	}
	// Reverse.
	for l, r := 0, len(buf)-1; l < r; l, r = l+1, r-1 {
		buf[l], buf[r] = buf[r], buf[l]
	}
	return string(buf)
}
