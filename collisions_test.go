package memcache

import "testing"

func TestWithCollisionTrackingDefaultOffNoCounter(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for i := range 100 {
		_ = c.Set(itoaSimple(i), i)
	}
	if got := c.Stats().HashCollisions; got != 0 {
		t.Errorf("HashCollisions = %d with tracking off; want 0", got)
	}
}

func TestWithCollisionTrackingBumpsOnCollision(t *testing.T) {
	// Custom hasher that maps every odd i to 1, every even i to 0
	// — guaranteed collisions across many distinct keys.
	hasher := WithHasher[int](func(k int) uint64 {
		return uint64(k & 1)
	})
	c, _ := New[int, int](
		WithMaxEntries(64),
		WithShards(1),
		hasher,
		WithCollisionTracking(true),
	)
	defer c.Close()
	for i := range 10 {
		_ = c.Set(i, i)
	}
	// Each subsequent insert that lands on the same hash as a
	// previously-inserted distinct key bumps the counter. With 5
	// even and 5 odd keys, 4 even-on-even + 4 odd-on-odd = 8
	// collisions.
	if got := c.Stats().HashCollisions; got != 8 {
		t.Errorf("HashCollisions = %d, want 8", got)
	}
}

func TestWithCollisionTrackingForgetsOnDelete(t *testing.T) {
	hasher := WithHasher[int](func(k int) uint64 { return uint64(k & 1) })
	c, _ := New[int, int](
		WithMaxEntries(64),
		WithShards(1),
		hasher,
		WithCollisionTracking(true),
	)
	defer c.Close()
	// Insert key 1 (hash=1), Delete it, insert key 3 (hash=1).
	// Without forgetting, key 3 would be marked as a collision
	// against key 1 even though key 1 is no longer present.
	_ = c.Set(1, 1)
	c.Delete(1)
	_ = c.Set(3, 3)
	if got := c.Stats().HashCollisions; got != 0 {
		t.Errorf("HashCollisions = %d after delete-then-insert; want 0 (forgetting failed)", got)
	}
}
