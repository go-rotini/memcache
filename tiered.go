package memcache

import (
	"context"
	"sync/atomic"
	"time"
)

// TieredStats reports observability for a [Tiered] cache split
// between its two tiers. L1Hits are hits served from the fast
// (in-memory) tier; L2Hits are served from the slower-but-larger
// tier and trigger a promotion. Misses are queries that found
// neither tier.
type TieredStats struct {
	// L1 is the L1 cache's full Stats snapshot.
	L1 Stats
	// L2 is the L2 cache's full Stats snapshot.
	L2 Stats
	// L1Hits is the count of Tiered.Get calls served from L1.
	L1Hits uint64
	// L2Hits is the count of Tiered.Get calls served from L2
	// (with subsequent promotion to L1).
	L2Hits uint64
	// Misses is the count of Tiered.Get calls that found neither
	// tier.
	Misses uint64
	// Promotions is the count of L2 hits that were successfully
	// written to L1.
	Promotions uint64
}

// Tiered combines two [Cache] instances into an L1/L2 hierarchy. Reads
// hit L1 first; L2 hits are promoted to L1. Writes are write-through.
// Tiered exposes the high-traffic methods directly; reach the rest via
// [Tiered.L1] / [Tiered.L2]. Both caches must be non-nil.
type Tiered[K comparable, V any] struct {
	l1 *Cache[K, V]
	l2 *Cache[K, V]

	l1Hits     atomic.Uint64
	l2Hits     atomic.Uint64
	misses     atomic.Uint64
	promotions atomic.Uint64

	// locks serializes Tiered writes of each key.
	locks *keyLockTable[K]

	closed atomic.Bool
}

// NewTiered constructs a [Tiered] cache from two non-nil [Cache]
// instances. [Tiered.Close] closes both; share an underlying cache only
// if you do not need to outlive Tiered.Close.
func NewTiered[K comparable, V any](l1, l2 *Cache[K, V]) *Tiered[K, V] {
	if l1 == nil {
		panic("memcache: NewTiered: l1 is nil")
	}
	if l2 == nil {
		panic("memcache: NewTiered: l2 is nil")
	}
	return &Tiered[K, V]{l1: l1, l2: l2, locks: &keyLockTable[K]{}}
}

// Get returns the cached value for key. Lookup order: L1, then L2 with
// best-effort promotion to L1 (keeping the L2 entry's tags and expiry),
// then miss. A failed L1 promotion still
// returns the L2 hit.
func (t *Tiered[K, V]) Get(key K) (V, bool) {
	var zero V
	if t.closed.Load() {
		return zero, false
	}
	if v, ok := t.l1.Get(key); ok {
		t.l1Hits.Add(1)
		return v, true
	}
	v, ok, _ := t.getAndPromote(key, func() (V, bool, error) { //nolint:errcheck // the getter never fails
		v, ok := t.l2.Get(key)
		return v, ok, nil
	})
	if !ok {
		t.misses.Add(1)
		return zero, false
	}
	return v, true
}

// watchToken records whether a key was written, deleted or invalidated
// since the watch began. Guarded by the owning shard's lock.
type watchToken struct {
	superseded bool
	refs       int
}

// watchKey starts watching key: any later write, delete or
// invalidation of it marks the returned token superseded. Callers MUST
// call unwatchKey when done.
func (c *Cache[K, V]) watchKey(key K) *watchToken {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watchLocked(key)
}

// unwatchKey ends a watch started by watchKey.
func (c *Cache[K, V]) unwatchKey(key K, w *watchToken) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unwatchLocked(key, w)
}

// watchLocked is watchKey for a caller holding s.mu (write).
func (s *shard[K, V]) watchLocked(key K) *watchToken {
	return s.watchLockedSpare(key, nil)
}

// watchLockedSpare is watchLocked that uses spare, when not nil, as the
// key's token if the key isn't watched yet.
func (s *shard[K, V]) watchLockedSpare(key K, spare *watchToken) *watchToken {
	if s.watches == nil {
		s.watches = make(map[K]*watchToken)
	}
	w := s.watches[key]
	if w == nil {
		w = spare
		if w == nil {
			w = &watchToken{}
		}
		s.watches[key] = w
	}
	w.refs++
	return w
}

// unwatchLocked is unwatchKey for a caller holding s.mu (write).
func (s *shard[K, V]) unwatchLocked(key K, w *watchToken) {
	w.refs--
	if w.refs == 0 && s.watches[key] == w {
		delete(s.watches, key)
	}
}

// watchCurrent reports whether key is unchanged since w began.
func (c *Cache[K, V]) watchCurrent(key K, w *watchToken) bool {
	s := c.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !w.superseded
}

// getAndPromote reads key from L2 through get and copies a hit into L1.
// Both tiers watch the key from before the read, and the L1 write
// happens only if neither has seen a write, delete or invalidation of
// it since, checked under the L1 shard lock: a Delete or InvalidateTag
// clears L2 before L1, so a copy that passes the check is made before
// the L1 clear and is removed by it.
func (t *Tiered[K, V]) getAndPromote(key K, get func() (V, bool, error)) (V, bool, error) {
	if t.l1 == t.l2 {
		return get()
	}
	w1 := t.l1.watchKey(key)
	defer t.l1.unwatchKey(key, w1)
	w2 := t.l2.watchKey(key)
	defer t.l2.unwatchKey(key, w2)
	v, ok, err := get()
	if err != nil || !ok {
		return v, ok, err
	}
	t.l2Hits.Add(1)
	// Promotion is best-effort: an L1 rejection (e.g. MaxValueWeight)
	// or a superseded copy still returns the L2 value.
	guard := func() bool { return !w1.superseded && t.l2.watchCurrent(key, w2) }
	if t.promote(key, v, guard) == nil {
		t.promotions.Add(1)
	}
	return v, true, nil
}

// promote copies an L2 hit into L1 with L2's tags, expiring no later
// than the L2 entry (or L1's default TTL, if that is sooner), so an L1
// copy never outlives its L2 entry or escapes a tag invalidation. guard
// runs under the L1 shard lock; false drops the copy. A value L2 no
// longer holds fresh, such as one served stale under
// [WithStaleWhileRevalidate], is not copied.
func (t *Tiered[K, V]) promote(key K, v V, guard func() bool) error {
	exp, ok := t.l2.Expiry(key)
	if !ok {
		return errWriteSuperseded
	}
	opts := make([]SetOption, 0, 3)
	opts = append(opts, setGuard(guard))
	if tags := t.l2.Tags(key); len(tags) > 0 {
		opts = append(opts, SetTags(tags...))
	}
	if !exp.IsZero() {
		if d := t.l1.cfg.defaultTTL; d > 0 {
			if l1Exp := t.l1.cfg.clock.Now().Add(d); l1Exp.Before(exp) {
				exp = l1Exp
			}
		}
		opts = append(opts, SetExpireAt(exp))
	}
	return t.l1.SetWithOptions(key, v, opts...)
}

// Set writes value to L1, then L2, and returns the first error. If the
// L1 write fails, L2 is not written; if only the L2 write fails, L1
// holds a newer value than L2. Tiered writes of one key run one at a
// time, so concurrent writers leave both tiers with the same value. The
// tiers' hooks fire once both writes are done, so a hook may call
// Tiered for any key.
func (t *Tiered[K, V]) Set(key K, value V) error {
	if t.closed.Load() {
		return ErrClosed
	}
	return t.write(key, func(c *Cache[K, V], after *[]func()) error {
		return c.setCtx(context.Background(), key, value, after)
	})
}

// write applies set to L1 and then, when that succeeds, to L2, holding
// key's Tiered write lock. The hooks both writes fire run after the lock
// is released.
func (t *Tiered[K, V]) write(key K, set func(c *Cache[K, V], after *[]func()) error) error {
	var after []func()
	defer runAll(&after)
	defer t.locks.lock(key)()
	if err := set(t.l1, &after); err != nil {
		return err
	}
	return set(t.l2, &after)
}

// runAll calls each function in *fns in order.
func runAll(fns *[]func()) {
	for _, fn := range *fns {
		fn()
	}
}

// Delete removes key from BOTH tiers. Returns true when at least
// one tier had an entry to remove. As with [Tiered.Set], the tiers'
// hooks fire once both deletes are done.
func (t *Tiered[K, V]) Delete(key K) bool {
	if t.closed.Load() {
		return false
	}
	var after []func()
	defer runAll(&after)
	defer t.locks.lock(key)()
	// L2 first, so a concurrent Get can't promote the old L2 value
	// back into an already-cleared L1.
	d2 := t.l2.deleteInto(key, &after)
	d1 := t.l1.deleteInto(key, &after)
	return d1 || d2
}

// Has reports whether either tier has a fresh entry for key.
func (t *Tiered[K, V]) Has(key K) bool {
	if t.closed.Load() {
		return false
	}
	return t.l1.Has(key) || t.l2.Has(key)
}

// GetCtx is the context-aware variant of [Tiered.Get]. The ctx is
// threaded through to each tier's [Cache.GetCtx]; either tier's
// [WithStore] read-through honors cancellation. The boolean
// reports whether either tier had the entry; the error surfaces
// any underlying ctx or Store failure.
func (t *Tiered[K, V]) GetCtx(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if t.closed.Load() {
		return zero, false, ErrClosed
	}
	if v, ok, err := t.l1.GetCtx(ctx, key); err != nil || ok {
		if ok {
			t.l1Hits.Add(1)
		}
		return v, ok, err
	}
	v, ok, err := t.getAndPromote(key, func() (V, bool, error) {
		return t.l2.GetCtx(ctx, key)
	})
	if err != nil {
		return zero, false, err
	}
	if !ok {
		t.misses.Add(1)
		return zero, false, nil
	}
	return v, true, nil
}

// SetWithTTL is [Tiered.Set] with the given TTL.
func (t *Tiered[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	if t.closed.Load() {
		return ErrClosed
	}
	return t.write(key, func(c *Cache[K, V], after *[]func()) error {
		return c.setWithTTL(key, value, ttl, after)
	})
}

// SetWithOptions is [Tiered.Set] with the same per-call options for
// both tiers. For per-tier overrides, call into [Tiered.L1] /
// [Tiered.L2] directly.
func (t *Tiered[K, V]) SetWithOptions(key K, value V, opts ...SetOption) error {
	if t.closed.Load() {
		return ErrClosed
	}
	return t.write(key, func(c *Cache[K, V], after *[]func()) error {
		return c.setWithOptions(key, value, after, opts...)
	})
}

// InvalidateTag removes every entry tagged with tag from both tiers and
// returns the total number removed.
func (t *Tiered[K, V]) InvalidateTag(tag string) int {
	if t.closed.Load() {
		return 0
	}
	n := t.l2.InvalidateTag(tag)
	return n + t.l1.InvalidateTag(tag)
}

// InvalidateTags removes every entry tagged with at least one of
// tags from BOTH tiers. Returns the total removed across both.
func (t *Tiered[K, V]) InvalidateTags(tags ...string) int {
	if t.closed.Load() {
		return 0
	}
	n := t.l2.InvalidateTags(tags...)
	return n + t.l1.InvalidateTags(tags...)
}

// Sync waits for any background work in BOTH tiers (tag-cleanup
// drainer, async-writes apply goroutine) to drain. Useful in tests
// before reading state that depends on completed writes.
func (t *Tiered[K, V]) Sync(ctx context.Context) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if err := t.l1.Sync(ctx); err != nil {
		return err
	}
	return t.l2.Sync(ctx)
}

// Len returns the number of entries in L2, since every Tiered write
// reaches L2. Use [Tiered.L1] and [Tiered.L2] for per-tier counts.
func (t *Tiered[K, V]) Len() int {
	if t.closed.Load() {
		return 0
	}
	return t.l2.Len()
}

// Stats returns the combined observability snapshot.
func (t *Tiered[K, V]) Stats() TieredStats {
	return TieredStats{
		L1:         t.l1.Stats(),
		L2:         t.l2.Stats(),
		L1Hits:     t.l1Hits.Load(),
		L2Hits:     t.l2Hits.Load(),
		Misses:     t.misses.Load(),
		Promotions: t.promotions.Load(),
	}
}

// L1 returns the underlying L1 [Cache], for operations Tiered does not
// expose.
func (t *Tiered[K, V]) L1() *Cache[K, V] { return t.l1 }

// L2 returns the underlying L2 [Cache]. Same caveats as [Tiered.L1].
func (t *Tiered[K, V]) L2() *Cache[K, V] { return t.l2 }

// Close closes both tiers and returns the first error. Subsequent
// calls return nil.
func (t *Tiered[K, V]) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return nil
	}
	err1 := t.l1.Close()
	err2 := t.l2.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
