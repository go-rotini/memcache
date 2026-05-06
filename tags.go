package memcache

import (
	"fmt"
	"sync"
)

// tagIndex maps each tag to the set of keys carrying it. Lives at
// the cache level (not per-shard) because a single InvalidateTag
// must enumerate every key — possibly across many shards — before
// touching any of them.
//
// Lock-ordering rule (spec §19.2.1):
//   - Insert/update path holds shard.mu THEN takes idx.mu.
//   - InvalidateTag path takes idx.mu, snapshots the key set,
//     RELEASES idx.mu, and only then acquires shard.mu (one shard
//     at a time). It NEVER holds both simultaneously.
//
// This keeps both directions deadlock-free without forcing a global
// lock order between shard mutexes and idx.mu.
type tagIndex[K comparable] struct {
	mu sync.RWMutex
	// keysByTag is the inverted index used by InvalidateTag.
	keysByTag map[string]map[K]struct{}
}

// newTagIndex constructs an empty tagIndex.
func newTagIndex[K comparable]() *tagIndex[K] {
	return &tagIndex[K]{keysByTag: make(map[string]map[K]struct{})}
}

// tag records that key carries every tag in tags. Adding the same
// (key, tag) twice is idempotent. The shard lock for key MUST be
// held by the caller (per the lock-ordering rule).
func (idx *tagIndex[K]) tag(key K, tags []string) {
	if len(tags) == 0 {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, t := range tags {
		set, ok := idx.keysByTag[t]
		if !ok {
			set = make(map[K]struct{})
			idx.keysByTag[t] = set
		}
		set[key] = struct{}{}
	}
}

// untag removes (key, t) for every t in tags. Empty per-tag sets
// are deleted to keep the index from accumulating stale tag
// entries. The shard lock for key SHOULD be held by the caller
// when this is invoked from the eviction path; not strictly
// required (the index has its own mutex), but consistent with the
// "shard then index" ordering used by tag().
func (idx *tagIndex[K]) untag(key K, tags []string) {
	if len(tags) == 0 {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, t := range tags {
		set, ok := idx.keysByTag[t]
		if !ok {
			continue
		}
		delete(set, key)
		if len(set) == 0 {
			delete(idx.keysByTag, t)
		}
	}
}

// snapshot returns a freshly-allocated slice of every key currently
// carrying tag. Callers iterate the slice WITHOUT holding idx.mu so
// they can safely take per-key shard locks (avoiding the reverse
// of the standard ordering).
func (idx *tagIndex[K]) snapshot(tag string) []K {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	set, ok := idx.keysByTag[tag]
	if !ok {
		return nil
	}
	keys := make([]K, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}

// reset drops every tag and key from the index. Used by [Cache.Reset]
// and [Cache.Clear].
func (idx *tagIndex[K]) reset() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.keysByTag = make(map[string]map[K]struct{})
}

// SetWithTags stores value under key and indexes the entry under
// each supplied tag. The cache-level default TTL applies. Tags can
// later be used to remove this and similarly-tagged entries in one
// call via [Cache.InvalidateTag].
//
// Returns the same errors as [Cache.Set] plus, in future revisions,
// [ErrTooManyTags] when limit options are added.
func (c *Cache[K, V]) SetWithTags(key K, value V, tags ...string) error {
	return c.SetWithOptions(key, value, SetTags(tags...))
}

// InvalidateTag removes every entry currently tagged with tag.
// Returns the number of entries removed; eviction stats record
// each removal under [EvictReasonTag].
//
// Lock discipline: the tag→keys snapshot is taken under the index's
// read lock and that lock is released before any per-shard write
// lock is acquired, so InvalidateTag never holds both
// simultaneously (matching spec §19.2.1).
func (c *Cache[K, V]) InvalidateTag(tag string) int {
	if c.closed.Load() || c.tags == nil {
		return 0
	}
	keys := c.tags.snapshot(tag)
	count := 0
	for _, k := range keys {
		s := c.shardFor(k)
		s.mu.Lock()
		if e, ok := s.entries[k]; ok {
			c.removeLocked(s, e, EvictReasonTag)
			count++
		}
		s.mu.Unlock()
	}
	if count > 0 {
		c.publishEvent(Event[K, V]{
			Kind: EventInvalidateTag,
			At:   c.cfg.clock.Now(),
			Tags: []string{tag},
		})
	}
	return count
}

// InvalidateTags removes every entry tagged with ANY of the supplied
// tags (set union). An entry tagged with multiple matching tags is
// counted once.
func (c *Cache[K, V]) InvalidateTags(tags ...string) int {
	if c.closed.Load() || c.tags == nil || len(tags) == 0 {
		return 0
	}
	doomed := make(map[K]struct{})
	for _, tag := range tags {
		for _, k := range c.tags.snapshot(tag) {
			doomed[k] = struct{}{}
		}
	}
	count := 0
	for k := range doomed {
		s := c.shardFor(k)
		s.mu.Lock()
		if e, ok := s.entries[k]; ok {
			c.removeLocked(s, e, EvictReasonTag)
			count++
		}
		s.mu.Unlock()
	}
	if count > 0 {
		c.publishEvent(Event[K, V]{
			Kind: EventInvalidateTag,
			At:   c.cfg.clock.Now(),
			Tags: append([]string(nil), tags...),
		})
	}
	return count
}

// Tags returns a copy of the tags carried by the entry at key, or
// nil when the key is absent. The returned slice is independent of
// cache storage; callers may mutate it freely.
func (c *Cache[K, V]) Tags(key K) []string {
	if c.closed.Load() {
		return nil
	}
	s := c.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	if !ok {
		return nil
	}
	if len(e.tags) == 0 {
		return nil
	}
	return append([]string(nil), e.tags...)
}

// enforceGroupBudgets bounds every configured group whose name
// appears in tags by evicting the oldest members (by inserted
// time) until the group's member count is within the configured
// capacity. Called from public Set-style methods AFTER the shard
// lock is released so it can take per-key shard locks freely
// without inverting the standard lock order.
//
// Race tolerance: between checking len(members) and evicting,
// concurrent Sets may push the group further over. enforceGroupBudgets
// only brings the count down to capacity FROM ITS PERSPECTIVE; a
// subsequent Set will run another pass. The bound is therefore
// soft (eventually consistent) rather than strictly enforced
// per-Set.
func (c *Cache[K, V]) enforceGroupBudgets(tags []string) {
	if len(c.cfg.groups) == 0 || len(tags) == 0 || c.tags == nil {
		return
	}
	for _, tag := range tags {
		capacity, ok := c.cfg.groups[tag]
		if !ok {
			continue
		}
		c.shrinkGroup(tag, capacity)
	}
}

// shrinkGroup evicts oldest-first members of group `tag` until at
// most `capacity` members remain.
func (c *Cache[K, V]) shrinkGroup(tag string, capacity int) {
	for {
		members := c.tags.snapshot(tag)
		if len(members) <= capacity {
			return
		}
		oldestKey, ok := c.findOldestMember(members)
		if !ok {
			return
		}
		s := c.shardFor(oldestKey)
		s.mu.Lock()
		if e, exists := s.entries[oldestKey]; exists {
			c.removeLocked(s, e, EvictReasonTag)
		}
		s.mu.Unlock()
	}
}

// findOldestMember returns the key with the oldest `inserted`
// timestamp from members. Returns (zero K, false) when members is
// empty or every key has been concurrently removed.
func (c *Cache[K, V]) findOldestMember(members []K) (K, bool) {
	var oldestKey K
	var oldestTime int64
	found := false
	for _, k := range members {
		s := c.shardFor(k)
		s.mu.RLock()
		e, ok := s.entries[k]
		var t int64
		if ok {
			t = e.inserted
		}
		s.mu.RUnlock()
		if !ok {
			continue
		}
		if !found || t < oldestTime {
			oldestKey = k
			oldestTime = t
			found = true
		}
	}
	return oldestKey, found
}

// retagLocked updates the tag index when an entry's tag set
// changes. The caller MUST hold the shard write lock for key. Pass
// nil for either slice to indicate "no tags on that side".
func (c *Cache[K, V]) retagLocked(key K, oldTags, newTags []string) {
	if c.tags == nil {
		return
	}
	if len(oldTags) > 0 {
		c.tags.untag(key, oldTags)
	}
	if len(newTags) > 0 {
		c.tags.tag(key, newTags)
	}
}

// validateTagLimits checks that the proposed tag set respects
// [WithMaxTagsPerEntry] and [WithMaxTagsTotal]. Returns a
// [*CapacityError] wrapping [ErrTooManyTags] when either bound
// would be violated; nil otherwise.
//
// The newDistinctTagsCount calculation is approximate — it walks
// tags and checks the cache-level index for membership, accepting
// a small race window where a concurrent Set might also be
// introducing a new tag and both observers see "OK". The cap
// remains a soft bound; absolute enforcement requires a stricter
// (and much slower) cross-shard atomic check.
func (c *Cache[K, V]) validateTagLimits(tags []string) error {
	if c.cfg.maxTagsPerEntry > 0 && len(tags) > c.cfg.maxTagsPerEntry {
		return &CapacityError{
			Reason:     fmt.Sprintf("entry has %d tags; max is %d", len(tags), c.cfg.maxTagsPerEntry),
			LimitField: "MaxTagsPerEntry",
		}
	}
	if c.cfg.maxTagsTotal > 0 && c.tags != nil {
		c.tags.mu.RLock()
		current := len(c.tags.keysByTag)
		newDistinct := 0
		for _, t := range tags {
			if _, ok := c.tags.keysByTag[t]; !ok {
				newDistinct++
			}
		}
		c.tags.mu.RUnlock()
		if current+newDistinct > c.cfg.maxTagsTotal {
			return &CapacityError{
				Reason:     fmt.Sprintf("introducing %d new tags would exceed cache-level cap %d", newDistinct, c.cfg.maxTagsTotal),
				LimitField: "MaxTagsTotal",
			}
		}
	}
	return nil
}
