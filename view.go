package memcache

import "time"

// CacheView is a read-only handle that shares storage with a [Cache].
// View methods that touch a closed cache return zero values.
type CacheView[K comparable, V any] struct {
	cache *Cache[K, V]
}

// View returns a [CacheView] that shares storage with c. Closing c
// invalidates the view's reads but does not free it.
func (c *Cache[K, V]) View() *CacheView[K, V] {
	return &CacheView[K, V]{cache: c}
}

// Get delegates to [Cache.Get], including eviction-policy promotion on
// hit. Use [CacheView.Peek] for a no-promotion read.
func (v *CacheView[K, V]) Get(key K) (V, bool) {
	if v == nil || v.cache == nil {
		var zero V
		return zero, false
	}
	return v.cache.Get(key)
}

// Peek delegates to [Cache.Peek] (no eviction-policy side effect).
func (v *CacheView[K, V]) Peek(key K) (V, bool) {
	if v == nil || v.cache == nil {
		var zero V
		return zero, false
	}
	return v.cache.Peek(key)
}

// Has delegates to [Cache.Has].
func (v *CacheView[K, V]) Has(key K) bool {
	if v == nil || v.cache == nil {
		return false
	}
	return v.cache.Has(key)
}

// TTL delegates to [Cache.TTL].
func (v *CacheView[K, V]) TTL(key K) (time.Duration, bool) {
	if v == nil || v.cache == nil {
		return 0, false
	}
	return v.cache.TTL(key)
}

// Expiry delegates to [Cache.Expiry].
func (v *CacheView[K, V]) Expiry(key K) (time.Time, bool) {
	if v == nil || v.cache == nil {
		return time.Time{}, false
	}
	return v.cache.Expiry(key)
}

// Range iterates entries via [Cache.Range].
func (v *CacheView[K, V]) Range(fn func(key K, value V) bool) {
	if v == nil || v.cache == nil {
		return
	}
	v.cache.Range(fn)
}

// Keys returns a snapshot of cache keys via [Cache.Keys].
func (v *CacheView[K, V]) Keys() []K {
	if v == nil || v.cache == nil {
		return nil
	}
	return v.cache.Keys()
}

// Len returns the underlying cache's [Cache.Len].
func (v *CacheView[K, V]) Len() int {
	if v == nil || v.cache == nil {
		return 0
	}
	return v.cache.Len()
}

// Bytes returns the underlying cache's [Cache.Bytes].
func (v *CacheView[K, V]) Bytes() int64 {
	if v == nil || v.cache == nil {
		return 0
	}
	return v.cache.Bytes()
}

// Stats returns the underlying cache's [Cache.Stats].
func (v *CacheView[K, V]) Stats() Stats {
	if v == nil || v.cache == nil {
		return Stats{}
	}
	return v.cache.Stats()
}

// Clone returns a new [Cache] populated with a snapshot of c's live
// entries. Hit counts and policy positioning are not preserved. Holds
// each source shard's read lock per pass; concurrent mutations produce
// a per-shard-consistent (not cache-wide-consistent) snapshot.
func (c *Cache[K, V]) Clone() (*Cache[K, V], error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	cfgCopy := *c.cfg
	allowUnbounded := cfgCopy.maxEntries <= 0 && cfgCopy.maxBytes <= 0
	clone, err := build[K, V](&cfgCopy, allowUnbounded)
	if err != nil {
		return nil, err
	}

	type cloneEntry struct {
		key        K
		value      V
		weight     int64
		expireAt   int64
		inserted   int64
		lastAccess int64
		slidingTTL int64
		tags       []string
		flags      entryFlags
	}

	now := c.cfg.clock.Now().UnixNano()
	var staged []cloneEntry
	for _, s := range c.shards {
		s.mu.RLock()
		s.storage.each(func(e *entry[K, V]) bool {
			// entryExpiredLocked also runs WithExpireFunc, matching the
			// predicate used by Get-style methods.
			if c.entryExpiredLocked(e, now) || e.flags.has(flagNegative) {
				return true
			}
			tagsCopy := append([]string(nil), e.tags...)
			staged = append(staged, cloneEntry{
				key:        e.key,
				value:      e.loadValue(),
				weight:     e.weight,
				expireAt:   e.expireAt.Load(),
				inserted:   e.inserted,
				lastAccess: e.lastAccess.Load(),
				slidingTTL: e.slidingTTL,
				tags:       tagsCopy,
				flags:      e.flags,
			})
			return true
		})
		s.mu.RUnlock()
	}

	for _, ce := range staged {
		ns := clone.shardFor(ce.key)
		ns.mu.Lock()
		ne := ns.pool.get()
		ne.key = ce.key
		ne.storeValue(ce.value)
		ne.weight = ce.weight
		// Preserve original timestamps so refresh-ahead /
		// stale-while-revalidate / sliding-TTL heuristics on the
		// clone behave the same as on the source.
		ne.inserted = ce.inserted
		ne.lastAccess.Store(ce.lastAccess)
		ne.expireAt.Store(ce.expireAt)
		ne.flags = ce.flags
		ne.slidingTTL = ce.slidingTTL
		ne.tags = ce.tags
		ns.storage.set(ce.key, ne)
		// Register the entry with the shard's TTL backend so
		// expirations are observable via the janitor (not just
		// lazily on Get).
		ns.expiryAdd(ne)
		ns.policy.OnInsert(ne)
		// Mirror upsertLocked's pattern: register tags inside the
		// shard lock (retagLocked takes c.tags.mu internally; the
		// "shard then index" lock order is preserved).
		clone.retagLocked(ce.key, nil, ce.tags)
		if ce.expireAt > 0 {
			clone.startJanitorLocked(ns)
		}
		clone.counters.entries.Add(1)
		clone.counters.bytes.Add(ce.weight)
		ns.mu.Unlock()
	}
	return clone, nil
}
