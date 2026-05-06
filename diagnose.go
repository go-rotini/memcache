package memcache

import (
	"fmt"
	"io"
	"sort"
	"time"
)

// Items returns a snapshot of every live entry in the cache,
// including each entry's metadata. The returned slice is freshly
// allocated and may be mutated by the caller. Iteration order is
// shard-by-shard and within-shard map-iteration order — neither is
// stable.
//
// Cost: O(n) time and allocation. Use [Cache.Range] for non-
// allocating iteration when only the (key, value) pair is needed.
func (c *Cache[K, V]) Items() []KeyedItem[K, V] {
	if c.closed.Load() {
		return nil
	}
	out := make([]KeyedItem[K, V], 0, c.Len())
	now := c.cfg.clock.Now().UnixNano()
	for _, s := range c.shards {
		s.mu.RLock()
		for _, e := range s.entries {
			if e.expired(now) || e.flags.has(flagNegative) {
				continue
			}
			out = append(out, KeyedItem[K, V]{
				Key:  e.key,
				Item: e.item(),
			})
		}
		s.mu.RUnlock()
	}
	return out
}

// ItemMetadata returns the metadata of the entry at key without
// copying its value. The boolean reports presence; an absent or
// negative-tombstone entry returns (zero Metadata, false).
func (c *Cache[K, V]) ItemMetadata(key K) (Metadata, bool) {
	if c.closed.Load() {
		return Metadata{}, false
	}
	s := c.shardFor(key)
	now := c.cfg.clock.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	if !ok || e.expired(now) || e.flags.has(flagNegative) {
		return Metadata{}, false
	}
	return e.metadata(), true
}

// Hottest returns the n entries with the highest hit counts in
// descending order. n=0 returns every entry. Implementation cost
// is O(N log min(n, N)) for cache size N.
//
// "Hit count" is the per-entry hits counter — a coarse but cheap
// proxy for popularity. Frequency-aware policies (LFU, S3-FIFO,
// TinyLFU) maintain their own internal popularity state that is
// NOT directly exposed here; if you need the policy's exact view
// of "hot", consult the policy's internal state through a
// dedicated test build (the public API surface keeps this lossy
// abstraction stable).
func (c *Cache[K, V]) Hottest(n int) []KeyedItem[K, V] {
	all := c.Items()
	sort.Slice(all, func(i, j int) bool {
		return all[i].Hits > all[j].Hits
	})
	return topN(all, n)
}

// Coldest is the inverse of [Cache.Hottest] — entries with the
// lowest hit counts come first. Useful for "what's about to fall
// out?" diagnostics under hit-count-based eviction.
func (c *Cache[K, V]) Coldest(n int) []KeyedItem[K, V] {
	all := c.Items()
	sort.Slice(all, func(i, j int) bool {
		return all[i].Hits < all[j].Hits
	})
	return topN(all, n)
}

// LongestLived returns the n entries with the oldest insertion
// time, oldest first. n=0 returns every entry.
func (c *Cache[K, V]) LongestLived(n int) []KeyedItem[K, V] {
	all := c.Items()
	sort.Slice(all, func(i, j int) bool {
		return all[i].Inserted.Before(all[j].Inserted)
	})
	return topN(all, n)
}

// SoonestExpiring returns the n entries closest to TTL expiry,
// soonest first. Entries without a TTL are excluded entirely.
func (c *Cache[K, V]) SoonestExpiring(n int) []KeyedItem[K, V] {
	all := c.Items()
	out := all[:0]
	for _, ki := range all {
		if !ki.Expiry.IsZero() {
			out = append(out, ki)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Expiry.Before(out[j].Expiry)
	})
	return topN(out, n)
}

// Histogram returns a coarse multi-dimensional summary of the
// cache's contents bucketed by age, weight, and hit count. Useful
// for `:cache hist` REPL commands.
//
// Bucket bounds (matching the [Histogram] doc):
//   - Age: 1s, 10s, 1m, 10m, 1h, 1d, 7d, +∞
//   - Weight: 1, 16, 256, 4Ki, 64Ki, 1Mi, 16Mi, +∞
//   - Hits: 0, 1, 2, 4, 16, 64, 256, +∞
func (c *Cache[K, V]) Histogram() Histogram {
	var h Histogram
	if c.closed.Load() {
		return h
	}
	now := c.cfg.clock.Now()
	for _, s := range c.shards {
		s.mu.RLock()
		for _, e := range s.entries {
			if e.expired(now.UnixNano()) || e.flags.has(flagNegative) {
				continue
			}
			h.TotalEntries++
			h.TotalWeight += e.weight
			h.AgeBuckets[ageBucket(now.Sub(time.Unix(0, e.inserted)))]++
			h.WeightBuckets[weightBucket(e.weight)]++
			h.HitsBuckets[hitsBucket(e.hits.Load())]++
		}
		s.mu.RUnlock()
	}
	return h
}

// Dump writes a human-readable summary of every entry to w. Output
// is intended for debugging only — its format is NOT stable across
// releases. One line per entry, plus a header summary.
func (c *Cache[K, V]) Dump(w io.Writer) error {
	st := c.Stats()
	if _, err := fmt.Fprintf(w,
		"# memcache dump  entries=%d bytes=%d hits=%d misses=%d hit-rate=%.3f\n",
		st.Entries, st.Bytes, st.Hits, st.Misses, st.HitRate(),
	); err != nil {
		return fmt.Errorf("memcache dump header: %w", err)
	}
	items := c.Items()
	for _, ki := range items {
		expiry := "no-ttl"
		if !ki.Expiry.IsZero() {
			expiry = ki.Expiry.Format(time.RFC3339Nano)
		}
		if _, err := fmt.Fprintf(w,
			"  key=%v hits=%d weight=%d inserted=%s expiry=%s tags=%v sliding=%v\n",
			ki.Key, ki.Hits, ki.Weight,
			ki.Inserted.Format(time.RFC3339Nano), expiry, ki.Tags, ki.Sliding,
		); err != nil {
			return fmt.Errorf("memcache dump entry: %w", err)
		}
	}
	return nil
}

// item returns a value-bearing Item snapshot. Caller must hold the
// shard read lock for the entry. We add this here (rather than in
// entry.go) because it's only used by diagnostic methods —
// keeping it in the diagnostic file documents the boundary.
func (e *entry[K, V]) item() Item[V] {
	m := e.metadata()
	return Item[V]{
		Value:      e.value,
		Expiry:     m.Expiry,
		LastAccess: m.LastAccess,
		Inserted:   m.Inserted,
		Hits:       m.Hits,
		Weight:     m.Weight,
		Tags:       m.Tags,
		Sliding:    m.Sliding,
	}
}

// topN truncates a slice to the first n elements. n=0 returns the
// whole slice (caller asked for "everything"). The trailing
// underscore avoids shadowing builtin `cap`.
func topN[T any](s []T, n int) []T {
	if n <= 0 || n >= len(s) {
		return s
	}
	return s[:n]
}

// ageBucket maps a duration to its [Histogram] AgeBucket index.
// Bounds: 1s, 10s, 1m, 10m, 1h, 1d, 7d, +∞.
func ageBucket(d time.Duration) int {
	switch {
	case d < time.Second:
		return 0
	case d < 10*time.Second:
		return 1
	case d < time.Minute:
		return 2
	case d < 10*time.Minute:
		return 3
	case d < time.Hour:
		return 4
	case d < 24*time.Hour:
		return 5
	case d < 7*24*time.Hour:
		return 6
	default:
		return 7
	}
}

// weightBucket maps a weight to its [Histogram] WeightBucket index.
// Bounds: 1, 16, 256, 4Ki, 64Ki, 1Mi, 16Mi, +∞.
func weightBucket(w int64) int {
	switch {
	case w < 1:
		return 0
	case w < 16:
		return 1
	case w < 256:
		return 2
	case w < 4*1024:
		return 3
	case w < 64*1024:
		return 4
	case w < 1024*1024:
		return 5
	case w < 16*1024*1024:
		return 6
	default:
		return 7
	}
}

// hitsBucket maps a hit count to its [Histogram] HitsBucket index.
// Bounds: 0, 1, 2, 4, 16, 64, 256, +∞.
func hitsBucket(h uint32) int {
	switch {
	case h < 1:
		return 0
	case h < 2:
		return 1
	case h < 4:
		return 2
	case h < 16:
		return 3
	case h < 64:
		return 4
	case h < 256:
		return 5
	default:
		return 6
	}
}
