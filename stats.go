package memcache

import (
	"sync/atomic"
	"time"
)

// Stats is a point-in-time snapshot of per-cache statistics.
//
// All counters are monotonic from the cache's perspective — they are
// reset only via [Cache.ResetStats]. Loads of [Stats] from a running
// cache are best-effort consistent: each field is loaded atomically,
// but the snapshot as a whole may straddle concurrent operations.
type Stats struct {
	// Hits is the number of Get-style operations that returned a
	// fresh value.
	Hits uint64

	// Misses is the number of Get-style operations that returned
	// nothing (missing or expired).
	Misses uint64

	// Inserts is the number of new keys added to the cache.
	Inserts uint64

	// Updates is the number of existing keys whose value was
	// replaced.
	Updates uint64

	// Deletes is the number of explicit deletes that removed a key.
	Deletes uint64

	// Evictions is the total number of entries removed by the
	// eviction policy or by capacity-driven sweeps.
	Evictions uint64

	// Expirations is the total number of entries removed by TTL
	// expiry (lazy or via the janitor).
	Expirations uint64

	// EvictionsByReason counts the per-reason eviction breakdown,
	// indexed by [EvictionReason].
	EvictionsByReason [numEvictionReasons]uint64

	// LoadsTotal is the number of Loader invocations.
	LoadsTotal uint64

	// LoadHits is the count of loader calls that resolved successfully.
	LoadHits uint64

	// LoadErrors is the count of loader calls that returned an error.
	LoadErrors uint64

	// LoadCoalesced is the count of waiters that joined an in-flight
	// load (singleflight savings).
	LoadCoalesced uint64

	// EventsDropped is the count of subscriber events dropped because
	// a subscriber's channel was full.
	EventsDropped uint64

	// HashCollisions is the count of distinct keys that hashed to the
	// same bucket. Populated only when [WithCollisionTracking] is on.
	HashCollisions uint64

	// Entries is the current number of entries in the cache (live
	// Len at snapshot time).
	Entries int64

	// Bytes is the current total weight of all entries (sum of the
	// configured Weigher's results), or the entry count when no
	// Weigher is configured.
	Bytes int64

	// Capacity is the cache's current configured bound (entries when
	// MaxEntries is set, bytes otherwise).
	Capacity int64

	// At is the wall-clock time at which the snapshot was taken.
	At time.Time
}

// HitRate returns Hits / (Hits + Misses) as a float in [0, 1]. Returns
// 0 when no Get operations have been observed.
func (s Stats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// statsCounters is the live, mutable counter set updated on the hot
// path. It is converted to a [Stats] value on demand by Snapshot.
type statsCounters struct {
	hits              atomic.Uint64
	misses            atomic.Uint64
	inserts           atomic.Uint64
	updates           atomic.Uint64
	deletes           atomic.Uint64
	evictions         atomic.Uint64
	expirations       atomic.Uint64
	evictionsByReason [numEvictionReasons]atomic.Uint64
	loadsTotal        atomic.Uint64
	loadHits          atomic.Uint64
	loadErrors        atomic.Uint64
	loadCoalesced     atomic.Uint64
	eventsDropped     atomic.Uint64
	hashCollisions    atomic.Uint64

	// Live entry/byte counts. The cache updates these directly on
	// insert/evict.
	entries atomic.Int64
	bytes   atomic.Int64
}

// snapshot atomically reads every counter and returns a [Stats].
// entries/bytes/capacity are left to the caller to populate based on
// the cache's bound configuration.
func (s *statsCounters) snapshot(now time.Time) Stats {
	out := Stats{
		Hits:           s.hits.Load(),
		Misses:         s.misses.Load(),
		Inserts:        s.inserts.Load(),
		Updates:        s.updates.Load(),
		Deletes:        s.deletes.Load(),
		Evictions:      s.evictions.Load(),
		Expirations:    s.expirations.Load(),
		LoadsTotal:     s.loadsTotal.Load(),
		LoadHits:       s.loadHits.Load(),
		LoadErrors:     s.loadErrors.Load(),
		LoadCoalesced:  s.loadCoalesced.Load(),
		EventsDropped:  s.eventsDropped.Load(),
		HashCollisions: s.hashCollisions.Load(),
		Entries:        s.entries.Load(),
		Bytes:          s.bytes.Load(),
		At:             now,
	}
	for i := range out.EvictionsByReason {
		out.EvictionsByReason[i] = s.evictionsByReason[i].Load()
	}
	return out
}

// reset zeroes every counter. Live entry and byte counts are NOT
// reset; they reflect the cache's current state, not a window.
func (s *statsCounters) reset() {
	s.hits.Store(0)
	s.misses.Store(0)
	s.inserts.Store(0)
	s.updates.Store(0)
	s.deletes.Store(0)
	s.evictions.Store(0)
	s.expirations.Store(0)
	s.loadsTotal.Store(0)
	s.loadHits.Store(0)
	s.loadErrors.Store(0)
	s.loadCoalesced.Store(0)
	s.eventsDropped.Store(0)
	s.hashCollisions.Store(0)
	for i := range s.evictionsByReason {
		s.evictionsByReason[i].Store(0)
	}
}
