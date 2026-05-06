package memcache

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-rotini/memcache/internal/tdigest"
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

	// LoadCalls is a spec-§12.1 alias for [Stats.LoadsTotal]. Both
	// fields carry the same value; new code should prefer LoadsTotal.
	// Tracked here so callers reading the spec's field names compile
	// against the implementation.
	LoadCalls uint64

	// LoadHits is the count of loader calls that resolved successfully.
	LoadHits uint64

	// LoadErrors is the count of loader calls that returned an error.
	LoadErrors uint64

	// LoadCoalesced is the count of waiters that joined an in-flight
	// load (singleflight savings).
	LoadCoalesced uint64

	// Singleflights is a spec-§12.1 alias for [Stats.LoadCoalesced].
	// Both fields carry the same value; new code should prefer
	// LoadCoalesced.
	Singleflights uint64

	// EventsDropped is the count of subscriber events dropped because
	// a subscriber's channel was full.
	EventsDropped uint64

	// HashCollisions is the count of distinct keys that hashed to the
	// same bucket. Populated only when [WithCollisionTracking] is on.
	HashCollisions uint64

	// AdmissionRejects counts inserts dropped by the admission
	// policy ([WithAdmissionPolicy] / [WithDoorkeeper]). Each
	// rejection means a [Cache.Set] call returned without storing
	// the value because the policy refused the candidate.
	AdmissionRejects uint64

	// LoadTimeouts counts loader invocations that exceeded
	// [WithLoaderTimeout]. Each is also a [LoadErrors] increment.
	LoadTimeouts uint64

	// LoadRateLimited counts GetOrLoad calls rejected by
	// [WithLoaderRateLimit] before the loader fired.
	LoadRateLimited uint64

	// LoadCachedError counts GetOrLoad calls that short-circuited
	// against a tombstone produced by [WithErrorTTL].
	LoadCachedError uint64

	// RefreshAhead counts asynchronous loader calls triggered by
	// [WithRefreshAhead].
	RefreshAhead uint64

	// StaleWhileRevalidate counts asynchronous loader calls
	// triggered by [WithStaleWhileRevalidate].
	StaleWhileRevalidate uint64

	// NegativeHits counts Get-style calls that short-circuited
	// against a [WithNegativeCache] tombstone.
	NegativeHits uint64

	// Resizes counts [Cache.Resize] invocations.
	Resizes uint64

	// TagInvalidations counts [Cache.InvalidateTag] /
	// [Cache.InvalidateTags] calls (one per call, not per dropped
	// entry — those land in EvictionsByReason[EvictReasonTag]).
	TagInvalidations uint64

	// TagsTracked is the live tag-index size (distinct tag count).
	TagsTracked int

	// TagCleanupBacklog is the depth of the per-cache async untag
	// queue. A persistently-positive value indicates the eviction
	// rate is outpacing the index drainer; consider raising the
	// queue capacity (compile-time `tagCleanupBuffer` constant) or
	// reducing tag churn.
	TagCleanupBacklog int

	// Uptime is the wall-clock duration since [New] returned.
	Uptime time.Duration

	// LastSnapshotAt is the wall-clock time of the most recent
	// successful Save / Load. Zero when no snapshot has run.
	LastSnapshotAt time.Time

	// LastResetAt is the wall-clock time of the most recent
	// [Cache.ResetStats]. Zero when never reset.
	LastResetAt time.Time

	// PolicyName is the canonical name of the configured eviction
	// policy (e.g. "S3FIFO", "LRU"). Filled from
	// [Policy.String].
	PolicyName string

	// PolicyDetail is the per-policy diagnostic struct returned by
	// [evictionPolicy.Snapshot] for shard 0; concrete type depends
	// on the configured policy ([PolicyDetailLRU] / [PolicyDetailS3FIFO]
	// / etc.). Use a type switch or assertion when consuming.
	// nil when the cache has no shards (degenerate state).
	PolicyDetail any

	// LoadLatency is the arithmetic mean of every Loader call that
	// completed since the last [Cache.ResetStats].
	LoadLatency time.Duration

	// LoadLatencyP50 is the median Loader-call duration. Computed
	// from a fixed-bucket log-spaced histogram with roughly half-
	// bucket precision (see [internal/tdigest]).
	LoadLatencyP50 time.Duration

	// LoadLatencyP99 is the 99th-percentile Loader-call duration.
	// Same precision caveat as P50.
	LoadLatencyP99 time.Duration

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
//
// Hits and misses use [shardedCounter] when [WithShardedStats] is
// enabled (slot count = runtime.GOMAXPROCS×4, capped at 256) so
// >32-core machines avoid cache-line contention. The remaining
// fields stay as plain atomic.Uint64 — they are touched
// significantly less often (only on inserts, evictions, etc.) and
// the constant-factor savings would not justify the indirection.
// hitsShard / missesShard are nil when sharded stats are off; in
// that case hits / misses are the live counters.
type statsCounters struct {
	hits                 atomic.Uint64
	misses               atomic.Uint64
	hitsShard            *shardedCounter
	missesShard          *shardedCounter
	inserts              atomic.Uint64
	updates              atomic.Uint64
	deletes              atomic.Uint64
	evictions            atomic.Uint64
	expirations          atomic.Uint64
	evictionsByReason    [numEvictionReasons]atomic.Uint64
	loadsTotal           atomic.Uint64
	loadHits             atomic.Uint64
	loadErrors           atomic.Uint64
	loadCoalesced        atomic.Uint64
	eventsDropped        atomic.Uint64
	hashCollisions       atomic.Uint64
	admissionRejects     atomic.Uint64
	loadTimeouts         atomic.Uint64
	loadRateLimited      atomic.Uint64
	loadCachedError      atomic.Uint64
	refreshAhead         atomic.Uint64
	staleWhileRevalidate atomic.Uint64
	negativeHits         atomic.Uint64
	resizes              atomic.Uint64
	tagInvalidations     atomic.Uint64

	// loadLatency tracks Loader-call duration for the
	// [Stats.LoadLatency] / P50 / P99 fields. Pointer so the
	// counter has a stable address through reset.
	loadLatency *tdigest.Histogram

	// Wall-clock timestamps. createdAt is set once by New and never
	// reset; lastSnapshotAt and lastResetAt are stamped on the
	// corresponding events. All three are protected by tsMu so the
	// snapshot reads stay consistent under concurrent updates.
	tsMu           sync.Mutex
	createdAt      time.Time
	lastSnapshotAt time.Time
	lastResetAt    time.Time

	// Live entry/byte counts. The cache updates these directly on
	// insert/evict.
	entries atomic.Int64
	bytes   atomic.Int64
}

// newStatsCounters constructs a counter set; sharded ⇒ allocates
// per-CPU shadow counters for the hot-path Hits/Misses fields.
// now stamps createdAt so [Stats.Uptime] is computable from the
// first Stats call onward.
func newStatsCounters(sharded bool, now time.Time) *statsCounters {
	c := &statsCounters{
		createdAt:   now,
		loadLatency: &tdigest.Histogram{},
	}
	if sharded {
		slots := shardedStatsSize(runtime.GOMAXPROCS(0))
		c.hitsShard = newShardedCounter(slots)
		c.missesShard = newShardedCounter(slots)
	}
	return c
}

// stampSnapshot records `at` as the most recent snapshot Save/Load
// time.
func (s *statsCounters) stampSnapshot(at time.Time) {
	s.tsMu.Lock()
	s.lastSnapshotAt = at
	s.tsMu.Unlock()
}

// stampReset records `at` as the most recent ResetStats time.
func (s *statsCounters) stampReset(at time.Time) {
	s.tsMu.Lock()
	s.lastResetAt = at
	s.tsMu.Unlock()
}

// addHit increments the hit counter, routing through the sharded
// shadow when [WithShardedStats] is enabled.
func (s *statsCounters) addHit() {
	if s.hitsShard != nil {
		s.hitsShard.Add(1)
		return
	}
	s.hits.Add(1)
}

// addMiss is the miss-side counterpart of addHit.
func (s *statsCounters) addMiss() {
	if s.missesShard != nil {
		s.missesShard.Add(1)
		return
	}
	s.misses.Add(1)
}

// readHits returns the live hit count, summing the sharded shadow
// when present.
func (s *statsCounters) readHits() uint64 {
	if s.hitsShard != nil {
		return s.hitsShard.Load()
	}
	return s.hits.Load()
}

// readMisses is the miss-side counterpart of readHits.
func (s *statsCounters) readMisses() uint64 {
	if s.missesShard != nil {
		return s.missesShard.Load()
	}
	return s.misses.Load()
}

// snapshot atomically reads every counter and returns a [Stats].
// entries/bytes/capacity are left to the caller to populate based on
// the cache's bound configuration.
func (s *statsCounters) snapshot(now time.Time) Stats {
	out := Stats{
		Hits:                 s.readHits(),
		Misses:               s.readMisses(),
		Inserts:              s.inserts.Load(),
		Updates:              s.updates.Load(),
		Deletes:              s.deletes.Load(),
		Evictions:            s.evictions.Load(),
		Expirations:          s.expirations.Load(),
		LoadsTotal:           s.loadsTotal.Load(),
		LoadCalls:            s.loadsTotal.Load(),
		LoadHits:             s.loadHits.Load(),
		LoadErrors:           s.loadErrors.Load(),
		LoadCoalesced:        s.loadCoalesced.Load(),
		Singleflights:        s.loadCoalesced.Load(),
		EventsDropped:        s.eventsDropped.Load(),
		HashCollisions:       s.hashCollisions.Load(),
		AdmissionRejects:     s.admissionRejects.Load(),
		LoadTimeouts:         s.loadTimeouts.Load(),
		LoadRateLimited:      s.loadRateLimited.Load(),
		LoadCachedError:      s.loadCachedError.Load(),
		RefreshAhead:         s.refreshAhead.Load(),
		StaleWhileRevalidate: s.staleWhileRevalidate.Load(),
		NegativeHits:         s.negativeHits.Load(),
		Resizes:              s.resizes.Load(),
		TagInvalidations:     s.tagInvalidations.Load(),
		Entries:              s.entries.Load(),
		Bytes:                s.bytes.Load(),
		At:                   now,
	}
	s.tsMu.Lock()
	if !s.createdAt.IsZero() {
		out.Uptime = now.Sub(s.createdAt)
	}
	out.LastSnapshotAt = s.lastSnapshotAt
	out.LastResetAt = s.lastResetAt
	s.tsMu.Unlock()
	if s.loadLatency != nil {
		ls := s.loadLatency.Snapshot()
		out.LoadLatency = ls.Mean
		out.LoadLatencyP50 = ls.P50
		out.LoadLatencyP99 = ls.P99
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
	if s.hitsShard != nil {
		s.hitsShard.Store(0)
	}
	if s.missesShard != nil {
		s.missesShard.Store(0)
	}
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
	s.admissionRejects.Store(0)
	s.loadTimeouts.Store(0)
	s.loadRateLimited.Store(0)
	s.loadCachedError.Store(0)
	s.refreshAhead.Store(0)
	s.staleWhileRevalidate.Store(0)
	s.negativeHits.Store(0)
	s.resizes.Store(0)
	s.tagInvalidations.Store(0)
	if s.loadLatency != nil {
		s.loadLatency.Reset()
	}
	for i := range s.evictionsByReason {
		s.evictionsByReason[i].Store(0)
	}
}
