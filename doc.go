// Package memcache implements a bounded, generic, thread-safe in-memory
// cache designed for long-running Go programs: REPLs, daemons, watch-mode
// build tools, and other CLI workloads where cache state must survive across
// many requests but the host process restarts often enough that warm-restart
// matters.
//
// The primary type is the generic [Cache], constructed via [New] with one
// or more [Option] functional options:
//
//	c, err := memcache.New[string, *Profile](
//	    memcache.WithMaxEntries(10_000),
//	    memcache.WithDefaultTTL(15*time.Minute),
//	    memcache.WithPolicy(memcache.PolicyS3FIFO),
//	)
//	if err != nil { panic(err) }
//	defer c.Close()
//
//	c.Set("alice", profile)
//	got, ok := c.Get("alice")
//
// # Bounded by Default
//
// [New] returns [ErrUnbounded] if neither [WithMaxEntries] nor
// [WithMaxBytes] is supplied. Use
// [NewUnbounded] only when the key space is provably bounded by something
// else.
//
// # Eviction Policies
//
// The default eviction policy is [PolicyS3FIFO], a modern algorithm that
// achieves hit rates comparable to TinyLFU with substantially simpler state.
// Other policies are available via [WithPolicy]: [PolicyLRU], [PolicyLFU],
// [PolicyTinyLFU], [PolicyFIFO], [PolicyARC], [Policy2Q].
//
// # Stampede Protection
//
// When a [Loader] is configured via [WithLoader], the cache deduplicates
// concurrent misses for the same key (singleflight semantics). Use
// [WithRefreshAhead] to pre-fetch entries before they expire and
// [WithStaleWhileRevalidate] to serve stale values during reload.
// [WithNegativeCache] caches "not found" results to avoid repeated upstream
// calls on missing keys.
//
// # Snapshot & Restore
//
// The cache can be persisted to disk and restored on the next process
// launch, so a restarted program starts warm. [Cache.Save] and
// [Cache.Load] handle the basic flow; [WithAutoSave] and [WithAutoLoad]
// make persistence automatic.
//
// # Tags
//
// Entries can be tagged at insert time and invalidated as a group:
//
//	c.SetWithTags("user:42:profile", profile, "user-42")
//	c.SetWithTags("user:42:settings", settings, "user-42")
//	// Later, invalidate everything tagged "user-42":
//	c.InvalidateTag("user-42")
//
// Tag invalidation is O(k) where k is the number of tagged entries, far
// cheaper than scanning the cache.
//
// # Errors
//
// Failures are reported as sentinel errors such as [ErrClosed] and
// [ErrNotFound], or as the typed errors [ConfigError], [CapacityError],
// [SnapshotError] and [CodecError]. Match both kinds with [errors.Is]
// and [errors.As]; a typed error also matches the sentinel it carries:
//
//	if errors.Is(err, memcache.ErrNotFound) { ... }
//	if errors.Is(err, memcache.ErrTooManyTags) { ... } // a *CapacityError
//
// Errors returned by a [Loader] reach the caller unwrapped; a load that
// exceeds [WithLoaderTimeout] also matches [ErrLoaderTimeout].
package memcache
