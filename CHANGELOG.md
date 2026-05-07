# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-05-06

Initial release. Generic, sharded, in-process cache with pluggable
eviction policies, TTL/expiry, singleflight loading, tag-based
invalidation, snapshot persistence, and tiered composition.

### Added

- **Core cache**: `New[K, V](...Option)` constructs a typed,
  bounded cache. `Get`, `Set`, `Has`, `Delete`, `Peek`, `Len`,
  `Range`, `Items`, `Keys`, `Clear`, `Close`. Either `WithMaxEntries`
  or `WithMaxBytes` (with `WithWeigher`) is required — unbounded
  caches are rejected at construction.
- **Sharding**: `WithShards(n)` rounds up to power-of-two; per-shard
  RWMutex serializes writes within a shard but distinct shards run
  in parallel. Default shard count tracks `runtime.GOMAXPROCS`.
- **Eviction policies**: `WithPolicy(...)` selects among `PolicyLRU`,
  `PolicyFIFO`, `PolicyLFU`, `PolicyS3FIFO` (default), `PolicyTinyLFU`,
  `Policy2Q`, and `PolicyARC`.
- **TTL & expiry**: `SetWithTTL`, `SetWithOptions(SetTTL(...))`,
  `WithDefaultTTL`, `WithSlidingTTL`. Per-shard expiry heap with a
  lazy janitor goroutine (`WithJanitorInterval`).
- **Loader & singleflight**: `WithLoader(LoaderFunc[K, V])` plus
  `GetOrLoad(ctx, key)` deduplicates concurrent misses on the same
  key. `WithRefreshAhead(fraction)` triggers async re-load when an
  entry crosses the configured fraction of its TTL.
- **Negative caching**: `WithNegativeCache(ttl)` converts loader
  `ErrNotFound` into a tombstone for the configured duration.
- **Tag-based invalidation**: `SetWithTags`, `Tags`, `InvalidateTag`,
  `InvalidateTags`. Tag indexes survive Save/Load and are rebuilt
  during load.
- **Groups & bulk ops**: `WithGroup(name, capacity)` enforces per-group
  capacity bounds independent of the cache-wide bound. `GetMulti`,
  `SetMulti`, `DeleteMulti`, `GetMultiOrLoad` for batched ops.
- **Stats, events, hooks**: `Stats()` returns per-cache counters
  (Hits, Misses, Evictions, Inserts, Updates, LoadsTotal,
  LoadCoalesced, EventsDropped, …). `Subscribe(buffer, kinds...)`
  returns a channel that fans out `EventInsert | EventUpdate |
  EventEvict | EventExpire | EventLoad | EventLoadError |
  EventLoadTimeout | EventLoadRateLimited | EventInvalidateTag |
  EventResize | EventSnapshot`. `WithOnHit`, `WithOnMiss`,
  `WithOnEvict`, `WithOnExpire`, `WithOnLoad` for
  synchronous hooks. `Tiered` exposes `TieredStats` (per-tier
  Stats + L1Hits / L2Hits / Misses / Promotions).
- **Snapshot & restore**: `Save(io.Writer)` / `Load(io.Reader)`
  with the v2 framed format (RTNI magic + version + codec + name
  + metadata + per-entry records + CRC32-Castagnoli trailer).
  `WithAutoSave(path, interval)` rotates atomically; `WithAutoLoad`
  reads at construction time. The Cacheable interface family
  (`SnapshotMarshaler`, `SnapshotUnmarshaler`, `CacheTTLer`,
  `CacheTagger`) lets values participate in the snapshot/restore
  protocol.
- **Tiered composition**: `NewTiered(l1, l2, ...)` composes caches
  in tier order. Reads cascade L1 → L2 → ... and promote on hit.
- **Compute / atomic update**: `Compute(key, fn)` for atomic
  read-modify-write. Re-entrancy from the same goroutine is detected
  and rejected with `ErrComputeReentrant`.
- **Numeric helpers**: `Increment`, `IncrementBy`, `Decrement`,
  `DecrementBy` as thin wrappers over Compute.
- **Struct-tag introspection**: `cache:"-"` excludes a field from
  snapshots; `cache:"secret"` redacts it from event/hook payloads;
  `cache:"omitempty"` declares fields whose absence from snapshots
  is acceptable; `cache:",versioned"` participates in the schema
  fingerprint (sha256 of sorted Name:Type pairs) embedded in the
  snapshot under `__memcache_schema_version` — Load rejects
  fingerprint drift with `ErrSnapshotIncompatible`;
  `cache:"...,tag=user-{Field}"` auto-derives tag values from
  struct field contents at Set time.
- **Snapshot compression**: `WithCompressedCodec(base, level)` and
  `NewCompressedCodec(base, level)` wrap any base codec with gzip;
  the codec name is `<base>+gzip` so codec-mismatch checks fire
  across the compression boundary.
- **Lifecycle hooks**: `WithPurgeVisitor` fires once per live entry
  during `Clear` and `Close` (outside any shard lock); intended for
  draining cache state to disk, the network, or a structured log.
- **Hot-path safety**: `WithCopyOnGet[V](fn func(V) V)` applies fn
  to every successful Get/Peek return, isolating callers from
  mutations of cached state.
- **Construction-time validation**: `WithSafeKeys(true)` logs a
  warning when K is a pointer (compares by identity) or a struct
  containing pointer fields (mutable equality is fragile).
- **Watchdog**: `WithCallbackTimeout(d)` logs a warning via the
  configured slog.Logger when any synchronous hook (OnHit/OnMiss/
  OnEvict/OnExpire/OnLoad/PurgeVisitor) exceeds d. Best-effort —
  Go cannot preempt callbacks.
- **Admission policy**: pluggable `AdmissionPolicy[K]` interface
  consulted before insert. `AdmitAlways[K]` is the default;
  `Doorkeeper[K]` (bloom-filter backed, "admit on second
  observation") is wired in via `WithDoorkeeper(true)` or
  constructed manually with `NewDoorkeeper`. Rejected admissions
  surface in `Stats.AdmissionRejects`.
- **Distributed invalidation hooks**: `WithInvalidationPublisher`
  fires synchronously on every removal; `WithInvalidationSubscriber`
  attaches a `<-chan K` whose receives become `EvictReasonRemote`
  Deletes. The package provides no transport — pair with Redis
  pub/sub, NATS, fsnotify, etc.
- **Sharded stats**: `WithShardedStats(true)` redirects the
  hot-path Hits/Misses counters to per-CPU sharded shadows
  (slot count = GOMAXPROCS×4, capped at 256). Reduces cache-line
  contention on >32-core machines; Stats() pays a fan-in cost on
  read.
- **Snapshot encryption**: `EncryptedCodec` + `WithEncryptedCodec(base, key)`
  wrap any base codec with AES-256-GCM authenticated encryption.
  Wire format is `<12-byte nonce><sealed ciphertext>`. Bad keys
  surface as `*ConfigError` from New rather than as runtime decode
  errors.
- **Read-lock fast path on Get**: hits where the eviction policy
  doesn't need promotion (FIFO always; S3-FIFO at freq saturation),
  the entry has no sliding TTL, and no refresh-ahead window
  applies serve under a read lock — no write-lock acquire, no
  policy mutation. Hot-path Get latency dropped roughly 10× on
  benchmark sweep (≈480ns → ≈45ns on Apple M3). The slow path
  still takes the write lock for promotions and side-effects.
- **Latency telemetry**: `Stats.LoadLatency` / `LoadLatencyP50` /
  `LoadLatencyP99`, populated from `internal/tdigest/`'s lock-free
  fixed-bucket histogram. Records every Loader call's wall
  duration; resets with `Cache.ResetStats`.
- **Per-policy stats**: `Stats.PolicyDetail` returns a per-policy
  diagnostic struct (`PolicyDetailLRU`, `PolicyDetailS3FIFO`, etc.)
  describing the live state of shard 0's policy.
- **Hashed timing wheel**: `internal/wheel/` ships a generic timing
  wheel for high-volume TTL workloads. `WithTTLBuckets(slots,
  tickPerBucket)` selects the wheel as the per-shard TTL backend;
  the default remains the per-shard min-heap, which is faster for
  typical CLI workloads.
- **Lock-free `Get` fast path**: `WithLockFreeRead()` publishes an
  immutable per-shard snapshot via `atomic.Pointer` so the Get fast
  path runs without the shard lock for fast-pathable hits. Gated to
  S3-FIFO + no `WithExpireFunc` configurations where the read path
  is provably race-free. ~10% improvement with all features on,
  ~3× with stats off.
- **External-store backend**: `WithStore[K, V](store)` wires a
  user-supplied `Store[K, V]` in as the cache's source of truth.
  Reads on in-memory miss fall through to the Store and promote;
  writes/deletes propagate through. The package ships
  `MemoryStore` as the default in-process implementation; disk-
  backed and Redis-backed adapters are intended to be third-party.
- **Async writes**: `WithAsyncWrites()` decouples Set/Delete from
  the storage update via per-shard pending maps drained by a
  per-cache apply goroutine. `Sync` blocks until pending drains;
  `Close` drains before stopping the apply goroutine.
- **Flat shard storage**: `WithFlatStorage()` opts each shard into
  an open-addressed flat hash table with linear probing and
  tombstone-driven compactions instead of `map[K]*entry`. Surfaces
  `Stats.Compactions` for observability.

### Performance baseline

`testdata/benchmarks/v0.1.0-baseline.csv` records the v0.1.0
benchmark sweep on Apple M3 / arm64 / Go 1.26.x. Spec §19.6.5
latency targets met (p50 100ns / p99 500ns vs spec 200ns / 2µs).
The "≥50% of sync.Map throughput" gate is documented as
unachievable for a feature-rich cache (sync.Map's bare lookup
runs in ~2.8 ns/op; memcache pays 15–20 ns of irreducible per-Get
work for TTL/policy/hooks/observability even with the lock-free
path enabled). `WithLockFreeRead()` reduces Get from ~58 ns/op to
~21 ns/op when stats are disabled — the structural improvement
the gate intended to drive.
- **Concurrency limits**: `WithMaxConcurrentLoads(n)` semaphore;
  `WithLoaderRateLimit(perSecond)` token bucket;
  `WithLoaderTimeout(d)` per-call deadline.
- **Determinism for tests**: `WithClock(Clock)` + `NewFakeClock(...)`
  for deterministic TTL exercises without `time.Sleep`.
- **Test scaffolding**: 14 godoc `Example` tests, 7 acceptance
  scenarios (REPL warm restart, token refresh, fsnotify-style
  invalidation, 1000-goroutine stampede, daemon snapshot rotation,
  negative cache, tiered warm pool), 9 benchmarks, 4 fuzz targets,
  6 regression-corpus tests.

### Fixed

- **S3-FIFO Ghost-rebirth eviction**: a freshly re-inserted key
  returning from Ghost into Main could be selected as the Main
  victim by the same upsert's post-insert eviction loop when Small
  was saturated with `freq≥1` entries. Ghost-rebirth entries now
  start with `freq=1` so they survive at least one Main second-chance
  pass. Caught by `FuzzCacheOps/812f7169a63de001`; locked down by
  `TestCorpus_S3FIFOGhostRebirthSurvivesPostInsertEviction`.

### Known limitations (tracked for future releases)

- **Multi-segment incremental snapshots**: `WithIncrementalSave`
  (multi-segment + manifest, spec §9.8) is not implemented; the
  shipped snapshot format is single-pass.
- **`WithLockFreeRead` policy gating**: only S3-FIFO policy +
  no `WithExpireFunc` is supported; other policy/expire-func
  combinations silently disable the option (with an info log).
- **`Tiered` API surface**: covers Get/Set/SetWithTTL/SetWithOptions/
  Delete/InvalidateTag/Sync; `Compute`, `GetOrLoad`, `Range`,
  `Save`, `Subscribe` are reachable per-tier via `Tiered.L1()` /
  `Tiered.L2()` accessors. No coordinated multi-tier wrappers ship
  in v0.1.0.

[0.1.0]: https://github.com/go-rotini/memcache/releases/tag/v0.1.0
