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
  `InvalidateAnyTag`, `InvalidateAllTags`. Tag indexes survive
  Save/Load and are rebuilt eagerly during the load.
- **Groups & bulk ops**: `WithGroup(name, budget)` enforces per-group
  budgets independent of the cache-wide budget. `GetMulti`, `SetMulti`,
  `DeleteMulti` for batched amortized-lock operations.
- **Stats, events, hooks**: `Stats()` returns per-cache counters
  (Hits, Misses, Evictions, Inserts, Updates, LoadCalls, EventsDropped,
  L2Hits when tiered). `Subscribe(buffer, kinds...)` returns a
  channel that fans out `EventInsert | EventUpdate | EventEvict |
  EventExpire | EventDelete | EventClear`. `WithOnEvict(...)`,
  `WithOnInsert(...)`, `WithOnEvent(...)` for synchronous hooks.
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
  and rejected with `ErrComputeReentry`.
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
- **Concurrency limits**: `WithMaxConcurrentLoads(n)` semaphore;
  `WithLoadRateLimit(rps, burst)` token bucket.
- **Determinism for tests**: `WithClock(Clock)` + `NewFakeClock(...)`
  for deterministic TTL exercises without `time.Sleep`.
- **Test scaffolding**: 13 godoc `Example` tests, 7 acceptance
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

- Get RLock fast path (Phase 4 deferred): hot reads currently take
  the per-shard write lock for policy promotion. A future release
  will route policy promotions through a write-batched intent log
  so Get can run under RLock when the entry is fresh.
- Snapshot codec adapters (Phase 9 deferred): `WithIncrementalSave`
  and the gzip / zstd / encrypt streaming codec adapters are not
  yet wired into Save/Load.
- External-store backend (Phase 10 deferred): `WithStore(...)` for
  Redis / S3 / disk has stub interfaces but no shipping implementation.
- Struct-tag extensions (Phase 11 deferred): `cache:"omitempty"`,
  `cache:"versioned"`, `cache:"tag=template"`, and `CacheKey`
  derivation are not yet implemented.

[0.1.0]: https://github.com/go-rotini/memcache/releases/tag/v0.1.0
