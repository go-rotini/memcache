# go-rotini/memcache

A bounded, generic, thread-safe in-memory cache for long-running Go programs (REPLs, daemons, watch-mode CLIs).

Used as the default caching package for [rotini](https://github.com/go-rotini/rotini).

## Features

- Generic API: `Cache[K comparable, V any]`
- Modern eviction policies: S3-FIFO (default), LRU, LFU, TinyLFU, FIFO, ARC, 2Q
- TinyLFU admission policy composable with any eviction policy
- Per-key TTL with absolute, sliding, and refresh-ahead modes
- Stale-while-revalidate and negative caching
- Singleflight loader integration with rate limiting and per-load timeouts
- Tag-based invalidation, group capacity, prefix delete
- Snapshot/restore (gob, JSON, raw bytes, gzip-compressed, AES-256-GCM-encrypted) with atomic file replacement
- Auto-save/auto-load — warm-restart for REPL-style workloads
- Tiered (L1/L2) caching with pluggable backends
- Atomic compute family: `Compute`, `ComputeIfAbsent`, `CompareAndSwap`, `Increment`/`Decrement`
- Diagnostics: `Hottest`, `Coldest`, `Histogram`, `Items`, `Dump`
- Subscribe to invalidation events; distributed-invalidation hooks
- Comprehensive stats (hit rate, latency p50/p99, eviction breakdown, hash collisions)
- DoS protection: bounded by construction, max key/value size, snapshot size limit
- Zero runtime dependencies

## Installation

```bash
go get github.com/go-rotini/memcache
```

Requires Go 1.23 or later (Go 1.26 recommended).

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/go-rotini/memcache"
)

type Profile struct {
    Name string
    Age  int
}

func main() {
    cache, err := memcache.New[string, *Profile](
        memcache.WithMaxEntries(10_000),
        memcache.WithDefaultTTL(15*time.Minute),
        memcache.WithLoader(memcache.LoaderFunc[string, *Profile](
            func(ctx context.Context, key string) (*Profile, time.Duration, error) {
                // Fetch from upstream — called at most once per concurrent miss.
                return &Profile{Name: key, Age: 30}, 15 * time.Minute, nil
            },
        )),
    )
    if err != nil {
        panic(err)
    }
    defer cache.Close()

    // Synchronous get/set
    cache.Set("alice", &Profile{Name: "alice", Age: 30})
    if p, ok := cache.Get("alice"); ok {
        fmt.Println(p.Name)
    }

    // Loader-backed get
    ctx := context.Background()
    p, err := cache.GetOrLoad(ctx, "bob")
    if err != nil {
        panic(err)
    }
    _ = p

    // Stats
    s := cache.Stats()
    fmt.Printf("hit rate: %.2f%%\n", s.HitRate()*100)
}
```

## Choosing a Policy

| Workload | Policy |
|----------|--------|
| Mixed CLI workload (default) | `PolicyS3FIFO` |
| Compiled-once-forever (regexp, templates) | `PolicyLRU` |
| Burst-heavy with low locality | `PolicyTinyLFU` |
| Streaming / scanning | `PolicyFIFO` |
| Skewed-and-stable | `PolicyLFU` |
| Mixed recency/frequency | `PolicyARC` |

## Snapshots & Restart

```go
cache, _ := memcache.New[string, *Result](
    memcache.WithMaxEntries(10_000),
    memcache.WithDefaultTTL(15*time.Minute),
    memcache.WithAutoLoad("~/.cache/myrepl/cache.gob"),
    memcache.WithAutoSave("~/.cache/myrepl/cache.gob", 30*time.Second),
    memcache.WithAutoLoadIgnoreErrors(true),
)
```

The cache warm-starts from disk on launch and writes a final snapshot on `Close`.

## Stampede Protection

```go
cache, _ := memcache.New[string, *Token](
    memcache.WithMaxEntries(1_000),
    memcache.WithDefaultTTL(1*time.Hour),
    memcache.WithRefreshAhead(0.75),                  // refresh at 75% TTL
    memcache.WithStaleWhileRevalidate(5*time.Minute), // serve stale 5m past expiry
    memcache.WithLoader(myLoader),
)

// 1000 concurrent goroutines all calling GetOrLoad("token") will trigger
// the loader exactly once.
token, err := cache.GetOrLoad(ctx, "token")
```

## Documentation

Full API reference: [pkg.go.dev/github.com/go-rotini/memcache](https://pkg.go.dev/github.com/go-rotini/memcache)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Code of Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Security

See [SECURITY.md](SECURITY.md).

## License

MIT — see [LICENSE](LICENSE).
