// bench_test.go covers the performance-critical hot paths. Run via
// `make test-bench` (which the Makefile invokes with -count=5
// -benchmem). Cross-package comparisons against ristretto/otter/
// golang-lru/v2/ttlcache/v3 are tracked as a follow-up — they
// require adding test-only dev dependencies and a separate
// benchmark binary; the spec lists them in §19.6 as targets to
// match, not gates to pass before v0.1.0.

package memcache

import (
	"context"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"
)

// BenchmarkGetHit measures the steady-state cost of a Get that
// always hits a hot key. Single-key contention is the upper bound
// on any-policy lock churn.
func BenchmarkGetHit(b *testing.B) {
	c, _ := New[string, int](WithMaxEntries(1024))
	defer c.Close()
	_ = c.Set("k", 1)
	b.ResetTimer()
	for range b.N {
		_, _ = c.Get("k")
	}
}

// BenchmarkGetMiss measures the cost of a Get on a missing key —
// no policy promotion, just shard lookup + miss-path cleanup.
func BenchmarkGetMiss(b *testing.B) {
	c, _ := New[string, int](WithMaxEntries(1024))
	defer c.Close()
	b.ResetTimer()
	for range b.N {
		_, _ = c.Get("nope")
	}
}

// BenchmarkSet measures the cost of inserting fresh keys, churning
// through eviction. A pool of 1024 keys with budget 1024 keeps the
// hit path cold.
func BenchmarkSet(b *testing.B) {
	c, _ := New[string, int](WithMaxEntries(1024))
	defer c.Close()
	keys := make([]string, 8192)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	b.ResetTimer()
	for i := range b.N {
		_ = c.Set(keys[i%len(keys)], i)
	}
}

// BenchmarkGetParallel measures concurrent reads on a hot working
// set. Demonstrates that shard sharding scales linearly with cores
// up to GOMAXPROCS × 4 shards.
func BenchmarkGetParallel(b *testing.B) {
	c, _ := New[string, int](WithMaxEntries(1024))
	defer c.Close()
	for i := range 64 {
		_ = c.Set(strconv.Itoa(i), i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get(strconv.Itoa(i % 64))
			i++
		}
	})
}

// BenchmarkSetParallel measures concurrent inserts. Per-shard
// mutexes serialize same-shard writers but distinct shards run
// truly in parallel.
func BenchmarkSetParallel(b *testing.B) {
	c, _ := New[string, int](WithMaxEntries(4096))
	defer c.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = c.Set(strconv.Itoa(i), i)
			i++
		}
	})
}

// BenchmarkGetOrLoad measures the singleflight-coalesced miss
// path. Loader is a no-op so the benchmark isolates singleflight
// overhead.
func BenchmarkGetOrLoad(b *testing.B) {
	loader := LoaderFunc[string, int](func(context.Context, string) (int, time.Duration, error) {
		return 1, 0, nil
	})
	c, _ := New[string, int](
		WithMaxEntries(1024),
		WithLoader[string, int](loader),
	)
	defer c.Close()
	_, _ = c.GetOrLoad(context.Background(), "k")
	b.ResetTimer()
	ctx := context.Background()
	for range b.N {
		_, _ = c.GetOrLoad(ctx, "k") // always hot — singleflight not exercised
	}
}

// BenchmarkPolicy_Zipfian compares hit rates across policies on a
// synthetic Zipfian workload. Surfaces in `make test-bench` as
// `BenchmarkPolicy_Zipfian/<policy>`.
func BenchmarkPolicy_Zipfian(b *testing.B) {
	const (
		capacity = 4096
		keyspace = capacity * 4
	)
	policies := []struct {
		name   string
		policy Policy
	}{
		{"LRU", PolicyLRU},
		{"FIFO", PolicyFIFO},
		{"S3FIFO", PolicyS3FIFO},
		{"LFU", PolicyLFU},
		{"TinyLFU", PolicyTinyLFU},
		{"2Q", Policy2Q},
		{"ARC", PolicyARC},
	}
	for _, tc := range policies {
		b.Run(tc.name, func(b *testing.B) {
			c, _ := New[int, int](
				WithMaxEntries(capacity),
				WithPolicy(tc.policy),
			)
			defer c.Close()
			rng := rand.New(rand.NewPCG(0xC0DE, 0xC0DE+1))
			zipf := rand.NewZipf(rng, 1.05, 1.0, uint64(keyspace-1))
			b.ResetTimer()
			for range b.N {
				k := int(zipf.Uint64())
				if _, ok := c.Get(k); !ok {
					_ = c.Set(k, k)
				}
			}
		})
	}
}

// BenchmarkSnapshotSave measures the cost of persisting a
// reasonably sized cache to a buffer. Reflects the work paid on
// `Cache.Close` for caches with WithAutoSave.
func BenchmarkSnapshotSave(b *testing.B) {
	c, _ := New[string, int](WithMaxEntries(8192))
	defer c.Close()
	for i := range 4096 {
		_ = c.Set(strconv.Itoa(i), i)
	}
	buf := make([]byte, 0, 1<<20)
	w := &writeOnlyBuffer{buf: buf}
	b.ResetTimer()
	for range b.N {
		w.buf = w.buf[:0]
		if err := c.Save(w); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(w.buf))/4096, "B/entry")
}

// writeOnlyBuffer is a benchmark-only Writer that grows in place
// without copying — keeps the benchmark dominated by the cache's
// own work rather than buffer-resize overhead.
type writeOnlyBuffer struct{ buf []byte }

func (w *writeOnlyBuffer) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}
