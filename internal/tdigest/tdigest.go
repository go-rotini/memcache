// Package tdigest hosts the latency-quantile estimator used by
// [memcache.Stats.LoadLatencyP50] / P99 / LoadLatency. Despite the name,
// the implementation is a fixed-bucket log-spaced histogram (24 buckets
// covering 100ns-5s). Quantiles are accurate to roughly half a bucket
// width. Out-of-range values clamp to the extremes.
package tdigest

import (
	"sync/atomic"
	"time"
)

// numBuckets is the histogram's resolution. The bucket layout is
// fixed so [Histogram] needs no allocation per Record call.
const numBuckets = 24

// boundsNs is the inclusive upper-bound (ns) for each bucket; schedule
// is roughly 1, 2, 5 times {100ns, 1us, 10us, 100us, 1ms, 10ms, 100ms,
// 1s}: 8 decades, 3 buckets per decade.
var boundsNs = [numBuckets]int64{
	100, 200, 500, // 100ns, 200ns, 500ns
	1_000, 2_000, 5_000, // 1µs, 2µs, 5µs
	10_000, 20_000, 50_000, // 10µs, 20µs, 50µs
	100_000, 200_000, 500_000, // 100µs, 200µs, 500µs
	1_000_000, 2_000_000, 5_000_000, // 1ms, 2ms, 5ms
	10_000_000, 20_000_000, 50_000_000, // 10ms, 20ms, 50ms
	100_000_000, 200_000_000, 500_000_000, // 100ms, 200ms, 500ms
	1_000_000_000, 2_000_000_000, 5_000_000_000, // 1s, 2s, 5s
}

// Histogram is a thread-safe, lock-free fixed-bucket histogram.
// Record / Snapshot / Reset are all goroutine-safe; Reset is best-
// effort consistent (a concurrent Record may land in the new state
// or the old, never split across the two).
type Histogram struct {
	counts [numBuckets]atomic.Uint64
	sumNs  atomic.Int64
	count  atomic.Uint64
}

// Record adds d to the histogram. Negative durations are dropped
// (callers shouldn't pass them, but they would skew the mean if
// included).
func (h *Histogram) Record(d time.Duration) {
	if h == nil || d < 0 {
		return
	}
	ns := int64(d)
	idx := bucketFor(ns)
	h.counts[idx].Add(1)
	h.sumNs.Add(ns)
	h.count.Add(1)
}

// Reset zeroes every bucket and the total. Concurrent Record calls
// during Reset land in either the pre- or post-reset state, never
// split.
func (h *Histogram) Reset() {
	if h == nil {
		return
	}
	for i := range h.counts {
		h.counts[i].Store(0)
	}
	h.sumNs.Store(0)
	h.count.Store(0)
}

// Snapshot is the read-side view: total count, arithmetic mean, and
// approximate quantiles.
type Snapshot struct {
	Count uint64
	Mean  time.Duration
	P50   time.Duration
	P99   time.Duration
}

// Snapshot returns the current Mean, P50, P99, and total count.
// Empty histograms produce a zero-valued Snapshot.
func (h *Histogram) Snapshot() Snapshot {
	if h == nil {
		return Snapshot{}
	}
	count := h.count.Load()
	if count == 0 {
		return Snapshot{}
	}
	sum := h.sumNs.Load()
	out := Snapshot{
		Count: count,
		Mean:  time.Duration(sum / int64(count)),
		P50:   h.quantile(count, 50),
		P99:   h.quantile(count, 99),
	}
	return out
}

// quantile walks the buckets accumulating count and returns the
// upper bound of the bucket whose cumulative count first crosses
// pct/100 of total.
func (h *Histogram) quantile(total uint64, pct int) time.Duration {
	target := total * uint64(pct) / 100
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i := range h.counts {
		cum += h.counts[i].Load()
		if cum >= target {
			return time.Duration(boundsNs[i])
		}
	}
	return time.Duration(boundsNs[numBuckets-1])
}

// bucketFor binary-searches boundsNs for the smallest bucket whose
// upper bound is >= ns. ns past the top range pegs the last bucket.
func bucketFor(ns int64) int {
	lo, hi := 0, numBuckets-1
	for lo < hi {
		mid := (lo + hi) / 2
		if boundsNs[mid] < ns {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
