package tdigest

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

func TestHistogramEmpty(t *testing.T) {
	var h Histogram
	s := h.Snapshot()
	if s.Count != 0 || s.Mean != 0 || s.P50 != 0 || s.P99 != 0 {
		t.Errorf("empty histogram snapshot = %+v, want zero", s)
	}
}

func TestHistogramSingleRecord(t *testing.T) {
	var h Histogram
	h.Record(500 * time.Microsecond)
	s := h.Snapshot()
	if s.Count != 1 {
		t.Errorf("Count = %d, want 1", s.Count)
	}
	if s.Mean != 500*time.Microsecond {
		t.Errorf("Mean = %v, want 500µs", s.Mean)
	}
}

func TestHistogramBucketing(t *testing.T) {
	cases := []struct {
		input time.Duration
		want  int // bucket index
	}{
		{50 * time.Nanosecond, 0},       // < first bound
		{100 * time.Nanosecond, 0},      // == first bound
		{150 * time.Nanosecond, 1},      // 200ns bucket
		{1 * time.Microsecond, 3},       // 1µs bucket
		{1500 * time.Microsecond, 13},   // 2ms bucket (1.5ms > 1ms bound)
		{1 * time.Hour, numBuckets - 1}, // way past range → top
	}
	for _, tc := range cases {
		got := bucketFor(int64(tc.input))
		if got != tc.want {
			t.Errorf("bucketFor(%v) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestHistogramQuantilesUniform(t *testing.T) {
	var h Histogram
	for range 100 {
		h.Record(1 * time.Microsecond)
	}
	s := h.Snapshot()
	if s.P50 != time.Microsecond {
		t.Errorf("P50 = %v, want 1µs", s.P50)
	}
	if s.P99 != time.Microsecond {
		t.Errorf("P99 = %v, want 1µs", s.P99)
	}
}

func TestHistogramQuantilesSkewed(t *testing.T) {
	var h Histogram
	// 90 fast loads, 10 slow loads; P99 must include the tail.
	for range 90 {
		h.Record(1 * time.Microsecond)
	}
	for range 10 {
		h.Record(500 * time.Millisecond)
	}
	s := h.Snapshot()
	if s.P50 != time.Microsecond {
		t.Errorf("P50 = %v, want 1µs", s.P50)
	}
	if s.P99 < 100*time.Millisecond {
		t.Errorf("P99 = %v, want >= 100ms (skewed by tail)", s.P99)
	}
}

func TestHistogramReset(t *testing.T) {
	var h Histogram
	h.Record(1 * time.Microsecond)
	h.Record(1 * time.Millisecond)
	h.Reset()
	s := h.Snapshot()
	if s.Count != 0 {
		t.Errorf("after Reset Count = %d, want 0", s.Count)
	}
}

func TestHistogramConcurrentRecord(t *testing.T) {
	var h Histogram
	const G, N = 16, 1000
	var wg sync.WaitGroup
	for range G {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(0xDEAD, 0xBEEF))
			for range N {
				h.Record(time.Duration(rng.Int64N(1_000_000)) * time.Nanosecond)
			}
		})
	}
	wg.Wait()
	s := h.Snapshot()
	if s.Count != G*N {
		t.Errorf("after concurrent Record Count = %d, want %d", s.Count, G*N)
	}
}

func TestHistogramNilSafe(t *testing.T) {
	var h *Histogram
	h.Record(time.Microsecond) // must not panic
	h.Reset()                  // must not panic
	if got := h.Snapshot(); got.Count != 0 {
		t.Errorf("nil histogram Snapshot = %+v, want zero", got)
	}
}

func TestHistogramNegativeDurationDropped(t *testing.T) {
	var h Histogram
	h.Record(-1 * time.Millisecond)
	if got := h.Snapshot().Count; got != 0 {
		t.Errorf("negative-duration record was kept: Count = %d", got)
	}
}
