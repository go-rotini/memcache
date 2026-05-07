// options_ttlbuckets_test.go covers the WithTTLBuckets option:
// the cache wires through the hashed-wheel TTL backend and TTL
// behavior matches the heap path within the wheel's tick precision.

package memcache

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBufferTTL mirrors the sync-buffer helper used elsewhere; the
// log-output capture races with the cache's own goroutines under
// -race without it.
type syncBufferTTL struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBufferTTL) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBufferTTL) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestWithTTLBucketsSelectsWheelBackend(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithTTLBuckets(64, 4),
	)
	defer c.Close()
	s := c.shardFor("any")
	if _, ok := s.ttl.(*wheelBackend[string, int]); !ok {
		t.Errorf("expected wheelBackend, got %T", s.ttl)
	}
}

func TestWithoutTTLBucketsDefaultsToHeap(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	s := c.shardFor("any")
	if _, ok := s.ttl.(*expiryHeapBackend[string, int]); !ok {
		t.Errorf("expected expiryHeapBackend, got %T", s.ttl)
	}
}

func TestWithTTLBucketsLogsActiveAtDebug(t *testing.T) {
	var buf syncBufferTTL
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLogger(logger),
		WithTTLBuckets(64, 4),
	)
	defer c.Close()
	if !strings.Contains(buf.String(), "WithTTLBuckets active") {
		t.Errorf("expected debug log about wheel backend, got: %s", buf.String())
	}
}

func TestWithTTLBucketsTTLExpires(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithClock(clk),
		WithTTLJitter(0),
		WithJanitorInterval(time.Hour),
		WithTTLBuckets(64, 4),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Second)
	// Wheel tick = janitorInterval / tickPerBucket = 1h / 4 = 15min.
	// A 1-second TTL rounds up to 1 wheel tick. Advancing past a
	// tick triggers expiry on the next sweep.
	clk.Advance(2 * time.Hour)
	// Lazy expiry on Get also fires (entry.expireAt is the
	// authoritative TTL; the wheel only schedules sweeps).
	if _, ok := c.Get("k"); ok {
		t.Error("entry should expire even when WithTTLBuckets is set")
	}
}

func TestWithTTLBucketsRemoveBeforeExpire(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithTTLJitter(0),
		WithJanitorInterval(time.Hour),
		WithTTLBuckets(32, 1),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, 10*time.Minute)
	if !c.Delete("k") {
		t.Fatal("Delete should succeed")
	}
	if c.Has("k") {
		t.Error("entry should be gone after Delete")
	}
}

func TestWithTTLBucketsUpdateRescheduleS(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithClock(clk),
		WithTTLJitter(0),
		WithJanitorInterval(time.Hour),
		WithTTLBuckets(32, 1),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Hour)
	// Re-set with a longer TTL; wheel re-tracks.
	_ = c.SetWithTTL("k", 2, 24*time.Hour)
	clk.Advance(2 * time.Hour)
	v, ok := c.Get("k")
	if !ok || v != 2 {
		t.Errorf("after extension Get = (%d, %v); want (2, true)", v, ok)
	}
}
