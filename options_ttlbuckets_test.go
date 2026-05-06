// options_ttlbuckets_test.go covers the WithTTLBuckets option's
// current contract: parsed and recognized, but the wheel-backed
// TTL backend isn't wired in. The test confirms the option is
// accepted, an info log fires, and the cache continues to use the
// per-shard heap (TTL behavior is unaffected).

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

func TestWithTTLBucketsParsedButNotYetActive(t *testing.T) {
	var buf syncBufferTTL
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c, err := New[string, int](
		WithMaxEntries(8),
		WithLogger(logger),
		WithTTLBuckets(64, 4),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !strings.Contains(buf.String(), "WithTTLBuckets is recognized but not yet wired") {
		t.Errorf("expected info log about WithTTLBuckets, got: %s", buf.String())
	}
}

func TestWithTTLBucketsTTLStillWorks(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithClock(clk),
		WithTTLJitter(0),
		WithTTLBuckets(64, 4), // ignored — heap still in use
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Second)
	clk.Advance(2 * time.Second)
	if _, ok := c.Get("k"); ok {
		t.Error("entry should expire even when WithTTLBuckets is set (heap path still active)")
	}
}
