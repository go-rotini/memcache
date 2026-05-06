package memcache

import (
	"bytes"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// syncBuffer wraps bytes.Buffer with a mutex so test loggers can be
// read from one goroutine while a watchdog writes from another
// without tripping -race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func TestWithSafeKeys_PointerKeyEmitsWarning(t *testing.T) {
	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	type ptrKey *int
	c, err := New[ptrKey, string](
		WithMaxEntries(8),
		WithLogger(logger),
		WithSafeKeys(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !strings.Contains(buf.String(), "K is a pointer type") {
		t.Errorf("expected pointer-key warning, got: %s", buf.String())
	}
}

func TestWithSafeKeys_DefaultOff(t *testing.T) {
	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	type ptrKey *int
	c, _ := New[ptrKey, string](
		WithMaxEntries(8),
		WithLogger(logger),
		// WithSafeKeys NOT set
	)
	defer c.Close()
	if buf.Len() != 0 {
		t.Errorf("expected silence by default; got: %s", buf.String())
	}
}

func TestWithCallbackTimeout_LogsWhenHookExceeds(t *testing.T) {
	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLogger(logger),
		WithCallbackTimeout(20*time.Millisecond),
		WithOnHit[string, int](func(string, int) {
			time.Sleep(80 * time.Millisecond)
		}),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	_, _ = c.Get("k")
	// Give the watchdog goroutine time to fire.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "exceeded WithCallbackTimeout") {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("expected watchdog warning, got: %s", buf.String())
}

func TestWithCallbackTimeout_NoWarningWhenWithinBudget(t *testing.T) {
	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	called := atomic.Int32{}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLogger(logger),
		WithCallbackTimeout(time.Second),
		WithOnHit[string, int](func(string, int) { called.Add(1) }),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	_, _ = c.Get("k")
	if called.Load() != 1 {
		t.Errorf("hook fired %d times", called.Load())
	}
	if strings.Contains(buf.String(), "exceeded") {
		t.Errorf("unexpected watchdog warning: %s", buf.String())
	}
}

func TestWithPurgeVisitor_FiresOnClear(t *testing.T) {
	visited := map[string]int{}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithPurgeVisitor[string, int](func(k string, v int) error {
			visited[k] = v
			return nil
		}),
	)
	defer c.Close()
	_ = c.Set("a", 1)
	_ = c.Set("b", 2)
	_ = c.Set("c", 3)
	c.Clear()
	if len(visited) != 3 {
		t.Errorf("PurgeVisitor visited %d entries, want 3 (got %v)", len(visited), visited)
	}
	if visited["a"] != 1 || visited["b"] != 2 || visited["c"] != 3 {
		t.Errorf("PurgeVisitor saw wrong values: %v", visited)
	}
}

func TestWithPurgeVisitor_FiresOnClose(t *testing.T) {
	visited := atomic.Int32{}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithPurgeVisitor[string, int](func(string, int) error {
			visited.Add(1)
			return nil
		}),
	)
	for i := range 5 {
		_ = c.Set(itoaSimple(i), i)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if visited.Load() != 5 {
		t.Errorf("PurgeVisitor fired %d times on Close, want 5", visited.Load())
	}
}

func TestWithPurgeVisitor_ErrorIsLogged(t *testing.T) {
	var buf syncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithLogger(logger),
		WithPurgeVisitor[string, int](func(string, int) error {
			return errors.New("disk full")
		}),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	c.Clear()
	if !strings.Contains(buf.String(), "disk full") {
		t.Errorf("expected error logged, got: %s", buf.String())
	}
}

func TestWithCopyOnGet_ReturnsIndependentCopy(t *testing.T) {
	c, _ := New[string, []int](
		WithMaxEntries(8),
		WithCopyOnGet[[]int](func(s []int) []int {
			out := make([]int, len(s))
			copy(out, s)
			return out
		}),
	)
	defer c.Close()
	original := []int{1, 2, 3}
	_ = c.Set("k", original)

	got, ok := c.Get("k")
	if !ok {
		t.Fatal("Get miss")
	}
	got[0] = 999 // mutate the returned slice
	got2, _ := c.Get("k")
	if got2[0] != 1 {
		t.Errorf("WithCopyOnGet leaked mutation: got2 = %v", got2)
	}
	// Cache's own value untouched too.
	if !reflect.DeepEqual(original, []int{1, 2, 3}) {
		t.Errorf("original slice mutated: %v", original)
	}
}

func TestWithCopyOnGet_NotAppliedOnPeekWhenAbsent(t *testing.T) {
	calls := atomic.Int32{}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithCopyOnGet[int](func(v int) int {
			calls.Add(1)
			return v
		}),
	)
	defer c.Close()
	_, _ = c.Peek("missing")
	if calls.Load() != 0 {
		t.Errorf("copy fn fired %d times on miss", calls.Load())
	}
	_ = c.Set("present", 42)
	_, _ = c.Peek("present")
	if calls.Load() != 1 {
		t.Errorf("copy fn fired %d times on hit, want 1", calls.Load())
	}
}
