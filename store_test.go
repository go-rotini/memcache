package memcache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryStoreRoundTrip(t *testing.T) {
	s := NewMemoryStore[string, int](nil)
	defer s.Close()
	ctx := context.Background()

	if _, ok, _ := s.Get(ctx, "k"); ok {
		t.Error("Get on empty store should miss")
	}
	if err := s.Set(ctx, "k", 42, 0); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.Get(ctx, "k")
	if err != nil || !ok || v != 42 {
		t.Errorf("Get = (%d, %v, %v), want (42, true, nil)", v, ok, err)
	}
}

func TestMemoryStoreTTL(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	s := NewMemoryStore[string, int](clk)
	defer s.Close()
	ctx := context.Background()
	if err := s.Set(ctx, "k", 1, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get(ctx, "k"); !ok {
		t.Fatal("expected hit before expiry")
	}
	clk.Advance(2 * time.Second)
	if _, ok, _ := s.Get(ctx, "k"); ok {
		t.Error("expected miss after expiry")
	}
}

func TestMemoryStoreDelete(t *testing.T) {
	s := NewMemoryStore[string, int](nil)
	defer s.Close()
	ctx := context.Background()
	_ = s.Set(ctx, "k", 1, 0)
	d, _ := s.Delete(ctx, "k")
	if !d {
		t.Error("Delete on present key should return true")
	}
	d, _ = s.Delete(ctx, "k")
	if d {
		t.Error("Delete on absent key should return false")
	}
}

func TestMemoryStoreIterate(t *testing.T) {
	s := NewMemoryStore[string, int](nil)
	defer s.Close()
	ctx := context.Background()
	_ = s.Set(ctx, "a", 1, 0)
	_ = s.Set(ctx, "b", 2, 0)
	_ = s.Set(ctx, "c", 3, 0)

	got := map[string]int{}
	if err := s.Iterate(ctx, func(k string, v int) bool {
		got[k] = v
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got["a"] != 1 || got["b"] != 2 || got["c"] != 3 {
		t.Errorf("Iterate result = %v", got)
	}
}

func TestMemoryStoreIterateStops(t *testing.T) {
	s := NewMemoryStore[string, int](nil)
	defer s.Close()
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c", "d"} {
		_ = s.Set(ctx, k, 0, 0)
	}
	count := 0
	_ = s.Iterate(ctx, func(string, int) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("Iterate visited %d before stop, want 2", count)
	}
}

func TestMemoryStoreLen(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	s := NewMemoryStore[string, int](clk)
	defer s.Close()
	ctx := context.Background()
	_ = s.Set(ctx, "a", 1, 0)               // no TTL
	_ = s.Set(ctx, "b", 2, time.Second)     // expires in 1s
	_ = s.Set(ctx, "c", 3, 100*time.Second) // expires later

	n, _ := s.Len(ctx)
	if n != 3 {
		t.Errorf("Len before expiry = %d, want 3", n)
	}
	clk.Advance(2 * time.Second) // b expires
	n, _ = s.Len(ctx)
	if n != 2 {
		t.Errorf("Len after partial expiry = %d, want 2", n)
	}
}

func TestMemoryStoreClose(t *testing.T) {
	s := NewMemoryStore[string, int](nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := s.Get(ctx, "k"); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
	if err := s.Set(ctx, "k", 1, 0); !errors.Is(err, ErrClosed) {
		t.Errorf("Set after Close = %v, want ErrClosed", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("double Close = %v, want nil (idempotent)", err)
	}
}

func TestMemoryStoreCtxCancel(t *testing.T) {
	s := NewMemoryStore[string, int](nil)
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.Get(ctx, "k"); err == nil {
		t.Error("Get with canceled ctx should error")
	}
	if err := s.Set(ctx, "k", 1, 0); err == nil {
		t.Error("Set with canceled ctx should error")
	}
}

func TestMemoryStoreConcurrent(t *testing.T) {
	s := NewMemoryStore[string, int](nil)
	defer s.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for j := range 200 {
				k := itoaSimple(i*1000 + j%100)
				_ = s.Set(ctx, k, j, 0)
				_, _, _ = s.Get(ctx, k)
			}
		})
	}
	wg.Wait()
}

// nilClockUsesRealClock verifies the convenience nil-Clock path
// without requiring a wall-clock-dependent assertion.
func TestMemoryStoreNilClockSafe(t *testing.T) {
	s := NewMemoryStore[string, int](nil)
	defer s.Close()
	ctx := context.Background()
	if err := s.Set(ctx, "k", 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get(ctx, "k"); !ok {
		t.Error("nil Clock should fall back to RealClock and the entry should be fresh")
	}
}
