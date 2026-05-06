package memcache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestComputeStoreOnAbsent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	v, err := c.Compute("k", func(cur int, ok bool) (int, ComputeAction, error) {
		if ok {
			t.Error("expected absent")
		}
		return 42, ComputeStore, nil
	})
	if err != nil || v != 42 {
		t.Fatalf("Compute = (%d, %v), want (42, nil)", v, err)
	}
	got, _ := c.Get("k")
	if got != 42 {
		t.Errorf("Get after Compute = %d, want 42", got)
	}
}

func TestComputeStoreOnPresent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	v, err := c.Compute("k", func(cur int, ok bool) (int, ComputeAction, error) {
		if !ok || cur != 1 {
			t.Errorf("Compute saw cur=%d ok=%v, want 1, true", cur, ok)
		}
		return cur + 100, ComputeStore, nil
	})
	if err != nil || v != 101 {
		t.Errorf("Compute = (%d, %v), want (101, nil)", v, err)
	}
}

func TestComputeDelete(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	_, err := c.Compute("k", func(int, bool) (int, ComputeAction, error) {
		return 0, ComputeDelete, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.Has("k") {
		t.Error("Compute(Delete) should remove the entry")
	}
	st := c.Stats()
	if got := st.EvictionsByReason[EvictReasonComputed]; got != 1 {
		t.Errorf("EvictionsByReason[Computed] = %d, want 1", got)
	}
}

func TestComputeNoOp(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 7)
	v, err := c.Compute("k", func(cur int, _ bool) (int, ComputeAction, error) {
		return cur + 999, ComputeNoOp, nil
	})
	if err != nil || v != 7 {
		t.Errorf("Compute(NoOp) = (%d, %v), want (7, nil)", v, err)
	}
	got, _ := c.Get("k")
	if got != 7 {
		t.Errorf("entry should be unchanged after NoOp; got %d", got)
	}
}

func TestComputeFnError(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	wantErr := errors.New("boom")
	_, err := c.Compute("k", func(int, bool) (int, ComputeAction, error) {
		return 0, ComputeStore, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("Compute returned %v, want %v", err, wantErr)
	}
	if c.Has("k") {
		t.Error("Compute should NOT store when fn returns an error")
	}
}

func TestComputeNilFn(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	if _, err := c.Compute("k", nil); err == nil {
		t.Error("Compute(nil) should return an error")
	}
}

func TestComputeIfAbsent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()

	calls := 0
	v, computed, err := c.ComputeIfAbsent("k", func() (int, time.Duration, error) {
		calls++
		return 100, 0, nil
	})
	if err != nil || !computed || v != 100 || calls != 1 {
		t.Fatalf("first ComputeIfAbsent = (%d, %v, %v), calls=%d", v, computed, err, calls)
	}

	// Second call should not invoke fn.
	v, computed, err = c.ComputeIfAbsent("k", func() (int, time.Duration, error) {
		calls++
		return 999, 0, nil
	})
	if err != nil || computed || v != 100 || calls != 1 {
		t.Errorf("second ComputeIfAbsent = (%d, %v, %v), calls=%d (should still be 1)",
			v, computed, err, calls)
	}
}

func TestComputeIfAbsentRespectsTTL(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(4), WithClock(clk))
	defer c.Close()

	if _, _, err := c.ComputeIfAbsent("k", func() (int, time.Duration, error) {
		return 1, 5 * time.Second, nil
	}); err != nil {
		t.Fatalf("ComputeIfAbsent: %v", err)
	}
	d, ok := c.TTL("k")
	if !ok || d != 5*time.Second {
		t.Errorf("TTL after ComputeIfAbsent = (%v, %v), want (5s, true)", d, ok)
	}
}

func TestComputeIfAbsentNegativeTTL(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_, _, err := c.ComputeIfAbsent("k", func() (int, time.Duration, error) {
		return 1, -time.Second, nil
	})
	if !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("expected ErrInvalidTTL, got %v", err)
	}
}

func TestComputeIfPresent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()

	// Absent: fn must not be called.
	called := false
	v, err := c.ComputeIfPresent("k", func(int) (int, ComputeAction, error) {
		called = true
		return 0, ComputeStore, nil
	})
	if err != nil || v != 0 || called {
		t.Errorf("absent ComputeIfPresent = (%d, %v); called=%v", v, err, called)
	}

	_ = c.Set("k", 5)
	v, err = c.ComputeIfPresent("k", func(cur int) (int, ComputeAction, error) {
		return cur * 2, ComputeStore, nil
	})
	if err != nil || v != 10 {
		t.Fatalf("present ComputeIfPresent = (%d, %v)", v, err)
	}
}

func TestUpdate(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()

	_, err := c.Update("missing", func(cur int) int { return cur + 1 })
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Update on missing = %v, want ErrNotFound", err)
	}

	_ = c.Set("k", 5)
	v, err := c.Update("k", func(cur int) int { return cur * 3 })
	if err != nil || v != 15 {
		t.Fatalf("Update = (%d, %v)", v, err)
	}
}

func TestCompareAndSwap(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 5)

	if !c.CompareAndSwap("k", 5, 10) {
		t.Fatal("CAS with matching old should succeed")
	}
	if got, _ := c.Get("k"); got != 10 {
		t.Errorf("after CAS: Get = %d, want 10", got)
	}
	if c.CompareAndSwap("k", 5, 20) {
		t.Error("CAS with stale old should fail")
	}
	if c.CompareAndSwap("missing", 1, 2) {
		t.Error("CAS on missing key should fail")
	}
}

func TestCompareAndSwapDeepEqual(t *testing.T) {
	// Slice values: == doesn't work, but reflect.DeepEqual does.
	c, _ := New[string, []int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", []int{1, 2, 3})
	if !c.CompareAndSwap("k", []int{1, 2, 3}, []int{4, 5}) {
		t.Error("CAS with deeply-equal slices should succeed")
	}
	got, _ := c.Get("k")
	if len(got) != 2 || got[0] != 4 {
		t.Errorf("after CAS: Get = %v", got)
	}
}

func TestIncrementBy(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	v, err := IncrementBy(c, "hits", 5)
	if err != nil || v != 5 {
		t.Fatalf("IncrementBy(absent, +5) = (%d, %v), want (5, nil)", v, err)
	}
	v, err = IncrementBy(c, "hits", 3)
	if err != nil || v != 8 {
		t.Fatalf("IncrementBy(=5, +3) = (%d, %v)", v, err)
	}
}

func TestIncrementDecrement(t *testing.T) {
	c, _ := New[string, int64](WithMaxEntries(4))
	defer c.Close()
	v, _ := Increment(c, "k")
	if v != 1 {
		t.Errorf("Increment from zero = %d, want 1", v)
	}
	_, _ = Increment(c, "k")
	v, _ = Decrement(c, "k")
	if v != 1 {
		t.Errorf("after Increment x2, Decrement = %d, want 1", v)
	}
}

func TestIncrementFloat(t *testing.T) {
	c, _ := New[string, float64](WithMaxEntries(4))
	defer c.Close()
	v, _ := IncrementBy(c, "rate", 2.5)
	if v != 2.5 {
		t.Errorf("IncrementBy(float, 2.5) = %v, want 2.5", v)
	}
}

func TestComputeAtomicityUnderConcurrency(t *testing.T) {
	// 100 goroutines × 100 increments must produce exactly 10000.
	c, _ := New[string, int64](WithMaxEntries(4))
	defer c.Close()
	const G = 100
	const N = 100
	var wg sync.WaitGroup
	wg.Add(G)
	for range G {
		go func() {
			defer wg.Done()
			for range N {
				_, _ = Increment(c, "counter")
			}
		}()
	}
	wg.Wait()
	got, _ := c.Get("counter")
	if got != G*N {
		t.Errorf("counter = %d, want %d", got, G*N)
	}
}

func TestComputeOnExpiredEntryTreatsAsAbsent(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(4), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Second)
	clk.Advance(2 * time.Second)
	called := atomic.Bool{}
	v, err := c.Compute("k", func(_ int, ok bool) (int, ComputeAction, error) {
		called.Store(true)
		if ok {
			t.Error("expired entry should look absent to Compute")
		}
		return 99, ComputeStore, nil
	})
	if !called.Load() || err != nil || v != 99 {
		t.Errorf("Compute on expired = (%d, %v); called=%v", v, err, called.Load())
	}
}

func TestComputeReentrancyPanic(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on re-entrant Compute")
		}
		err, ok := r.(error)
		if !ok || !errors.Is(err, ErrComputeReentrant) {
			t.Errorf("panic value = %v, want ErrComputeReentrant", r)
		}
	}()
	_, _ = c.Compute("k", func(int, bool) (int, ComputeAction, error) {
		// Re-enter the cache from within the callback.
		_, _ = c.Compute("other", func(int, bool) (int, ComputeAction, error) {
			return 0, ComputeNoOp, nil
		})
		return 0, ComputeNoOp, nil
	})
	t.Fatal("Compute should have panicked")
}

func TestComputeReentrancyDeferredCleanup(t *testing.T) {
	// After a Compute exits cleanly, subsequent Computes from the
	// same goroutine MUST work — confirms the registry properly
	// removes the goroutine on normal exit.
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	for i := range 3 {
		_, err := c.Compute(itoaSimple(i), func(int, bool) (int, ComputeAction, error) {
			return i, ComputeStore, nil
		})
		if err != nil {
			t.Fatalf("Compute %d: %v", i, err)
		}
	}
}

func TestComputeReentrancyAcrossGoroutinesAllowed(t *testing.T) {
	// Re-entrancy detection is per-goroutine — concurrent Computes
	// from distinct goroutines must NOT trip the guard.
	c, _ := New[string, int](WithMaxEntries(64))
	defer c.Close()
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			_, _ = c.Compute(itoaSimple(i), func(int, bool) (int, ComputeAction, error) {
				return i, ComputeStore, nil
			})
		})
	}
	wg.Wait()
}

func TestComputeClosedReturnsErrClosed(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	_ = c.Close()
	_, err := c.Compute("k", func(int, bool) (int, ComputeAction, error) {
		return 0, ComputeNoOp, nil
	})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Compute on closed cache = %v, want ErrClosed", err)
	}
}
