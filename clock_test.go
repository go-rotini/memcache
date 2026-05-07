package memcache

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRealClockNowMovesForward(t *testing.T) {
	c := RealClock{}
	a := c.Now()
	time.Sleep(time.Millisecond)
	b := c.Now()
	if !b.After(a) {
		t.Errorf("RealClock did not advance: %v -> %v", a, b)
	}
}

func TestRealClockAfterFunc(t *testing.T) {
	c := RealClock{}
	done := make(chan struct{})
	timer := c.AfterFunc(5*time.Millisecond, func() { close(done) })
	defer timer.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("AfterFunc did not fire")
	}
}

func TestFakeClockNow(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewFakeClock(t0)
	if got := c.Now(); !got.Equal(t0) {
		t.Errorf("Now() = %v, want %v", got, t0)
	}
}

func TestFakeClockAdvance(t *testing.T) {
	c := NewFakeClock(time.Unix(1000, 0))
	c.Advance(5 * time.Second)
	got := c.Now()
	want := time.Unix(1005, 0)
	if !got.Equal(want) {
		t.Errorf("after Advance: Now() = %v, want %v", got, want)
	}

	// Advance with non-positive duration should be a no-op.
	c.Advance(-time.Second)
	c.Advance(0)
	if !c.Now().Equal(want) {
		t.Errorf("non-positive Advance should not change time")
	}
}

func TestFakeClockSetMustNotGoBackwards(t *testing.T) {
	c := NewFakeClock(time.Unix(1000, 0))
	c.Set(time.Unix(500, 0))
	if !c.Now().Equal(time.Unix(1000, 0)) {
		t.Error("FakeClock.Set should refuse to move backwards")
	}
}

func TestFakeClockAfterFuncFiresOnAdvance(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	var fired atomic.Int32
	c.AfterFunc(time.Second, func() { fired.Add(1) })

	c.Advance(500 * time.Millisecond)
	if fired.Load() != 0 {
		t.Errorf("timer fired prematurely: %d", fired.Load())
	}

	c.Advance(time.Second)
	if fired.Load() != 1 {
		t.Errorf("timer should have fired once, got %d", fired.Load())
	}

	c.Advance(time.Hour)
	if fired.Load() != 1 {
		t.Errorf("timer should not re-fire, got %d", fired.Load())
	}
}

func TestFakeClockAfterFuncOrdering(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	var order []int
	c.AfterFunc(2*time.Second, func() { order = append(order, 2) })
	c.AfterFunc(1*time.Second, func() { order = append(order, 1) })
	c.AfterFunc(3*time.Second, func() { order = append(order, 3) })

	c.Advance(5 * time.Second)
	if len(order) != 3 {
		t.Fatalf("expected 3 fires, got %d", len(order))
	}
	if order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Errorf("timers fired out of order: %v", order)
	}
}

func TestFakeClockTimerStop(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	var fired bool
	timer := c.AfterFunc(time.Second, func() { fired = true })
	if !timer.Stop() {
		t.Error("Stop on active timer should return true")
	}
	c.Advance(2 * time.Second)
	if fired {
		t.Error("stopped timer should not fire")
	}
}

func TestFakeClockTimerReset(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	var fired atomic.Int32
	timer := c.AfterFunc(time.Second, func() { fired.Add(1) })

	c.Advance(500 * time.Millisecond)
	timer.Reset(2 * time.Second) // re-arm: now fires at t=2.5s

	c.Advance(time.Second) // t=1.5s
	if fired.Load() != 0 {
		t.Errorf("timer should not have fired yet (Reset moved it later)")
	}
	c.Advance(2 * time.Second) // t=3.5s
	if fired.Load() != 1 {
		t.Errorf("timer should have fired once, got %d", fired.Load())
	}
}

func TestFakeClockConcurrent(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	done := make(chan struct{})
	go func() {
		for range 100 {
			_ = c.Now()
		}
		close(done)
	}()
	for range 100 {
		c.Advance(time.Millisecond)
	}
	<-done
}
