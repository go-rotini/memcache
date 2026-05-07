package memcache

import (
	"sort"
	"sync"
	"time"
)

// Clock is the abstraction the cache uses to read time and schedule
// callbacks. Tests use FakeClock for deterministic timing.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// AfterFunc schedules fn to run after d. The returned Timer can be
	// stopped or reset.
	AfterFunc(d time.Duration, fn func()) Timer
}

// Timer is the abstraction over time.Timer-like values returned by
// Clock.AfterFunc.
type Timer interface {
	// Stop attempts to cancel the timer. It returns true if the timer
	// was canceled before firing.
	Stop() bool

	// Reset reschedules the timer to fire after d. Returns true if the
	// timer had been active.
	Reset(d time.Duration) bool
}

// RealClock is the wall-clock implementation. Methods are safe for
// concurrent use. The zero value is usable.
type RealClock struct{}

// Now returns time.Now.
func (RealClock) Now() time.Time { return time.Now() }

// AfterFunc returns a Timer wrapping time.AfterFunc.
func (RealClock) AfterFunc(d time.Duration, fn func()) Timer {
	return realTimer{t: time.AfterFunc(d, fn)}
}

type realTimer struct{ t *time.Timer }

func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// FakeClock is a controllable Clock for tests. Calls to Advance and Set
// drive any scheduled timers whose deadline has been reached.
//
// FakeClock is safe for concurrent use.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock returns a FakeClock seeded at the given time. If t is the
// zero value, it is replaced with the Unix epoch midnight.
func NewFakeClock(t time.Time) *FakeClock {
	if t.IsZero() {
		t = time.Unix(0, 0)
	}
	return &FakeClock{now: t}
}

// Now returns the FakeClock's current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Set sets the FakeClock's current time. If t is in the past relative to
// the clock's current time, Set is a no-op (clocks do not run backwards).
// Any timers due before or at t fire in deadline order.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	if t.Before(c.now) {
		c.mu.Unlock()
		return
	}
	c.now = t
	due := c.collectDueLocked(t)
	c.mu.Unlock()
	for _, ft := range due {
		ft.fire()
	}
}

// Advance moves the FakeClock forward by d. If d is non-positive, no
// effect. Any timers due fire in deadline order.
func (c *FakeClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.Set(c.Now().Add(d))
}

// AfterFunc registers fn to fire at now+d.
func (c *FakeClock) AfterFunc(d time.Duration, fn func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft := &fakeTimer{
		clock:    c,
		deadline: c.now.Add(d),
		fn:       fn,
		active:   true,
		inList:   true,
	}
	c.timers = append(c.timers, ft)
	return ft
}

// collectDueLocked returns timers whose deadline is at or before t and
// removes them from c.timers. Caller must hold c.mu.
func (c *FakeClock) collectDueLocked(t time.Time) []*fakeTimer {
	if len(c.timers) == 0 {
		return nil
	}
	sort.SliceStable(c.timers, func(i, j int) bool {
		return c.timers[i].deadline.Before(c.timers[j].deadline)
	})
	var due []*fakeTimer
	keep := c.timers[:0]
	for _, ft := range c.timers {
		if !ft.active {
			ft.inList = false
			continue
		}
		if !ft.deadline.After(t) {
			ft.active = false
			ft.inList = false
			due = append(due, ft)
			continue
		}
		keep = append(keep, ft)
	}
	for i := len(keep); i < len(c.timers); i++ {
		c.timers[i] = nil
	}
	c.timers = keep
	return due
}

type fakeTimer struct {
	clock    *FakeClock
	deadline time.Time
	fn       func()
	active   bool
	// inList tracks whether this timer is in FakeClock.timers; Reset on
	// a fired timer must re-add it for periodic callers to work.
	inList bool
}

func (ft *fakeTimer) fire() {
	if ft.fn != nil {
		ft.fn()
	}
}

// Stop deactivates the timer. Returns true if the timer was active.
func (ft *fakeTimer) Stop() bool {
	ft.clock.mu.Lock()
	defer ft.clock.mu.Unlock()
	wasActive := ft.active
	ft.active = false
	return wasActive
}

// Reset sets a new deadline for the timer and returns true if the timer
// was active before the reset. Reset on a fired timer re-inserts it into
// the pending list, matching time.Timer.Reset semantics.
func (ft *fakeTimer) Reset(d time.Duration) bool {
	ft.clock.mu.Lock()
	defer ft.clock.mu.Unlock()
	wasActive := ft.active
	ft.deadline = ft.clock.now.Add(d)
	ft.active = true
	if !ft.inList {
		ft.clock.timers = append(ft.clock.timers, ft)
		ft.inList = true
	}
	return wasActive
}
