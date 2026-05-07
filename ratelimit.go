package memcache

import (
	"sync"
	"time"
)

// rateLimiter is a token bucket used by [WithLoaderRateLimit]. Capacity
// equals the per-second refill rate. Allow consumes a token if available
// and returns false without blocking otherwise. Safe for concurrent use.
type rateLimiter struct {
	mu       sync.Mutex
	clock    Clock
	rate     float64   // tokens per second
	capacity float64   // bucket size, in tokens
	tokens   float64   // current bucket fill
	last     time.Time // last update timestamp
}

// newRateLimiter returns a limiter or nil when perSecond <= 0 ("no
// limiter configured"). clock controls refill scheduling.
func newRateLimiter(perSecond int, clock Clock) *rateLimiter {
	if perSecond <= 0 {
		return nil
	}
	if clock == nil {
		clock = RealClock{}
	}
	r := float64(perSecond)
	return &rateLimiter{
		clock:    clock,
		rate:     r,
		capacity: r,
		tokens:   r, // bucket starts full so the first burst is allowed
		last:     clock.Now(),
	}
}

// Allow consumes one token if available, otherwise returns false. Does
// NOT block and does NOT consult any caller context.
func (l *rateLimiter) Allow() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock.Now()
	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens = minFloat(l.capacity, l.tokens+elapsed*l.rate)
		l.last = now
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
