package memcache

import (
	"sync"
	"time"
)

// rateLimiter is a simple token-bucket rate limiter used by
// [WithLoaderRateLimit]. Tokens accrue at the configured rate
// (tokens/second) up to the bucket's capacity, which equals the
// configured rate (so the bucket holds at most one second's worth
// of work).
//
// Semantics: Allow returns true and consumes a token when one is
// available; otherwise it returns false WITHOUT blocking. This
// matches the spec's "excess callers receive ErrLoaderRateLimited
// rather than blocking" contract.
//
// rateLimiter is safe for concurrent use.
type rateLimiter struct {
	mu       sync.Mutex
	clock    Clock
	rate     float64   // tokens per second
	capacity float64   // bucket size, in tokens
	tokens   float64   // current bucket fill
	last     time.Time // last update timestamp
}

// newRateLimiter constructs a steady-rate limiter. perSecond is
// the steady refill rate AND the bucket capacity. clock controls
// the refill schedule; pass [RealClock] for production and a
// [FakeClock] in tests so [FakeClock.Advance] drives accrual.
//
// Returns nil when perSecond <= 0 — the convention "no limiter
// configured" stays nil for cheap nil-checks on the hot path.
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

// Allow returns true and consumes one token when the bucket has
// at least one. Returns false otherwise; the caller's context is
// NOT consulted (the limiter rejects synchronously, matching
// spec §5.9).
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

// minFloat returns the smaller of a, b. Tiny helper to keep the
// limiter code free of math/min imports.
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
