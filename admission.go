package memcache

import (
	"sync"

	"github.com/go-rotini/memcache/internal/sketch"
)

// AdmissionPolicy decides whether to admit a candidate key into the cache.
// It is consulted whenever a write would insert a key that is not yet
// cached; updates to existing keys bypass it. Returning false drops the
// write silently. Implementations must be safe for concurrent use.
type AdmissionPolicy[K comparable] interface {
	// Admit returns true if the candidate key should be inserted into
	// the cache.
	Admit(key K) bool

	// Observe records a key access. Called after every Admit
	// (regardless of its verdict) and on Get hits.
	Observe(key K)

	// Reset clears the policy's internal state.
	Reset()
}

// AdmitAlways is an [AdmissionPolicy] that accepts every candidate. It is
// the package default when no admission option is supplied.
type AdmitAlways[K comparable] struct{}

// Admit always returns true.
func (AdmitAlways[K]) Admit(K) bool { return true }

// Observe is a no-op.
func (AdmitAlways[K]) Observe(K) {}

// Reset is a no-op.
func (AdmitAlways[K]) Reset() {}

// Doorkeeper is a "seen-twice" admission filter backed by a bloom filter.
// A candidate is admitted only after [Doorkeeper.Observe] has been called
// for it at least once previously. The filter is cleared each time it has
// recorded its expected number of distinct keys, so it keeps rejecting
// one-hit keys rather than filling up. Doorkeeper is safe for concurrent
// use.
type Doorkeeper[K comparable] struct {
	mu       sync.Mutex
	bloom    *sketch.Bloom
	hasher   func(K) uint64
	expected int
	added    int
}

// NewDoorkeeper builds a [Doorkeeper] sized for expected distinct keys,
// hashing through fn.
func NewDoorkeeper[K comparable](expected int, fn func(K) uint64) *Doorkeeper[K] {
	return &Doorkeeper[K]{
		bloom:    sketch.NewBloom(expected, []uint64{0xC0DECAFE, 0xBEEFFACE, 0xDEADBEEF, 0xFEEDFACE}),
		hasher:   fn,
		expected: max(expected, 1),
	}
}

// Admit reports whether the doorkeeper has previously observed key.
func (d *Doorkeeper[K]) Admit(key K) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bloom.Test(d.hasher(key))
}

// Observe records key so subsequent Admit calls return true for the same
// key, until [Doorkeeper.Reset] or until the filter has recorded its
// expected number of distinct keys and clears itself.
func (d *Doorkeeper[K]) Observe(key K) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h := d.hasher(key)
	if d.bloom.Test(h) {
		return
	}
	if d.added >= d.expected {
		d.bloom.Reset()
		d.added = 0
	}
	d.bloom.Set(h)
	d.added++
}

// Reset clears the bloom so every key starts fresh.
func (d *Doorkeeper[K]) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bloom.Reset()
	d.added = 0
}

// WithAdmissionPolicy installs a custom [AdmissionPolicy]. Composes freely
// with [WithPolicy]: admission decides whether a key is tracked, eviction
// decides which tracked key is dropped.
func WithAdmissionPolicy[K comparable](p AdmissionPolicy[K]) Option {
	return func(c *config) {
		if p != nil {
			c.admissionPolicy = p
		}
	}
}

// WithDoorkeeper enables a [Doorkeeper] sized for the cache's
// [WithMaxEntries] bound (or 1024 keys when there is none). It is
// ignored when [WithAdmissionPolicy] is also set.
func WithDoorkeeper(b bool) Option {
	return func(c *config) { c.doorkeeperEnabled = b }
}

func resolveAdmissionPolicy[K comparable](cfg *config, hasher func(K) uint64) (AdmissionPolicy[K], error) {
	if cfg.admissionPolicy != nil {
		p, ok := cfg.admissionPolicy.(AdmissionPolicy[K])
		if !ok {
			return nil, &ConfigError{
				Field:   "AdmissionPolicy",
				Message: "type mismatch: admission policy does not match cache key type",
			}
		}
		return p, nil
	}
	if cfg.doorkeeperEnabled {
		expected := cfg.maxEntries
		if expected <= 0 {
			expected = 1024
		}
		return NewDoorkeeper[K](expected, hasher), nil
	}
	return AdmitAlways[K]{}, nil
}
