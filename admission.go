package memcache

import (
	"sync"

	"github.com/go-rotini/memcache/internal/sketch"
)

// AdmissionPolicy decides whether to admit a candidate key into
// the cache. It is consulted on each [Cache.Set] BEFORE the entry
// is written; returning false drops the candidate silently — the
// cache is not modified, no event fires, and no eviction takes
// place.
//
// AdmissionPolicy is orthogonal to the eviction policy ([Policy]):
// admission decides whether a new key is worth tracking, eviction
// decides which existing key to evict when capacity is reached.
// Composing `WithPolicy(PolicyLRU) + WithAdmissionPolicy(...)` is
// the canonical way to add frequency-aware admission to any
// eviction policy.
//
// Implementations must be safe for concurrent use.
type AdmissionPolicy[K comparable] interface {
	// Admit returns true if the candidate key should be inserted
	// into the cache. Called once per [Cache.Set] before the
	// entry is written. False rejects the insert.
	Admit(key K) bool

	// Observe records a key access. Called by the cache on Set
	// (regardless of Admit's verdict) and on every successful
	// Get hit so that frequency-tracking implementations can
	// build their picture of the workload.
	Observe(key K)

	// Reset clears the policy's internal state. Called by the
	// cache during [Cache.Reset] / [Cache.Clear] so the policy
	// re-learns from scratch.
	Reset()
}

// AdmitAlways accepts every candidate. It is the package's default
// when no [WithAdmissionPolicy] or [WithDoorkeeper] option is
// supplied — Set succeeds for every key regardless of frequency.
type AdmitAlways[K comparable] struct{}

// Admit always returns true.
func (AdmitAlways[K]) Admit(K) bool { return true }

// Observe is a no-op.
func (AdmitAlways[K]) Observe(K) {}

// Reset is a no-op.
func (AdmitAlways[K]) Reset() {}

// Doorkeeper is the canonical "seen-twice" admission filter. It is
// backed by a bloom filter sized for the configured expected key
// count. A candidate is admitted only after [Doorkeeper.Observe]
// has been called for it at least once previously — single-access
// keys never make it past the gate, eliminating the most common
// source of LRU pollution in workloads with long-tailed key
// distributions.
//
// Doorkeeper is safe for concurrent use; reads and writes to the
// underlying bloom are serialized through an internal mutex.
type Doorkeeper[K comparable] struct {
	mu     sync.Mutex
	bloom  *sketch.Bloom
	hasher func(K) uint64
}

// NewDoorkeeper builds a doorkeeper sized for `expected` distinct
// keys, hashing through fn. The default cache hasher (resolved
// from [WithHasher]) is a sensible choice; passing a different
// hash makes the doorkeeper independent of the cache's collision
// behavior.
func NewDoorkeeper[K comparable](expected int, fn func(K) uint64) *Doorkeeper[K] {
	return &Doorkeeper[K]{
		bloom:  sketch.NewBloom(expected, []uint64{0xC0DECAFE, 0xBEEFFACE, 0xDEADBEEF, 0xFEEDFACE}),
		hasher: fn,
	}
}

// Admit reports whether the doorkeeper has previously observed
// key.
func (d *Doorkeeper[K]) Admit(key K) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bloom.Test(d.hasher(key))
}

// Observe records key in the bloom so subsequent Admit calls
// return true for the same key (until [Doorkeeper.Reset]).
func (d *Doorkeeper[K]) Observe(key K) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bloom.Set(d.hasher(key))
}

// Reset clears the bloom so every key starts fresh.
func (d *Doorkeeper[K]) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bloom.Reset()
}

// WithAdmissionPolicy installs a custom admission policy. Composes
// freely with [WithPolicy] — admission decides "should this key
// be tracked", eviction decides "which tracked key gets dropped
// next".
func WithAdmissionPolicy[K comparable](p AdmissionPolicy[K]) Option {
	return func(c *config) {
		if p != nil {
			c.admissionPolicy = p
		}
	}
}

// WithDoorkeeper enables a default-sized [Doorkeeper] sized for
// the cache's max-entries budget (or 1024 entries when unbounded).
// Equivalent to constructing a [Doorkeeper] with [NewDoorkeeper]
// and passing it to [WithAdmissionPolicy], but defers hasher
// resolution to cache-construction time.
//
// Pass false to opt back out (useful for clearing an admission
// policy from a chained option list).
func WithDoorkeeper(b bool) Option {
	return func(c *config) { c.doorkeeperEnabled = b }
}

// resolveAdmissionPolicy returns the typed admission policy from
// cfg, or AdmitAlways[K]{} when none was supplied. Doorkeeper is
// constructed here so it can pick up the cache's resolved hasher.
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
