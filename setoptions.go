package memcache

import (
	"errors"
	"time"
)

// SetOption applies a per-call override to [Cache.SetWithOptions].
// Options are applied in order; later options win for the same
// setting. The zero SetOption is nil and is silently ignored.
type SetOption func(*setConfig)

// setConfig is the internal accumulator populated by [SetOption]s.
type setConfig struct {
	ttl       time.Duration // negative ⇒ "use cache default"
	weight    int64         // 0 ⇒ "use the configured Weigher"
	tags      []string      // attached to the entry and recorded in the tag index
	expireAt  time.Time     // populated when SetExpireAt is used; overrides ttl
	sliding   bool
	hasTTL    bool
	hasWeight bool
	hasExpiry bool
	// hasTags is set by [SetTags] (even when called with an empty
	// list) so [Cache.SetWithOptions] can distinguish "caller said
	// nothing about tags" (auto-tag from CacheTagger) from "caller
	// explicitly opted out of tags."
	hasTags bool
	// guard, when set, is checked under the shard lock just before the
	// write; false drops the write with errWriteSuperseded. Used by
	// [Tiered] promotion.
	guard func() bool
}

// errWriteSuperseded reports a guarded write dropped because the data it
// carried was superseded.
var errWriteSuperseded = errors.New("memcache: write superseded")

// setGuard sets setConfig.guard.
func setGuard(fn func() bool) SetOption {
	return func(s *setConfig) { s.guard = fn }
}

// defaultSetConfig returns a setConfig pre-populated from the cache's
// default TTL/sliding behavior. Per-call options override fields
// they touch.
func defaultSetConfig(cfg *config) setConfig {
	return setConfig{
		ttl:     cfg.defaultTTL,
		sliding: cfg.slidingTTL,
	}
}

// SetTTL overrides the per-call TTL. A TTL of 0 stores the entry
// with no expiry; a negative TTL produces [ErrInvalidTTL] when the
// resulting [Cache.SetWithOptions] runs.
func SetTTL(ttl time.Duration) SetOption {
	return func(s *setConfig) {
		s.ttl = ttl
		s.hasTTL = true
	}
}

// SetWeight overrides the per-call weight, ignoring the cache's
// configured [Weigher] for this insert. Weights are clamped to the
// range 1 to 1<<40.
func SetWeight(w int64) SetOption {
	return func(s *setConfig) {
		s.weight = w
		s.hasWeight = true
	}
}

// SetTags attaches tags to the entry, replacing the tags derived from
// [CacheTagger] or a cache struct-tag template. Calling SetTags with no
// arguments stores the entry with no tags.
func SetTags(tags ...string) SetOption {
	return func(s *setConfig) {
		s.tags = append(s.tags[:0], tags...)
		s.hasTags = true
	}
}

// SetSliding marks the entry's TTL as sliding: every Get refreshes
// the expiry to now + TTL. Pass false to opt out of the cache's
// [WithSlidingTTL] default for this insert.
func SetSliding(sliding bool) SetOption {
	return func(s *setConfig) { s.sliding = sliding }
}

// SetExpireAt sets an absolute expiry time, taking precedence over
// [SetTTL], [SetSliding] and the cache defaults. A zero t stores the
// entry with no expiry.
func SetExpireAt(t time.Time) SetOption {
	return func(s *setConfig) {
		s.expireAt = t
		s.hasExpiry = true
	}
}
