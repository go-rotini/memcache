package memcache

import "time"

// SetOption applies a per-call override to [Cache.SetWithOptions].
// Options are applied in order; later options win for the same
// setting. The zero SetOption is nil and is silently ignored.
type SetOption func(*setConfig)

// setConfig is the internal accumulator populated by [SetOption]s.
type setConfig struct {
	ttl       time.Duration // negative ⇒ "use cache default"
	weight    int64         // 0 ⇒ "use the configured Weigher"
	tags      []string      // attached to the entry; tag-index work lands in Phase 7
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
// configured [Weigher] for this insert. Weights ≤ 0 are clamped to
// 1 by the cache's internal accounting.
func SetWeight(w int64) SetOption {
	return func(s *setConfig) {
		s.weight = w
		s.hasWeight = true
	}
}

// SetTags attaches tags to the entry. Calling SetTags (even with no
// arguments) is treated as the caller explicitly opting OUT of any
// auto-tag derivation that would otherwise happen via [CacheTagger]
// or template tags — `SetTags()` means "no tags," not "use defaults."
// Pass tag names to attach them; `SetTags("a", "b")` overrides
// `CacheTagger.CacheTags()` and any `cache:"...,tag=..."` template.
func SetTags(tags ...string) SetOption {
	return func(s *setConfig) {
		s.tags = append(s.tags[:0], tags...)
		s.hasTags = true
	}
}

// SetSliding marks the entry's TTL as sliding: every Get refreshes
// the expiry to `now + TTL`. Pass false to opt out of the cache's
// configured sliding default for this single insert.
func SetSliding(sliding bool) SetOption {
	return func(s *setConfig) { s.sliding = sliding }
}

// SetExpireAt sets an absolute expiry time, taking precedence over
// any TTL set by [SetTTL] or the cache default. The supplied time
// is wall-clock — a backwards NTP jump may temporarily un-expire
// the entry. Use [SetTTL] for monotonic-clock-safe expiry.
func SetExpireAt(t time.Time) SetOption {
	return func(s *setConfig) {
		s.expireAt = t
		s.hasExpiry = true
	}
}
