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
	priority  int8          // hint for future priority-aware policies
	sliding   bool
	hasTTL    bool
	hasWeight bool
	hasExpiry bool
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

// SetTags attaches tags to the entry. Tag-based invalidation lands
// in Phase 7; for now the tags are stored on the entry but no global
// index is maintained, so [Cache.InvalidateTag] is not yet wired.
func SetTags(tags ...string) SetOption {
	return func(s *setConfig) {
		s.tags = append(s.tags[:0], tags...)
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

// SetPriority hints to the eviction policy that this entry is more
// (or less) valuable than its raw access pattern would suggest.
// Higher values reduce eviction probability. Range -100 to +100;
// values outside the range are clamped.
//
// No v0 policy consumes priority; the value is recorded on the
// entry for forward compatibility. Future priority-aware policies
// (e.g., a weighted variant of S3-FIFO) will read it.
func SetPriority(p int) SetOption {
	return func(s *setConfig) {
		switch {
		case p > 100:
			s.priority = 100
		case p < -100:
			s.priority = -100
		default:
			s.priority = int8(p)
		}
	}
}
