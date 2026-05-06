package memcache

import "time"

// Cacheable is the umbrella interface for V types that want to
// participate in cache-specific behaviors. Every method is
// independently optional — concrete types may implement any
// subset; the cache uses individual type assertions against the
// sub-interfaces ([CacheTTLer], [CacheTagger], [SnapshotMarshaler],
// [SnapshotUnmarshaler]) rather than requiring all-or-nothing
// implementation. Cacheable itself is mostly documentation; users
// rarely need to type-assert against the umbrella.
//
// The pattern mirrors stdlib's split between an interface like
// `encoding.TextMarshaler` and the broader idea of "things you
// can marshal" — a concrete type only needs to implement what it
// wants to override.
type Cacheable interface {
	CacheTTLer
	CacheTagger
	SnapshotMarshaler
	SnapshotUnmarshaler
}

// CacheTTLer is implemented by values that supply their own TTL
// at insert time. When the value's type implements this method,
// [Cache.Set] uses the returned duration in place of the cache's
// configured [WithDefaultTTL]. [Cache.SetWithTTL] and
// [Cache.SetWithOptions] still take precedence — explicit TTLs
// always win over the value's preference.
//
// Returning 0 means "no expiry"; returning a negative duration is
// treated as "fall back to the cache default".
type CacheTTLer interface {
	CacheTTL() time.Duration
}

// CacheTagger is implemented by values that auto-tag themselves at
// insert time. [Cache.Set] merges the returned tags into the
// entry's tag set; [Cache.SetWithTags] and [Cache.SetWithOptions]
// override (the explicit tag list wins, the auto-tags are
// dropped).
//
// Returning a nil or empty slice is the same as not implementing
// the interface — the entry stays untagged.
type CacheTagger interface {
	CacheTags() []string
}

// SnapshotMarshaler is implemented by values with custom snapshot
// encoding. The cache calls SnapshotMarshal in place of the
// configured [Codec] for every value that implements it during
// [Cache.Save] / [Cache.SaveFile]. Use this when the value's
// in-memory representation differs from its on-disk form (e.g.,
// caching a `*sql.DB` that should snapshot as just its DSN).
type SnapshotMarshaler interface {
	SnapshotMarshal() ([]byte, error)
}

// SnapshotUnmarshaler is the receive-side counterpart. The cache
// calls SnapshotUnmarshal during [Cache.Load] / [Cache.LoadFile]
// for every value whose pointer implements it.
//
// Implementations almost always have a pointer receiver because
// the method must mutate the value. [Cache.Load] passes a pointer
// to the freshly-decoded entry's value field, so types like
//
//	type User struct{ ... }
//	func (u *User) SnapshotUnmarshal(b []byte) error { ... }
//
// work correctly when the cache's V is `User` (the cache
// dereferences *User during the assertion).
type SnapshotUnmarshaler interface {
	SnapshotUnmarshal(data []byte) error
}

// extractCacheableTTL returns the TTL from a value that
// implements [CacheTTLer], or fallback when it doesn't. Negative
// returns from CacheTTL fall back to the cache default.
func extractCacheableTTL[V any](value V, fallback time.Duration) time.Duration {
	if t, ok := any(value).(CacheTTLer); ok {
		ttl := t.CacheTTL()
		if ttl < 0 {
			return fallback
		}
		return ttl
	}
	if t, ok := any(&value).(CacheTTLer); ok {
		ttl := t.CacheTTL()
		if ttl < 0 {
			return fallback
		}
		return ttl
	}
	return fallback
}

// extractCacheableTags returns the auto-tags from a value that
// implements [CacheTagger], or nil when it doesn't.
func extractCacheableTags[V any](value V) []string {
	if t, ok := any(value).(CacheTagger); ok {
		return t.CacheTags()
	}
	if t, ok := any(&value).(CacheTagger); ok {
		return t.CacheTags()
	}
	return nil
}
