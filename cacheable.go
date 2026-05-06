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
	CacheKeyer
	CacheTTLer
	CacheTagger
	SnapshotMarshaler
	SnapshotUnmarshaler
}

// CacheKeyer is implemented by values whose natural cache key is a
// deterministic function of the value itself. Pair with the
// top-level [SetCacheable], [GetCacheable], and [DeleteCacheable]
// helpers (which constrain the cache's K to string at compile time)
// to avoid repeating the key-derivation expression at every call
// site:
//
//	type User struct {
//	    ID   int
//	    Name string
//	}
//	func (u User) CacheKey() string { return fmt.Sprintf("user:%d", u.ID) }
//
//	c, _ := memcache.New[string, User](memcache.WithMaxEntries(1024))
//	memcache.SetCacheable(c, User{ID: 42, Name: "alice"})
//	got, _ := memcache.GetCacheable(c, User{ID: 42})
//
// Implementations should be deterministic — the cache assumes that
// `v.CacheKey()` produces the same string every call for equal V
// values. Returning different keys for the same V breaks the
// Get-by-prototype pattern.
type CacheKeyer interface {
	CacheKey() string
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

// SetCacheable stores v in c under the key returned by
// v.CacheKey(). Compile-time constrained to caches with `K=string`
// because `CacheKey()` returns string; non-string-keyed caches use
// [Cache.Set] with a manual key derivation.
//
// With no opts, routes through [Cache.Set] so [CacheTTLer],
// [CacheTagger], and template-tag extractors all fire as usual. With
// opts, routes through [Cache.SetWithOptions] — the caller is taking
// explicit control of TTL / tags / weight, and the value's
// CacheTTLer / CacheTagger are bypassed (matching the rest of the
// package's "explicit per-call options always win" rule).
//
// Top-level rather than a method because Go generics do not allow a
// method on `*Cache[K, V]` to introduce a tighter constraint on K
// than the receiver declared. The same idiom appears in
// [Increment] / [Decrement] / [IncrementBy].
func SetCacheable[V CacheKeyer](c *Cache[string, V], v V, opts ...SetOption) error {
	if len(opts) == 0 {
		return c.Set(v.CacheKey(), v)
	}
	return c.SetWithOptions(v.CacheKey(), v, opts...)
}

// GetCacheable looks up v in c by deriving the key from
// `proto.CacheKey()`. Useful when the caller has a partial value
// (e.g., a request payload) and wants to fetch the cached version
// without restating the key derivation. Returns the cached value
// or the proto's zero on miss.
func GetCacheable[V CacheKeyer](c *Cache[string, V], proto V) (V, bool) {
	return c.Get(proto.CacheKey())
}

// DeleteCacheable removes the entry whose key equals
// `proto.CacheKey()`. Returns true when an entry was removed.
func DeleteCacheable[V CacheKeyer](c *Cache[string, V], proto V) bool {
	return c.Delete(proto.CacheKey())
}
