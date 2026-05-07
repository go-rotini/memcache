package memcache

import "time"

// Cacheable is the umbrella interface for V types that participate in
// cache-specific behaviors. Every method is independently optional; the
// cache type-asserts against the sub-interfaces individually.
type Cacheable interface {
	CacheKeyer
	CacheTTLer
	CacheTagger
	SnapshotMarshaler
	SnapshotUnmarshaler
}

// CacheKeyer is implemented by values whose natural cache key is a
// deterministic function of the value itself. Pair with [SetCacheable],
// [GetCacheable], and [DeleteCacheable] to avoid repeating the
// key-derivation expression at every call site. Implementations must be
// deterministic.
type CacheKeyer interface {
	CacheKey() string
}

// CacheTTLer is implemented by values that supply their own TTL at insert
// time. [Cache.Set] uses the returned duration in place of the configured
// default; explicit TTLs from [Cache.SetWithTTL] / [Cache.SetWithOptions]
// take precedence. Returning 0 means no expiry; a negative duration falls
// back to the cache default.
type CacheTTLer interface {
	CacheTTL() time.Duration
}

// CacheTagger is implemented by values that auto-tag themselves at insert
// time. [Cache.Set] merges the returned tags into the entry's tag set;
// [Cache.SetWithTags] and [Cache.SetWithOptions] override and drop
// auto-tags. A nil or empty slice leaves the entry untagged.
type CacheTagger interface {
	CacheTags() []string
}

// SnapshotMarshaler is implemented by values with custom snapshot
// encoding. The cache calls SnapshotMarshal in place of the configured
// [Codec] during [Cache.Save] / [Cache.SaveFile].
type SnapshotMarshaler interface {
	SnapshotMarshal() ([]byte, error)
}

// SnapshotUnmarshaler is the receive-side counterpart of
// [SnapshotMarshaler]. The cache calls SnapshotUnmarshal during
// [Cache.Load] / [Cache.LoadFile] for every value whose pointer
// implements it. Implementations almost always have a pointer receiver.
type SnapshotUnmarshaler interface {
	SnapshotUnmarshal(data []byte) error
}

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

func extractCacheableTags[V any](value V) []string {
	if t, ok := any(value).(CacheTagger); ok {
		return t.CacheTags()
	}
	if t, ok := any(&value).(CacheTagger); ok {
		return t.CacheTags()
	}
	return nil
}

func deriveAutoTags[V any](value V) []string {
	tags := extractCacheableTags(value)
	if tpl := extractTemplateTags(value); len(tpl) > 0 {
		tags = append(tags, tpl...)
	}
	return tags
}

// SetCacheable stores v in c under the key returned by v.CacheKey().
// Without opts, routes through [Cache.Set] so [CacheTTLer], [CacheTagger],
// and template tags fire as usual; with opts, routes through
// [Cache.SetWithOptions] and explicit options override auto-extraction.
func SetCacheable[V CacheKeyer](c *Cache[string, V], v V, opts ...SetOption) error {
	if len(opts) == 0 {
		return c.Set(v.CacheKey(), v)
	}
	return c.SetWithOptions(v.CacheKey(), v, opts...)
}

// GetCacheable looks up an entry in c by deriving the key from
// proto.CacheKey(). Returns the cached value or proto's zero on miss.
func GetCacheable[V CacheKeyer](c *Cache[string, V], proto V) (V, bool) {
	return c.Get(proto.CacheKey())
}

// DeleteCacheable removes the entry whose key equals proto.CacheKey() and
// returns true when an entry was removed.
func DeleteCacheable[V CacheKeyer](c *Cache[string, V], proto V) bool {
	return c.Delete(proto.CacheKey())
}
