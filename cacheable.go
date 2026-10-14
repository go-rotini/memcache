package memcache

import (
	"reflect"
	"time"
)

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
// time. [Cache.Set] and [Cache.SetCtx] use the returned duration in place
// of [WithDefaultTTL]; other write methods ignore it. Returning 0 means no
// expiry; a negative duration falls back to the cache default.
type CacheTTLer interface {
	CacheTTL() time.Duration
}

// CacheTagger is implemented by values that tag themselves at insert
// time. [Cache.Set] and the other value-only writes ([Cache.SetWithTTL],
// [Cache.SetIfAbsent], [Cache.GetOrSet], ...) tag the entry with the
// returned tags plus any cache struct-tag template tags, replacing the
// entry's previous tags. [Cache.SetWithTags], or [Cache.SetWithOptions]
// with [SetTags], uses the explicit tags instead.
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

// valueTraits records, per cache, which of the value hooks Set consults
// V can have, so a V with none of them skips the checks. dynamic is set
// when V is an interface type, whose values can have any of them.
type valueTraits struct {
	dynamic   bool
	ttler     bool
	tagger    bool
	templates bool
}

var (
	cacheTTLerType  = reflect.TypeFor[CacheTTLer]()
	cacheTaggerType = reflect.TypeFor[CacheTagger]()
)

// valueTraitsOf works out V's [valueTraits].
func valueTraitsOf[V any]() valueTraits {
	t := reflect.TypeFor[V]()
	if t.Kind() == reflect.Interface {
		return valueTraits{dynamic: true}
	}
	pt := reflect.PointerTo(t)
	return valueTraits{
		ttler:     t.Implements(cacheTTLerType) || pt.Implements(cacheTTLerType),
		tagger:    t.Implements(cacheTaggerType) || pt.Implements(cacheTaggerType),
		templates: metaFor(t).hasTemplates,
	}
}

// valueTTL is [extractCacheableTTL] for the cache's V.
func (c *Cache[K, V]) valueTTL(value V, fallback time.Duration) time.Duration {
	if !c.traits.dynamic && !c.traits.ttler {
		return fallback
	}
	return extractCacheableTTL(value, fallback)
}

// autoTags is [deriveAutoTags] for the cache's V.
func (c *Cache[K, V]) autoTags(value V) []string {
	if !c.traits.dynamic && !c.traits.tagger && !c.traits.templates {
		return nil
	}
	return deriveAutoTags(value)
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
// proto.CacheKey(). It returns the zero V and false on a miss.
func GetCacheable[V CacheKeyer](c *Cache[string, V], proto V) (V, bool) {
	return c.Get(proto.CacheKey())
}

// DeleteCacheable removes the entry whose key equals proto.CacheKey() and
// returns true when an entry was removed.
func DeleteCacheable[V CacheKeyer](c *Cache[string, V], proto V) bool {
	return c.Delete(proto.CacheKey())
}
