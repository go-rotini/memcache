package memcache

import (
	"reflect"
	"time"
)

// safeKeysCheck logs a warning at [New] time when K is a pointer or a
// struct containing pointer fields. Runs once per cache.
func safeKeysCheck[K comparable](cfg *config) {
	if !cfg.safeKeys || cfg.logger == nil {
		return
	}
	var zero K
	t := reflect.TypeOf(zero)
	if t == nil {
		return
	}
	switch {
	case t.Kind() == reflect.Pointer:
		cfg.logger.Warn("memcache: WithSafeKeys: K is a pointer type, compares by identity not contents",
			"type", t.String())
	case t.Kind() == reflect.Struct && structContainsPointer(t):
		cfg.logger.Warn("memcache: WithSafeKeys: K is a struct containing pointer fields, equality is fragile",
			"type", t.String())
	}
}

func structContainsPointer(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	for _, f := range reflect.VisibleFields(t) {
		switch f.Type.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
			return true
		case reflect.Struct:
			if structContainsPointer(f.Type) {
				return true
			}
		}
	}
	return false
}

// WithSafeKeys enables a construction-time check that warns when K is a
// pointer or contains pointer fields. Off by default.
func WithSafeKeys(b bool) Option {
	return func(c *config) { c.safeKeys = b }
}

// WithCallbackTimeout bounds synchronous-hook duration before a warning
// is logged. Best-effort: Go cannot preempt the callback. d <= 0
// disables; default 0.
func WithCallbackTimeout(d time.Duration) Option {
	return func(c *config) { c.callbackTimeout = d }
}

// WithPurgeVisitor registers fn called for every entry during
// [Cache.Clear] and [Cache.Close]. Called outside any shard lock; may
// block, do I/O, or re-enter the cache. Errors are logged.
func WithPurgeVisitor[K comparable, V any](fn func(key K, value V) error) Option {
	return func(c *config) {
		if fn != nil {
			c.purgeVisitor = fn
		}
	}
}

// WithCopyOnGet installs fn applied to V on every successful Get/Peek
// return; callers receive fn(value). fn is called under the shard's
// read lock and MUST be fast and side-effect-free.
func WithCopyOnGet[V any](fn func(V) V) Option {
	return func(c *config) {
		if fn != nil {
			c.copyOnGet = fn
		}
	}
}
