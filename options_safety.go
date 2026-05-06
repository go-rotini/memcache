package memcache

import (
	"reflect"
	"time"
)

// safeKeysCheck inspects K's reflect.Type at construction time.
// When [WithSafeKeys] is on, it logs a warning via cfg.logger for
// patterns that almost always indicate user error: pointer key
// types (compare by identity, not contents), and structs that
// contain pointer fields (mutable equality is fragile across
// snapshots).
//
// The check has zero runtime cost — it runs once during New and
// emits at most one warning per cache.
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
		cfg.logger.Warn("memcache: WithSafeKeys: K is a pointer type — compares by identity, not contents",
			"type", t.String())
	case t.Kind() == reflect.Struct && structContainsPointer(t):
		cfg.logger.Warn("memcache: WithSafeKeys: K is a struct containing pointer fields — equality is fragile",
			"type", t.String())
	}
}

// structContainsPointer recursively reports whether t (a struct)
// has any pointer-typed field at any depth.
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

// WithSafeKeys enables construction-time validation of the cache's
// key type. When K is a pointer or contains pointer fields the
// constructor logs a warning via the configured [WithLogger]. The
// option is opt-in; default is off so legitimate pointer keys
// (e.g., `*atomic.Value` for sentinel singletons) don't spam logs.
func WithSafeKeys(b bool) Option {
	return func(c *config) { c.safeKeys = b }
}

// WithCallbackTimeout bounds how long synchronous hooks (OnHit /
// OnMiss / OnEvict / OnExpire / OnLoad / OnEvent / PurgeVisitor)
// may run before the cache logs a warning. The watchdog is
// best-effort: Go has no goroutine cancellation, so the callback
// continues to run after the warning is emitted. Setting d <= 0
// disables the watchdog (callbacks may block indefinitely);
// default is 0.
func WithCallbackTimeout(d time.Duration) Option {
	return func(c *config) { c.callbackTimeout = d }
}

// WithPurgeVisitor registers a function called for every entry
// during [Cache.Clear] and [Cache.Close]. Distinct from
// [WithOnEvict] — purge visitors fire exactly once per entry
// during whole-cache teardown, on the path to draining state.
//
// The visitor is called outside any shard lock, so it may block,
// perform I/O, or call back into the cache (no re-entry detection
// is applied to purge — Clear has already snapshotted the entries).
//
// Errors returned by the visitor are logged and otherwise ignored;
// the purge proceeds.
func WithPurgeVisitor[K comparable, V any](fn func(key K, value V) error) Option {
	return func(c *config) {
		if fn != nil {
			c.purgeVisitor = fn
		}
	}
}

// WithCopyOnGet installs a copy function applied to V on every
// successful [Cache.Get] / [Cache.Peek] return. The cached value
// is preserved as-is; callers receive the result of fn(value), so
// they can mutate it without affecting the cache. Useful when V is
// a slice, map, or struct containing pointers and callers can't be
// trusted to copy-on-read.
//
// fn must be deterministic and side-effect-free. It is called
// under the shard's read lock on the Get path; expensive copy
// implementations will serialize concurrent reads of the same key.
func WithCopyOnGet[V any](fn func(V) V) Option {
	return func(c *config) {
		if fn != nil {
			c.copyOnGet = fn
		}
	}
}
