package memcache

import (
	"errors"
	"fmt"
)

// Sentinel errors. Match them with [errors.Is]; several are carried as
// the Cause of a [*CapacityError] or the Err of a [*SnapshotError].
var (
	// ErrNotFound is returned by [Cache.Update] when the key is absent
	// or expired, and by [Cache.GetOrLoad] on a negative-cache hit.
	// A Loader returns it to signal a not-found result, which
	// [WithNegativeCache] caches.
	ErrNotFound = errors.New("memcache: not found")

	// ErrClosed is returned when an operation is attempted on a closed
	// cache.
	ErrClosed = errors.New("memcache: cache is closed")

	// ErrUnbounded is returned by [New] when neither [WithMaxEntries]
	// nor [WithMaxBytes] is configured.
	ErrUnbounded = errors.New("memcache: must specify WithMaxEntries or WithMaxBytes")

	// ErrNoLoader is returned by the loading methods ([Cache.GetOrLoad],
	// [Cache.Refresh], [Cache.GetMultiOrLoad]) when no [Loader] is
	// configured, and by [Cache.GetOrLoadFn] when fn is nil.
	ErrNoLoader = errors.New("memcache: no loader configured")

	// ErrInvalidTTL is returned when a write is given a negative TTL.
	ErrInvalidTTL = errors.New("memcache: ttl must be non-negative")

	// ErrSnapshotIncompatible is the Err of the [*SnapshotError] a load
	// returns when the snapshot's signature, format version, codec or
	// schema fingerprint does not match the cache.
	ErrSnapshotIncompatible = errors.New("memcache: snapshot version not supported")

	// ErrSnapshotCorrupt is the Err of the [*SnapshotError] a load
	// returns when the snapshot fails its CRC check or is otherwise
	// malformed.
	ErrSnapshotCorrupt = errors.New("memcache: snapshot corrupt")

	// ErrKeyTooLarge is the Cause of the [*CapacityError] a write
	// returns when a key exceeds [WithMaxKeySize].
	ErrKeyTooLarge = errors.New("memcache: key exceeds size limit")

	// ErrValueTooLarge is the Cause of the [*CapacityError] a write
	// returns when a value's weight exceeds [WithMaxValueWeight].
	ErrValueTooLarge = errors.New("memcache: value exceeds weight limit")

	// ErrTooManyTags is the Cause of the [*CapacityError] a write
	// returns when its tags exceed [WithMaxTagsPerEntry] or
	// [WithMaxTagsTotal].
	ErrTooManyTags = errors.New("memcache: too many tags for entry")

	// ErrLoaderRateLimited is returned by [Cache.GetOrLoad] and
	// [Cache.GetOrLoadFn] when [WithLoaderRateLimit] rejects a load.
	ErrLoaderRateLimited = errors.New("memcache: loader rate limit exceeded")

	// ErrLoaderTimeout is joined with [context.DeadlineExceeded] when a
	// Loader call exceeds [WithLoaderTimeout].
	ErrLoaderTimeout = errors.New("memcache: loader timeout exceeded")

	// ErrLoaderTooManyInFlight is returned by [Cache.GetOrLoad] when
	// [WithMaxConcurrentLoads] is reached and no slot frees up before
	// the load's context ends ([WithLoaderTimeout] expires, or every
	// waiting caller gives up).
	ErrLoaderTooManyInFlight = errors.New("memcache: too many in-flight loads")

	// ErrComputeReentrant is the panic value raised when a callback of
	// [Cache.Compute], [Cache.ComputeIfAbsent], [Cache.ComputeIfPresent]
	// or [Cache.Update] calls one of those methods again on the same
	// goroutine, which would otherwise deadlock.
	ErrComputeReentrant = errors.New("memcache: compute callback re-entered cache")
)

// ConfigError describes an option rejected by [New].
type ConfigError struct {
	Field   string
	Message string
}

// Error implements the error interface.
func (e *ConfigError) Error() string {
	if e.Field == "" {
		return "memcache: " + e.Message
	}
	return "memcache: " + e.Field + ": " + e.Message
}

// Is reports whether target is a *ConfigError.
func (e *ConfigError) Is(target error) bool {
	_, ok := target.(*ConfigError)
	return ok
}

// CapacityError describes a write rejected by a size or tag limit.
type CapacityError struct {
	Key        any
	Reason     string
	LimitField string
	// Cause is the sentinel for the limit that was hit:
	// [ErrKeyTooLarge], [ErrValueTooLarge] or [ErrTooManyTags].
	Cause error
}

// Error implements the error interface.
func (e *CapacityError) Error() string {
	return fmt.Sprintf("memcache: capacity (%s): %s", e.LimitField, e.Reason)
}

// Unwrap returns the underlying sentinel cause, if any.
func (e *CapacityError) Unwrap() error { return e.Cause }

// Is reports whether target is a *CapacityError or matches Cause.
func (e *CapacityError) Is(target error) bool {
	if _, ok := target.(*CapacityError); ok {
		return true
	}
	if e.Cause != nil && errors.Is(e.Cause, target) {
		return true
	}
	return false
}

// LoadError wraps an error returned by a Loader.
type LoadError struct {
	Key any
	Err error
}

// Error implements the error interface.
func (e *LoadError) Error() string {
	return fmt.Sprintf("memcache: load %v: %v", e.Key, e.Err)
}

// Unwrap returns the underlying error.
func (e *LoadError) Unwrap() error { return e.Err }

// Is reports whether target is a *LoadError.
func (e *LoadError) Is(target error) bool {
	_, ok := target.(*LoadError)
	return ok
}

// SnapshotError describes a snapshot save or load failure.
type SnapshotError struct {
	Op      string // "save" or "load"
	Path    string
	Offset  int64
	Message string
	Err     error
}

// Error implements the error interface.
func (e *SnapshotError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("memcache: snapshot %s %s: %s", e.Op, e.Path, e.Message)
	}
	return fmt.Sprintf("memcache: snapshot %s: %s", e.Op, e.Message)
}

// Unwrap returns the underlying error.
func (e *SnapshotError) Unwrap() error { return e.Err }

// Is reports whether target is a *SnapshotError or matches Err (for
// example [ErrSnapshotCorrupt] or [ErrSnapshotIncompatible]).
func (e *SnapshotError) Is(target error) bool {
	if _, ok := target.(*SnapshotError); ok {
		return true
	}
	return errors.Is(e.Err, target)
}

// CodecError wraps an error from a [Codec] implementation.
type CodecError struct {
	Op    string // "marshal" or "unmarshal"
	Codec string // codec name ("gob", "json", "raw", ...)
	Err   error
}

// Error implements the error interface.
func (e *CodecError) Error() string {
	return fmt.Sprintf("memcache: codec %s %s: %v", e.Codec, e.Op, e.Err)
}

// Unwrap returns the underlying error.
func (e *CodecError) Unwrap() error { return e.Err }

// Is reports whether target is a *CodecError.
func (e *CodecError) Is(target error) bool {
	_, ok := target.(*CodecError)
	return ok
}
