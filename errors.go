package memcache

import (
	"errors"
	"fmt"
)

// Sentinel errors. All errors returned by this package satisfy errors.Is
// against one of these.
var (
	// ErrNotFound is returned by Get-style methods when the key is absent
	// or expired. Loaders may also return this to signal a not-found
	// result, which the cache may negatively cache.
	ErrNotFound = errors.New("memcache: not found")

	// ErrClosed is returned when an operation is attempted on a closed
	// cache.
	ErrClosed = errors.New("memcache: cache is closed")

	// ErrUnbounded is returned by New when no size bound is configured.
	ErrUnbounded = errors.New("memcache: must specify WithMaxEntries or WithMaxBytes")

	// ErrNoLoader is returned by GetOrLoad when no Loader is configured.
	ErrNoLoader = errors.New("memcache: no loader configured")

	// ErrInvalidTTL is returned by SetWithTTL on a negative TTL.
	ErrInvalidTTL = errors.New("memcache: ttl must be non-negative")

	// ErrSnapshotIncompatible is returned by Load when the snapshot format
	// version is not understood.
	ErrSnapshotIncompatible = errors.New("memcache: snapshot version not supported")

	// ErrSnapshotCorrupt is returned by Load when the snapshot fails its
	// CRC check or is otherwise malformed.
	ErrSnapshotCorrupt = errors.New("memcache: snapshot corrupt")

	// ErrKeyTooLarge is returned by Set when a key exceeds WithMaxKeySize.
	ErrKeyTooLarge = errors.New("memcache: key exceeds size limit")

	// ErrValueTooLarge is returned by Set when a value's weight exceeds
	// WithMaxValueWeight.
	ErrValueTooLarge = errors.New("memcache: value exceeds weight limit")

	// ErrTooManyTags is returned by SetWithTags when the entry has more
	// tags than WithMaxTagsPerEntry permits.
	ErrTooManyTags = errors.New("memcache: too many tags for entry")

	// ErrPolicyConfig is returned by New when WithPolicy is incompatible
	// with other options.
	ErrPolicyConfig = errors.New("memcache: policy configuration invalid")

	// ErrLoaderRateLimited is returned by GetOrLoad when the loader rate
	// limit (WithLoaderRateLimit) has been exceeded for this tick.
	ErrLoaderRateLimited = errors.New("memcache: loader rate limit exceeded")

	// ErrLoaderTimeout is returned when a Loader exceeds the per-call
	// deadline set by WithLoaderTimeout.
	ErrLoaderTimeout = errors.New("memcache: loader timeout exceeded")

	// ErrLoaderTooManyInFlight is returned by GetOrLoad when the number of
	// in-flight Loader calls has reached WithMaxConcurrentLoads and the
	// caller's context cancels before a slot opens.
	ErrLoaderTooManyInFlight = errors.New("memcache: too many in-flight loads")

	// ErrComputeReentrant is returned (panicked) when a Compute callback
	// attempts to call back into the cache for the same key.
	ErrComputeReentrant = errors.New("memcache: compute callback re-entered cache")

	// ErrUnsupportedKeyType is returned by DeletePrefix when called on a
	// cache whose K is not string and does not implement Prefixer.
	ErrUnsupportedKeyType = errors.New("memcache: key type does not support requested operation")

	// ErrViewReadOnly is returned by mutation attempts on a CacheView.
	ErrViewReadOnly = errors.New("memcache: view is read-only")
)

// ConfigError describes an invalid configuration passed to New.
type ConfigError struct {
	Field   string
	Message string
}

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

// CapacityError describes a Set rejected because of size limits.
type CapacityError struct {
	Key        any
	Reason     string
	LimitField string
}

func (e *CapacityError) Error() string {
	return fmt.Sprintf("memcache: capacity (%s): %s", e.LimitField, e.Reason)
}

// Is reports whether target is a *CapacityError.
func (e *CapacityError) Is(target error) bool {
	_, ok := target.(*CapacityError)
	return ok
}

// LoadError wraps an error returned by a Loader.
type LoadError struct {
	Key any
	Err error
}

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

func (e *SnapshotError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("memcache: snapshot %s %s: %s", e.Op, e.Path, e.Message)
	}
	return fmt.Sprintf("memcache: snapshot %s: %s", e.Op, e.Message)
}

// Unwrap returns the underlying error.
func (e *SnapshotError) Unwrap() error { return e.Err }

// Is reports whether target is a *SnapshotError or one of the snapshot
// sentinels (ErrSnapshotCorrupt, ErrSnapshotIncompatible) when the
// corresponding error is wrapped.
func (e *SnapshotError) Is(target error) bool {
	if _, ok := target.(*SnapshotError); ok {
		return true
	}
	return errors.Is(e.Err, target)
}

// CodecError wraps an error from a Codec implementation.
type CodecError struct {
	Op    string // "marshal" or "unmarshal"
	Codec string // codec name ("gob", "json", "raw", ...)
	Err   error
}

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
