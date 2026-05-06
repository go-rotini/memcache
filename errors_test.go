package memcache

import (
	"errors"
	"io"
	"testing"
)

func TestSentinelErrorsDistinct(t *testing.T) {
	sentinels := []error{
		ErrNotFound, ErrClosed, ErrUnbounded, ErrNoLoader, ErrInvalidTTL,
		ErrSnapshotIncompatible, ErrSnapshotCorrupt,
		ErrKeyTooLarge, ErrValueTooLarge, ErrTooManyTags, ErrPolicyConfig,
		ErrLoaderRateLimited, ErrLoaderTimeout, ErrLoaderTooManyInFlight,
		ErrComputeReentrant, ErrUnsupportedKeyType, ErrViewReadOnly,
	}
	seen := make(map[string]bool)
	for _, s := range sentinels {
		if s == nil {
			t.Errorf("sentinel is nil")
			continue
		}
		if seen[s.Error()] {
			t.Errorf("duplicate sentinel message: %q", s.Error())
		}
		seen[s.Error()] = true
	}
}

func TestConfigErrorIsAndError(t *testing.T) {
	err := &ConfigError{Field: "MaxEntries", Message: "must be positive"}
	if !errors.Is(err, &ConfigError{}) {
		t.Errorf("errors.Is should match peer ConfigError")
	}
	if got := err.Error(); got != "memcache: MaxEntries: must be positive" {
		t.Errorf("unexpected Error(): %q", got)
	}

	err2 := &ConfigError{Message: "bad"}
	if got := err2.Error(); got != "memcache: bad" {
		t.Errorf("unexpected Error() with empty Field: %q", got)
	}
}

func TestCapacityErrorIs(t *testing.T) {
	err := &CapacityError{Key: "k", Reason: "too big", LimitField: "MaxBytes"}
	if !errors.Is(err, &CapacityError{}) {
		t.Errorf("errors.Is should match peer CapacityError")
	}
	if err.Error() == "" {
		t.Error("Error() should be non-empty")
	}
}

func TestLoadErrorUnwrap(t *testing.T) {
	root := io.EOF
	err := &LoadError{Key: "k", Err: root}
	if !errors.Is(err, root) {
		t.Errorf("errors.Is should follow Unwrap to %v", root)
	}
	if !errors.Is(err, &LoadError{}) {
		t.Errorf("errors.Is should match peer LoadError")
	}
	if errors.Unwrap(err) != root {
		t.Errorf("Unwrap mismatch")
	}
}

func TestSnapshotErrorIs(t *testing.T) {
	err := &SnapshotError{Op: "load", Path: "/tmp/x", Message: "bad", Err: ErrSnapshotCorrupt}
	if !errors.Is(err, &SnapshotError{}) {
		t.Errorf("errors.Is should match peer SnapshotError")
	}
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Errorf("errors.Is should follow Unwrap to ErrSnapshotCorrupt")
	}
	if errors.Unwrap(err) != ErrSnapshotCorrupt {
		t.Errorf("Unwrap should return wrapped error")
	}
}

func TestCodecErrorIs(t *testing.T) {
	err := &CodecError{Op: "marshal", Codec: "gob", Err: io.EOF}
	if !errors.Is(err, &CodecError{}) {
		t.Errorf("errors.Is should match peer CodecError")
	}
	if !errors.Is(err, io.EOF) {
		t.Errorf("errors.Is should follow Unwrap to io.EOF")
	}
}
