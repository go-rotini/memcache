package memcache

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestSentinelErrorsDistinct(t *testing.T) {
	sentinels := []error{
		ErrNotFound, ErrClosed, ErrUnbounded, ErrNoLoader, ErrInvalidTTL,
		ErrSnapshotIncompatible, ErrSnapshotCorrupt,
		ErrKeyTooLarge, ErrValueTooLarge, ErrTooManyTags,
		ErrLoaderRateLimited, ErrLoaderTimeout, ErrLoaderTooManyInFlight,
		ErrComputeReentrant,
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

func TestCapacityErrorUnwrapAndIsCause(t *testing.T) {
	// Unwrap returns the wrapped sentinel when set.
	err := &CapacityError{
		Key:        "k",
		Reason:     "too many tags",
		LimitField: "MaxTagsPerEntry",
		Cause:      ErrTooManyTags,
	}
	if got := errors.Unwrap(err); got != ErrTooManyTags {
		t.Errorf("Unwrap = %v, want %v", got, ErrTooManyTags)
	}
	// Is should match the wrapped sentinel via the Cause branch.
	if !errors.Is(err, ErrTooManyTags) {
		t.Error("errors.Is should follow Cause to the sentinel")
	}
	// Without a Cause, Unwrap returns nil and Is(non-peer) is false.
	bare := &CapacityError{Key: "k", Reason: "too big", LimitField: "MaxBytes"}
	if got := errors.Unwrap(bare); got != nil {
		t.Errorf("Unwrap with nil Cause = %v, want nil", got)
	}
	if errors.Is(bare, io.EOF) {
		t.Error("Is should NOT match an unrelated sentinel")
	}
}

func TestLoadErrorErrorAndUnwrap(t *testing.T) {
	root := io.EOF
	err := &LoadError{Key: "k", Err: root}
	if err.Error() == "" {
		t.Error("LoadError.Error() should be non-empty")
	}
	if got := err.Unwrap(); got != root {
		t.Errorf("Unwrap = %v, want %v", got, root)
	}
}

func TestSnapshotErrorErrorEmptyPath(t *testing.T) {
	// Path empty branch.
	err := &SnapshotError{Op: "save", Message: "io fail"}
	if err.Error() == "" {
		t.Error("SnapshotError.Error() with empty path should still be non-empty")
	}
	if errors.Unwrap(err) != nil {
		t.Error("Unwrap on SnapshotError without Err should return nil")
	}
	if errors.Is(err, io.EOF) {
		t.Error("Is on SnapshotError without wrapped Err should be false")
	}
}

func TestSnapshotErrorErrorWithPath(t *testing.T) {
	err := &SnapshotError{Op: "load", Path: "/tmp/x", Message: "bad"}
	got := err.Error()
	if got == "" || !strings.Contains(got, "/tmp/x") {
		t.Errorf("Error() with path = %q, want it to contain path", got)
	}
}

func TestCodecErrorErrorAndUnwrap(t *testing.T) {
	err := &CodecError{Op: "unmarshal", Codec: "json", Err: io.ErrUnexpectedEOF}
	if err.Error() == "" {
		t.Error("CodecError.Error() should be non-empty")
	}
	if got := err.Unwrap(); got != io.ErrUnexpectedEOF {
		t.Errorf("Unwrap = %v, want %v", got, io.ErrUnexpectedEOF)
	}
}

func TestCapacityErrorIsNonPeerNonCause(t *testing.T) {
	// Cover the final "return false" branch in CapacityError.Is.
	err := &CapacityError{Key: "k", Reason: "x", LimitField: "y"}
	if errors.Is(err, ErrNotFound) {
		t.Error("Is should be false for unrelated sentinel and no Cause")
	}
}

func TestLoadErrorIsNonPeer(t *testing.T) {
	// Cover the negative path in LoadError.Is by passing a non-peer.
	err := &LoadError{Key: "k", Err: io.EOF}
	if errors.Is(err, &CodecError{}) {
		t.Error("LoadError.Is should not match a CodecError")
	}
}
