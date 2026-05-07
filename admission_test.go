package memcache

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestAdmitAlways(t *testing.T) {
	a := AdmitAlways[string]{}
	if !a.Admit("k") {
		t.Error("AdmitAlways must admit every key")
	}
	a.Observe("k") // no-op
	a.Reset()
}

func TestAdmitAlwaysObserveAndResetCovered(t *testing.T) {
	// Both methods are no-ops but their bodies still need execution
	// to count toward coverage.
	var a AdmitAlways[int]
	a.Observe(1)
	a.Observe(2)
	a.Reset()
	if !a.Admit(99) {
		t.Error("AdmitAlways.Admit must be true after Observe/Reset")
	}
}

func TestResolveAdmissionPolicyTypeMismatch(t *testing.T) {
	// An AdmissionPolicy[string] supplied to a cache built around
	// integer keys must surface a ConfigError with a useful message.
	policy := AdmitAlways[string]{}
	_, err := New[int, int](
		WithMaxEntries(8),
		WithAdmissionPolicy[string](policy),
	)
	if err == nil {
		t.Fatal("expected ConfigError on key-type mismatch")
	}
	var ce *ConfigError
	if !errors.As(err, &ce) {
		t.Errorf("expected *ConfigError, got %T (%v)", err, err)
	}
}

func TestResolveAdmissionPolicyDefaultPath(t *testing.T) {
	// No admission option supplied → AdmitAlways path.
	c, err := New[string, int](WithMaxEntries(8))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer c.Close()
	if err := c.Set("k", 1); err != nil {
		t.Fatal(err)
	}
	if v, ok := c.Get("k"); !ok || v != 1 {
		t.Errorf("got (%d, %v), want (1, true)", v, ok)
	}
}

func TestResolveAdmissionPolicyDoorkeeperUnboundedFallbackSize(t *testing.T) {
	// Byte-bounded cache (no maxEntries) + WithDoorkeeper exercises
	// the fallback expected=1024 path inside resolveAdmissionPolicy.
	c, err := New[string, int](
		WithMaxBytes(1<<20),
		WithWeigher(func(int) int64 { return 8 }),
		WithDoorkeeper(true),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer c.Close()
	// First Set is rejected by doorkeeper, second admitted.
	_ = c.Set("k", 1)
	_ = c.Set("k", 2)
	if v, ok := c.Get("k"); !ok || v != 2 {
		t.Errorf("doorkeeper second-set: got (%d, %v), want (2, true)", v, ok)
	}
}

func TestDoorkeeperRejectsFirstSeen(t *testing.T) {
	d := NewDoorkeeper(64, defaultHasher[string]())
	if d.Admit("k") {
		t.Error("first Admit should be false (key not yet observed)")
	}
	d.Observe("k")
	if !d.Admit("k") {
		t.Error("second Admit should succeed after Observe")
	}
}

func TestDoorkeeperResetClears(t *testing.T) {
	d := NewDoorkeeper(64, defaultHasher[string]())
	d.Observe("k")
	if !d.Admit("k") {
		t.Fatal("Admit before Reset should succeed")
	}
	d.Reset()
	if d.Admit("k") {
		t.Error("Admit after Reset should fail again")
	}
}

func TestWithDoorkeeper_RejectsFirstSet(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithDoorkeeper(true),
	)
	defer c.Close()

	if err := c.Set("k", 1); err != nil {
		t.Fatal(err)
	}
	// Doorkeeper hadn't seen "k" before; first Set rejected.
	if _, ok := c.Get("k"); ok {
		t.Error("first Set under WithDoorkeeper should be rejected")
	}
	// Second Set is admitted (observed during the first call).
	if err := c.Set("k", 2); err != nil {
		t.Fatal(err)
	}
	if v, ok := c.Get("k"); !ok || v != 2 {
		t.Errorf("second Set should be admitted; got (%d, %v)", v, ok)
	}
	if got := c.Stats().AdmissionRejects; got != 1 {
		t.Errorf("AdmissionRejects = %d, want 1", got)
	}
}

func TestWithDoorkeeper_UpdateBypassesGate(t *testing.T) {
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithDoorkeeper(true),
	)
	defer c.Close()

	// Two Sets to land the value.
	_ = c.Set("k", 1)
	_ = c.Set("k", 2)
	// Mutating an existing entry MUST always succeed: the
	// admission policy gates inserts, not updates.
	if err := c.Set("k", 99); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Get("k"); v != 99 {
		t.Errorf("update under doorkeeper got %d, want 99", v)
	}
}

type rejectAll[K comparable] struct {
	rejects atomic.Int64
}

func (r *rejectAll[K]) Admit(K) bool {
	r.rejects.Add(1)
	return false
}
func (r *rejectAll[K]) Observe(K) {}
func (r *rejectAll[K]) Reset()    {}

func TestWithAdmissionPolicy_CustomRejectAll(t *testing.T) {
	policy := &rejectAll[string]{}
	c, _ := New[string, int](
		WithMaxEntries(8),
		WithAdmissionPolicy(policy),
	)
	defer c.Close()

	for i := range 5 {
		_ = c.Set("k"+itoaSimple(i), i)
	}
	if c.Len() != 0 {
		t.Errorf("Len = %d, want 0 (every Set rejected)", c.Len())
	}
	if policy.rejects.Load() != 5 {
		t.Errorf("Admit fired %d times, want 5", policy.rejects.Load())
	}
	if c.Stats().AdmissionRejects != 5 {
		t.Errorf("AdmissionRejects = %d, want 5", c.Stats().AdmissionRejects)
	}
}
