package memcache

import (
	"testing"
	"time"
)

func TestEntryFlagsHas(t *testing.T) {
	var f entryFlags
	if f.has(flagSliding) {
		t.Error("zero flags should not has(flagSliding)")
	}
	f |= flagSliding
	if !f.has(flagSliding) {
		t.Error("after setting flagSliding, has should report true")
	}
	if f.has(flagNegative) {
		t.Error("flagNegative not set")
	}
	f |= flagNegative
	if !f.has(flagSliding | flagNegative) {
		t.Error("has should require all bits")
	}
}

func TestEntryExpired(t *testing.T) {
	e := &entry[string, int]{}
	now := time.Now().UnixNano()

	// expireAt == 0 means "no TTL"; never expired.
	if e.expired(now) {
		t.Error("entry with no TTL should not be expired")
	}

	e.expireAt.Store(now - 1)
	if !e.expired(now) {
		t.Error("expired entry should report expired")
	}

	e.expireAt.Store(now + int64(time.Hour))
	if e.expired(now) {
		t.Error("future-expiring entry should not be expired now")
	}
}

func TestEntryTouchAccessSliding(t *testing.T) {
	e := &entry[string, int]{
		flags:      flagSliding,
		slidingTTL: int64(time.Minute),
	}
	now := time.Now().UnixNano()
	e.touchAccess(now)
	if got := e.lastAccess.Load(); got != now {
		t.Errorf("lastAccess = %d, want %d", got, now)
	}
	if got := e.expireAt.Load(); got != now+int64(time.Minute) {
		t.Errorf("expireAt = %d, want %d", got, now+int64(time.Minute))
	}
}

func TestEntryTouchAccessNonSliding(t *testing.T) {
	e := &entry[string, int]{
		slidingTTL: int64(time.Minute),
	}
	e.expireAt.Store(999) // pre-existing absolute expiry
	now := time.Now().UnixNano()
	e.touchAccess(now)
	// Non-sliding: lastAccess updates but expireAt does not.
	if e.expireAt.Load() != 999 {
		t.Error("non-sliding entry expireAt should not change on touchAccess")
	}
	if e.lastAccess.Load() != now {
		t.Error("lastAccess should still update on non-sliding entries")
	}
}

func TestEntryMetadataSnapshotCopiesTags(t *testing.T) {
	e := &entry[string, int]{
		key:      "k",
		value:    1,
		inserted: time.Now().UnixNano(),
		weight:   42,
		tags:     []string{"a", "b"},
	}
	m := e.metadata()
	if m.Weight != 42 {
		t.Errorf("Weight = %d, want 42", m.Weight)
	}
	if len(m.Tags) != 2 {
		t.Fatalf("Tags = %v, want len 2", m.Tags)
	}
	// The returned tags slice is a copy: mutating it should not affect
	// the entry's tags.
	m.Tags[0] = "MUTATED"
	if e.tags[0] != "a" {
		t.Error("metadata Tags should be a defensive copy")
	}
}

func TestEntryMetadataNoTTL(t *testing.T) {
	e := &entry[string, int]{
		key:      "k",
		value:    1,
		inserted: time.Now().UnixNano(),
	}
	m := e.metadata()
	if !m.Expiry.IsZero() {
		t.Errorf("Expiry should be zero for no-TTL entry, got %v", m.Expiry)
	}
}

func TestEntryReset(t *testing.T) {
	e := &entry[string, *int]{
		key:        "k",
		value:      new(int),
		inserted:   1,
		weight:     5,
		tags:       []string{"x"},
		flags:      flagSliding,
		slidingTTL: int64(time.Minute),
		policyData: "anything",
	}
	e.expireAt.Store(999)
	e.lastAccess.Store(999)
	e.hits.Store(7)
	e.generation.Store(3)

	e.reset()

	if e.key != "" || e.value != nil {
		t.Error("reset should zero key and value")
	}
	if e.weight != 0 || e.flags != 0 || e.slidingTTL != 0 {
		t.Error("reset should zero scalar fields")
	}
	if e.tags != nil || e.policyData != nil {
		t.Error("reset should nil pointer-bearing fields")
	}
	if e.expireAt.Load() != 0 || e.lastAccess.Load() != 0 {
		t.Error("reset should zero atomic timestamps")
	}
	if e.hits.Load() != 0 || e.generation.Load() != 0 {
		t.Error("reset should zero atomic counters")
	}
}

func TestEntryPoolReuse(t *testing.T) {
	p := newEntryPool[string, int]()
	a := p.get()
	a.key = "x"
	a.value = 42
	p.put(a)

	// We can't strictly assert that the next Get returns the exact
	// same struct (sync.Pool may legitimately drop pooled items under
	// GC pressure), but we can assert that whatever we get back is
	// zero-valued.
	b := p.get()
	if b.key != "" || b.value != 0 {
		t.Errorf("entry from pool should be reset; got key=%q value=%d", b.key, b.value)
	}
}

func TestEntryPoolNilSafe(t *testing.T) {
	p := newEntryPool[string, int]()
	p.put(nil) // should not panic
}
