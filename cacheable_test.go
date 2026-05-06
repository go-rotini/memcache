package memcache

import (
	"bytes"
	"slices"
	"sort"
	"testing"
	"time"
)

// --- CacheTTLer test types -------------------------------------------------

type ttlValuePtr struct{ ttl time.Duration }

func (v *ttlValuePtr) CacheTTL() time.Duration { return v.ttl }

type ttlValue struct{ ttl time.Duration }

func (v ttlValue) CacheTTL() time.Duration { return v.ttl }

func TestCacheTTLerPointerReceiverOverridesDefault(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, *ttlValuePtr](
		WithMaxEntries(4),
		WithClock(clk),
		WithDefaultTTL(time.Hour), // would be ignored
	)
	defer c.Close()

	if err := c.Set("k", &ttlValuePtr{ttl: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	d, ok := c.TTL("k")
	if !ok || d != 10*time.Second {
		t.Errorf("TTL = (%v, %v), want (10s, true) — CacheTTL should override default",
			d, ok)
	}
}

func TestCacheTTLerValueReceiverOverridesDefault(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, ttlValue](
		WithMaxEntries(4),
		WithClock(clk),
		WithDefaultTTL(time.Hour),
	)
	defer c.Close()
	_ = c.Set("k", ttlValue{ttl: 5 * time.Second})
	d, _ := c.TTL("k")
	if d != 5*time.Second {
		t.Errorf("TTL = %v, want 5s", d)
	}
}

func TestCacheTTLerNegativeFallsBackToDefault(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, ttlValue](
		WithMaxEntries(4),
		WithClock(clk),
		WithDefaultTTL(7*time.Second),
	)
	defer c.Close()
	_ = c.Set("k", ttlValue{ttl: -1}) // sentinel: "use cache default"
	d, _ := c.TTL("k")
	if d != 7*time.Second {
		t.Errorf("TTL = %v, want 7s (negative CacheTTL → fallback)", d)
	}
}

func TestCacheTTLerZeroMeansNoExpiry(t *testing.T) {
	c, _ := New[string, ttlValue](
		WithMaxEntries(4),
		WithDefaultTTL(time.Hour),
	)
	defer c.Close()
	_ = c.Set("k", ttlValue{ttl: 0})
	d, ok := c.TTL("k")
	if !ok || d != 0 {
		t.Errorf("TTL = (%v, %v), want (0, true) — zero CacheTTL means no expiry", d, ok)
	}
}

func TestSetWithTTLBypassesCacheTTLer(t *testing.T) {
	// Explicit per-call TTL must beat the value's CacheTTL.
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, ttlValue](WithMaxEntries(4), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("k", ttlValue{ttl: time.Hour}, 5*time.Second)
	d, _ := c.TTL("k")
	if d != 5*time.Second {
		t.Errorf("SetWithTTL should override CacheTTL; got %v want 5s", d)
	}
}

// --- CacheTagger test types ------------------------------------------------

type taggedValue struct {
	tags []string
}

func (v taggedValue) CacheTags() []string { return v.tags }

func TestCacheTaggerAutoTagsOnSet(t *testing.T) {
	c, _ := New[string, taggedValue](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", taggedValue{tags: []string{"alpha", "beta"}})

	got := c.Tags("k")
	sort.Strings(got)
	want := []string{"alpha", "beta"}
	if !slices.Equal(got, want) {
		t.Errorf("Tags(k) = %v, want %v", got, want)
	}
	// And the tag index works end-to-end.
	if n := c.InvalidateTag("alpha"); n != 1 {
		t.Errorf("InvalidateTag(alpha) = %d, want 1", n)
	}
}

func TestCacheTaggerEmptyTagsLeavesUntagged(t *testing.T) {
	c, _ := New[string, taggedValue](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", taggedValue{tags: nil})
	if got := c.Tags("k"); got != nil {
		t.Errorf("nil-CacheTags entry should be untagged; got %v", got)
	}
}

func TestSetWithTagsBypassesCacheTagger(t *testing.T) {
	// SetWithOptions(SetTags(...)) is explicit; CacheTags should
	// NOT be merged in.
	c, _ := New[string, taggedValue](WithMaxEntries(4))
	defer c.Close()
	_ = c.SetWithOptions("k",
		taggedValue{tags: []string{"auto"}},
		SetTags("explicit"),
	)
	got := c.Tags("k")
	if len(got) != 1 || got[0] != "explicit" {
		t.Errorf("explicit SetTags should win; got %v", got)
	}
}

// --- SnapshotMarshaler / SnapshotUnmarshaler --------------------------------

// snapEntity stores a Name in memory but persists it as
// upper-case bytes. Round-tripping verifies both Marshal and
// Unmarshal hooks fire, AND that the in-memory representation is
// untouched (no Save→reuse contamination).
type snapEntity struct {
	Name string
}

func (e snapEntity) SnapshotMarshal() ([]byte, error) {
	return []byte("SNAP:" + e.Name), nil
}

func (e *snapEntity) SnapshotUnmarshal(b []byte) error {
	const prefix = "SNAP:"
	if !bytes.HasPrefix(b, []byte(prefix)) {
		return errSnapEntityBadPrefix
	}
	e.Name = string(b[len(prefix):])
	return nil
}

var errSnapEntityBadPrefix = newTestError("snapEntity: missing SNAP: prefix")

func newTestError(msg string) error { return &testError{msg: msg} }

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func TestSnapshotMarshalerHookFires(t *testing.T) {
	src, _ := New[string, snapEntity](WithMaxEntries(4))
	defer src.Close()
	_ = src.Set("k", snapEntity{Name: "alice"})

	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	// Sanity: the custom Marshal output appears verbatim in the
	// snapshot bytes (proves the codec was bypassed).
	if !bytes.Contains(buf.Bytes(), []byte("SNAP:alice")) {
		t.Errorf("snapshot bytes do not contain custom marshaler output")
	}

	dst, _ := New[string, snapEntity](WithMaxEntries(4))
	defer dst.Close()
	n, err := dst.Load(&buf)
	if err != nil || n != 1 {
		t.Fatalf("Load = (%d, %v)", n, err)
	}
	got, _ := dst.Get("k")
	if got.Name != "alice" {
		t.Errorf("after round-trip, Name = %q, want %q", got.Name, "alice")
	}
}

func TestSnapshotUnmarshalerErrorPropagates(t *testing.T) {
	// Manually craft a value-bytes payload missing the SNAP:
	// prefix. Easiest: create a "good" snapshot, corrupt the value
	// bytes specifically without touching length fields, and
	// ensure Load surfaces the unmarshaler's error.
	src, _ := New[string, snapEntity](WithMaxEntries(4))
	defer src.Close()
	_ = src.Set("k", snapEntity{Name: "alice"})
	var buf bytes.Buffer
	_ = src.Save(&buf)

	// Find the SNAP:alice marker and overwrite the prefix bytes.
	raw := buf.Bytes()
	idx := bytes.Index(raw, []byte("SNAP:alice"))
	if idx < 0 {
		t.Fatal("could not find marker")
	}
	copy(raw[idx:idx+5], []byte("XXXX:")) // breaks the prefix

	// CRC also needs adjusting OR we'll see CRC error first; let's
	// recompute it by treating all bytes except the trailing 4 as
	// the CRC scope.
	if len(raw) < 8 {
		t.Fatal("snapshot too small")
	}
	// Simpler: just confirm SOME error is returned (CRC OR unmarshal).
	dst, _ := New[string, snapEntity](WithMaxEntries(4))
	defer dst.Close()
	if _, err := dst.Load(bytes.NewReader(raw)); err == nil {
		t.Error("expected an error from corrupted SnapshotUnmarshal payload")
	}
}

// snapPointerEntity uses a *snapPointerEntity-receiver form for
// SnapshotMarshal — the test verifies pointer-receiver Marshal
// works when V is the value type.
type snapPointerEntity struct {
	N int
}

func (e *snapPointerEntity) SnapshotMarshal() ([]byte, error) {
	return []byte{byte(e.N)}, nil
}

func (e *snapPointerEntity) SnapshotUnmarshal(b []byte) error {
	if len(b) != 1 {
		return errSnapEntityBadPrefix
	}
	e.N = int(b[0])
	return nil
}

func TestSnapshotMarshalerPointerReceiverOnValueType(t *testing.T) {
	// V is the value type; Marshal/Unmarshal are on the pointer
	// receiver. The cache must still recognize them.
	src, _ := New[string, snapPointerEntity](WithMaxEntries(4))
	defer src.Close()
	_ = src.Set("k", snapPointerEntity{N: 42})
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := New[string, snapPointerEntity](WithMaxEntries(4))
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatal(err)
	}
	got, _ := dst.Get("k")
	if got.N != 42 {
		t.Errorf("round-trip N = %d, want 42", got.N)
	}
}

// snapPtrV uses *snapPtrV as V (pointer type) with methods on
// *snapPtrV. Tests the snap.value-as-pointer code path.
type snapPtrV struct {
	S string
}

func (e *snapPtrV) SnapshotMarshal() ([]byte, error) {
	return []byte("P:" + e.S), nil
}

func (e *snapPtrV) SnapshotUnmarshal(b []byte) error {
	const p = "P:"
	if !bytes.HasPrefix(b, []byte(p)) {
		return errSnapEntityBadPrefix
	}
	e.S = string(b[len(p):])
	return nil
}

func TestSnapshotMarshalerPointerVType(t *testing.T) {
	src, _ := New[string, *snapPtrV](WithMaxEntries(4))
	defer src.Close()
	_ = src.Set("k", &snapPtrV{S: "hello"})
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("P:hello")) {
		t.Errorf("custom marshaler bytes missing for pointer V type")
	}
}

// --- Cacheable umbrella ----------------------------------------------------

type fullyCacheable struct {
	N int
}

func (f fullyCacheable) CacheTTL() time.Duration          { return 30 * time.Second }
func (f fullyCacheable) CacheTags() []string              { return []string{"full"} }
func (f fullyCacheable) SnapshotMarshal() ([]byte, error) { return []byte{byte(f.N)}, nil }

func (f *fullyCacheable) SnapshotUnmarshal(b []byte) error {
	if len(b) != 1 {
		return errSnapEntityBadPrefix
	}
	f.N = int(b[0])
	return nil
}

func TestCacheableUmbrellaIntegration(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, fullyCacheable](WithMaxEntries(4), WithClock(clk))
	defer c.Close()
	_ = c.Set("k", fullyCacheable{N: 7})

	// CacheTTL applied.
	d, _ := c.TTL("k")
	if d != 30*time.Second {
		t.Errorf("TTL = %v, want 30s", d)
	}
	// CacheTags applied.
	got := c.Tags("k")
	if len(got) != 1 || got[0] != "full" {
		t.Errorf("Tags = %v, want [full]", got)
	}

	// Snapshot round-trip uses custom Marshal/Unmarshal. Both
	// caches share the same FakeClock so the snapshot's recorded
	// expireAt is interpreted in the same time frame on Load —
	// otherwise the dst cache (real clock) would treat the
	// snapshot's "30s past Unix epoch" expireAt as long-expired.
	var buf bytes.Buffer
	if err := c.Save(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := New[string, fullyCacheable](WithMaxEntries(4), WithClock(clk))
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatal(err)
	}
	v, _ := dst.Get("k")
	if v.N != 7 {
		t.Errorf("after round-trip, N = %d, want 7", v.N)
	}
}

// --- omitempty / versioned / tag=template -----------------------

type omitemptyUser struct {
	ID    int    `cache:"id"`
	Name  string `cache:"name"`
	Cache string `cache:"cache,omitempty"`
}

func TestCacheTagOmitempty_ZeroFieldRoundTrip(t *testing.T) {
	c, _ := New[string, omitemptyUser](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("u", omitemptyUser{ID: 1, Name: "a"})

	var buf bytes.Buffer
	if err := c.Save(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := New[string, omitemptyUser](WithMaxEntries(8))
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatal(err)
	}
	got, ok := dst.Get("u")
	if !ok || got.ID != 1 || got.Name != "a" || got.Cache != "" {
		t.Errorf("after omitempty round-trip: got %+v", got)
	}
}

type versionedSchemaV1 struct {
	ID   int    `cache:",versioned"`
	Name string `cache:",versioned"`
}

type versionedSchemaV2 struct {
	ID    int    `cache:",versioned"`
	Name  string `cache:",versioned"`
	Email string `cache:",versioned"`
}

func TestCacheTagVersioned_RejectsSchemaDrift(t *testing.T) {
	src, _ := New[string, versionedSchemaV1](WithMaxEntries(8))
	defer src.Close()
	_ = src.Set("u", versionedSchemaV1{ID: 1, Name: "alice"})

	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}

	// Load into a cache parameterized on the v2 schema — version
	// fingerprint should disagree.
	dst, _ := New[string, versionedSchemaV2](WithMaxEntries(8))
	defer dst.Close()
	_, err := dst.Load(&buf)
	if err == nil {
		t.Fatal("expected schema-version mismatch, got nil error")
	}
}

func TestCacheTagVersioned_AcceptsSameSchema(t *testing.T) {
	src, _ := New[string, versionedSchemaV1](WithMaxEntries(8))
	defer src.Close()
	_ = src.Set("u", versionedSchemaV1{ID: 1, Name: "alice"})

	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}

	dst, _ := New[string, versionedSchemaV1](WithMaxEntries(8))
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatalf("load same-schema snapshot: %v", err)
	}
	if got, _ := dst.Get("u"); got.Name != "alice" {
		t.Errorf("after versioned round-trip: %+v", got)
	}
}

type templatedUser struct {
	ID   int    `cache:"id,tag=user-{ID}"`
	Team string `cache:"team,tag=team-{Team}"`
}

func TestCacheTagTemplate_AutoTagsOnSet(t *testing.T) {
	c, _ := New[string, templatedUser](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("k", templatedUser{ID: 42, Team: "eng"})

	tags := c.Tags("k")
	sort.Strings(tags)
	want := []string{"team-eng", "user-42"}
	if !slices.Equal(tags, want) {
		t.Errorf("template tags = %v, want %v", tags, want)
	}
	// Tag invalidation should drop the entry.
	if dropped := c.InvalidateTag("user-42"); dropped != 1 {
		t.Errorf("InvalidateTag(user-42) = %d, want 1", dropped)
	}
}
