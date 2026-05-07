package memcache

import (
	"bytes"
	"reflect"
	"testing"
)

func TestParseCacheTagSkipDash(t *testing.T) {
	if got := parseCacheTag("-"); !got.skip {
		t.Errorf("parseCacheTag(\"-\").skip = false, want true")
	}
}

func TestParseCacheTagSecretOption(t *testing.T) {
	if got := parseCacheTag("name,secret"); !got.secret {
		t.Errorf("parseCacheTag(\"name,secret\").secret = false, want true")
	}
}

func TestParseCacheTagSecretAfterDash(t *testing.T) {
	got := parseCacheTag("-,secret")
	if !got.skip || !got.secret {
		t.Errorf("parseCacheTag(\"-,secret\") = %+v, want skip+secret", got)
	}
}

func TestParseCacheTagUnknownOptionsIgnored(t *testing.T) {
	got := parseCacheTag("name,futureopt,secret,morefuture")
	if !got.secret {
		t.Errorf("unknown options should not suppress recognized ones; got %+v", got)
	}
}

// secretValue carries a normal Public field and a Secret field
// tagged so the snapshot filter zeros it before encoding.
type secretValue struct {
	Public string
	Secret string `cache:"-,secret"`
}

func TestApplySnapshotFilterZerosSecretFields(t *testing.T) {
	v := secretValue{Public: "kept", Secret: "private"}
	got := applySnapshotFilter(v)
	if got.Public != "kept" {
		t.Errorf("non-secret field lost: %+v", got)
	}
	if got.Secret != "" {
		t.Errorf("secret field not zeroed: %q", got.Secret)
	}
	// Original must be untouched.
	if v.Secret != "private" {
		t.Errorf("filter mutated source: %q", v.Secret)
	}
}

func TestApplySnapshotFilterPointerType(t *testing.T) {
	v := &secretValue{Public: "kept", Secret: "private"}
	got := applySnapshotFilter(v)
	if got == v {
		t.Error("filter must clone the pointed-at struct, not return the same pointer")
	}
	if got.Public != "kept" {
		t.Errorf("non-secret field lost: %+v", got)
	}
	if got.Secret != "" {
		t.Errorf("secret field not zeroed: %q", got.Secret)
	}
	if v.Secret != "private" {
		t.Errorf("filter mutated source: %q", v.Secret)
	}
}

type plainValue struct {
	Name string
}

func TestApplySnapshotFilterNoTagsReturnsOriginal(t *testing.T) {
	// V types with no secret/skip tags MUST pay no copy cost:
	// applySnapshotFilter returns the same struct value.
	v := plainValue{Name: "abc"}
	got := applySnapshotFilter(v)
	if got != v {
		t.Errorf("unfiltered V should round-trip unchanged: in=%+v out=%+v", v, got)
	}
}

func TestSnapshotOmitsSecretFields(t *testing.T) {
	src, _ := New[string, secretValue](WithMaxEntries(4))
	defer src.Close()
	_ = src.Set("creds", secretValue{
		Public: "user@example.com",
		Secret: "supersecret-token",
	})

	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf.Bytes(), []byte("supersecret-token")) {
		t.Error("snapshot bytes contain the secret value; filter did not fire")
	}
	if !bytes.Contains(buf.Bytes(), []byte("user@example.com")) {
		t.Error("snapshot bytes lost the public value")
	}

	// Round-trip: secret reloads as zero, public survives.
	dst, _ := New[string, secretValue](WithMaxEntries(4))
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatal(err)
	}
	got, _ := dst.Get("creds")
	if got.Public != "user@example.com" {
		t.Errorf("Public after reload = %q, want user@example.com", got.Public)
	}
	if got.Secret != "" {
		t.Errorf("Secret after reload = %q, want empty (zero value)", got.Secret)
	}
}

// embeddedSecret tests recursion into anonymous struct embeds.
type baseCreds struct {
	APIKey string `cache:"-,secret"`
}

type composite struct {
	baseCreds
	Name string
}

func TestSnapshotFilterFollowsEmbeddedFields(t *testing.T) {
	v := composite{baseCreds: baseCreds{APIKey: "topsecret"}, Name: "alice"}
	got := applySnapshotFilter(v)
	if got.APIKey != "" {
		t.Errorf("APIKey on embedded struct not zeroed: %q", got.APIKey)
	}
	if got.Name != "alice" {
		t.Errorf("Name lost: %q", got.Name)
	}
}

// TestMetaForCachesAcrossCalls confirms the per-type metadata
// cache returns the SAME *cacheTypeMeta pointer for the same
// reflect.Type.
func TestMetaForCachesAcrossCalls(t *testing.T) {
	a := metaFor(reflect.TypeFor[secretValue]())
	b := metaFor(reflect.TypeFor[secretValue]())
	if a != b {
		t.Error("metaFor did not memoize")
	}
}
