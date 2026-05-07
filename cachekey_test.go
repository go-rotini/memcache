package memcache

import (
	"fmt"
	"testing"
)

// userByID is the canonical CacheKeyer test type.
type userByID struct {
	ID   int
	Name string
}

func (u userByID) CacheKey() string { return fmt.Sprintf("user:%d", u.ID) }

func TestCacheKeyer_SetCacheable_StoresUnderDerivedKey(t *testing.T) {
	c, _ := New[string, userByID](WithMaxEntries(8))
	defer c.Close()
	if err := SetCacheable(c, userByID{ID: 42, Name: "alice"}); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Get("user:42")
	if !ok || got.Name != "alice" {
		t.Errorf("Get(\"user:42\") = (%+v, %v); want alice", got, ok)
	}
}

func TestCacheKeyer_GetCacheable_PrototypePattern(t *testing.T) {
	c, _ := New[string, userByID](WithMaxEntries(8))
	defer c.Close()
	_ = SetCacheable(c, userByID{ID: 7, Name: "bob"})

	// Look up using only the ID-bearing prototype; CacheKey() ignores Name.
	got, ok := GetCacheable(c, userByID{ID: 7})
	if !ok || got.Name != "bob" {
		t.Errorf("GetCacheable proto = (%+v, %v); want bob", got, ok)
	}
}

func TestCacheKeyer_DeleteCacheable(t *testing.T) {
	c, _ := New[string, userByID](WithMaxEntries(8))
	defer c.Close()
	_ = SetCacheable(c, userByID{ID: 1, Name: "x"})
	if !DeleteCacheable(c, userByID{ID: 1}) {
		t.Error("DeleteCacheable should return true on hit")
	}
	if c.Has("user:1") {
		t.Error("entry should be gone after DeleteCacheable")
	}
}

func TestCacheKeyer_SetCacheable_HonorsSetOptions(t *testing.T) {
	c, _ := New[string, userByID](WithMaxEntries(8), WithTTLJitter(0))
	defer c.Close()
	_ = SetCacheable(c, userByID{ID: 9}, SetTags("hot", "user"))
	tags := c.Tags("user:9")
	wantTags := map[string]bool{"hot": true, "user": true}
	if len(tags) != 2 {
		t.Fatalf("Tags = %v, want two of {hot, user}", tags)
	}
	for _, tg := range tags {
		if !wantTags[tg] {
			t.Errorf("unexpected tag %q", tg)
		}
	}
}

// userBoth implements both CacheKeyer and CacheTagger to confirm
// the umbrella interface composition still works.
type userBoth struct {
	ID int
}

func (u userBoth) CacheKey() string    { return fmt.Sprintf("u:%d", u.ID) }
func (u userBoth) CacheTags() []string { return []string{fmt.Sprintf("user-%d", u.ID)} }

func TestCacheKeyer_ComposesWithCacheTagger(t *testing.T) {
	c, _ := New[string, userBoth](WithMaxEntries(8))
	defer c.Close()
	_ = SetCacheable(c, userBoth{ID: 5})
	// CacheTags fires through the regular Set path.
	tags := c.Tags("u:5")
	if len(tags) != 1 || tags[0] != "user-5" {
		t.Errorf("CacheTags not honored: tags = %v", tags)
	}
}

func TestCacheKeyer_AssertableViaSubInterface(t *testing.T) {
	// Sanity: CacheKeyer is independently type-assertable from
	// the umbrella, like the other Cacheable sub-interfaces.
	var v any = userByID{ID: 1}
	if _, ok := v.(CacheKeyer); !ok {
		t.Error("userByID should satisfy CacheKeyer")
	}
}
