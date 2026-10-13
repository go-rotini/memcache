package memexpvar_test

import (
	"expvar"
	"strings"
	"testing"

	"github.com/go-rotini/memcache"
	"github.com/go-rotini/memcache/memexpvar"
)

func newCache(t *testing.T, opts ...memcache.Option) *memcache.Cache[string, int] {
	t.Helper()
	c, err := memcache.New[string, int](append(opts, memcache.WithMaxEntries(8))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestPublishRendersStats(t *testing.T) {
	c := newCache(t)
	if err := memexpvar.Publish("memexpvar_test_stats", c); err != nil {
		t.Fatal(err)
	}
	_ = c.Set("a", 1)
	_, _ = c.Get("a")
	_, _ = c.Get("absent")
	// The same object, key for key, as the root package's WithExpvar.
	const want = `{"hits":1,"misses":1,"inserts":1,"updates":0,"deletes":0,"evictions":0,` +
		`"expirations":0,"loads_total":0,"load_hits":0,"load_errors":0,"load_coalesced":0,` +
		`"events_dropped":0,"entries":1,"bytes":1,"capacity":8,"hit_rate_permille":500}`
	if got := expvar.Get("memexpvar_test_stats").String(); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestPublishAgainReplacesSource(t *testing.T) {
	first, second := newCache(t), newCache(t)
	_ = second.Set("a", 1)
	if err := memexpvar.Publish("memexpvar_test_replace", first); err != nil {
		t.Fatal(err)
	}
	if err := memexpvar.Publish("memexpvar_test_replace", second); err != nil {
		t.Fatal(err)
	}
	want := `"entries":1`
	if got := expvar.Get("memexpvar_test_replace").String(); !strings.Contains(got, want) {
		t.Fatalf("got %s, want the second cache's stats (%s)", got, want)
	}
}

func TestPublishRejectsForeignNameAndBadArgs(t *testing.T) {
	expvar.NewInt("memexpvar_test_foreign")
	if err := memexpvar.Publish("memexpvar_test_foreign", newCache(t)); err == nil {
		t.Error("Publish over a variable it didn't register succeeded")
	}
	if err := memexpvar.Publish("", newCache(t)); err == nil {
		t.Error("Publish with an empty name succeeded")
	}
	if err := memexpvar.Publish("memexpvar_test_nil", nil); err == nil {
		t.Error("Publish with a nil source succeeded")
	}
}
