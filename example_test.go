package memcache_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-rotini/memcache"
)

// ExampleNew shows the canonical zero-config construction. Either
// WithMaxEntries or WithMaxBytes must be supplied — an unbounded
// cache is a memory leak in disguise and the constructor refuses
// it.
func ExampleNew() {
	c, err := memcache.New[string, int](memcache.WithMaxEntries(1000))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	_ = c.Set("answer", 42)
	v, ok := c.Get("answer")
	fmt.Println(v, ok)
	// Output: 42 true
}

// ExampleNew_unboundedRejected demonstrates the spec-mandated
// safety check: New refuses to construct an unbounded cache.
func ExampleNew_unboundedRejected() {
	_, err := memcache.New[string, int]()
	fmt.Println(errors.Is(err, memcache.ErrUnbounded))
	// Output: true
}

// ExampleCache_SetWithTTL stores a value with a per-call TTL.
func ExampleCache_SetWithTTL() {
	// WithTTLJitter(0) keeps the TTL exact for the example; the
	// default 5%-of-TTL jitter would produce a non-deterministic
	// godoc output.
	c, _ := memcache.New[string, int](
		memcache.WithMaxEntries(8),
		memcache.WithTTLJitter(0),
	)
	defer c.Close()
	_ = c.SetWithTTL("session-token", 1234, 30*time.Minute)
	d, _ := c.TTL("session-token")
	fmt.Println(d <= 30*time.Minute)
	// Output: true
}

// ExampleCache_SetWithOptions composes per-call overrides.
func ExampleCache_SetWithOptions() {
	c, _ := memcache.New[string, string](memcache.WithMaxEntries(8))
	defer c.Close()
	_ = c.SetWithOptions("user:42",
		"alice@example.com",
		memcache.SetTTL(time.Hour),
		memcache.SetTags("user-42", "team-eng"),
	)
	tags := c.Tags("user:42")
	sort.Strings(tags)
	fmt.Println(tags)
	// Output: [team-eng user-42]
}

// ExampleCache_GetOrLoad demonstrates the singleflight-deduplicated
// loader path. 1000 concurrent gets on a missing key would still
// invoke the loader exactly once.
func ExampleCache_GetOrLoad() {
	loader := memcache.LoaderFunc[string, int](
		func(_ context.Context, k string) (int, time.Duration, error) {
			return len(k), 5 * time.Minute, nil
		},
	)
	c, _ := memcache.New[string, int](
		memcache.WithMaxEntries(64),
		memcache.WithLoader[string, int](loader),
	)
	defer c.Close()

	v, _ := c.GetOrLoad(context.Background(), "hello")
	fmt.Println(v)
	// Output: 5
}

// ExampleCache_SetWithTags + InvalidateTag is the "drop everything
// owned by a user" pattern.
func ExampleCache_InvalidateTag() {
	c, _ := memcache.New[string, int](memcache.WithMaxEntries(64))
	defer c.Close()
	_ = c.SetWithTags("user:42:profile", 1, "user-42")
	_ = c.SetWithTags("user:42:settings", 2, "user-42")
	_ = c.SetWithTags("user:7:profile", 3, "user-7")

	dropped := c.InvalidateTag("user-42")
	fmt.Println("dropped", dropped)
	fmt.Println("user-7 survives:", c.Has("user:7:profile"))
	// Output:
	// dropped 2
	// user-7 survives: true
}

// ExampleCache_Compute is the canonical atomic read-modify-write
// for a counter.
func ExampleCache_Compute() {
	c, _ := memcache.New[string, int](memcache.WithMaxEntries(8))
	defer c.Close()
	for range 5 {
		_, _ = c.Compute("hits", func(cur int, _ bool) (int, memcache.ComputeAction, error) {
			return cur + 1, memcache.ComputeStore, nil
		})
	}
	v, _ := c.Get("hits")
	fmt.Println(v)
	// Output: 5
}

// ExampleIncrement shows the typed numeric helper as a thin
// wrapper over Compute.
func ExampleIncrement() {
	c, _ := memcache.New[string, int64](memcache.WithMaxEntries(8))
	defer c.Close()
	_, _ = memcache.Increment(c, "events")
	_, _ = memcache.Increment(c, "events")
	v, _ := memcache.IncrementBy(c, "events", 10)
	fmt.Println(v)
	// Output: 12
}

// ExampleCache_Subscribe shows event-driven observability.
// EventInsert/Update/Evict and friends fan out to subscribers
// through a non-blocking publish; full channels drop and
// EventsDropped tracks the loss.
func ExampleCache_Subscribe() {
	c, _ := memcache.New[string, int](memcache.WithMaxEntries(4))
	defer c.Close()

	ch, cancel := c.Subscribe(8, memcache.EventInsert)
	defer cancel()
	_ = c.Set("k", 1)
	e := <-ch
	fmt.Println(e.Kind, e.Key, e.Value)
	// Output: insert k 1
}

// ExampleNewTiered composes a small fast L1 with a larger warm L2.
func ExampleNewTiered() {
	l1, _ := memcache.New[string, int](memcache.WithMaxEntries(16))
	l2, _ := memcache.New[string, int](memcache.WithMaxEntries(1024))
	t := memcache.NewTiered(l1, l2)
	defer t.Close()
	_ = t.Set("k", 42)
	v, _ := t.Get("k")
	fmt.Println(v)
	// Output: 42
}

// ExampleCache_SaveFile demonstrates the warm-restart pattern for
// REPLs and daemons: snapshot to disk, reload on restart.
func ExampleCache_SaveFile() {
	dir, _ := os.MkdirTemp("", "memcache-example-*")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "cache.snap")

	c, _ := memcache.New[string, int](memcache.WithMaxEntries(64))
	_ = c.Set("warm", 99)
	if err := c.SaveFile(path); err != nil {
		fmt.Println("save:", err)
		return
	}
	_ = c.Close()

	c2, _ := memcache.New[string, int](memcache.WithMaxEntries(64))
	defer c2.Close()
	if _, err := c2.LoadFile(path); err != nil {
		fmt.Println("load:", err)
		return
	}
	v, ok := c2.Get("warm")
	fmt.Println(v, ok)
	// Output: 99 true
}

// ExampleCache_Items is the canonical diagnostic pattern.
func ExampleCache_Items() {
	c, _ := memcache.New[string, int](memcache.WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("a", 1)
	_ = c.Set("b", 2)
	keys := []string{}
	for _, ki := range c.Items() {
		keys = append(keys, ki.Key)
	}
	sort.Strings(keys)
	fmt.Println(keys)
	// Output: [a b]
}

// ExampleCache_Hottest sorts by hit count for "what's been getting
// the most attention?" REPL views.
func ExampleCache_Hottest() {
	c, _ := memcache.New[string, int](memcache.WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("cold", 0)
	_ = c.Set("hot", 1)
	for range 5 {
		_, _ = c.Get("hot")
	}
	top := c.Hottest(1)
	fmt.Println(top[0].Key)
	// Output: hot
}

// ExampleSetCacheable demonstrates the CacheKeyer pattern: define a
// type whose values know their own cache key, then store them
// without restating the derivation at every call site.
type exampleProfile struct {
	UserID int
	Email  string
}

func (p exampleProfile) CacheKey() string { return fmt.Sprintf("user:%d", p.UserID) }

func ExampleSetCacheable() {
	c, _ := memcache.New[string, exampleProfile](memcache.WithMaxEntries(64))
	defer c.Close()
	_ = memcache.SetCacheable(c, exampleProfile{UserID: 42, Email: "a@b.c"})

	// Look up by passing only the ID-bearing prototype.
	got, _ := memcache.GetCacheable(c, exampleProfile{UserID: 42})
	fmt.Println(got.Email)
	// Output: a@b.c
}
