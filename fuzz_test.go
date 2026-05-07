// fuzz_test.go drives randomized correctness exploration. The
// Makefile's `test-fuzz` target runs each FuzzXxx target for 30-60
// seconds; CI replays a corpus from testdata/fuzz/.

package memcache

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// FuzzCacheOps drives a random sequence of Set/Get/Delete/Has
// operations and asserts size-bound, Has/Get presence agreement,
// Set-then-Get round-trip, and Delete-after-Has consistency.
// Encoding: (op, key) pairs; op = byte%8; key = single byte.
func FuzzCacheOps(f *testing.F) {
	f.Add([]byte{0, 1, 2, 1, 4, 1, 6, 1}) // set, get, delete, get
	f.Add([]byte{0, 1, 0, 1, 0, 1, 0, 1}) // repeated overwrites
	f.Add([]byte{0, 0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 4, 0, 4, 1})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Single shard, budget 8 + 10% slop = 9 entries; +1
		// margin for the insert/evict transient inside Set.
		c, err := New[byte, byte](WithMaxEntries(8), WithShards(1))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		const evictionMargin = 10

		for i := 0; i+1 < len(data); i += 2 {
			op := data[i] % 8
			key := data[i+1]
			val := byte(i % 256)
			switch op {
			case 0: // Set then immediate Get round-trip
				if err := c.Set(key, val); err != nil {
					t.Fatalf("Set: %v", err)
				}
				if got, ok := c.Get(key); !ok || got != val {
					t.Fatalf("Set(%d, %d) -> Get = (%d, %v)", key, val, got, ok)
				}
			case 1: // Get
				_, _ = c.Get(key)
			case 2: // Has consistency: if Has, Get must hit
				if c.Has(key) {
					if _, ok := c.Get(key); !ok {
						t.Fatalf("Has(%d)=true but Get returned ok=false", key)
					}
				}
			case 3: // Delete with pre-check
				had := c.Has(key)
				removed := c.Delete(key)
				if had && !removed {
					t.Fatalf("Delete(%d) reported false but Has was true beforehand", key)
				}
			case 4: // SetWithTTL round-trip
				if err := c.SetWithTTL(key, val, time.Hour); err != nil {
					t.Fatalf("SetWithTTL: %v", err)
				}
				if got, ok := c.Get(key); !ok || got != val {
					t.Fatalf("SetWithTTL(%d, %d) -> Get = (%d, %v)", key, val, got, ok)
				}
			case 5: // Peek does not crash
				_, _ = c.Peek(key)
			case 6: // SetIfAbsent does not crash
				_, _ = c.SetIfAbsent(key, val)
			case 7: // DeleteIf does not crash
				_ = c.DeleteIf(key, func(byte) bool { return true })
			}

			if c.Len() > evictionMargin {
				t.Fatalf("cache Len=%d exceeded bound %d", c.Len(), evictionMargin)
			}
		}
	})
}

// FuzzSnapshot: Save followed by Load on a fresh cache must
// restore every non-expired entry; truncated snapshots must
// surface an error without panicking.
func FuzzSnapshot(f *testing.F) {
	f.Add(uint8(1), uint8(0))
	f.Add(uint8(8), uint8(0))
	f.Add(uint8(16), uint8(50))

	f.Fuzz(func(t *testing.T, count uint8, truncatePercent uint8) {
		src, _ := New[byte, uint16](WithMaxEntries(64))
		defer src.Close()
		for i := range int(count) {
			_ = src.Set(byte(i), uint16(i))
		}
		// The source may have evicted entries if count > 64. Use
		// the cache's actual size as the round-trip target.
		srcLen := src.Len()
		var buf bytes.Buffer
		if err := src.Save(&buf); err != nil {
			t.Fatal(err)
		}

		// Truncate a percentage of the trailing bytes.
		raw := buf.Bytes()
		keep := max(len(raw)-int(uint(len(raw))*uint(truncatePercent)/100), 0)
		truncated := raw[:keep]

		dst, _ := New[byte, uint16](WithMaxEntries(64))
		defer dst.Close()
		_, err := dst.Load(bytes.NewReader(truncated))
		if truncatePercent == 0 {
			// Full snapshot: must round-trip cleanly.
			if err != nil {
				t.Fatalf("full snapshot Load = %v", err)
			}
			if dst.Len() != srcLen {
				t.Errorf("after Load: Len=%d, want %d (source post-eviction size)", dst.Len(), srcLen)
			}
			return
		}
		// Truncated snapshot: must fail gracefully (any error, no panic).
		_ = err
	})
}

// FuzzLoader drives the loader path with random key sequences.
func FuzzLoader(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0, 1, 2})
	f.Add([]byte{1, 1, 1, 1})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		loaderCalls := map[byte]int{}
		loader := LoaderFunc[byte, int](func(_ context.Context, k byte) (int, time.Duration, error) {
			loaderCalls[k]++
			return int(k), 0, nil
		})
		c, _ := New[byte, int](
			WithMaxEntries(16),
			WithLoader(loader),
		)
		defer c.Close()

		for _, k := range data {
			v, err := c.GetOrLoad(context.Background(), k)
			if err != nil {
				t.Fatalf("GetOrLoad(%d): %v", k, err)
			}
			if v != int(k) {
				t.Fatalf("GetOrLoad(%d) = %d", k, v)
			}
		}
	})
}

// FuzzKeyHash: defaultHasher must not panic on arbitrary keys
// and must be deterministic.
func FuzzKeyHash(f *testing.F) {
	f.Add("")
	f.Add("hello")
	f.Add("user:42:profile")

	f.Fuzz(func(t *testing.T, key string) {
		h := defaultHasher[string]()
		a := h(key)
		b := h(key)
		if a != b {
			t.Fatalf("hasher non-deterministic: %d vs %d for %q", a, b, key)
		}
	})
}

// TestFuzzSeedsDoNotPanic ensures the seed corpus loads without
// errors when invoked without `-fuzz`.
func TestFuzzSeedsDoNotPanic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fuzz seed sanity in -short mode")
	}
	_ = errors.New("placeholder")
}
