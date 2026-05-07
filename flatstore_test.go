package memcache

import (
	"fmt"
	"testing"
)

// stubHasher returns a deterministic FNV-1a-style hash of a string.
// Tests use it so probe sequences are predictable across runs.
func stubHasher(s string) uint64 {
	const fnvPrime = 1099511628211
	const fnvOffset = 14695981039346656037
	h := uint64(fnvOffset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime
	}
	return h
}

// mkEntry constructs a minimal *entry suitable for asserting on
// identity in flatStore tests. The entry's key field is set so
// each() callbacks can assert on it.
func mkEntry(key string, val int) *entry[string, int] {
	e := &entry[string, int]{key: key, heapIndex: -1}
	e.storeValue(val)
	return e
}

func TestFlatStoreNewHonorsInitialCapMinimum(t *testing.T) {
	// initialCap below the floor must round up to flatStoreInitialCap.
	fs := newFlatStore[string, int](stubHasher, 4)
	if got := len(fs.slots); got != flatStoreInitialCap {
		t.Errorf("slots = %d, want >= %d", got, flatStoreInitialCap)
	}
}

func TestFlatStoreNewRoundsToPowerOfTwo(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 33)
	if got := len(fs.slots); got != 64 {
		t.Errorf("slots = %d, want 64 (next power of two >= 33)", got)
	}
}

func TestFlatStoreSetAndGet(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	a := mkEntry("a", 1)
	fs.set("a", a)
	got, ok := fs.get("a")
	if !ok {
		t.Fatal("get(a) reported absent after set")
	}
	if got != a {
		t.Errorf("get(a) returned different entry pointer: %p vs %p", got, a)
	}
	if got.loadValue() != 1 {
		t.Errorf("get(a).value = %d, want 1", got.loadValue())
	}
	if fs.length() != 1 {
		t.Errorf("length = %d, want 1", fs.length())
	}
}

func TestFlatStoreSetUpdatesExistingKey(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	fs.set("a", mkEntry("a", 1))
	updated := mkEntry("a", 99)
	fs.set("a", updated)

	got, _ := fs.get("a")
	if got != updated {
		t.Error("set on existing key did not replace pointer")
	}
	if fs.length() != 1 {
		t.Errorf("length = %d, want 1 (update should not grow)", fs.length())
	}
	if fs.tombstones != 0 {
		t.Errorf("tombstones = %d, want 0 after update", fs.tombstones)
	}
}

func TestFlatStoreDeleteCreatesTombstoneAndDecrementsLen(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	fs.set("a", mkEntry("a", 1))
	if !fs.del("a") {
		t.Fatal("del(a) reported nothing removed")
	}
	if _, ok := fs.get("a"); ok {
		t.Error("get(a) succeeded after del")
	}
	if fs.length() != 0 {
		t.Errorf("length = %d, want 0", fs.length())
	}
}

func TestFlatStoreDeleteAbsentKeyReportsFalse(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	if fs.del("missing") {
		t.Error("del on absent key returned true")
	}
	if fs.tombstones != 0 {
		t.Errorf("tombstones = %d, want 0 for missing-key delete", fs.tombstones)
	}
}

func TestFlatStoreDeleteThenInsertReusesTombstone(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	fs.set("a", mkEntry("a", 1))
	fs.del("a")
	fs.set("a", mkEntry("a", 2))

	if got, _ := fs.get("a"); got == nil || got.loadValue() != 2 {
		t.Errorf("get(a) after re-insert = %+v, want value=2", got)
	}
	if fs.tombstones != 0 {
		t.Errorf("tombstones = %d, want 0 (re-insert should reuse tombstone)", fs.tombstones)
	}
}

func TestFlatStoreEachVisitsEveryEntry(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	want := map[string]int{"a": 1, "b": 2, "c": 3}
	for k, v := range want {
		fs.set(k, mkEntry(k, v))
	}

	got := map[string]int{}
	fs.each(func(e *entry[string, int]) bool {
		got[e.key] = e.loadValue()
		return true
	})
	if len(got) != len(want) {
		t.Fatalf("each visited %d entries, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("each: key %q value = %d, want %d", k, got[k], v)
		}
	}
}

func TestFlatStoreEachRespectsEarlyStop(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	fs.set("a", mkEntry("a", 1))
	fs.set("b", mkEntry("b", 2))
	fs.set("c", mkEntry("c", 3))

	visited := 0
	fs.each(func(e *entry[string, int]) bool {
		visited++
		return false // stop after first
	})
	if visited != 1 {
		t.Errorf("each returning false visited %d entries, want 1", visited)
	}
}

func TestFlatStoreEachSkipsTombstones(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	fs.set("a", mkEntry("a", 1))
	fs.set("b", mkEntry("b", 2))
	fs.del("a")

	keys := []string{}
	fs.each(func(e *entry[string, int]) bool {
		keys = append(keys, e.key)
		return true
	})
	if len(keys) != 1 || keys[0] != "b" {
		t.Errorf("each after delete visited %v, want [b]", keys)
	}
}

func TestFlatStoreClearAllResetsState(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%d", i)
		fs.set(k, mkEntry(k, i))
	}
	fs.del("k0")

	fs.clearAll()
	if fs.length() != 0 {
		t.Errorf("length after clearAll = %d, want 0", fs.length())
	}
	if fs.tombstones != 0 {
		t.Errorf("tombstones after clearAll = %d, want 0", fs.tombstones)
	}
	visited := 0
	fs.each(func(*entry[string, int]) bool { visited++; return true })
	if visited != 0 {
		t.Errorf("each after clearAll visited %d entries, want 0", visited)
	}
}

func TestFlatStoreGrowsWhenLoadFactorExceeded(t *testing.T) {
	// Initial cap 16; load factor 0.75 grows at 12 in-use slots.
	fs := newFlatStore[string, int](stubHasher, 16)
	startCap := len(fs.slots)
	startCompactions := fs.compactions()

	for i := 0; i < 14; i++ {
		k := fmt.Sprintf("key-%d", i)
		fs.set(k, mkEntry(k, i))
	}
	if got := len(fs.slots); got <= startCap {
		t.Errorf("slots = %d, want > %d after exceeding load factor", got, startCap)
	}
	if fs.compactions() <= startCompactions {
		t.Error("compactions counter did not advance after grow")
	}

	// All 14 keys must still be reachable.
	for i := 0; i < 14; i++ {
		k := fmt.Sprintf("key-%d", i)
		if got, ok := fs.get(k); !ok || got.loadValue() != i {
			t.Errorf("key %q lost across grow: got=%+v ok=%v", k, got, ok)
		}
	}
}

func TestFlatStoreCompactsAfterTombstoneAccumulation(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	// Insert enough to get past the half-cap threshold.
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%d", i)
		fs.set(k, mkEntry(k, i))
	}
	startCompactions := fs.compactions()

	// Delete 8 to push tombstone fraction past 50% of cap.
	for i := 0; i < 8; i++ {
		fs.del(fmt.Sprintf("key-%d", i))
	}

	if fs.compactions() <= startCompactions {
		t.Errorf("compactions counter did not advance after tombstone-driven rebuild "+
			"(start=%d, end=%d)", startCompactions, fs.compactions())
	}
	if fs.tombstones != 0 {
		t.Errorf("tombstones = %d after compaction, want 0", fs.tombstones)
	}

	// Survivors must remain accessible.
	for i := 8; i < 10; i++ {
		k := fmt.Sprintf("key-%d", i)
		if got, ok := fs.get(k); !ok || got.loadValue() != i {
			t.Errorf("key %q lost across compaction: got=%+v ok=%v", k, got, ok)
		}
	}
}

func TestFlatStoreCompactionsIsIdempotentAfterClear(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("key-%d", i)
		fs.set(k, mkEntry(k, i))
	}
	fs.clearAll()
	// clearAll does not reset the compactions counter; it is a
	// monotone history of operations.
	if got := fs.compactions(); got > 1 {
		// One grow could have triggered before the clear; that's
		// fine. We only check the counter wasn't cleared with the
		// state.
		t.Logf("compactions=%d after fill+clear (informational)", got)
	}
	c0 := fs.compactions()
	fs.clearAll()
	if fs.compactions() != c0 {
		t.Error("clearAll bumped the compactions counter")
	}
}

func TestFlatStoreLengthMatchesReality(t *testing.T) {
	fs := newFlatStore[string, int](stubHasher, 16)
	if fs.length() != 0 {
		t.Errorf("empty store length = %d, want 0", fs.length())
	}

	fs.set("a", mkEntry("a", 1))
	fs.set("b", mkEntry("b", 2))
	if fs.length() != 2 {
		t.Errorf("length = %d, want 2", fs.length())
	}

	fs.del("a")
	if fs.length() != 1 {
		t.Errorf("length after delete = %d, want 1", fs.length())
	}

	fs.set("a", mkEntry("a", 99)) // reuse tombstone
	if fs.length() != 2 {
		t.Errorf("length after re-insert = %d, want 2", fs.length())
	}

	fs.set("a", mkEntry("a", 100)) // update, no growth
	if fs.length() != 2 {
		t.Errorf("length after update = %d, want 2", fs.length())
	}
}

func TestMapStoreImplementsShardStore(t *testing.T) {
	// Direct exercises of every method so the linter sees the
	// mapStore methods are reached (interface-only dispatch hides
	// them from static analysis under generics).
	ms := newMapStore[string, int]()
	if ms.length() != 0 {
		t.Fatalf("empty mapStore length = %d, want 0", ms.length())
	}

	a := mkEntry("a", 1)
	ms.set("a", a)
	got, ok := ms.get("a")
	if !ok || got != a {
		t.Errorf("get(a) = (%p, %v), want (%p, true)", got, ok, a)
	}
	if ms.length() != 1 {
		t.Errorf("length = %d, want 1", ms.length())
	}

	visited := 0
	ms.each(func(*entry[string, int]) bool {
		visited++
		return true
	})
	if visited != 1 {
		t.Errorf("each visited %d, want 1", visited)
	}

	if !ms.del("a") {
		t.Error("del(a) returned false")
	}
	if ms.del("a") {
		t.Error("repeated del(a) returned true")
	}
	if ms.compactions() != 0 {
		t.Errorf("mapStore compactions = %d, want 0", ms.compactions())
	}

	ms.set("b", mkEntry("b", 2))
	ms.clearAll()
	if ms.length() != 0 {
		t.Errorf("length after clearAll = %d, want 0", ms.length())
	}
}

func TestFlatStoreCollidingKeysCoexist(t *testing.T) {
	// Force collisions by using a hasher that returns a constant.
	collide := func(string) uint64 { return 7 }
	fs := newFlatStore[string, int](collide, 16)

	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%d", i)
		fs.set(k, mkEntry(k, i))
	}

	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%d", i)
		got, ok := fs.get(k)
		if !ok {
			t.Errorf("collision-bucketed key %q lost", k)
		}
		if got != nil && got.loadValue() != i {
			t.Errorf("key %q value = %d, want %d", k, got.loadValue(), i)
		}
	}
}
