package memcache

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestItemsReturnsLiveEntries(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for k, v := range map[string]int{"a": 1, "b": 2, "c": 3} {
		_ = c.Set(k, v)
	}
	got := c.Items()
	if len(got) != 3 {
		t.Errorf("Items() returned %d, want 3", len(got))
	}
	seen := map[string]int{}
	for _, ki := range got {
		seen[ki.Key] = ki.Value
	}
	if seen["a"] != 1 || seen["b"] != 2 || seen["c"] != 3 {
		t.Errorf("Items() values = %v", seen)
	}
}

func TestItemsSkipsExpired(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	defer c.Close()
	_ = c.SetWithTTL("alive", 1, time.Hour)
	_ = c.SetWithTTL("dead", 2, time.Second)
	clk.Advance(2 * time.Second)
	got := c.Items()
	if len(got) != 1 || got[0].Key != "alive" {
		t.Errorf("Items() after expiry = %v, want only alive", got)
	}
}

func TestItemMetadata(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.SetWithTags("k", 42, "tag1", "tag2")
	_, _ = c.Get("k") // bump hits
	_, _ = c.Get("k")

	m, ok := c.ItemMetadata("k")
	if !ok {
		t.Fatal("ItemMetadata: key not found")
	}
	if m.Hits != 2 {
		t.Errorf("Hits = %d, want 2", m.Hits)
	}
	if len(m.Tags) != 2 {
		t.Errorf("Tags = %v, want len 2", m.Tags)
	}
}

func TestItemMetadataMissingKey(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	if _, ok := c.ItemMetadata("missing"); ok {
		t.Error("ItemMetadata on absent should report ok=false")
	}
}

func TestHottest(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for k, v := range map[string]int{"a": 1, "b": 2, "c": 3} {
		_ = c.Set(k, v)
	}
	// Bump b's hit count more than the others.
	for range 10 {
		_, _ = c.Get("b")
	}
	for range 5 {
		_, _ = c.Get("a")
	}
	got := c.Hottest(2)
	if len(got) != 2 {
		t.Fatalf("Hottest(2) returned %d entries", len(got))
	}
	if got[0].Key != "b" {
		t.Errorf("Hottest[0] = %s, want b", got[0].Key)
	}
	if got[1].Key != "a" {
		t.Errorf("Hottest[1] = %s, want a", got[1].Key)
	}
}

func TestColdest(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("a", 1)
	_ = c.Set("b", 2)
	for range 5 {
		_, _ = c.Get("a")
	}
	got := c.Coldest(1)
	if len(got) != 1 || got[0].Key != "b" {
		t.Errorf("Coldest(1) = %v, want b first", got)
	}
}

func TestHottestNZeroReturnsAll(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(k, 0)
	}
	got := c.Hottest(0)
	if len(got) != 3 {
		t.Errorf("Hottest(0) returned %d, want 3", len(got))
	}
}

func TestLongestLived(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	defer c.Close()
	_ = c.Set("first", 1)
	clk.Advance(time.Second)
	_ = c.Set("second", 2)
	clk.Advance(time.Second)
	_ = c.Set("third", 3)

	got := c.LongestLived(2)
	if len(got) != 2 {
		t.Fatalf("got %d, want 2", len(got))
	}
	if got[0].Key != "first" || got[1].Key != "second" {
		t.Errorf("order = %v, %v; want first, second", got[0].Key, got[1].Key)
	}
}

func TestSoonestExpiring(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](WithMaxEntries(8), WithClock(clk))
	defer c.Close()
	_ = c.Set("no-ttl", 1)
	_ = c.SetWithTTL("soon", 2, 5*time.Second)
	_ = c.SetWithTTL("later", 3, time.Hour)

	got := c.SoonestExpiring(0)
	// no-ttl is excluded
	if len(got) != 2 {
		t.Fatalf("SoonestExpiring excluded no-TTL? got %d entries", len(got))
	}
	if got[0].Key != "soon" {
		t.Errorf("[0] = %s, want soon", got[0].Key)
	}
}

func TestHistogram(t *testing.T) {
	c, _ := New[string, []byte](
		WithMaxBytes(1<<20),
		WithWeigher(BytesWeigher()),
	)
	defer c.Close()
	_ = c.Set("a", make([]byte, 8))    // weight bucket 1 (1..15)
	_ = c.Set("b", make([]byte, 1024)) // weight bucket 3 (256..4095)
	_ = c.Set("c", make([]byte, 100))  // weight bucket 2 (16..255)
	for range 5 {
		_, _ = c.Get("a")
	}

	h := c.Histogram()
	if h.TotalEntries != 3 {
		t.Errorf("TotalEntries = %d, want 3", h.TotalEntries)
	}
	if h.TotalWeight == 0 {
		t.Error("TotalWeight = 0")
	}
	// At least one entry should land in each of weight buckets 1/2/3.
	if h.WeightBuckets[1]+h.WeightBuckets[2]+h.WeightBuckets[3] < 3 {
		t.Errorf("expected entries spread across weight buckets 1-3; got %v",
			h.WeightBuckets)
	}
}

func TestDump(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	var buf bytes.Buffer
	if err := c.Dump(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "memcache dump") {
		t.Errorf("dump missing header: %q", out)
	}
	if !strings.Contains(out, "key=k") {
		t.Errorf("dump missing entry: %q", out)
	}
}

func TestAgeBucket(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want int
	}{
		{500 * time.Millisecond, 0},
		{2 * time.Second, 1},
		{30 * time.Second, 2},
		{5 * time.Minute, 3},
		{30 * time.Minute, 4},
		{12 * time.Hour, 5},
		{3 * 24 * time.Hour, 6},
		{30 * 24 * time.Hour, 7},
	}
	for _, tc := range cases {
		if got := ageBucket(tc.d); got != tc.want {
			t.Errorf("ageBucket(%v) = %d, want %d", tc.d, got, tc.want)
		}
	}
}

func TestWeightBucket(t *testing.T) {
	cases := []struct {
		w    int64
		want int
	}{
		{0, 0}, {1, 1}, {15, 1}, {16, 2}, {255, 2},
		{256, 3}, {4095, 3}, {4096, 4}, {65535, 4},
		{65536, 5}, {1024 * 1024, 6}, {32 * 1024 * 1024, 7},
	}
	for _, tc := range cases {
		if got := weightBucket(tc.w); got != tc.want {
			t.Errorf("weightBucket(%d) = %d, want %d", tc.w, got, tc.want)
		}
	}
}

func TestHitsBucket(t *testing.T) {
	cases := []struct {
		h    uint32
		want int
	}{
		{0, 0}, {1, 1}, {2, 2}, {3, 2}, {4, 3},
		{15, 3}, {16, 4}, {64, 5}, {255, 5}, {256, 6},
	}
	for _, tc := range cases {
		if got := hitsBucket(tc.h); got != tc.want {
			t.Errorf("hitsBucket(%d) = %d, want %d", tc.h, got, tc.want)
		}
	}
}
