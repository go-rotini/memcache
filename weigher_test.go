package memcache

import "testing"

func TestUnitWeigher(t *testing.T) {
	w := UnitWeigher[string]()
	if got := w("anything"); got != 1 {
		t.Errorf("UnitWeigher should always return 1, got %d", got)
	}
	if got := w(""); got != 1 {
		t.Errorf("UnitWeigher should return 1 even for zero value, got %d", got)
	}
}

func TestStringWeigher(t *testing.T) {
	w := StringWeigher()
	if got := w(""); got != 0 {
		t.Errorf("StringWeigher empty: got %d, want 0", got)
	}
	if got := w("hello"); got != 5 {
		t.Errorf("StringWeigher %q: got %d, want 5", "hello", got)
	}
}

func TestBytesWeigher(t *testing.T) {
	w := BytesWeigher()
	if got := w(nil); got != 0 {
		t.Errorf("BytesWeigher nil: got %d, want 0", got)
	}
	if got := w([]byte("hi")); got != 2 {
		t.Errorf("BytesWeigher: got %d, want 2", got)
	}
}

func TestClampWeight(t *testing.T) {
	cases := []struct{ in, want int64 }{
		{-100, 1},
		{-1, 1},
		{0, 1},
		{1, 1},
		{2, 2},
		{100, 100},
	}
	for _, c := range cases {
		if got := clampWeight(c.in); got != c.want {
			t.Errorf("clampWeight(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestWithMaxBytesEnforcedForEveryPolicy(t *testing.T) {
	for _, p := range []Policy{PolicyLRU, PolicyFIFO, PolicyLFU, PolicyS3FIFO, PolicyTinyLFU, Policy2Q, PolicyARC} {
		// maxEntries 0 is bytes-only; 1000 is an entry bound the byte
		// budget hits first.
		for _, maxEntries := range []int{0, 1000} {
			t.Run(p.String(), func(t *testing.T) {
				testMaxBytesEnforced(t, p, maxEntries)
			})
		}
	}
}

func testMaxBytesEnforced(t *testing.T, p Policy, maxEntries int) {
	t.Helper()
	c, err := New[int, string](WithMaxEntries(maxEntries), WithMaxBytes(100), WithWeigher(StringWeigher()), WithShards(4), WithPolicy(p))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := range 200 {
		_ = c.Set(i, "0123456789")
		if got := c.Bytes(); got > 100 {
			t.Fatalf("after %d sets Bytes() = %d, want <= 100", i+1, got)
		}
	}
	if c.Len() == 0 {
		t.Fatal("cache evicted everything")
	}
	evicted := c.Resize(50)
	if got := c.Bytes(); got > 50 || evicted == 0 {
		t.Fatalf("after Resize(50): Bytes() = %d, evicted = %d", got, evicted)
	}
	// A heavy insert sheds from other shards too.
	_ = c.Set(-1, string(make([]byte, 45)))
	if got := c.Bytes(); got > 50 {
		t.Fatalf("after heavy Set: Bytes() = %d, want <= 50", got)
	}
	if !c.Has(-1) {
		t.Fatal("heavy Set evicted its own entry while others remained")
	}
}
