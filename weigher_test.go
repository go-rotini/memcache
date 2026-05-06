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
