package sketch

import "testing"

func TestCountMinSketchBasic(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	c := New(1024, seeds)

	if c.Estimate(42) != 0 {
		t.Fatal("freshly constructed sketch should report 0")
	}

	for i := 0; i < 5; i++ {
		c.Increment(42)
	}
	if got := c.Estimate(42); got < 5 {
		t.Fatalf("after 5 increments, estimate should be >=5, got %d", got)
	}
}

func TestCountMinSketchSaturation(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	c := New(64, seeds)
	for i := 0; i < 100; i++ {
		c.Increment(7)
	}
	if got := c.Estimate(7); got != 15 {
		t.Fatalf("counter should saturate at 15, got %d", got)
	}
}

func TestCountMinSketchReset(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	c := New(64, seeds)
	for i := 0; i < 10; i++ {
		c.Increment(99)
	}
	before := c.Estimate(99)
	c.Reset()
	after := c.Estimate(99)
	if after >= before {
		t.Fatalf("Reset should halve counters: before=%d after=%d", before, after)
	}
	if after > before/2+1 {
		t.Fatalf("Reset overshoot: before=%d after=%d", before, after)
	}
}

func TestCountMinSketchWidthIsPowerOfTwo(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	for _, expected := range []int{1, 10, 100, 1000, 10000} {
		c := New(expected, seeds)
		w := c.Width()
		if w&(w-1) != 0 {
			t.Errorf("Width must be a power of two; expected=%d width=%d", expected, w)
		}
	}
}

func TestCountMinSketchDistinctKeysIndependent(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	c := New(2048, seeds)

	const a, b = uint64(0xaaaa), uint64(0xbbbb)
	for i := 0; i < 50; i++ {
		c.Increment(a)
	}
	bv := c.Estimate(b)
	if bv > 5 {
		t.Fatalf("untouched key should have low estimate; got %d", bv)
	}
}

func TestCountMinSketchConservativeUpdate(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	c := New(64, seeds)

	// Conservative update: incrementing a single key should produce an
	// estimate that grows at the increment rate.
	for i := 1; i <= 10; i++ {
		c.Increment(1234)
		got := c.Estimate(1234)
		if int(got) != i {
			t.Fatalf("after %d increments, estimate=%d (want %d)", i, got, i)
		}
	}
}
