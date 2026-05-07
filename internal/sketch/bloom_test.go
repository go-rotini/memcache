package sketch

import "testing"

func TestBloomBasic(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	b := NewBloom(1024, seeds)

	if b.Test(42) {
		t.Fatal("freshly constructed bloom filter should not report set")
	}
	b.Set(42)
	if !b.Test(42) {
		t.Fatal("after Set, Test should return true")
	}
}

func TestBloomNoFalseNegatives(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	b := NewBloom(2048, seeds)
	const n = 100
	for i := range uint64(n) {
		b.Set(i)
	}
	for i := range uint64(n) {
		if !b.Test(i) {
			t.Errorf("set key %d incorrectly reported missing (false negative)", i)
		}
	}
}

func TestBloomReset(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	b := NewBloom(1024, seeds)
	b.Set(7)
	if !b.Test(7) {
		t.Fatal("Set/Test")
	}
	b.Reset()
	if b.Test(7) {
		t.Fatal("Reset should clear bits")
	}
}

func TestBloomTestAndSet(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	b := NewBloom(1024, seeds)

	if b.TestAndSet(99) {
		t.Fatal("first TestAndSet should report not-seen (false)")
	}
	if !b.TestAndSet(99) {
		t.Fatal("second TestAndSet should report seen (true)")
	}
}

func TestBloomBitCountIsPowerOfTwo(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4}
	for _, expected := range []int{1, 10, 100, 1000} {
		b := NewBloom(expected, seeds)
		bc := b.BitCount()
		if bc&(bc-1) != 0 {
			t.Errorf("BitCount must be a power of two; expected=%d bits=%d", expected, bc)
		}
	}
}
