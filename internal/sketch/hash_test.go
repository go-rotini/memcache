package sketch

import "testing"

// SipHash-2-4 reference vector from the official paper.
// Key (k0, k1) is bytes 00..0f little-endian; data = 00..0e (15 bytes);
// expected: 0xa129ca6149be45e5.
func TestSipHash24ReferenceVector(t *testing.T) {
	const (
		k0 = uint64(0x0706050403020100)
		k1 = uint64(0x0f0e0d0c0b0a0908)
	)
	data := make([]byte, 15)
	for i := range data {
		data[i] = byte(i)
	}
	got := SipHash24(k0, k1, data)
	const want = uint64(0xa129ca6149be45e5)
	if got != want {
		t.Fatalf("SipHash24 reference vector mismatch: got %#x want %#x", got, want)
	}
}

func TestSipHash24EmptyInput(t *testing.T) {
	const (
		k0 = uint64(0x0706050403020100)
		k1 = uint64(0x0f0e0d0c0b0a0908)
	)
	got := SipHash24(k0, k1, nil)
	got2 := SipHash24(k0, k1, []byte{})
	if got != got2 {
		t.Fatalf("nil and empty slice should hash identically: %#x vs %#x", got, got2)
	}
}

func TestSipHash24DistinctKeys(t *testing.T) {
	d := []byte("hello, world")
	a := SipHash24(1, 2, d)
	b := SipHash24(3, 4, d)
	if a == b {
		t.Fatalf("distinct keys should not collide on this input: %#x", a)
	}
}

func TestMixUint64Avalanche(t *testing.T) {
	for i := uint64(0); i < 100; i++ {
		a := MixUint64(i)
		b := MixUint64(i + 1)
		// Hamming distance between a and b should be > 8 (out of 64).
		diff := a ^ b
		bits := 0
		for diff != 0 {
			bits++
			diff &= diff - 1
		}
		if bits < 8 {
			t.Fatalf("MixUint64 avalanche failure at %d: hamming=%d", i, bits)
		}
	}
}

func TestHashStringMatchesSipHash(t *testing.T) {
	s := "the quick brown fox"
	a := HashString(11, 22, s)
	b := SipHash24(11, 22, []byte(s))
	if a != b {
		t.Fatalf("HashString should match SipHash24 over bytes: %#x vs %#x", a, b)
	}
}
