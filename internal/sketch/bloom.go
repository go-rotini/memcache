package sketch

// Bloom is a small bit-array bloom filter used as the W-TinyLFU
// "doorkeeper": one-hit wonders are filtered out before they pollute
// the count-min sketch.
//
// The filter is sized for ~8 bits/key with 4 hash functions, giving a
// false-positive rate of ~3% — adequate for an admission filter where
// the cost of a false-positive (an extra count-min sketch increment)
// is negligible.
type Bloom struct {
	bits  []uint64 // bit array; 64 bits per entry
	mask  uint64   // (bit count - 1); bit count is power of two
	seeds []uint64 // per-hash seed (4 hashes)
}

// NewBloom constructs a Bloom filter sized for the given expected key
// count. seeds must contain at least 4 random uint64 values.
func NewBloom(expected int, seeds []uint64) *Bloom {
	const k = 4
	if len(seeds) < k {
		base := uint64(0x6a09e667f3bcc908)
		filled := make([]uint64, k)
		for i := range k {
			filled[i] = MixUint64(base + uint64(i))
		}
		seeds = filled
	}
	bits := 64
	target := max(expected*8, 64)
	for bits < target {
		bits <<= 1
	}
	return &Bloom{
		bits:  make([]uint64, bits/64),
		mask:  uint64(bits - 1),
		seeds: seeds[:k],
	}
}

// Test reports whether h has been Set since the last Reset. May return
// true for keys that were never set (false positive).
func (b *Bloom) Test(h uint64) bool {
	for _, s := range b.seeds {
		bit := MixUint64(h^s) & b.mask
		if b.bits[bit>>6]&(uint64(1)<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

// Set marks h as seen.
func (b *Bloom) Set(h uint64) {
	for _, s := range b.seeds {
		bit := MixUint64(h^s) & b.mask
		b.bits[bit>>6] |= uint64(1) << (bit & 63)
	}
}

// TestAndSet returns Test(h) and unconditionally Sets h. Useful for the
// W-TinyLFU pattern "if first sighting then admit + record, otherwise
// admit unconditionally".
func (b *Bloom) TestAndSet(h uint64) bool {
	seen := true
	for _, s := range b.seeds {
		bit := MixUint64(h^s) & b.mask
		w := bit >> 6
		mask := uint64(1) << (bit & 63)
		if b.bits[w]&mask == 0 {
			seen = false
		}
		b.bits[w] |= mask
	}
	return seen
}

// Reset clears every bit. Called periodically by W-TinyLFU to keep
// the doorkeeper responsive to recent workload changes.
func (b *Bloom) Reset() {
	for i := range b.bits {
		b.bits[i] = 0
	}
}

// BitCount returns the size of the filter in bits. Useful for tests and
// memory accounting.
func (b *Bloom) BitCount() int { return int(b.mask) + 1 }
