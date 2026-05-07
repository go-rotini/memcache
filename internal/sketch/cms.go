package sketch

// CountMinSketch is a 4-bit-counter count-min sketch with conservative
// update, used by W-TinyLFU as the frequency estimator.
//
// The sketch has `depth` rows of `width` 4-bit counters each. Two 4-bit
// counters share each byte of the underlying storage. Width is rounded
// up to the next power of two so that hash-to-column reduction is a bit
// mask, not a modulo.
//
// "Conservative update" means: when incrementing a key's counters, only
// the minimum-valued counter(s) are incremented. This reduces overcount
// from hash collisions at the cost of slightly more work per update.
//
// Counters saturate at 15 (the max representable value in 4 bits). To
// preserve recency information over long timescales the sketch supports
// Reset, which halves every counter.
type CountMinSketch struct {
	rows  [][]byte // depth rows; each row is width/2 bytes
	mask  uint64   // width - 1 (width is power of two)
	depth uint64
	seeds []uint64 // per-row seed for hash decorrelation
}

// New constructs a CountMinSketch sized for the given expected key count.
// It picks width = next-power-of-two(max(64, expected*10)) and depth=4,
// matching Caffeine/W-TinyLFU defaults. seeds must contain at least
// `depth` random uint64 values; only the first `depth` are used.
func New(expected int, seeds []uint64) *CountMinSketch {
	const depth = 4
	if len(seeds) < depth {
		// Caller misuse: self-derive seeds. Not HashDoS-resistant but
		// functional for testing.
		base := uint64(0xa3f7c1d2e8b95406)
		filled := make([]uint64, depth)
		for i := range depth {
			filled[i] = MixUint64(base + uint64(i))
		}
		seeds = filled
	}
	w := 64
	target := max(expected*10, 64)
	for w < target {
		w <<= 1
	}
	rows := make([][]byte, depth)
	for i := range rows {
		rows[i] = make([]byte, w/2)
	}
	return &CountMinSketch{
		rows:  rows,
		mask:  uint64(w - 1),
		depth: uint64(depth),
		seeds: seeds[:depth],
	}
}

// Estimate returns the minimum across all rows of the counter for hash h.
// The estimate is an upper bound on the true frequency.
func (c *CountMinSketch) Estimate(h uint64) uint8 {
	lowest := uint8(15)
	for i := range c.depth {
		col := MixUint64(h^c.seeds[i]) & c.mask
		v := c.read(int(i), col)
		if v < lowest {
			lowest = v
		}
	}
	return lowest
}

// Increment adds 1 to all of hash h's counters, clamped at 15, using
// conservative update. Only counters that equal the current minimum are
// incremented.
func (c *CountMinSketch) Increment(h uint64) {
	lowest := uint8(15)
	cols := [4]uint64{}
	for i := range c.depth {
		col := MixUint64(h^c.seeds[i]) & c.mask
		cols[i] = col
		v := c.read(int(i), col)
		if v < lowest {
			lowest = v
		}
	}
	if lowest >= 15 {
		return
	}
	for i := range c.depth {
		col := cols[i]
		v := c.read(int(i), col)
		if v == lowest {
			c.write(int(i), col, v+1)
		}
	}
}

// Reset halves every counter. Called periodically by W-TinyLFU to keep
// frequency estimates responsive to recent workload changes.
func (c *CountMinSketch) Reset() {
	for ri := range c.rows {
		row := c.rows[ri]
		for i := range row {
			// Each byte holds two counters; halve each independently.
			b := row[i]
			low := (b & 0x0f) >> 1
			high := (b & 0xf0) >> 1 & 0xf0
			row[i] = high | low
		}
	}
}

// Width returns the number of columns (counters per row). Useful for
// debugging and tests.
func (c *CountMinSketch) Width() int { return int(c.mask) + 1 }

// Depth returns the number of rows. Useful for debugging and tests.
func (c *CountMinSketch) Depth() int { return int(c.depth) }

// read returns the 4-bit counter at (row, col).
func (c *CountMinSketch) read(row int, col uint64) uint8 {
	b := c.rows[row][col/2]
	if col&1 == 0 {
		return b & 0x0f
	}
	return (b >> 4) & 0x0f
}

// write stores v (clamped to 4 bits) at (row, col).
func (c *CountMinSketch) write(row int, col uint64, v uint8) {
	v &= 0x0f
	off := col / 2
	b := c.rows[row][off]
	if col&1 == 0 {
		b = (b & 0xf0) | v
	} else {
		b = (b & 0x0f) | (v << 4)
	}
	c.rows[row][off] = b
}
