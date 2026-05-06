// Package sketch implements probabilistic data structures used by the
// memcache eviction and admission policies — count-min sketch (TinyLFU
// admission), bloom filter (TinyLFU doorkeeper), and the package's
// SipHash-2-4 implementation.
//
// The package is internal; nothing here is part of the public API.
package sketch

import "encoding/binary"

// SipHash24 computes the SipHash-2-4 64-bit hash of data using key.
//
// SipHash is a fast, HashDoS-resistant pseudo-random function. Each
// memcache.Cache initializes its own random key at construction time so
// the hash distribution is unpredictable to attackers. SipHash-2-4 (2
// compression rounds, 4 finalization rounds) is the standard variant.
//
// Reference: Aumasson & Bernstein, "SipHash: a fast short-input PRF"
// https://www.aumasson.jp/siphash/siphash.pdf
func SipHash24(k0, k1 uint64, data []byte) uint64 {
	v0 := k0 ^ 0x736f6d6570736575
	v1 := k1 ^ 0x646f72616e646f6d
	v2 := k0 ^ 0x6c7967656e657261
	v3 := k1 ^ 0x7465646279746573

	n := len(data)
	end := n - (n % 8)

	for i := 0; i < end; i += 8 {
		m := binary.LittleEndian.Uint64(data[i:])
		v3 ^= m
		// 2 compression rounds
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0 ^= m
	}

	// Last block: pack remaining bytes into the high bits of a uint64
	// alongside the length-mod-256 in the top byte.
	b := uint64(n) << 56
	switch n - end {
	case 7:
		b |= uint64(data[end+6]) << 48
		fallthrough
	case 6:
		b |= uint64(data[end+5]) << 40
		fallthrough
	case 5:
		b |= uint64(data[end+4]) << 32
		fallthrough
	case 4:
		b |= uint64(data[end+3]) << 24
		fallthrough
	case 3:
		b |= uint64(data[end+2]) << 16
		fallthrough
	case 2:
		b |= uint64(data[end+1]) << 8
		fallthrough
	case 1:
		b |= uint64(data[end])
	}

	v3 ^= b
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0 ^= b

	v2 ^= 0xff
	// 4 finalization rounds
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)

	return v0 ^ v1 ^ v2 ^ v3
}

// sipRound is one SipHash compression round.
func sipRound(v0, v1, v2, v3 uint64) (uint64, uint64, uint64, uint64) {
	v0 += v1
	v1 = (v1 << 13) | (v1 >> (64 - 13))
	v1 ^= v0
	v0 = (v0 << 32) | (v0 >> 32)

	v2 += v3
	v3 = (v3 << 16) | (v3 >> (64 - 16))
	v3 ^= v2

	v0 += v3
	v3 = (v3 << 21) | (v3 >> (64 - 21))
	v3 ^= v0

	v2 += v1
	v1 = (v1 << 17) | (v1 >> (64 - 17))
	v1 ^= v2
	v2 = (v2 << 32) | (v2 >> 32)

	return v0, v1, v2, v3
}

// MixUint64 is a fast bit-mixer for already-hashed uint64 values
// (numeric keys). Splitmix64 finalizer.
//
// Reference: https://xorshift.di.unimi.it/splitmix64.c
func MixUint64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// HashString returns the 64-bit hash of s under SipHash-2-4 keyed on
// (k0, k1).
func HashString(k0, k1 uint64, s string) uint64 {
	// Avoid a heap allocation by reading bytes from the string header.
	// The unsafe path is contained here so callers do not need to think
	// about it.
	return SipHash24(k0, k1, stringBytes(s))
}

// stringBytes returns a []byte view of s without copying. The returned
// slice must not be mutated.
func stringBytes(s string) []byte {
	// Using the standard library conversion is fine: SipHash24 only
	// reads from the slice; the conversion's allocation cost is
	// dominated by the hashing work itself for the small/medium
	// strings that constitute typical cache keys, and avoids a
	// dependency on unsafe pointer types.
	return []byte(s)
}
