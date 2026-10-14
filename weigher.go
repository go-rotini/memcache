package memcache

// Weigher returns the weight of a value, used with WithMaxBytes to bound
// the cache by total weight rather than entry count. Implementations must
// be deterministic; weights are clamped to the range 1 to 1<<40.
type Weigher[V any] func(V) int64

// UnitWeigher returns a Weigher that reports 1 for every value.
func UnitWeigher[V any]() Weigher[V] {
	return func(V) int64 { return 1 }
}

// StringWeigher returns a Weigher that reports the byte length of a string.
func StringWeigher() Weigher[string] {
	return func(s string) int64 { return int64(len(s)) }
}

// BytesWeigher returns a Weigher that reports the byte length of a byte slice.
func BytesWeigher() Weigher[[]byte] {
	return func(b []byte) int64 { return int64(len(b)) }
}

// maxEntryWeight caps one entry's weight, so that summing weights into
// [Cache.Bytes] cannot overflow for any cache that fits in memory.
const maxEntryWeight = 1 << 40

// clampWeight bounds w to [1, maxEntryWeight].
func clampWeight(w int64) int64 {
	return min(max(w, 1), maxEntryWeight)
}
