package memcache

// Weigher returns the "weight" of a value. Used with WithMaxBytes to bound
// the cache by total weight rather than entry count.
//
// Implementations should be deterministic: the same value should produce
// the same weight on every call. If the value's weight changes after
// insert (e.g., the value contains a slice that grows), the cache's
// byte-bound accounting will drift; callers should re-insert in such
// cases.
//
// A weight of 0 is treated as 1 to ensure every entry contributes at
// least its slot to the count. Negative weights are also clamped to 1.
type Weigher[V any] func(V) int64

// UnitWeigher returns 1 for every value. This is the default weigher
// used when the cache is bounded by entry count.
func UnitWeigher[V any]() Weigher[V] {
	return func(V) int64 { return 1 }
}

// StringWeigher returns the byte length of a string. Convenient for
// Cache[string, string] or values implementing fmt.Stringer.
func StringWeigher() Weigher[string] {
	return func(s string) int64 { return int64(len(s)) }
}

// BytesWeigher returns the byte length of a byte slice. Convenient for
// Cache[K, []byte].
func BytesWeigher() Weigher[[]byte] {
	return func(b []byte) int64 { return int64(len(b)) }
}

// clampWeight applies the cache's "minimum weight is 1" policy. Internal
// helper used by the cache's accounting paths to keep behavior
// consistent across all configured weighers.
func clampWeight(w int64) int64 {
	if w < 1 {
		return 1
	}
	return w
}
