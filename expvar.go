package memcache

import (
	"expvar"
	"strconv"
	"sync"
)

// expvarRegistry tracks names already published to avoid panics
// from `expvar.Publish` on duplicate registration when the same
// process creates multiple caches with the same name. Subsequent
// caches with a colliding name silently no-op rather than panic.
var expvarRegistry sync.Map // map[string]struct{}

// expvarFunc is published as an [expvar.Func] that returns a
// snapshot of the cache's [Stats]. Reading the var emits the
// current counters; the live entry/byte counts always reflect the
// cache at read time.
//
// The published variable is structured: a `map[string]any` so
// stdlib `expvar.Handler` JSON-encodes it cleanly. Callers
// integrating with /debug/vars get the full Stats shape with one
// option call.
type expvarFunc[K comparable, V any] struct {
	c *Cache[K, V]
}

// publishExpvar registers c's Stats under the configured name.
// Idempotent across processes: a duplicate registration silently
// no-ops rather than panicking.
func publishExpvar[K comparable, V any](c *Cache[K, V]) {
	name := c.cfg.expvarName
	if name == "" {
		return
	}
	if _, loaded := expvarRegistry.LoadOrStore(name, struct{}{}); loaded {
		return
	}
	expvar.Publish(name, expvarFunc[K, V]{c: c})
}

// Value implements [expvar.Var].
func (f expvarFunc[K, V]) String() string {
	st := f.c.Stats()
	// Tiny hand-rolled JSON encoder so the package keeps zero
	// runtime deps and we don't pull encoding/json into expvar.
	var b []byte
	b = append(b, '{')
	addInt := func(name string, v uint64) {
		b = append(b, '"')
		b = append(b, name...)
		b = append(b, "\":"...)
		b = strconv.AppendUint(b, v, 10)
		b = append(b, ',')
	}
	addInt64 := func(name string, v int64) {
		b = append(b, '"')
		b = append(b, name...)
		b = append(b, "\":"...)
		b = strconv.AppendInt(b, v, 10)
		b = append(b, ',')
	}
	addInt("hits", st.Hits)
	addInt("misses", st.Misses)
	addInt("inserts", st.Inserts)
	addInt("updates", st.Updates)
	addInt("deletes", st.Deletes)
	addInt("evictions", st.Evictions)
	addInt("expirations", st.Expirations)
	addInt("loads_total", st.LoadsTotal)
	addInt("load_hits", st.LoadHits)
	addInt("load_errors", st.LoadErrors)
	addInt("load_coalesced", st.LoadCoalesced)
	addInt("events_dropped", st.EventsDropped)
	addInt64("entries", st.Entries)
	addInt64("bytes", st.Bytes)
	addInt64("capacity", st.Capacity)
	// Hit-rate as a float; multiply by 1000 and emit as integer
	// permille to keep the encoder dependency-free.
	rate := uint64(st.HitRate() * 1000)
	addInt("hit_rate_permille", rate)
	if len(b) > 0 && b[len(b)-1] == ',' {
		b[len(b)-1] = '}'
	} else {
		b = append(b, '}')
	}
	return string(b)
}
