package memcache

import (
	"expvar"
	"strconv"
	"sync"
)

// expvarSlots holds the [expvar.Var] memcache published for each
// [WithExpvar] name. A slot reports the stats of the live cache that
// holds it; Close releases it, so a later cache with the same name
// takes it over instead of the closed cache's stats being reported.
var (
	expvarMu    sync.Mutex
	expvarSlots = map[string]*expvarSlot{}
)

// expvarSlot is the [expvar.Var] published under one name.
type expvarSlot struct {
	mu     sync.Mutex
	owner  any
	render func() string
}

// String implements [expvar.Var]. A slot with no live cache renders {}.
func (s *expvarSlot) String() string {
	s.mu.Lock()
	render := s.render
	s.mu.Unlock()
	if render == nil {
		return "{}"
	}
	return render()
}

// expvarFunc renders a subset of the cache's current [Stats] as a JSON
// object.
type expvarFunc[K comparable, V any] struct {
	c *Cache[K, V]
}

// checkExpvarName reports a [*ConfigError] when name is already in
// expvar under a variable memcache didn't publish.
func checkExpvarName(name string) error {
	if name == "" {
		return nil
	}
	expvarMu.Lock()
	defer expvarMu.Unlock()
	if _, ours := expvarSlots[name]; !ours && expvar.Get(name) != nil {
		return &ConfigError{Field: "Expvar", Message: "name " + strconv.Quote(name) + " is already published in expvar"}
	}
	return nil
}

// publishExpvar registers c's Stats under the configured name. When a
// live cache already holds the name, it is a no-op.
func publishExpvar[K comparable, V any](c *Cache[K, V]) {
	name := c.cfg.expvarName
	if name == "" {
		return
	}
	expvarMu.Lock()
	defer expvarMu.Unlock()
	slot, ok := expvarSlots[name]
	if !ok {
		if expvar.Get(name) != nil {
			return
		}
		slot = &expvarSlot{}
		expvarSlots[name] = slot
		expvar.Publish(name, slot)
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.owner != nil {
		return
	}
	slot.owner = c
	slot.render = expvarFunc[K, V]{c: c}.String
}

// unpublishExpvar releases c's expvar name, if it holds it.
func unpublishExpvar[K comparable, V any](c *Cache[K, V]) {
	name := c.cfg.expvarName
	if name == "" {
		return
	}
	expvarMu.Lock()
	slot := expvarSlots[name]
	expvarMu.Unlock()
	if slot == nil {
		return
	}
	slot.mu.Lock()
	if slot.owner == any(c) {
		slot.owner, slot.render = nil, nil
	}
	slot.mu.Unlock()
}

// String implements [expvar.Var].
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
