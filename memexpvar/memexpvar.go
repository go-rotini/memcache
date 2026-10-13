// Package memexpvar publishes a [memcache.Cache]'s statistics through
// the standard expvar package.
//
// Importing this package links expvar, whose init registers a
// /debug/vars handler on [net/http.DefaultServeMux] that also serves the
// process's command line and memory statistics. A program that serves
// DefaultServeMux exposes all of it, so serve it only where that is
// acceptable.
package memexpvar

import (
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"sync"

	"github.com/go-rotini/memcache"
)

// Source is what Publish reports on; *[memcache.Cache] implements it.
type Source interface {
	Stats() memcache.Stats
}

var (
	errNoSource  = errors.New("memexpvar: Publish needs a name and a source")
	errNameTaken = errors.New("memexpvar: name is already published in expvar")

	mu    sync.Mutex
	slots = map[string]*slot{}
)

// slot is the [expvar.Var] Publish registers under one name.
type slot struct {
	mu  sync.Mutex
	src Source
}

// Publish publishes src's main [memcache.Stats] counters under name in
// expvar, as a JSON object with snake_case keys (hits, misses, entries,
// bytes, hit_rate_permille and so on). Publishing under a name again
// reports the new src from then on, so a cache that replaces a closed
// one can take over its name. A name that something other than Publish
// registered in expvar is an error.
func Publish(name string, src Source) error {
	if name == "" || src == nil {
		return errNoSource
	}
	mu.Lock()
	defer mu.Unlock()
	s, ok := slots[name]
	if !ok {
		if expvar.Get(name) != nil {
			return fmt.Errorf("%w: %q", errNameTaken, name)
		}
		s = &slot{}
		slots[name] = s
		expvar.Publish(name, s)
	}
	s.mu.Lock()
	s.src = src
	s.mu.Unlock()
	return nil
}

// statsJSON is the published form of [memcache.Stats].
type statsJSON struct {
	Hits            uint64 `json:"hits"`
	Misses          uint64 `json:"misses"`
	Inserts         uint64 `json:"inserts"`
	Updates         uint64 `json:"updates"`
	Deletes         uint64 `json:"deletes"`
	Evictions       uint64 `json:"evictions"`
	Expirations     uint64 `json:"expirations"`
	LoadsTotal      uint64 `json:"loads_total"`
	LoadHits        uint64 `json:"load_hits"`
	LoadErrors      uint64 `json:"load_errors"`
	LoadCoalesced   uint64 `json:"load_coalesced"`
	EventsDropped   uint64 `json:"events_dropped"`
	Entries         int64  `json:"entries"`
	Bytes           int64  `json:"bytes"`
	Capacity        int64  `json:"capacity"`
	HitRatePermille uint64 `json:"hit_rate_permille"`
}

// String implements [expvar.Var].
func (s *slot) String() string {
	s.mu.Lock()
	src := s.src
	s.mu.Unlock()
	st := src.Stats()
	b, err := json.Marshal(statsJSON{
		Hits:            st.Hits,
		Misses:          st.Misses,
		Inserts:         st.Inserts,
		Updates:         st.Updates,
		Deletes:         st.Deletes,
		Evictions:       st.Evictions,
		Expirations:     st.Expirations,
		LoadsTotal:      st.LoadsTotal,
		LoadHits:        st.LoadHits,
		LoadErrors:      st.LoadErrors,
		LoadCoalesced:   st.LoadCoalesced,
		EventsDropped:   st.EventsDropped,
		Entries:         st.Entries,
		Bytes:           st.Bytes,
		Capacity:        st.Capacity,
		HitRatePermille: uint64(st.HitRate() * 1000),
	})
	if err != nil {
		return "{}"
	}
	return string(b)
}
