package memcache

import (
	"sync"
	"sync/atomic"
)

// eventKindMask is a bitmask of [EventKind] values. A mask of 0
// matches every kind (the "subscribe to everything" default); any
// non-zero mask filters strictly to the bits set.
type eventKindMask uint32

// has reports whether mask matches the given kind. Empty masks
// match everything.
func (m eventKindMask) has(k EventKind) bool {
	if m == 0 {
		return true
	}
	return m&(1<<uint(k)) != 0
}

// kindMask folds a slice of EventKinds into an eventKindMask. A
// nil/empty slice produces the zero mask (subscribe-to-all).
func kindMask(kinds []EventKind) eventKindMask {
	var m eventKindMask
	for _, k := range kinds {
		m |= 1 << uint(k)
	}
	return m
}

// subscription is one entry in an [eventBus]: a buffered channel
// and the kind filter to apply before sending.
type subscription[K comparable, V any] struct {
	ch    chan Event[K, V]
	kinds eventKindMask
}

// eventBus is the cache-level fan-out. publish does a non-blocking send
// per matching channel; full channels drop the event and bump the drop
// counter. Slow subscribers must not block cache operations.
type eventBus[K comparable, V any] struct {
	mu     sync.RWMutex
	subs   map[uint64]*subscription[K, V]
	nextID atomic.Uint64
	closed atomic.Bool
}

// newEventBus constructs an empty bus.
func newEventBus[K comparable, V any]() *eventBus[K, V] {
	return &eventBus[K, V]{subs: make(map[uint64]*subscription[K, V])}
}

// subscribe registers sub and returns its identifier (used by
// unsubscribe). Returns 0 when the bus has been closed; the
// returned channel is closed immediately so the caller's range
// loop terminates without spinning.
func (b *eventBus[K, V]) subscribe(sub *subscription[K, V]) uint64 {
	if b.closed.Load() {
		close(sub.ch)
		return 0
	}
	id := b.nextID.Add(1)
	b.mu.Lock()
	b.subs[id] = sub
	b.mu.Unlock()
	return id
}

// unsubscribe removes the registration with the given id and
// closes its channel so any reader waiting on it wakes up.
// Idempotent: unknown ids are silently ignored.
func (b *eventBus[K, V]) unsubscribe(id uint64) {
	if id == 0 {
		return
	}
	b.mu.Lock()
	sub, ok := b.subs[id]
	delete(b.subs, id)
	b.mu.Unlock()
	if ok {
		close(sub.ch)
	}
}

// publish fans e out. dropCounter, when non-nil, is bumped per full
// channel. Safe to call after Close.
func (b *eventBus[K, V]) publish(e Event[K, V], dropCounter *atomic.Uint64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, sub := range b.subs {
		if !sub.kinds.has(e.Kind) {
			continue
		}
		select {
		case sub.ch <- e:
		default:
			if dropCounter != nil {
				dropCounter.Add(1)
			}
		}
	}
}

// close marks the bus shut and closes every subscriber channel.
// After close, subscribe rejects new registrations. publish is a
// safe no-op (all channels are already closed; the publish
// goroutine wouldn't reach the select on a non-existent
// subscription).
func (b *eventBus[K, V]) close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, sub := range b.subs {
		close(sub.ch)
		delete(b.subs, id)
	}
}

// Subscribe registers a buffered channel for events matching kinds (no
// kinds = all). The returned cancel function unsubscribes and closes
// the channel. Drops on full channels are counted in
// [Stats.EventsDropped].
func (c *Cache[K, V]) Subscribe(buf int, kinds ...EventKind) (<-chan Event[K, V], func()) {
	if buf <= 0 {
		buf = c.cfg.eventsBuffer
		if buf <= 0 {
			buf = 64
		}
	}
	sub := &subscription[K, V]{
		ch:    make(chan Event[K, V], buf),
		kinds: kindMask(kinds),
	}
	id := c.events.subscribe(sub)
	return sub.ch, func() { c.events.unsubscribe(id) }
}

// publishEvent fans an event out and bumps EventsDropped for any
// subscriber whose channel was full. Cheap when no subscribers are
// registered (the bus's RLock is uncontended).
func (c *Cache[K, V]) publishEvent(e Event[K, V]) {
	if c.events == nil {
		return
	}
	c.events.publish(e, &c.counters.eventsDropped)
}
