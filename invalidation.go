package memcache

// WithInvalidationPublisher registers a callback invoked
// synchronously when an entry is invalidated for any reason
// (capacity eviction, TTL expiry, explicit Delete, tag invalidation,
// Compute-driven delete, etc.). The callback receives the key and
// the [EvictionReason]; it is intended as the producer side of a
// distributed invalidation pipeline — e.g., publishing the key to a
// Redis pub/sub channel so peer caches can drop their own copies.
//
// The callback runs synchronously on the eviction path. It must be
// fast and non-blocking; slow callbacks delay every other operation
// on the same shard. Use [WithCallbackTimeout] to bound the
// runtime — the watchdog fires a warning when the deadline is
// exceeded but does not preempt the callback.
//
// The package provides no implementation of distributed
// invalidation; this option is purely a hook for users to compose
// their own. Callbacks fire AFTER stats and events have been
// updated so the publisher sees a consistent view.
func WithInvalidationPublisher[K comparable](fn func(key K, reason EvictionReason)) Option {
	return func(c *config) {
		if fn != nil {
			c.invalidationPublisher = fn
		}
	}
}

// WithInvalidationSubscriber attaches a channel of keys to
// invalidate. A goroutine consumes the channel until [Cache.Close]
// is called or the channel is closed; each received key triggers a
// [Cache.Delete] reported under [EvictReasonRemote]. This is the
// consumer side of distributed invalidation — pair it with
// [WithInvalidationPublisher] in another process.
//
// The subscriber goroutine is started during [New]; failure to
// type-assert ch into the cache's K parameter surfaces as a
// [*ConfigError].
func WithInvalidationSubscriber[K comparable](ch <-chan K) Option {
	return func(c *config) {
		if ch != nil {
			c.invalidationSubscriber = ch
		}
	}
}

// resolveInvalidationPublisher returns the typed publisher from a
// type-erased any, or nil when none is configured.
//
//nolint:nilnil // (nil, nil) signals "no publisher".
func resolveInvalidationPublisher[K comparable](raw any) (func(K, EvictionReason), error) {
	if raw == nil {
		return nil, nil
	}
	fn, ok := raw.(func(K, EvictionReason))
	if !ok {
		return nil, &ConfigError{
			Field:   "InvalidationPublisher",
			Message: "type mismatch: publisher does not match cache key type",
		}
	}
	return fn, nil
}

// resolveInvalidationSubscriber returns the typed subscriber
// channel, or nil when none is configured.
//
//nolint:nilnil // (nil, nil) signals "no subscriber".
func resolveInvalidationSubscriber[K comparable](raw any) (<-chan K, error) {
	if raw == nil {
		return nil, nil
	}
	ch, ok := raw.(<-chan K)
	if !ok {
		return nil, &ConfigError{
			Field:   "InvalidationSubscriber",
			Message: "type mismatch: subscriber channel does not match cache key type",
		}
	}
	return ch, nil
}

// startInvalidationSubscriber launches the consumer goroutine that
// drains the configured subscriber channel until either the cache
// closes or the channel is closed.
func (c *Cache[K, V]) startInvalidationSubscriber() {
	if c.invalidationSubscriber == nil {
		return
	}
	c.invalidationSubscriberExited = make(chan struct{})
	go c.runInvalidationSubscriber()
}

// runInvalidationSubscriber is the goroutine body. It exits cleanly
// on cache close or channel close. Closes
// `invalidationSubscriberExited` on the way out so [Cache.Close]
// can wait for the goroutine to finish before returning.
func (c *Cache[K, V]) runInvalidationSubscriber() {
	defer close(c.invalidationSubscriberExited)
	for {
		select {
		case key, ok := <-c.invalidationSubscriber:
			if !ok {
				return
			}
			// Bail out without mutating cache state if Close has
			// already started — every other long-lived goroutine
			// honors closed.Load() before re-entering the cache.
			if c.closed.Load() {
				return
			}
			c.deleteWithReason(key, EvictReasonRemote)
		case <-c.invalidationSubscriberDone:
			return
		}
	}
}

// publishInvalidation invokes the configured publisher (if any)
// for the given key/reason. Routed through the callback watchdog
// so a slow publisher trips [WithCallbackTimeout] like every other
// hook.
func (c *Cache[K, V]) publishInvalidation(key K, reason EvictionReason) {
	if c.invalidationPublisher == nil {
		return
	}
	c.runHook("InvalidationPublisher", func() {
		c.invalidationPublisher(key, reason)
	})
}
