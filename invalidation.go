package memcache

// WithInvalidationPublisher registers fn invoked synchronously on every
// eviction (any reason). The callback runs on the eviction path and MUST
// be fast; slow callbacks delay every operation on the shard. Bound
// runtime with [WithCallbackTimeout]. Fires after stats/events update.
func WithInvalidationPublisher[K comparable](fn func(key K, reason EvictionReason)) Option {
	return func(c *config) {
		if fn != nil {
			c.invalidationPublisher = fn
		}
	}
}

// WithInvalidationSubscriber attaches a channel of keys to invalidate.
// A goroutine drains it until close or [Cache.Close]; each key triggers
// a delete under [EvictReasonRemote]. Type mismatches return [*ConfigError].
func WithInvalidationSubscriber[K comparable](ch <-chan K) Option {
	return func(c *config) {
		if ch != nil {
			c.invalidationSubscriber = ch
		}
	}
}

//nolint:nilnil // (nil, nil) signals no publisher.
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

//nolint:nilnil // (nil, nil) signals no subscriber.
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

func (c *Cache[K, V]) startInvalidationSubscriber() {
	if c.invalidationSubscriber == nil {
		return
	}
	c.invalidationSubscriberExited = make(chan struct{})
	go c.runInvalidationSubscriber()
}

func (c *Cache[K, V]) runInvalidationSubscriber() {
	defer close(c.invalidationSubscriberExited)
	for {
		select {
		case key, ok := <-c.invalidationSubscriber:
			if !ok {
				return
			}
			if c.closed.Load() {
				return
			}
			c.deleteWithReason(key, EvictReasonRemote)
		case <-c.invalidationSubscriberDone:
			return
		}
	}
}

func (c *Cache[K, V]) publishInvalidation(key K, reason EvictionReason) {
	if c.invalidationPublisher == nil {
		return
	}
	c.runHook("InvalidationPublisher", func() {
		c.invalidationPublisher(key, reason)
	})
}
