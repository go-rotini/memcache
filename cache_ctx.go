package memcache

import "context"

// GetCtx is the context-aware variant of [Cache.Get]. When ctx is
// already canceled at the time of the call, GetCtx returns the zero
// value, ok=false, and ctx.Err(); otherwise it delegates to Get and
// returns (value, ok, nil). The context is checked once at the
// start — Get itself doesn't perform I/O, so there is no
// intermediate point at which a cancellation could land.
//
// Use this when the surrounding caller already has a context and
// wants the cancellation status surfaced as an error rather than as
// a silent miss.
// uses an internal context, not the caller's, by design.
//
//nolint:contextcheck // Get is fully synchronous; refresh-ahead
func (c *Cache[K, V]) GetCtx(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, false, err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	v, ok := c.Get(key)
	return v, ok, nil
}

// SetCtx is the context-aware variant of [Cache.Set]. Behavior
// matches Set; ctx cancellation prior to the call short-circuits
// with ctx.Err().
func (c *Cache[K, V]) SetCtx(ctx context.Context, key K, value V) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	return c.Set(key, value)
}

// DeleteCtx is the context-aware variant of [Cache.Delete]. The
// boolean reports whether an entry was removed (false on miss);
// the error is non-nil only when ctx was already canceled at call
// time.
func (c *Cache[K, V]) DeleteCtx(ctx context.Context, key K) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	return c.Delete(key), nil
}
