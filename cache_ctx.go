package memcache

import "context"

// GetCtx is the context-aware variant of [Cache.Get]. The ctx is checked
// at entry and threaded through to any [Store] consultation triggered by
// an in-memory miss; cancellation surfaces as ctx.Err().
func (c *Cache[K, V]) GetCtx(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, false, err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	return c.getCtx(ctx, key)
}

// SetCtx is the context-aware variant of [Cache.Set]. The ctx is checked
// at entry and threaded through to any [Store] write-through; on Store
// failure the in-memory entry is rolled back to keep both sides consistent.
func (c *Cache[K, V]) SetCtx(ctx context.Context, key K, value V) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	return c.setCtx(ctx, key, value)
}

// DeleteCtx is the context-aware variant of [Cache.Delete]. The boolean
// reports whether an entry was removed from the in-memory cache; the error
// is non-nil when ctx was canceled or a configured [Store] Delete failed.
// The in-memory delete completes regardless of Store error.
func (c *Cache[K, V]) DeleteCtx(ctx context.Context, key K) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	if c.async != nil {
		return c.asyncDelete(key), nil
	}
	removed := c.deleteWithReason(key, EvictReasonDeleted)
	if c.store == nil {
		return removed, nil
	}
	if err := c.deleteThroughStore(ctx, key); err != nil {
		return removed, err
	}
	return removed, nil
}
