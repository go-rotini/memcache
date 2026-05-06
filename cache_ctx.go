package memcache

import "context"

// GetCtx is the context-aware variant of [Cache.Get]. The ctx is
// checked at entry and threaded through to any [Store] consultation
// triggered by an in-memory miss; cancelation between the in-memory
// lookup and the Store call surfaces as ctx.Err().
//
// Without [WithStore] the ctx never reaches a blocking call —
// in-memory lookups are bound by the shard lock only — so the
// only way ctx affects the result is the entry check.
//
// Use this when the surrounding caller already has a context and
// wants the cancellation status surfaced as an error rather than as
// a silent miss.
func (c *Cache[K, V]) GetCtx(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, false, err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	return c.getCtx(ctx, key)
}

// SetCtx is the context-aware variant of [Cache.Set]. The ctx is
// checked at entry and threaded through to any [Store] write-
// through; cancellation between the in-memory upsert and the Store
// call surfaces as ctx.Err() and rolls back the in-memory entry to
// keep the two sides consistent.
func (c *Cache[K, V]) SetCtx(ctx context.Context, key K, value V) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	return c.setCtx(ctx, key, value)
}

// DeleteCtx is the context-aware variant of [Cache.Delete]. The
// boolean reports whether an entry was removed from the in-memory
// cache; the error is non-nil when ctx was canceled OR when a
// configured [Store]'s Delete failed. Even on Store error the
// in-memory delete still completes — the contract is "best-effort
// reach the Store; in-memory mutation is authoritative".
func (c *Cache[K, V]) DeleteCtx(ctx context.Context, key K) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err //nolint:wrapcheck // sentinel surfaces unwrapped for errors.Is
	}
	if c.async != nil {
		// Async path: enqueue and return. The Store error path is
		// not surfaced — async writes are best-effort by design.
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
