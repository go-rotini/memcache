package memcache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

//nolint:nilnil // (nil, nil) signals no Store configured.
func resolveStore[K comparable, V any](raw any) (Store[K, V], error) {
	if raw == nil {
		return nil, nil
	}
	s, ok := raw.(Store[K, V])
	if !ok {
		return nil, &ConfigError{
			Field:   "Store",
			Message: fmt.Sprintf("type mismatch: store does not match cache K/V (%T)", raw),
		}
	}
	return s, nil
}

// readThroughStore is consulted by Get-style methods after an in-memory
// miss. A Store hit is promoted into the in-memory cache and returned.
func (c *Cache[K, V]) readThroughStore(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if c.store == nil {
		return zero, false, nil
	}
	value, ok, err := c.store.Get(ctx, key)
	if err != nil {
		return zero, false, err
	}
	if !ok {
		return zero, false, nil
	}
	c.promoteFromStore(key, value)
	return c.returnValue(value), true, nil
}

// promoteFromStore inserts a Store-supplied value via the standard upsert.
// The promoted entry uses [WithDefaultTTL]; Store does not return TTLs.
// Eviction does NOT cascade to the Store.
func (c *Cache[K, V]) promoteFromStore(key K, value V) {
	ttl := c.cfg.defaultTTL
	tags := extractCacheableTags(value)
	if tpl := extractTemplateTags(value); len(tpl) > 0 {
		tags = append(tags, tpl...)
	}
	// Best-effort promotion; capacity/admission rejection silently
	// drops the in-memory copy.
	if err := c.setLocked(key, value, ttl, c.cfg.slidingTTL, tags); err != nil && c.cfg.logger != nil {
		c.cfg.logger.Debug("memcache: store promotion declined by in-memory cache",
			"key", key, "err", err)
	}
}

// writeThroughStore propagates a Set to the Store. ttl is the resolved
// per-call TTL so Store mirror agrees with in-memory expiry. Sliding
// TTLs are NOT propagated.
func (c *Cache[K, V]) writeThroughStore(ctx context.Context, key K, value V, ttl time.Duration) error {
	if c.store == nil {
		return nil
	}
	return c.store.Set(ctx, key, value, ttl)
}

// deleteThroughStore propagates a Delete to the Store; errors are
// logged. Use [Cache.DeleteCtx] to surface them.
func (c *Cache[K, V]) deleteThroughStore(ctx context.Context, key K) error {
	if c.store == nil {
		return nil
	}
	_, err := c.store.Delete(ctx, key)
	if err != nil && c.cfg.logger != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			c.cfg.logger.Debug("memcache: store Delete canceled", "key", key, "err", err)
		} else {
			c.cfg.logger.Warn("memcache: store Delete failed", "key", key, "err", err)
		}
	}
	return err
}

// rollbackInMemory removes the in-memory entry for key after a
// write-through Store failure so the two sides do not drift.
func (c *Cache[K, V]) rollbackInMemory(key K) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer c.unlockShard(s)
	if e, ok := s.storage.get(key); ok {
		c.removeLocked(s, e, EvictReasonStoreRollback)
	}
}

// propagateSetToStore mirrors the write to the Store; on Store failure
// the in-memory entry is rolled back so the sides do not drift.
func (c *Cache[K, V]) propagateSetToStore(ctx context.Context, key K, value V, ttl time.Duration) error {
	if err := c.writeThroughStore(ctx, key, value, ttl); err != nil {
		c.rollbackInMemory(key)
		return err
	}
	return nil
}
