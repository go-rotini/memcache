package memcache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// resolveStore narrows the type-erased Store from the cache config
// to its typed [Store][K, V] form. nil input yields (nil, nil) — the
// cache simply runs without a store. Type mismatches surface as a
// [ConfigError] before [build] returns.
//
//nolint:nilnil // the (nil, nil) return is the documented "no Store configured" sentinel
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

// readThroughStore is consulted by Get-style methods after the in-
// memory shard reports a miss. When a Store is configured and the
// key is present there, the value is promoted into the in-memory
// cache (write-through-cache semantics) and returned. Returning
// (zero, false, nil) covers both "no store configured" and "store
// also missed"; the caller treats both as terminal misses.
//
// ctx flows through to [Store.Get] verbatim — Ctx-aware cache
// methods pass the user's context; non-Ctx methods pass
// [context.Background] so the Store can still honor an internal
// deadline, but no caller-driven cancellation propagates.
func (c *Cache[K, V]) readThroughStore(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if c.store == nil {
		return zero, false, nil
	}
	value, ok, err := c.store.Get(ctx, key)
	if err != nil {
		// Surface the error verbatim so callers can detect
		// canceled contexts and underlying I/O failures.
		return zero, false, err
	}
	if !ok {
		return zero, false, nil
	}
	c.promoteFromStore(key, value)
	return c.returnValue(value), true, nil
}

// promoteFromStore inserts a Store-supplied value into the in-
// memory shard via the standard upsert path. Eviction events fire
// as on any insert; if the policy evicts another entry, the Store
// keeps that entry — eviction does not cascade to the Store.
//
// The TTL on the promoted entry is the cache's [WithDefaultTTL]:
// the Store interface does not return TTLs from Get, so we cannot
// preserve the original. Users who need exact TTL fidelity across
// the boundary should write through the cache rather than the
// Store directly, which keeps the two sides in sync.
func (c *Cache[K, V]) promoteFromStore(key K, value V) {
	ttl := c.cfg.defaultTTL
	tags := extractCacheableTags(value)
	if tpl := extractTemplateTags(value); len(tpl) > 0 {
		tags = append(tags, tpl...)
	}
	// Best-effort promotion. Capacity violations / admission
	// rejection silently drop the in-memory copy; the next read
	// will hit the Store again.
	if err := c.setLocked(key, value, ttl, c.cfg.slidingTTL, tags); err != nil && c.cfg.logger != nil {
		c.cfg.logger.Debug("memcache: store promotion declined by in-memory cache",
			"key", key, "err", err)
	}
}

// writeThroughStore propagates a Set to the configured Store. A
// Store error is surfaced to the caller so they can react (retry,
// log, etc.). On error the caller is expected to roll back the in-
// memory entry; rollbackInMemory does that work.
//
// ttl is the EFFECTIVE TTL for the entry — pre-jitter, post-resolved
// — so the Store mirror agrees with the in-memory expiry within the
// jitter window. Sliding-TTL semantics are NOT propagated to the
// Store: the Store sees a fixed TTL set at write time, since it has
// no way to observe Gets.
func (c *Cache[K, V]) writeThroughStore(ctx context.Context, key K, value V, ttl time.Duration) error {
	if c.store == nil {
		return nil
	}
	return c.store.Set(ctx, key, value, ttl)
}

// deleteThroughStore propagates a Delete to the configured Store.
// Errors are LOGGED rather than returned because [Cache.Delete]'s
// signature returns only a bool; users who need delete errors
// should use [Cache.DeleteCtx] or call the Store directly.
func (c *Cache[K, V]) deleteThroughStore(ctx context.Context, key K) error {
	if c.store == nil {
		return nil
	}
	_, err := c.store.Delete(ctx, key)
	if err != nil && c.cfg.logger != nil {
		// Demote canceled-context errors to debug — the caller
		// already knows; no need to alarm.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			c.cfg.logger.Debug("memcache: store Delete canceled", "key", key, "err", err)
		} else {
			c.cfg.logger.Warn("memcache: store Delete failed", "key", key, "err", err)
		}
	}
	return err
}

// rollbackInMemory removes the in-memory entry for key when a
// write-through Store call has failed. The caller has already
// applied an upsert and now needs to reverse it so the in-memory
// state doesn't drift away from the Store. removeLocked accepts
// the entry pointer; we look it up under the shard lock and
// silently drop if it's already gone (concurrent eviction).
func (c *Cache[K, V]) rollbackInMemory(key K) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer c.unlockShard(s)
	if e, ok := s.storage.get(key); ok {
		c.removeLocked(s, e, EvictReasonStoreRollback)
	}
}

// propagateSetToStore is called by every Set entry point after the
// in-memory upsert has run. When [WithStore] is configured it
// mirrors the write to the Store; on Store failure the in-memory
// entry is rolled back so the two sides do not drift.
//
// ttl is the resolved per-call TTL — pre-jitter, post-defaults —
// so the Store mirror sees the same expiration the cache applied.
// Sliding-TTL semantics aren't communicated to the Store; sliding
// TTLs are surfaced as their initial duration.
func (c *Cache[K, V]) propagateSetToStore(ctx context.Context, key K, value V, ttl time.Duration) error {
	if err := c.writeThroughStore(ctx, key, value, ttl); err != nil {
		c.rollbackInMemory(key)
		return err
	}
	return nil
}
