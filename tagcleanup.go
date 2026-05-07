package memcache

// untagOp is a queued tag-removal request from removeLocked, consumed
// by the drainer. Carries the tag slice by value so entry pool recycle
// is independent of queue drain order.
type untagOp[K comparable] struct {
	key  K
	tags []string
}

// tagCleanupBuffer sets the default per-cache untag queue size.
// Sized to absorb a generous capacity-driven eviction burst (the
// shard pool size × shard count) without backing up to the eviction
// hot path. Production caches should never see the queue full
// under a normal workload; if it does fill, removeLocked falls
// back to synchronous untag (so correctness is preserved at the
// cost of the synchronization the queue was avoiding).
const tagCleanupBuffer = 4096

// startTagCleanup launches the per-cache untag drainer. The goroutine
// runs until tagCleanupDone is closed (by Cache.Close); on shutdown
// it drains every remaining op so a Close immediately followed by
// `c.tags.distinctTagCount()` reports a settled state.
func (c *Cache[K, V]) startTagCleanup() {
	if c.tags == nil {
		return
	}
	c.tagCleanupQueue = make(chan untagOp[K], tagCleanupBuffer)
	c.tagCleanupDone = make(chan struct{})
	c.tagCleanupExited = make(chan struct{})
	go c.runTagCleanup()
}

// runTagCleanup batches incoming untag ops under a single
// tagIndex.mu acquisition per batch. Batching keeps the per-eviction
// fixed cost low without holding the index lock for the whole drain.
//
// `tagCleanupInflight` is incremented by [Cache.enqueueUntag]
// before the channel send and decremented here after each batch
// applies. Sync uses inflight alone (not queue length) so a Sync
// poll never sees a "drained" snapshot while ops are mid-flight in
// the drainer's batch slice.
func (c *Cache[K, V]) runTagCleanup() {
	defer close(c.tagCleanupExited)
	const batchCap = 64
	batch := make([]untagOp[K], 0, batchCap)
	for {
		// Block until at least one op arrives or shutdown fires.
		select {
		case op := <-c.tagCleanupQueue:
			batch = append(batch, op)
		case <-c.tagCleanupDone:
			c.drainRemaining()
			return
		}
		// Greedily drain whatever else is queued, up to batchCap.
		for len(batch) < batchCap {
			select {
			case op := <-c.tagCleanupQueue:
				batch = append(batch, op)
			default:
				goto apply
			}
		}
	apply:
		applied := int64(len(batch))
		c.applyUntagBatch(batch)
		c.tagCleanupInflight.Add(-applied)
		batch = batch[:0]
	}
}

// drainRemaining pulls every queued op off the channel and applies
// it before returning. Called by the drainer on shutdown so a
// caller's `defer c.Close()` settles the index state.
func (c *Cache[K, V]) drainRemaining() {
	const batchCap = 64
	batch := make([]untagOp[K], 0, batchCap)
	for {
		select {
		case op := <-c.tagCleanupQueue:
			batch = append(batch, op)
			if len(batch) >= batchCap {
				applied := int64(len(batch))
				c.applyUntagBatch(batch)
				c.tagCleanupInflight.Add(-applied)
				batch = batch[:0]
			}
		default:
			if len(batch) > 0 {
				applied := int64(len(batch))
				c.applyUntagBatch(batch)
				c.tagCleanupInflight.Add(-applied)
			}
			return
		}
	}
}

// applyUntagBatch updates the tag index for every op under one
// idx.mu acquisition.
func (c *Cache[K, V]) applyUntagBatch(batch []untagOp[K]) {
	if c.tags == nil || len(batch) == 0 {
		return
	}
	c.tags.mu.Lock()
	for _, op := range batch {
		for _, t := range op.tags {
			set, ok := c.tags.keysByTag[t]
			if !ok {
				continue
			}
			delete(set, op.key)
			if len(set) == 0 {
				delete(c.tags.keysByTag, t)
			}
		}
	}
	c.tags.mu.Unlock()
}

// enqueueUntag tries to push the op onto the async drainer; on a
// full queue it falls back to synchronous untag so correctness
// never depends on queue capacity. The fallback bumps a stat so
// users tuning the buffer size can see the saturations.
func (c *Cache[K, V]) enqueueUntag(key K, tags []string) {
	if c.tags == nil || len(tags) == 0 {
		return
	}
	if c.tagCleanupQueue == nil {
		// startTagCleanup hasn't run (cache without a tag index).
		c.tags.untag(key, tags)
		return
	}
	// Defensive copy: the entry's tag slice may be reused by
	// the pool after this returns.
	dup := make([]string, len(tags))
	copy(dup, tags)
	op := untagOp[K]{key: key, tags: dup}
	// Bump inflight BEFORE the channel send. The drainer
	// decrements after applying. Sync polls inflight; this
	// ordering means Sync can never observe "drained" while a
	// queued op hasn't yet been applied.
	c.tagCleanupInflight.Add(1)
	select {
	case c.tagCleanupQueue <- op:
		// queued; drainer will pick it up
	default:
		// Queue full; apply inline so the index doesn't drift.
		c.tags.untag(key, tags)
		c.tagCleanupInflight.Add(-1)
		c.tagCleanupOverflows.Add(1)
	}
}

// stopTagCleanup signals the drainer to exit and waits for it.
func (c *Cache[K, V]) stopTagCleanup() {
	if c.tagCleanupDone == nil {
		return
	}
	close(c.tagCleanupDone)
	<-c.tagCleanupExited
}

// tagCleanupBacklog reports the current pending-op count for
// [Stats.TagCleanupBacklog]. Counts every op enqueued but not yet
// applied (the inflight counter); this is independent of channel
// length so Sync sees a truly-drained signal even mid-batch.
func (c *Cache[K, V]) tagCleanupBacklog() int {
	if c.tagCleanupQueue == nil {
		return 0
	}
	return int(c.tagCleanupInflight.Load())
}
