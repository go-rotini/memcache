package memcache

// s3fifoPolicy implements the S3-FIFO eviction algorithm
// (Yang et al., SOSP 2023).
//
// State:
//   - Small: a FIFO queue holding ~10% of capacity. New entries land
//     here. On eviction, an entry with freq ≥ 1 is promoted to Main;
//     freq=0 entries are evicted and the key is recorded in Ghost.
//   - Main: a FIFO queue holding ~90% of capacity. On eviction, an
//     entry with freq ≥ 1 has its freq decremented and is re-pushed
//     to the tail (a "second chance"); freq=0 entries are evicted
//     outright (no Ghost record).
//   - Ghost: a key-only FIFO of recently evicted Small entries, sized
//     equal to Main. New inserts whose key is in Ghost go directly to
//     Main rather than Small (treating the previous eviction as a
//     hint that the key has reuse).
//
// Per-entry state lives in the entry's policyData field as an
// *s3Node. The node carries `freq` (a 2-bit saturating counter,
// 0..3), `inMain` (true once promoted), and intrusive list pointers.
//
// s3fifoPolicy is NOT safe for concurrent use; the shard mutex
// serializes all operations.
type s3fifoPolicy[K comparable, V any] struct {
	// Small queue: head is oldest, tail is newest.
	smallHead, smallTail *s3Node[K, V]
	smallSize            int
	smallBudget          int

	// Main queue: head is oldest, tail is newest.
	mainHead, mainTail *s3Node[K, V]
	mainSize           int
	mainBudget         int

	// Ghost queue: keys only, head is oldest. Membership lookup is
	// O(1) via ghostSet.
	ghostHead, ghostTail *s3GhostNode[K]
	ghostSize            int
	ghostBudget          int
	ghostSet             map[K]*s3GhostNode[K]
}

// s3Node is the intrusive list node attached to an entry's
// policyData. It belongs to either the Small or Main queue (never
// both); inMain disambiguates.
type s3Node[K comparable, V any] struct {
	entry      *entry[K, V]
	next, prev *s3Node[K, V]
	freq       uint8 // saturating 0..3
	inMain     bool
}

// s3GhostNode tracks a recently-evicted key (from Small). It carries
// no value pointer.
type s3GhostNode[K comparable] struct {
	key        K
	next, prev *s3GhostNode[K]
}

// s3FreqMax is the saturating ceiling for s3Node.freq.
const s3FreqMax uint8 = 3

// newS3FIFO constructs an empty S3-FIFO policy sized for the given
// shard budget. A non-positive budget produces a policy with
// zero-sized internal sub-queues (effectively FIFO across a single
// queue once entries arrive); the cache's eviction loop is what
// actually triggers Victim, so degenerate budgets remain consistent.
func newS3FIFO[K comparable, V any](budget int) *s3fifoPolicy[K, V] {
	small, main := s3SplitBudget(budget)
	return &s3fifoPolicy[K, V]{
		smallBudget: small,
		mainBudget:  main,
		ghostBudget: main,
		ghostSet:    make(map[K]*s3GhostNode[K]),
	}
}

// s3SplitBudget computes the (small, main) sub-budgets from a total.
// Small gets ~10% (rounded up; never larger than budget itself), Main
// gets the rest. Negative or zero total yields (0, 0).
func s3SplitBudget(budget int) (small, main int) {
	if budget <= 0 {
		return 0, 0
	}
	small = (budget + 9) / 10
	if small >= budget {
		small = 1
	}
	main = max(budget-small, 1)
	return small, main
}

// SetBudget recomputes the Small/Main/Ghost sub-budgets to match a
// new total. The policy does not preemptively evict — the cache will
// drive subsequent calls to Victim if the new budget is smaller.
func (p *s3fifoPolicy[K, V]) SetBudget(budget int) {
	small, main := s3SplitBudget(budget)
	p.smallBudget = small
	p.mainBudget = main
	p.ghostBudget = main
	for p.ghostSize > p.ghostBudget && p.ghostHead != nil {
		p.unlinkGhost(p.ghostHead)
	}
}

// OnInsert places e into Main if its key is in the Ghost queue,
// otherwise into Small. Fresh Small entries start at freq=0; Ghost-
// rebirth entries start at freq=1 so they survive at least one Main
// second-chance pass, which prevents the just-inserted entry from
// being chosen as the Main victim by the same operation's post-insert
// eviction loop (the entry sits at mainHead, and a freq=0 mainHead is
// the immediate eviction target whenever Main is over budget — most
// commonly because the same Victim call promoted Small entries to
// Main, pushing it over).
func (p *s3fifoPolicy[K, V]) OnInsert(e *entry[K, V]) {
	n := &s3Node[K, V]{entry: e}
	e.policyData = n
	if g, ok := p.ghostSet[e.key]; ok {
		// Promote on re-entry: was recently evicted, now back —
		// place directly into Main and clear the Ghost record.
		p.unlinkGhost(g)
		n.inMain = true
		n.freq = 1
		p.pushMainTail(n)
	} else {
		p.pushSmallTail(n)
	}
}

// OnAccess bumps the entry's freq counter (saturating at 3). Membership
// in Small vs Main does not change.
func (p *s3fifoPolicy[K, V]) OnAccess(e *entry[K, V]) {
	n, ok := e.policyData.(*s3Node[K, V])
	if !ok || n == nil {
		return
	}
	if n.freq < s3FreqMax {
		n.freq++
	}
}

// OnUpdate is identical to OnAccess: a write to an existing key counts
// as activity in the S3-FIFO model.
func (p *s3fifoPolicy[K, V]) OnUpdate(e *entry[K, V]) {
	p.OnAccess(e)
}

// OnRemove drops e from whichever queue holds it. Safe to call after
// Victim has already pulled the node out of its queue: the node's
// list pointers will be nil and the helper unlinks become no-ops.
func (p *s3fifoPolicy[K, V]) OnRemove(e *entry[K, V]) {
	n, ok := e.policyData.(*s3Node[K, V])
	if !ok || n == nil {
		return
	}
	if n.inMain {
		p.unlinkMain(n)
	} else {
		p.unlinkSmall(n)
	}
	n.entry = nil
	n.next = nil
	n.prev = nil
	e.policyData = nil
}

// Victim runs S3-FIFO's promotion/demotion logic and returns the next
// entry to evict from the cache. It mutates internal queue state
// freely (rotating between Small and Main); a nil return means the
// policy has no candidate (queues are within budget).
//
// The cache calls Victim repeatedly while the shard remains over
// budget, so internal rotations that don't yield an eviction simply
// continue the loop here.
func (p *s3fifoPolicy[K, V]) Victim() *entry[K, V] {
	// Up to (smallSize + mainSize + 1) iterations bounds the
	// promotion/demotion cycles; in practice each call returns
	// quickly. The outer loop guards against pathological cases
	// where every Small head promotes to Main.
	for range p.smallSize + p.mainSize + 1 {
		if p.smallSize > p.smallBudget && p.smallHead != nil {
			n := p.smallHead
			if n.freq >= 1 {
				// Promote to Main: clear freq per S3-FIFO
				// semantics so it gets a fair shake there.
				p.unlinkSmall(n)
				n.freq = 0
				n.inMain = true
				p.pushMainTail(n)
				continue
			}
			// Evict the entry; record its key in Ghost.
			p.unlinkSmall(n)
			p.recordGhost(n.entry.key)
			return n.entry
		}
		if p.mainSize > p.mainBudget && p.mainHead != nil {
			n := p.mainHead
			if n.freq >= 1 {
				n.freq--
				p.unlinkMain(n)
				p.pushMainTail(n)
				continue
			}
			p.unlinkMain(n)
			return n.entry
		}
		// Neither queue is over budget — nothing to evict.
		return nil
	}
	// Defensive fallback: if we somehow looped without returning,
	// pick whichever queue has an entry (oldest from Small first).
	if p.smallHead != nil {
		n := p.smallHead
		p.unlinkSmall(n)
		p.recordGhost(n.entry.key)
		return n.entry
	}
	if p.mainHead != nil {
		n := p.mainHead
		p.unlinkMain(n)
		return n.entry
	}
	return nil
}

// Len returns the total number of entries tracked by Small and Main.
// Ghost-only keys are not counted (they correspond to no live entry).
func (p *s3fifoPolicy[K, V]) Len() int {
	return p.smallSize + p.mainSize
}

// Reset clears all state: Small, Main, and Ghost queues.
//
//nolint:dupl // structurally similar to twoQPolicy.Reset but operates on different node types
func (p *s3fifoPolicy[K, V]) Reset() {
	for n := p.smallHead; n != nil; {
		nxt := n.next
		if n.entry != nil {
			n.entry.policyData = nil
		}
		n.entry = nil
		n.next, n.prev = nil, nil
		n = nxt
	}
	for n := p.mainHead; n != nil; {
		nxt := n.next
		if n.entry != nil {
			n.entry.policyData = nil
		}
		n.entry = nil
		n.next, n.prev = nil, nil
		n = nxt
	}
	p.smallHead, p.smallTail = nil, nil
	p.mainHead, p.mainTail = nil, nil
	p.ghostHead, p.ghostTail = nil, nil
	p.smallSize, p.mainSize, p.ghostSize = 0, 0, 0
	p.ghostSet = make(map[K]*s3GhostNode[K])
}

// pushSmallTail appends n to the tail (newest) of the Small queue.
func (p *s3fifoPolicy[K, V]) pushSmallTail(n *s3Node[K, V]) {
	n.inMain = false
	n.prev = p.smallTail
	n.next = nil
	if p.smallTail != nil {
		p.smallTail.next = n
	} else {
		p.smallHead = n
	}
	p.smallTail = n
	p.smallSize++
}

// pushMainTail appends n to the tail (newest) of the Main queue.
func (p *s3fifoPolicy[K, V]) pushMainTail(n *s3Node[K, V]) {
	n.inMain = true
	n.prev = p.mainTail
	n.next = nil
	if p.mainTail != nil {
		p.mainTail.next = n
	} else {
		p.mainHead = n
	}
	p.mainTail = n
	p.mainSize++
}

// unlinkSmall removes n from the Small queue. Safe to call on a node
// whose list pointers are already nil — it just decrements smallSize
// once and clears head/tail consistently.
func (p *s3fifoPolicy[K, V]) unlinkSmall(n *s3Node[K, V]) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case p.smallHead == n:
		p.smallHead = n.next
	default:
		// Already detached.
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case p.smallTail == n:
		p.smallTail = n.prev
	}
	n.next, n.prev = nil, nil
	p.smallSize--
}

// unlinkMain removes n from the Main queue.
func (p *s3fifoPolicy[K, V]) unlinkMain(n *s3Node[K, V]) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case p.mainHead == n:
		p.mainHead = n.next
	default:
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case p.mainTail == n:
		p.mainTail = n.prev
	}
	n.next, n.prev = nil, nil
	p.mainSize--
}

// recordGhost adds key to the tail of the Ghost queue and evicts the
// oldest Ghost record when the queue exceeds its budget.
func (p *s3fifoPolicy[K, V]) recordGhost(key K) {
	if _, exists := p.ghostSet[key]; exists {
		return
	}
	g := &s3GhostNode[K]{key: key, prev: p.ghostTail}
	if p.ghostTail != nil {
		p.ghostTail.next = g
	} else {
		p.ghostHead = g
	}
	p.ghostTail = g
	p.ghostSize++
	p.ghostSet[key] = g

	for p.ghostSize > p.ghostBudget && p.ghostHead != nil {
		drop := p.ghostHead
		p.unlinkGhost(drop)
	}
}

// unlinkGhost removes g from the Ghost queue and ghostSet.
func (p *s3fifoPolicy[K, V]) unlinkGhost(g *s3GhostNode[K]) {
	if g.prev != nil {
		g.prev.next = g.next
	} else if p.ghostHead == g {
		p.ghostHead = g.next
	}
	if g.next != nil {
		g.next.prev = g.prev
	} else if p.ghostTail == g {
		p.ghostTail = g.prev
	}
	delete(p.ghostSet, g.key)
	g.next, g.prev = nil, nil
	p.ghostSize--
}
