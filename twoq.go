package memcache

// twoQPolicy implements Johnson & Shasha's 2Q algorithm (VLDB 1994).
//
// Three queues:
//   - A1in (FIFO, 25% of capacity): newly inserted entries land here.
//     Hits while in A1in do not promote; the entry simply ages out.
//   - Am (LRU, 75% of capacity): entries promoted on re-insert
//     (after appearing in A1out). Hits move to MRU.
//   - A1out (ghost, ~50% of capacity, keys only): records keys
//     evicted from A1in. A subsequent insert of a ghosted key skips
//     A1in and goes directly to Am.
//
// The 25/75/50 splits come from the original paper; the cache scales
// them proportionally to the shard's budget.
//
// twoQPolicy is NOT safe for concurrent use.
type twoQPolicy[K comparable, V any] struct {
	// A1in: FIFO. inHead is oldest (next victim).
	inHead, inTail *twoQNode[K, V]
	inSize         int
	inBudget       int

	// Am: LRU. amHead is MRU, amTail is LRU (next victim from Am).
	amHead, amTail *twoQNode[K, V]
	amSize         int
	amBudget       int

	// A1out: ghost FIFO of keys evicted from A1in.
	outHead, outTail *twoQGhostNode[K]
	outSize          int
	outBudget        int
	outSet           map[K]*twoQGhostNode[K]
}

// twoQNode lives on entry.policyData. inAm distinguishes which queue
// owns the node.
type twoQNode[K comparable, V any] struct {
	entry      *entry[K, V]
	next, prev *twoQNode[K, V]
	inAm       bool
}

// twoQGhostNode tracks an evicted-from-A1in key (no value).
type twoQGhostNode[K comparable] struct {
	key        K
	next, prev *twoQGhostNode[K]
}

// newTwoQ constructs an empty 2Q policy sized for the given shard
// budget.
func newTwoQ[K comparable, V any](budget int) *twoQPolicy[K, V] {
	in, am, out := twoQSplitBudget(budget)
	return &twoQPolicy[K, V]{
		inBudget:  in,
		amBudget:  am,
		outBudget: out,
		outSet:    make(map[K]*twoQGhostNode[K]),
	}
}

// twoQSplitBudget computes A1in (~25%), Am (75%), and A1out (~50%)
// sub-budgets from a total. Non-positive budgets yield zeros.
func twoQSplitBudget(budget int) (in, am, out int) {
	if budget <= 0 {
		return 0, 0, 0
	}
	in = max((budget+3)/4, 1) // ~25%, rounded up
	am = max(budget-in, 1)
	out = max((budget+1)/2, 1) // ~50%
	return in, am, out
}

// SetBudget recomputes A1in / Am / A1out sub-budgets at runtime.
func (p *twoQPolicy[K, V]) SetBudget(budget int) {
	in, am, out := twoQSplitBudget(budget)
	p.inBudget = in
	p.amBudget = am
	p.outBudget = out
	for p.outSize > p.outBudget && p.outHead != nil {
		p.unlinkGhost(p.outHead)
	}
}

// OnInsert places e in Am if its key is in A1out (re-insert of a
// recently-evicted hot key); otherwise in A1in.
func (p *twoQPolicy[K, V]) OnInsert(e *entry[K, V]) {
	n := &twoQNode[K, V]{entry: e}
	e.policyData = n
	if g, ok := p.outSet[e.key]; ok {
		p.unlinkGhost(g)
		n.inAm = true
		p.pushAmHead(n)
	} else {
		p.pushInTail(n)
	}
}

// OnAccess promotes Am entries to MRU; A1in entries are unchanged
// (FIFO semantics — a hit while in A1in does not delay eviction).
func (p *twoQPolicy[K, V]) OnAccess(e *entry[K, V]) {
	n, ok := e.policyData.(*twoQNode[K, V])
	if !ok || n == nil {
		return
	}
	if !n.inAm {
		return
	}
	if n == p.amHead {
		return
	}
	p.unlinkAm(n)
	p.pushAmHead(n)
}

// OnUpdate is treated as access.
func (p *twoQPolicy[K, V]) OnUpdate(e *entry[K, V]) {
	p.OnAccess(e)
}

// OnRemove drops e from whichever queue holds it.
func (p *twoQPolicy[K, V]) OnRemove(e *entry[K, V]) {
	n, ok := e.policyData.(*twoQNode[K, V])
	if !ok || n == nil {
		return
	}
	if n.inAm {
		p.unlinkAm(n)
	} else {
		p.unlinkIn(n)
	}
	n.entry = nil
	n.next, n.prev = nil, nil
	e.policyData = nil
}

// Victim returns the next entry to evict. A1in is checked first
// (when over budget the head goes to A1out, then is evicted); if
// A1in is within budget, fall through to Am's tail (LRU).
func (p *twoQPolicy[K, V]) Victim() *entry[K, V] {
	if p.inSize > p.inBudget && p.inHead != nil {
		n := p.inHead
		p.unlinkIn(n)
		p.recordGhost(n.entry.key)
		return n.entry
	}
	if p.amSize > p.amBudget && p.amTail != nil {
		n := p.amTail
		p.unlinkAm(n)
		return n.entry
	}
	// Total over budget but neither queue alone is — pick the
	// oldest from A1in if it has anything, else Am's tail.
	if p.inHead != nil {
		n := p.inHead
		p.unlinkIn(n)
		p.recordGhost(n.entry.key)
		return n.entry
	}
	if p.amTail != nil {
		n := p.amTail
		p.unlinkAm(n)
		return n.entry
	}
	return nil
}

// Len returns the total entries tracked.
func (p *twoQPolicy[K, V]) Len() int { return p.inSize + p.amSize }

// Reset clears all queue and ghost state.
//
//nolint:dupl // structurally similar to s3fifoPolicy.Reset but operates on different node types
func (p *twoQPolicy[K, V]) Reset() {
	for n := p.inHead; n != nil; {
		nxt := n.next
		if n.entry != nil {
			n.entry.policyData = nil
		}
		n.entry = nil
		n.next, n.prev = nil, nil
		n = nxt
	}
	for n := p.amHead; n != nil; {
		nxt := n.next
		if n.entry != nil {
			n.entry.policyData = nil
		}
		n.entry = nil
		n.next, n.prev = nil, nil
		n = nxt
	}
	p.inHead, p.inTail = nil, nil
	p.amHead, p.amTail = nil, nil
	p.outHead, p.outTail = nil, nil
	p.inSize, p.amSize, p.outSize = 0, 0, 0
	p.outSet = make(map[K]*twoQGhostNode[K])
}

func (p *twoQPolicy[K, V]) pushInTail(n *twoQNode[K, V]) {
	n.inAm = false
	n.prev = p.inTail
	n.next = nil
	if p.inTail != nil {
		p.inTail.next = n
	} else {
		p.inHead = n
	}
	p.inTail = n
	p.inSize++
}

func (p *twoQPolicy[K, V]) pushAmHead(n *twoQNode[K, V]) {
	n.inAm = true
	n.next = p.amHead
	n.prev = nil
	if p.amHead != nil {
		p.amHead.prev = n
	} else {
		p.amTail = n
	}
	p.amHead = n
	p.amSize++
}

func (p *twoQPolicy[K, V]) unlinkIn(n *twoQNode[K, V]) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case p.inHead == n:
		p.inHead = n.next
	default:
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case p.inTail == n:
		p.inTail = n.prev
	}
	n.next, n.prev = nil, nil
	p.inSize--
}

func (p *twoQPolicy[K, V]) unlinkAm(n *twoQNode[K, V]) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case p.amHead == n:
		p.amHead = n.next
	default:
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case p.amTail == n:
		p.amTail = n.prev
	}
	n.next, n.prev = nil, nil
	p.amSize--
}

func (p *twoQPolicy[K, V]) recordGhost(key K) {
	if _, exists := p.outSet[key]; exists {
		return
	}
	g := &twoQGhostNode[K]{key: key, prev: p.outTail}
	if p.outTail != nil {
		p.outTail.next = g
	} else {
		p.outHead = g
	}
	p.outTail = g
	p.outSize++
	p.outSet[key] = g

	for p.outSize > p.outBudget && p.outHead != nil {
		p.unlinkGhost(p.outHead)
	}
}

func (p *twoQPolicy[K, V]) unlinkGhost(g *twoQGhostNode[K]) {
	if g.prev != nil {
		g.prev.next = g.next
	} else if p.outHead == g {
		p.outHead = g.next
	}
	if g.next != nil {
		g.next.prev = g.prev
	} else if p.outTail == g {
		p.outTail = g.prev
	}
	delete(p.outSet, g.key)
	g.next, g.prev = nil, nil
	p.outSize--
}

// Snapshot returns a [PolicyDetail2Q] summarizing the policy's
// current state.
func (p *twoQPolicy[K, V]) Snapshot() any {
	return PolicyDetail2Q{A1inSize: p.inSize, AmSize: p.amSize, A1outSize: p.outSize}
}

// PromotionNeeded reports true — 2Q may promote A1in → Am on
// access, so the fast path is not safe.
func (p *twoQPolicy[K, V]) PromotionNeeded(*entry[K, V]) bool { return true }
