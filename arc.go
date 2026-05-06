package memcache

// arcPolicy implements the Adaptive Replacement Cache (Megiddo &
// Modha, FAST 2003).
//
// State:
//   - T1: recency LRU. Entries arrive here on first cache miss.
//   - T2: frequency LRU. Entries promoted from T1 on a re-access.
//   - B1: ghost LRU of keys recently evicted from T1.
//   - B2: ghost LRU of keys recently evicted from T2.
//   - p: an adaptive target size for T1, in the range [0, c].
//
// Invariants:
//
//	|T1| + |T2|  ≤ c
//	|T1| + |B1|  ≤ c
//	|T2| + |B2|  ≤ 2c
//
// On a re-insert that falls in B1 or B2, p shifts to favor recency or
// frequency respectively. ARC then evicts according to whichever sub-
// cache is currently above its share of p.
//
// Compared to the canonical ARC pseudocode, this implementation
// reorders steps to fit the package's "insert first, then Victim()"
// shard architecture: ghost-driven eviction policy decisions happen
// at OnInsert (where p adjusts) and at Victim (where T1/T2 victim
// selection runs against the freshly-updated p).
//
// arcPolicy is NOT safe for concurrent use.
type arcPolicy[K comparable, V any] struct {
	c int // total capacity (budget)
	p int // adaptive target size of T1, 0..c

	// T1 LRU. t1Head is MRU, t1Tail is LRU.
	t1Head, t1Tail *arcNode[K, V]
	t1Size         int

	// T2 LRU. t2Head is MRU, t2Tail is LRU.
	t2Head, t2Tail *arcNode[K, V]
	t2Size         int

	// B1 ghost LRU. b1Head is MRU, b1Tail is LRU. Membership via
	// b1Set for O(1) lookup.
	b1Head, b1Tail *arcGhostNode[K]
	b1Size         int
	b1Set          map[K]*arcGhostNode[K]

	// B2 ghost LRU.
	b2Head, b2Tail *arcGhostNode[K]
	b2Size         int
	b2Set          map[K]*arcGhostNode[K]
}

// arcNode is the intrusive list node attached to entry.policyData.
// inT2 disambiguates which list owns it.
type arcNode[K comparable, V any] struct {
	entry      *entry[K, V]
	next, prev *arcNode[K, V]
	inT2       bool
}

// arcGhostNode tracks a recently-evicted key (no value).
type arcGhostNode[K comparable] struct {
	key        K
	next, prev *arcGhostNode[K]
}

// newARC constructs an empty ARC policy sized for the given shard
// capacity.
func newARC[K comparable, V any](budget int) *arcPolicy[K, V] {
	if budget < 1 {
		budget = 1
	}
	return &arcPolicy[K, V]{
		c:     budget,
		b1Set: make(map[K]*arcGhostNode[K]),
		b2Set: make(map[K]*arcGhostNode[K]),
	}
}

// OnInsert handles ARC's three insertion cases (paper §III, cases
// II–IV restated for the post-insert architecture):
//
//  1. key ∈ B1 — recency ghost hit: increase p, send entry to T2.
//  2. key ∈ B2 — frequency ghost hit: decrease p, send entry to T2.
//  3. key ∉ any list — new entry, send to T1.
func (p *arcPolicy[K, V]) OnInsert(e *entry[K, V]) {
	n := &arcNode[K, V]{entry: e}
	e.policyData = n
	switch {
	case p.inB1(e.key):
		// Recency adaptation.
		delta := 1
		if p.b1Size > 0 && p.b2Size > p.b1Size {
			delta = max(p.b2Size/p.b1Size, 1)
		}
		p.p = min(p.p+delta, p.c)
		p.removeFromB1(e.key)
		n.inT2 = true
		p.pushT2Head(n)
	case p.inB2(e.key):
		// Frequency adaptation.
		delta := 1
		if p.b2Size > 0 && p.b1Size > p.b2Size {
			delta = max(p.b1Size/p.b2Size, 1)
		}
		p.p = max(p.p-delta, 0)
		p.removeFromB2(e.key)
		n.inT2 = true
		p.pushT2Head(n)
	default:
		p.pushT1Head(n)
	}
}

// OnAccess handles ARC's "case I" (cache hit): if the entry is in
// T1, transition it to T2 (it has now been seen twice). If already
// in T2, just LRU-promote to MRU.
func (p *arcPolicy[K, V]) OnAccess(e *entry[K, V]) {
	n, ok := e.policyData.(*arcNode[K, V])
	if !ok || n == nil {
		return
	}
	if !n.inT2 {
		p.unlinkT1(n)
		n.inT2 = true
		p.pushT2Head(n)
		return
	}
	if n != p.t2Head {
		p.unlinkT2(n)
		p.pushT2Head(n)
	}
}

// OnUpdate is treated as an access.
func (p *arcPolicy[K, V]) OnUpdate(e *entry[K, V]) {
	p.OnAccess(e)
}

// OnRemove unlinks e from whichever sub-cache holds it.
func (p *arcPolicy[K, V]) OnRemove(e *entry[K, V]) {
	n, ok := e.policyData.(*arcNode[K, V])
	if !ok || n == nil {
		return
	}
	if n.inT2 {
		p.unlinkT2(n)
	} else {
		p.unlinkT1(n)
	}
	n.entry = nil
	n.next, n.prev = nil, nil
	e.policyData = nil
}

// Victim implements ARC's REPLACE decision: if T1's size exceeds the
// adaptive target p (or T2 is empty), drop the LRU of T1 and remember
// the key in B1; otherwise drop the LRU of T2 and remember the key
// in B2.
func (p *arcPolicy[K, V]) Victim() *entry[K, V] {
	if p.t1Size+p.t2Size == 0 {
		return nil
	}
	useT1 := p.t1Size > 0 && (p.t2Size == 0 || p.t1Size > p.p)
	if useT1 {
		n := p.t1Tail
		if n == nil {
			n = p.t2Tail
			if n == nil {
				return nil
			}
			p.unlinkT2(n)
			p.recordB2(n.entry.key)
			return n.entry
		}
		p.unlinkT1(n)
		p.recordB1(n.entry.key)
		return n.entry
	}
	n := p.t2Tail
	if n == nil {
		n = p.t1Tail
		if n == nil {
			return nil
		}
		p.unlinkT1(n)
		p.recordB1(n.entry.key)
		return n.entry
	}
	p.unlinkT2(n)
	p.recordB2(n.entry.key)
	return n.entry
}

// Len returns |T1| + |T2|. Ghost sizes are not included.
func (p *arcPolicy[K, V]) Len() int { return p.t1Size + p.t2Size }

// Reset clears all four lists and the adaptive parameter.
func (p *arcPolicy[K, V]) Reset() {
	for n := p.t1Head; n != nil; {
		nxt := n.next
		if n.entry != nil {
			n.entry.policyData = nil
		}
		n.entry = nil
		n.next, n.prev = nil, nil
		n = nxt
	}
	for n := p.t2Head; n != nil; {
		nxt := n.next
		if n.entry != nil {
			n.entry.policyData = nil
		}
		n.entry = nil
		n.next, n.prev = nil, nil
		n = nxt
	}
	p.t1Head, p.t1Tail = nil, nil
	p.t2Head, p.t2Tail = nil, nil
	p.b1Head, p.b1Tail = nil, nil
	p.b2Head, p.b2Tail = nil, nil
	p.t1Size, p.t2Size, p.b1Size, p.b2Size = 0, 0, 0, 0
	p.b1Set = make(map[K]*arcGhostNode[K])
	p.b2Set = make(map[K]*arcGhostNode[K])
	p.p = 0
}

func (p *arcPolicy[K, V]) inB1(k K) bool { _, ok := p.b1Set[k]; return ok }
func (p *arcPolicy[K, V]) inB2(k K) bool { _, ok := p.b2Set[k]; return ok }

func (p *arcPolicy[K, V]) pushT1Head(n *arcNode[K, V]) {
	n.inT2 = false
	n.prev = nil
	n.next = p.t1Head
	if p.t1Head != nil {
		p.t1Head.prev = n
	} else {
		p.t1Tail = n
	}
	p.t1Head = n
	p.t1Size++
}

func (p *arcPolicy[K, V]) pushT2Head(n *arcNode[K, V]) {
	n.inT2 = true
	n.prev = nil
	n.next = p.t2Head
	if p.t2Head != nil {
		p.t2Head.prev = n
	} else {
		p.t2Tail = n
	}
	p.t2Head = n
	p.t2Size++
}

func (p *arcPolicy[K, V]) unlinkT1(n *arcNode[K, V]) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case p.t1Head == n:
		p.t1Head = n.next
	default:
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case p.t1Tail == n:
		p.t1Tail = n.prev
	}
	n.next, n.prev = nil, nil
	p.t1Size--
}

func (p *arcPolicy[K, V]) unlinkT2(n *arcNode[K, V]) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case p.t2Head == n:
		p.t2Head = n.next
	default:
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case p.t2Tail == n:
		p.t2Tail = n.prev
	}
	n.next, n.prev = nil, nil
	p.t2Size--
}

// recordB1 pushes key onto B1's MRU end and trims B1 to satisfy
// |T1|+|B1| ≤ c.
func (p *arcPolicy[K, V]) recordB1(key K) {
	if _, exists := p.b1Set[key]; exists {
		return
	}
	g := &arcGhostNode[K]{key: key, next: p.b1Head}
	if p.b1Head != nil {
		p.b1Head.prev = g
	} else {
		p.b1Tail = g
	}
	p.b1Head = g
	p.b1Size++
	p.b1Set[key] = g

	limit := max(p.c-p.t1Size, 0)
	for p.b1Size > limit && p.b1Tail != nil {
		p.unlinkB1(p.b1Tail)
	}
}

// recordB2 pushes key onto B2's MRU end and trims B2 to satisfy
// |T2|+|B2| ≤ 2c.
func (p *arcPolicy[K, V]) recordB2(key K) {
	if _, exists := p.b2Set[key]; exists {
		return
	}
	g := &arcGhostNode[K]{key: key, next: p.b2Head}
	if p.b2Head != nil {
		p.b2Head.prev = g
	} else {
		p.b2Tail = g
	}
	p.b2Head = g
	p.b2Size++
	p.b2Set[key] = g

	limit := max(2*p.c-p.t2Size, 0)
	for p.b2Size > limit && p.b2Tail != nil {
		p.unlinkB2(p.b2Tail)
	}
}

func (p *arcPolicy[K, V]) removeFromB1(key K) {
	g, ok := p.b1Set[key]
	if !ok {
		return
	}
	p.unlinkB1(g)
}

func (p *arcPolicy[K, V]) removeFromB2(key K) {
	g, ok := p.b2Set[key]
	if !ok {
		return
	}
	p.unlinkB2(g)
}

func (p *arcPolicy[K, V]) unlinkB1(g *arcGhostNode[K]) {
	if g.prev != nil {
		g.prev.next = g.next
	} else if p.b1Head == g {
		p.b1Head = g.next
	}
	if g.next != nil {
		g.next.prev = g.prev
	} else if p.b1Tail == g {
		p.b1Tail = g.prev
	}
	delete(p.b1Set, g.key)
	g.next, g.prev = nil, nil
	p.b1Size--
}

func (p *arcPolicy[K, V]) unlinkB2(g *arcGhostNode[K]) {
	if g.prev != nil {
		g.prev.next = g.next
	} else if p.b2Head == g {
		p.b2Head = g.next
	}
	if g.next != nil {
		g.next.prev = g.prev
	} else if p.b2Tail == g {
		p.b2Tail = g.prev
	}
	delete(p.b2Set, g.key)
	g.next, g.prev = nil, nil
	p.b2Size--
}
