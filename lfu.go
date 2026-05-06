package memcache

// lfuPolicy implements an O(1)-eviction Least-Frequently-Used policy
// using a frequency-bucket list. Each distinct access count maps to
// a doubly-linked list of entries with that count; on access an
// entry hops from its current bucket to bucket+1. Eviction returns
// the head of the lowest non-empty bucket, breaking ties by oldest
// insertion within the bucket (FIFO order inside each list).
//
// minFreq is maintained so Victim is O(1). It increments only when
// the bucket at minFreq becomes empty due to a promotion; on insert
// it resets to 1 (every new entry enters at freq=1).
//
// lfuPolicy is NOT safe for concurrent use.
type lfuPolicy[K comparable, V any] struct {
	buckets map[uint64]*lfuList[K, V]
	minFreq uint64
	size    int
}

// lfuList is a doubly-linked list of entries at a single frequency
// level. head is oldest within the bucket (the next victim if minFreq
// targets this bucket); tail is most-recently-promoted-to-this-level.
type lfuList[K comparable, V any] struct {
	head, tail *lfuNode[K, V]
}

// lfuNode is the intrusive node attached to entry.policyData.
type lfuNode[K comparable, V any] struct {
	entry      *entry[K, V]
	next, prev *lfuNode[K, V]
	freq       uint64
}

// newLFU constructs an empty LFU policy.
func newLFU[K comparable, V any]() *lfuPolicy[K, V] {
	return &lfuPolicy[K, V]{
		buckets: make(map[uint64]*lfuList[K, V]),
	}
}

// OnInsert places e in the freq=1 bucket and resets minFreq so the
// next eviction targets the freshly inserted region. Inserting at
// freq=1 (not 0) keeps the algorithm consistent: freq counts visits,
// and an insert is the entry's first visit.
func (p *lfuPolicy[K, V]) OnInsert(e *entry[K, V]) {
	n := &lfuNode[K, V]{entry: e, freq: 1}
	e.policyData = n
	p.bucket(1).pushTail(n)
	p.minFreq = 1
	p.size++
}

// OnAccess promotes e to the next-higher frequency bucket.
func (p *lfuPolicy[K, V]) OnAccess(e *entry[K, V]) {
	n, ok := e.policyData.(*lfuNode[K, V])
	if !ok || n == nil {
		return
	}
	old := n.freq
	p.bucket(old).remove(n)
	if p.bucketEmpty(old) {
		delete(p.buckets, old)
		if p.minFreq == old {
			p.minFreq = old + 1
		}
	}
	n.freq = old + 1
	p.bucket(n.freq).pushTail(n)
}

// OnUpdate is identical to OnAccess: an update counts as activity.
func (p *lfuPolicy[K, V]) OnUpdate(e *entry[K, V]) {
	p.OnAccess(e)
}

// OnRemove removes e from its bucket. Safe to call after Victim has
// already pulled the node out (in which case n.freq's bucket was
// emptied; remove handles the no-op case).
func (p *lfuPolicy[K, V]) OnRemove(e *entry[K, V]) {
	n, ok := e.policyData.(*lfuNode[K, V])
	if !ok || n == nil {
		return
	}
	if list, exists := p.buckets[n.freq]; exists && (n.next != nil || n.prev != nil || list.head == n) {
		list.remove(n)
		if p.bucketEmpty(n.freq) {
			delete(p.buckets, n.freq)
		}
	}
	n.entry = nil
	n.next, n.prev = nil, nil
	e.policyData = nil
	p.size--
}

// Victim returns the head of the lowest non-empty bucket. The node is
// removed from its list before return; OnRemove will then clear the
// entry's policyData reference.
func (p *lfuPolicy[K, V]) Victim() *entry[K, V] {
	for p.size > 0 {
		list, ok := p.buckets[p.minFreq]
		if !ok || list.head == nil {
			// Walk forward to find the next non-empty bucket.
			next, found := p.nextNonEmpty(p.minFreq)
			if !found {
				return nil
			}
			p.minFreq = next
			continue
		}
		n := list.head
		list.remove(n)
		if p.bucketEmpty(n.freq) {
			delete(p.buckets, n.freq)
		}
		return n.entry
	}
	return nil
}

// Len returns the number of entries tracked by the policy.
func (p *lfuPolicy[K, V]) Len() int { return p.size }

// SetBudget is a no-op: LFU does not track a capacity bound; the
// cache's shard-level budget is the only signal it needs.
func (p *lfuPolicy[K, V]) SetBudget(int) {}

// Reset clears every bucket.
func (p *lfuPolicy[K, V]) Reset() {
	for _, list := range p.buckets {
		for n := list.head; n != nil; {
			nxt := n.next
			if n.entry != nil {
				n.entry.policyData = nil
			}
			n.entry = nil
			n.next, n.prev = nil, nil
			n = nxt
		}
	}
	p.buckets = make(map[uint64]*lfuList[K, V])
	p.minFreq = 0
	p.size = 0
}

// bucket returns the list for freq, creating it on demand.
func (p *lfuPolicy[K, V]) bucket(freq uint64) *lfuList[K, V] {
	list, ok := p.buckets[freq]
	if !ok {
		list = &lfuList[K, V]{}
		p.buckets[freq] = list
	}
	return list
}

// bucketEmpty reports whether the bucket at freq is empty (or
// missing).
func (p *lfuPolicy[K, V]) bucketEmpty(freq uint64) bool {
	list, ok := p.buckets[freq]
	return !ok || list.head == nil
}

// nextNonEmpty scans forward from `from` (exclusive) for the smallest
// freq with a non-empty bucket. The scan is O(distinct freqs) in the
// worst case; in practice freq levels cluster so the cost is small.
func (p *lfuPolicy[K, V]) nextNonEmpty(from uint64) (uint64, bool) {
	if len(p.buckets) == 0 {
		return 0, false
	}
	best := uint64(0)
	found := false
	for f, list := range p.buckets {
		if f <= from {
			continue
		}
		if list.head == nil {
			continue
		}
		if !found || f < best {
			best = f
			found = true
		}
	}
	return best, found
}

// pushTail appends n to the tail of the list.
func (l *lfuList[K, V]) pushTail(n *lfuNode[K, V]) {
	n.prev = l.tail
	n.next = nil
	if l.tail != nil {
		l.tail.next = n
	} else {
		l.head = n
	}
	l.tail = n
}

// remove unlinks n from the list. Safe to call once; subsequent calls
// on the same node are no-ops because n.next/n.prev get nil'd.
func (l *lfuList[K, V]) remove(n *lfuNode[K, V]) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case l.head == n:
		l.head = n.next
	default:
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case l.tail == n:
		l.tail = n.prev
	}
	n.next, n.prev = nil, nil
}
