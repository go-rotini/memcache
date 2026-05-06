package memcache

// lruPolicy is the classic doubly-linked-list + map LRU implementation
// specialized for the package's entry struct. The list is intrusive —
// pointers live on the entry's policyData field — so each access
// avoids per-call allocation.
//
// lruPolicy is NOT safe for concurrent use; the shard's mutex
// serializes all calls.
type lruPolicy[K comparable, V any] struct {
	head *lruNode[K, V] // most recently used
	tail *lruNode[K, V] // least recently used (next victim)
	size int
}

// lruNode is the doubly-linked-list node attached to an entry's
// policyData. We use a pointer-bearing wrapper (rather than embedding
// next/prev directly in entry) so that policies can be swapped at
// runtime without bloating every entry with policy-specific fields.
type lruNode[K comparable, V any] struct {
	entry *entry[K, V]
	next  *lruNode[K, V]
	prev  *lruNode[K, V]
}

// newLRU returns an empty LRU policy.
func newLRU[K comparable, V any]() *lruPolicy[K, V] {
	return &lruPolicy[K, V]{}
}

// OnInsert appends e at the head (most-recently-used) end.
func (p *lruPolicy[K, V]) OnInsert(e *entry[K, V]) {
	n := &lruNode[K, V]{entry: e}
	e.policyData = n
	p.pushFront(n)
	p.size++
}

// OnAccess promotes e to the head if not already there.
func (p *lruPolicy[K, V]) OnAccess(e *entry[K, V]) {
	n, ok := e.policyData.(*lruNode[K, V])
	if !ok || n == nil {
		return
	}
	if n == p.head {
		return
	}
	p.unlink(n)
	p.pushFront(n)
}

// OnUpdate is identical to OnAccess for LRU.
func (p *lruPolicy[K, V]) OnUpdate(e *entry[K, V]) {
	p.OnAccess(e)
}

// OnRemove drops the node from the list and detaches it from the entry.
func (p *lruPolicy[K, V]) OnRemove(e *entry[K, V]) {
	n, ok := e.policyData.(*lruNode[K, V])
	if !ok || n == nil {
		return
	}
	p.unlink(n)
	n.entry = nil
	e.policyData = nil
	p.size--
}

// Victim returns the entry at the LRU position without removing it.
// The caller is expected to follow up with OnRemove and the actual
// shard-map deletion.
func (p *lruPolicy[K, V]) Victim() *entry[K, V] {
	if p.tail == nil {
		return nil
	}
	return p.tail.entry
}

// Len returns the number of entries tracked.
func (p *lruPolicy[K, V]) Len() int { return p.size }

// Reset clears the entire list and detaches every node from its
// entry. Callers must stop using the policy until they re-OnInsert
// the entries they want to keep.
func (p *lruPolicy[K, V]) Reset() {
	for n := p.head; n != nil; {
		nxt := n.next
		if n.entry != nil {
			n.entry.policyData = nil
		}
		n.entry = nil
		n.next = nil
		n.prev = nil
		n = nxt
	}
	p.head = nil
	p.tail = nil
	p.size = 0
}

// pushFront inserts n at the head of the list. n must be detached.
func (p *lruPolicy[K, V]) pushFront(n *lruNode[K, V]) {
	n.prev = nil
	n.next = p.head
	if p.head != nil {
		p.head.prev = n
	}
	p.head = n
	if p.tail == nil {
		p.tail = n
	}
}

// unlink removes n from the list without detaching its entry pointer.
func (p *lruPolicy[K, V]) unlink(n *lruNode[K, V]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else if p.head == n {
		p.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else if p.tail == n {
		p.tail = n.prev
	}
	n.next = nil
	n.prev = nil
}
