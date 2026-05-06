package memcache

// fifoPolicy is the simplest possible eviction policy: entries leave
// in the order they arrived. No promotion on access. Used as a
// fallback for streaming workloads where access locality is poor and
// for tests that want predictable behavior.
//
// fifoPolicy is NOT safe for concurrent use; the shard's mutex
// serializes all calls.
type fifoPolicy[K comparable, V any] struct {
	head *fifoNode[K, V] // oldest (next victim)
	tail *fifoNode[K, V] // newest
	size int
}

type fifoNode[K comparable, V any] struct {
	entry *entry[K, V]
	next  *fifoNode[K, V]
	prev  *fifoNode[K, V]
}

// newFIFO returns an empty FIFO policy.
func newFIFO[K comparable, V any]() *fifoPolicy[K, V] {
	return &fifoPolicy[K, V]{}
}

// OnInsert appends e at the tail.
func (p *fifoPolicy[K, V]) OnInsert(e *entry[K, V]) {
	n := &fifoNode[K, V]{entry: e}
	e.policyData = n
	if p.tail == nil {
		p.head = n
		p.tail = n
	} else {
		n.prev = p.tail
		p.tail.next = n
		p.tail = n
	}
	p.size++
}

// OnAccess is a no-op: FIFO does not promote on access.
func (p *fifoPolicy[K, V]) OnAccess(_ *entry[K, V]) {}

// OnUpdate is a no-op: replacing a value does not change FIFO order.
func (p *fifoPolicy[K, V]) OnUpdate(_ *entry[K, V]) {}

// OnRemove unlinks the entry's node and clears its policy reference.
func (p *fifoPolicy[K, V]) OnRemove(e *entry[K, V]) {
	n, ok := e.policyData.(*fifoNode[K, V])
	if !ok || n == nil {
		return
	}
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		p.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		p.tail = n.prev
	}
	n.entry = nil
	n.next = nil
	n.prev = nil
	e.policyData = nil
	p.size--
}

// Victim returns the oldest entry without removing it.
func (p *fifoPolicy[K, V]) Victim() *entry[K, V] {
	if p.head == nil {
		return nil
	}
	return p.head.entry
}

// Len returns the number of entries tracked.
func (p *fifoPolicy[K, V]) Len() int { return p.size }

// SetBudget is a no-op: FIFO has no internal sub-budget.
func (p *fifoPolicy[K, V]) SetBudget(int) {}

// Reset clears the policy state.
func (p *fifoPolicy[K, V]) Reset() {
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

// Snapshot returns a [PolicyDetailFIFO] summarizing the policy's
// current state.
func (p *fifoPolicy[K, V]) Snapshot() any {
	return PolicyDetailFIFO{Size: p.size}
}

// PromotionNeeded reports false unconditionally — FIFO never
// promotes on access, so the read fast path can serve every hit
// under a read lock.
func (p *fifoPolicy[K, V]) PromotionNeeded(*entry[K, V]) bool { return false }
