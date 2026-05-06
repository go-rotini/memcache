package memcache

import "github.com/go-rotini/memcache/internal/sketch"

// tinyLFUPolicy implements a W-TinyLFU eviction policy with the
// full three-segment layout (Einziger et al., ACM TOS 2017):
//
//   - Window LRU (1% of capacity): freshly inserted entries.
//   - Probationary LRU (~20% of Main): entries demoted from Window
//     that won admission against Probationary's tail; the "trial"
//     segment that supplies eviction victims.
//   - Protected LRU (~80% of Main): hot working set. An entry
//     promoted here came from a hit in Probationary, signaling
//     reuse. Eviction never targets Protected directly; instead a
//     full Protected demotes its LRU tail back to Probationary.
//
// A 4-bit count-min sketch records access frequencies; the
// admission gate at Window→Probationary uses sketch frequencies.
// Sketch aging (halving every 2 × budget operations) keeps
// estimates responsive.
//
// tinyLFUPolicy is NOT safe for concurrent use.
type tinyLFUPolicy[K comparable, V any] struct {
	// Window LRU.
	windowHead, windowTail   *tinyLFUNode[K, V]
	windowSize, windowBudget int

	// Protected LRU (the larger, "hot" main segment).
	protectedHead, protectedTail   *tinyLFUNode[K, V]
	protectedSize, protectedBudget int

	// Probationary LRU (smaller "trial" segment, source of victims).
	probationaryHead, probationaryTail   *tinyLFUNode[K, V]
	probationarySize, probationaryBudget int

	// Frequency sketch.
	sketch *sketch.CountMinSketch
	hasher func(K) uint64

	// Operation counter for sketch aging.
	ops          uint64
	ageThreshold uint64
}

// tinyLFURegion enumerates the three segments an entry can live in.
type tinyLFURegion uint8

const (
	regionWindow       tinyLFURegion = 0
	regionProtected    tinyLFURegion = 1
	regionProbationary tinyLFURegion = 2
)

// tinyLFUNode is the intrusive LRU node attached to entry.policyData.
type tinyLFUNode[K comparable, V any] struct {
	entry      *entry[K, V]
	next, prev *tinyLFUNode[K, V]
	region     tinyLFURegion
}

// newTinyLFU constructs a fresh policy sized for the given shard
// budget. hasher may be nil; in that case the sketch falls back to
// a degenerate hash that still produces correct semantics on
// tests.
func newTinyLFU[K comparable, V any](budget int, hasher func(K) uint64) *tinyLFUPolicy[K, V] {
	window, protected, probationary := tinyLFUSplitBudget(budget)
	expected := max(budget, 64)
	cms := sketch.New(expected, nil)
	threshold := tinyLFUAgeThreshold(budget)
	return &tinyLFUPolicy[K, V]{
		windowBudget:       window,
		protectedBudget:    protected,
		probationaryBudget: probationary,
		sketch:             cms,
		hasher:             hasher,
		ageThreshold:       threshold,
	}
}

// tinyLFUSplitBudget computes (window, protected, probationary)
// sub-budgets. Window is ~1% of total; the remaining "Main" pool
// is split 80% Protected / 20% Probationary per the W-TinyLFU
// paper. Each sub-budget floors at 1 when the total budget allows
// any entries at all.
func tinyLFUSplitBudget(budget int) (window, protected, probationary int) {
	if budget <= 0 {
		return 0, 0, 0
	}
	window = max(budget/100, 1)
	if window >= budget {
		// Tiny budgets — keep window at 1, leave at most one slot
		// elsewhere.
		window = 1
	}
	main := max(budget-window, 1)
	protected = max((main*80)/100, 1)
	if protected >= main {
		protected = max(main-1, 1)
	}
	probationary = max(main-protected, 1)
	return window, protected, probationary
}

// tinyLFUAgeThreshold returns the operation count between sketch
// aging passes for the given budget.
func tinyLFUAgeThreshold(budget int) uint64 {
	t := uint64(2 * budget)
	if t == 0 {
		return 128
	}
	return t
}

// SetBudget recomputes all three sub-budgets and the aging
// threshold to match a new total.
func (p *tinyLFUPolicy[K, V]) SetBudget(budget int) {
	window, protected, probationary := tinyLFUSplitBudget(budget)
	p.windowBudget = window
	p.protectedBudget = protected
	p.probationaryBudget = probationary
	p.ageThreshold = tinyLFUAgeThreshold(budget)
}

// OnInsert places the new entry at the head of the Window LRU and
// records the key in the sketch.
func (p *tinyLFUPolicy[K, V]) OnInsert(e *entry[K, V]) {
	n := &tinyLFUNode[K, V]{entry: e}
	e.policyData = n
	p.pushHead(n, regionWindow)
	p.observe(e.key)
}

// OnAccess updates LRU position and bumps the sketch. A hit in
// Probationary triggers promotion to Protected — the signal that
// the entry is hot enough to graduate from the trial segment.
// Promotion may force a Protected→Probationary demotion to keep
// Protected within budget.
func (p *tinyLFUPolicy[K, V]) OnAccess(e *entry[K, V]) {
	n, ok := e.policyData.(*tinyLFUNode[K, V])
	if !ok || n == nil {
		return
	}
	switch n.region {
	case regionWindow:
		if n != p.windowHead {
			p.unlink(n)
			p.pushHead(n, regionWindow)
		}
	case regionProtected:
		if n != p.protectedHead {
			p.unlink(n)
			p.pushHead(n, regionProtected)
		}
	case regionProbationary:
		// Promote: Probationary → Protected.
		p.unlink(n)
		p.pushHead(n, regionProtected)
		// If Protected is now over budget, demote its tail back
		// to Probationary's head (it remains a candidate but
		// shows fresh activity by being at MRU of probationary).
		for p.protectedSize > p.protectedBudget && p.protectedTail != nil {
			demote := p.protectedTail
			p.unlink(demote)
			p.pushHead(demote, regionProbationary)
		}
	}
	p.observe(e.key)
}

// OnUpdate is treated as access.
func (p *tinyLFUPolicy[K, V]) OnUpdate(e *entry[K, V]) {
	p.OnAccess(e)
}

// OnRemove unlinks e from whichever segment holds it.
func (p *tinyLFUPolicy[K, V]) OnRemove(e *entry[K, V]) {
	n, ok := e.policyData.(*tinyLFUNode[K, V])
	if !ok || n == nil {
		return
	}
	p.unlink(n)
	n.entry = nil
	n.next, n.prev = nil, nil
	e.policyData = nil
}

// Victim runs the W-TinyLFU eviction step. Order of attention:
//  1. Window over budget: candidate = window tail. If
//     Probationary has spare capacity, demote in. Else compete
//     against Probationary's tail via sketch frequency.
//  2. Probationary over budget: evict its tail.
//  3. Protected over budget: demote its tail to Probationary
//     (no eviction yet — go around).
//  4. None over budget: return nil.
func (p *tinyLFUPolicy[K, V]) Victim() *entry[K, V] {
	for range p.windowSize + p.protectedSize + p.probationarySize + 1 {
		if p.windowSize > p.windowBudget && p.windowTail != nil {
			candidate := p.windowTail
			if v := p.handleWindowDemotion(candidate); v != nil {
				return v
			}
			continue
		}
		if p.probationarySize > p.probationaryBudget && p.probationaryTail != nil {
			n := p.probationaryTail
			p.unlink(n)
			return n.entry
		}
		if p.protectedSize > p.protectedBudget && p.protectedTail != nil {
			demote := p.protectedTail
			p.unlink(demote)
			p.pushHead(demote, regionProbationary)
			continue
		}
		return nil
	}
	// Defensive fallback — pick anything we have.
	for _, tail := range []*tinyLFUNode[K, V]{p.probationaryTail, p.windowTail, p.protectedTail} {
		if tail != nil {
			p.unlink(tail)
			return tail.entry
		}
	}
	return nil
}

// handleWindowDemotion runs the Window→Probationary admission
// step. Returns a non-nil entry when the candidate (or the
// probationary tail it displaced) should be evicted; nil when the
// candidate was just demoted in and the loop should continue.
func (p *tinyLFUPolicy[K, V]) handleWindowDemotion(candidate *tinyLFUNode[K, V]) *entry[K, V] {
	if p.probationarySize < p.probationaryBudget {
		// Free space — just demote.
		p.unlink(candidate)
		p.pushHead(candidate, regionProbationary)
		return nil
	}
	if p.probationaryTail == nil {
		// No room in Probationary at all — evict the candidate.
		p.unlink(candidate)
		return candidate.entry
	}
	candFreq := p.estimate(candidate.entry.key)
	victim := p.probationaryTail
	vicFreq := p.estimate(victim.entry.key)
	if candFreq > vicFreq {
		// Candidate wins admission: take victim's slot.
		p.unlink(candidate)
		p.unlink(victim)
		p.pushHead(candidate, regionProbationary)
		return victim.entry
	}
	// Probationary tail is hotter; evict the candidate.
	p.unlink(candidate)
	return candidate.entry
}

// Len returns the total number of entries tracked.
func (p *tinyLFUPolicy[K, V]) Len() int {
	return p.windowSize + p.protectedSize + p.probationarySize
}

// Reset clears every segment and the sketch.
func (p *tinyLFUPolicy[K, V]) Reset() {
	for _, head := range []*tinyLFUNode[K, V]{p.windowHead, p.protectedHead, p.probationaryHead} {
		for n := head; n != nil; {
			nxt := n.next
			if n.entry != nil {
				n.entry.policyData = nil
			}
			n.entry = nil
			n.next, n.prev = nil, nil
			n = nxt
		}
	}
	p.windowHead, p.windowTail = nil, nil
	p.protectedHead, p.protectedTail = nil, nil
	p.probationaryHead, p.probationaryTail = nil, nil
	p.windowSize, p.protectedSize, p.probationarySize = 0, 0, 0
	if p.sketch != nil {
		p.sketch.Reset()
	}
	p.ops = 0
}

// observe records an access in the sketch and triggers aging when
// ops crosses the configured threshold.
func (p *tinyLFUPolicy[K, V]) observe(key K) {
	if p.sketch == nil {
		return
	}
	p.sketch.Increment(p.hashKey(key))
	p.ops++
	if p.ops >= p.ageThreshold {
		p.sketch.Reset()
		p.ops = 0
	}
}

// estimate returns the sketch's frequency estimate for key.
func (p *tinyLFUPolicy[K, V]) estimate(key K) uint8 {
	if p.sketch == nil {
		return 0
	}
	return p.sketch.Estimate(p.hashKey(key))
}

// hashKey returns a stable uint64 hash of key. Falls back to a
// degenerate constant when no hasher is configured.
func (p *tinyLFUPolicy[K, V]) hashKey(key K) uint64 {
	if p.hasher == nil {
		return 0
	}
	return p.hasher(key)
}

// pushHead inserts n at the head of the named segment, updating
// node region and segment size. Caller must have already detached
// n from any prior list.
func (p *tinyLFUPolicy[K, V]) pushHead(n *tinyLFUNode[K, V], region tinyLFURegion) {
	n.region = region
	n.prev = nil
	switch region {
	case regionWindow:
		n.next = p.windowHead
		if p.windowHead != nil {
			p.windowHead.prev = n
		} else {
			p.windowTail = n
		}
		p.windowHead = n
		p.windowSize++
	case regionProtected:
		n.next = p.protectedHead
		if p.protectedHead != nil {
			p.protectedHead.prev = n
		} else {
			p.protectedTail = n
		}
		p.protectedHead = n
		p.protectedSize++
	case regionProbationary:
		n.next = p.probationaryHead
		if p.probationaryHead != nil {
			p.probationaryHead.prev = n
		} else {
			p.probationaryTail = n
		}
		p.probationaryHead = n
		p.probationarySize++
	}
}

// unlink removes n from its current segment, decrementing that
// segment's size. Safe to call once; subsequent calls on a node
// whose pointers have been nil'd are no-ops.
func (p *tinyLFUPolicy[K, V]) unlink(n *tinyLFUNode[K, V]) {
	switch n.region {
	case regionWindow:
		p.unlinkSegment(n, &p.windowHead, &p.windowTail, &p.windowSize)
	case regionProtected:
		p.unlinkSegment(n, &p.protectedHead, &p.protectedTail, &p.protectedSize)
	case regionProbationary:
		p.unlinkSegment(n, &p.probationaryHead, &p.probationaryTail, &p.probationarySize)
	}
}

// unlinkSegment is the shared list-detach helper.
func (p *tinyLFUPolicy[K, V]) unlinkSegment(n *tinyLFUNode[K, V], head, tail **tinyLFUNode[K, V], size *int) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case *head == n:
		*head = n.next
	default:
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case *tail == n:
		*tail = n.prev
	}
	n.next, n.prev = nil, nil
	*size--
}

// Snapshot returns a [PolicyDetailTinyLFU] summarizing the policy's
// current state.
func (p *tinyLFUPolicy[K, V]) Snapshot() any {
	return PolicyDetailTinyLFU{
		WindowSize: p.windowSize,
		MainSize:   p.protectedSize + p.probationarySize,
		SketchOps:  p.ops,
	}
}

// PromotionNeeded reports true — TinyLFU's OnAccess always bumps
// the frequency sketch and may move the entry between Window and
// the Main LRU. The fast path is therefore not safe.
func (p *tinyLFUPolicy[K, V]) PromotionNeeded(*entry[K, V]) bool { return true }
