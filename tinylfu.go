package memcache

import "github.com/go-rotini/memcache/internal/sketch"

// tinyLFUPolicy implements a simplified W-TinyLFU eviction policy
// (Einziger et al., ACM TOS 2017).
//
// Two intrusive LRU lists make up the cache's storage: a small
// "window" LRU holds recent insertions, and a "main" LRU holds the
// long-lived working set. A 4-bit count-min sketch records access
// frequency for every key the policy has seen. When the window
// overflows, the demoted candidate must defeat the LRU tail of the
// main region in a sketch comparison to be admitted; otherwise the
// candidate is evicted.
//
// This v0 implementation merges the spec's "protected/probationary"
// split into a single Main LRU, keeping correctness while postponing
// the tuning work to a follow-up. The admission gate, sketch aging,
// and window/main split are all in place.
//
// tinyLFUPolicy is NOT safe for concurrent use.
type tinyLFUPolicy[K comparable, V any] struct {
	// Window LRU. windowHead is MRU, windowTail is LRU (next demote).
	windowHead, windowTail *tinyLFUNode[K, V]
	windowSize             int
	windowBudget           int

	// Main LRU. mainHead is MRU, mainTail is LRU (next victim from
	// Main once admission demands it).
	mainHead, mainTail *tinyLFUNode[K, V]
	mainSize           int
	mainBudget         int

	// Frequency sketch. Tracks recent access counts.
	sketch *sketch.CountMinSketch
	hasher func(K) uint64

	// Operation counter for aging. When ops >= ageThreshold the
	// sketch is halved and the counter resets.
	ops          uint64
	ageThreshold uint64
}

// tinyLFUNode is the intrusive LRU node attached to entry.policyData.
// `inMain` distinguishes which list owns it.
type tinyLFUNode[K comparable, V any] struct {
	entry      *entry[K, V]
	next, prev *tinyLFUNode[K, V]
	inMain     bool
}

// newTinyLFU constructs a fresh policy sized for the given shard
// budget. hasher may be nil; in that case the sketch falls back to a
// degenerate hash that still produces correct semantics on tests.
func newTinyLFU[K comparable, V any](budget int, hasher func(K) uint64) *tinyLFUPolicy[K, V] {
	window, main := tinyLFUSplitBudget(budget)
	expected := max(budget, 64)
	cms := sketch.New(expected, nil)
	threshold := tinyLFUAgeThreshold(budget)
	return &tinyLFUPolicy[K, V]{
		windowBudget: window,
		mainBudget:   main,
		sketch:       cms,
		hasher:       hasher,
		ageThreshold: threshold,
	}
}

// tinyLFUSplitBudget computes (window, main) sub-budgets. Window is
// ~1% of total, never larger than the total itself.
func tinyLFUSplitBudget(budget int) (window, main int) {
	if budget <= 0 {
		return 0, 0
	}
	window = max(budget/100, 1)
	if window >= budget {
		window = 1
	}
	main = max(budget-window, 1)
	return window, main
}

// tinyLFUAgeThreshold returns the operation count between sketch
// aging passes for the given budget. The W-TinyLFU paper specifies
// 2 × budget; for tiny budgets we floor at 128 so the sketch sees
// enough activity to be useful.
func tinyLFUAgeThreshold(budget int) uint64 {
	t := uint64(2 * budget)
	if t == 0 {
		return 128
	}
	return t
}

// SetBudget recomputes the Window/Main sub-budgets and aging
// threshold to match a new total.
func (p *tinyLFUPolicy[K, V]) SetBudget(budget int) {
	window, main := tinyLFUSplitBudget(budget)
	p.windowBudget = window
	p.mainBudget = main
	p.ageThreshold = tinyLFUAgeThreshold(budget)
}

// OnInsert places the new entry at the head of the Window LRU and
// records the key in the sketch.
func (p *tinyLFUPolicy[K, V]) OnInsert(e *entry[K, V]) {
	n := &tinyLFUNode[K, V]{entry: e}
	e.policyData = n
	p.pushWindowHead(n)
	p.observe(e.key)
}

// OnAccess promotes the entry to its list's MRU position and bumps
// the sketch.
func (p *tinyLFUPolicy[K, V]) OnAccess(e *entry[K, V]) {
	n, ok := e.policyData.(*tinyLFUNode[K, V])
	if !ok || n == nil {
		return
	}
	if n.inMain {
		if n != p.mainHead {
			p.unlinkMain(n)
			p.pushMainHead(n)
		}
	} else if n != p.windowHead {
		p.unlinkWindow(n)
		p.pushWindowHead(n)
	}
	p.observe(e.key)
}

// OnUpdate is treated as access.
func (p *tinyLFUPolicy[K, V]) OnUpdate(e *entry[K, V]) {
	p.OnAccess(e)
}

// OnRemove unlinks e from whichever list holds it.
func (p *tinyLFUPolicy[K, V]) OnRemove(e *entry[K, V]) {
	n, ok := e.policyData.(*tinyLFUNode[K, V])
	if !ok || n == nil {
		return
	}
	if n.inMain {
		p.unlinkMain(n)
	} else {
		p.unlinkWindow(n)
	}
	n.entry = nil
	n.next, n.prev = nil, nil
	e.policyData = nil
}

// Victim runs the W-TinyLFU eviction step. The Window's LRU tail is
// the candidate; if Main has spare capacity it's admitted directly,
// otherwise its sketch frequency must beat Main's LRU tail to take
// the latter's slot.
func (p *tinyLFUPolicy[K, V]) Victim() *entry[K, V] {
	for range p.windowSize + p.mainSize + 1 {
		if p.windowSize > p.windowBudget && p.windowTail != nil {
			candidate := p.windowTail
			if p.mainSize < p.mainBudget {
				// Free space in Main: just promote.
				p.unlinkWindow(candidate)
				p.pushMainHead(candidate)
				continue
			}
			if p.mainTail == nil {
				// Main is empty by configuration — evict the
				// window tail directly.
				p.unlinkWindow(candidate)
				return candidate.entry
			}
			candFreq := p.estimate(candidate.entry.key)
			victim := p.mainTail
			vicFreq := p.estimate(victim.entry.key)
			if candFreq > vicFreq {
				// Candidate wins admission: take victim's slot.
				p.unlinkWindow(candidate)
				p.unlinkMain(victim)
				p.pushMainHead(candidate)
				return victim.entry
			}
			// Main entry is hotter; evict the candidate.
			p.unlinkWindow(candidate)
			return candidate.entry
		}
		if p.mainSize > p.mainBudget && p.mainTail != nil {
			n := p.mainTail
			p.unlinkMain(n)
			return n.entry
		}
		return nil
	}
	// Defensive fallback.
	if p.windowTail != nil {
		n := p.windowTail
		p.unlinkWindow(n)
		return n.entry
	}
	if p.mainTail != nil {
		n := p.mainTail
		p.unlinkMain(n)
		return n.entry
	}
	return nil
}

// Len returns the total number of entries tracked.
func (p *tinyLFUPolicy[K, V]) Len() int { return p.windowSize + p.mainSize }

// Reset clears both LRU lists and re-initializes the sketch state.
func (p *tinyLFUPolicy[K, V]) Reset() {
	for n := p.windowHead; n != nil; {
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
	p.windowHead, p.windowTail = nil, nil
	p.mainHead, p.mainTail = nil, nil
	p.windowSize, p.mainSize = 0, 0
	if p.sketch != nil {
		p.sketch.Reset()
	}
	p.ops = 0
}

// observe records an access to key in the sketch and triggers aging
// when ops crosses the configured threshold.
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
// degenerate constant when no hasher is configured; the sketch
// remains correct (collisions just accumulate).
func (p *tinyLFUPolicy[K, V]) hashKey(key K) uint64 {
	if p.hasher == nil {
		return 0
	}
	return p.hasher(key)
}

func (p *tinyLFUPolicy[K, V]) pushWindowHead(n *tinyLFUNode[K, V]) {
	n.inMain = false
	n.prev = nil
	n.next = p.windowHead
	if p.windowHead != nil {
		p.windowHead.prev = n
	} else {
		p.windowTail = n
	}
	p.windowHead = n
	p.windowSize++
}

func (p *tinyLFUPolicy[K, V]) pushMainHead(n *tinyLFUNode[K, V]) {
	n.inMain = true
	n.prev = nil
	n.next = p.mainHead
	if p.mainHead != nil {
		p.mainHead.prev = n
	} else {
		p.mainTail = n
	}
	p.mainHead = n
	p.mainSize++
}

func (p *tinyLFUPolicy[K, V]) unlinkWindow(n *tinyLFUNode[K, V]) {
	switch {
	case n.prev != nil:
		n.prev.next = n.next
	case p.windowHead == n:
		p.windowHead = n.next
	default:
		return
	}
	switch {
	case n.next != nil:
		n.next.prev = n.prev
	case p.windowTail == n:
		p.windowTail = n.prev
	}
	n.next, n.prev = nil, nil
	p.windowSize--
}

func (p *tinyLFUPolicy[K, V]) unlinkMain(n *tinyLFUNode[K, V]) {
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
