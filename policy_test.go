package memcache

import (
	"math/rand/v2"
	"testing"
)

// zipfianKeys generates a synthetic Zipfian access trace: a small
// number of keys are accessed disproportionately often. The
// distribution is a discrete Zipf with parameter s=1.05, range
// [0, n).
type zipfianKeys struct {
	s   *rand.Zipf
	rng *rand.Rand
}

func newZipfian(seed uint64, n uint64) *zipfianKeys {
	rng := rand.New(rand.NewPCG(seed, seed+1))
	z := rand.NewZipf(rng, 1.05, 1.0, n-1)
	return &zipfianKeys{s: z, rng: rng}
}

func (z *zipfianKeys) Next() int { return int(z.s.Uint64()) }

// hitRateUnderZipf runs a fixed-budget cache with the given policy
// against a Zipfian access stream and returns the observed hit
// rate. The policy is exercised purely through the policy interface
// (no shard, no map) so we are measuring the policy's choices, not
// the cache plumbing.
func hitRateUnderZipf(p evictionPolicy[int, struct{}], capacity int, ops int) float64 {
	live := make(map[int]*entry[int, struct{}], capacity*2)

	z := newZipfian(0xC0DE_C0DE, uint64(capacity*4))
	hits := 0
	for range ops {
		k := z.Next()
		if e, ok := live[k]; ok {
			p.OnAccess(e)
			hits++
			continue
		}
		// Miss: insert and (if needed) evict via the policy.
		ent := &entry[int, struct{}]{key: k}
		live[k] = ent
		p.OnInsert(ent)
		for len(live) > capacity {
			v := p.Victim()
			if v == nil {
				break
			}
			delete(live, v.key)
			p.OnRemove(v)
		}
	}
	return float64(hits) / float64(ops)
}

// TestPoliciesBeatRandomOnZipf is a sanity check: every implemented
// policy should achieve a hit rate noticeably above what a random
// cache would (~1/4 since capacity is 25% of the working set).
func TestPoliciesBeatRandomOnZipf(t *testing.T) {
	const (
		capacity = 64
		ops      = 20000
		minRate  = 0.30 // every policy in this set clears 30% on s=1.05 Zipf
	)
	cases := []struct {
		name string
		make func() evictionPolicy[int, struct{}]
	}{
		{"LRU", func() evictionPolicy[int, struct{}] { return newLRU[int, struct{}]() }},
		{"FIFO", func() evictionPolicy[int, struct{}] { return newFIFO[int, struct{}]() }},
		{"LFU", func() evictionPolicy[int, struct{}] { return newLFU[int, struct{}]() }},
		{"S3FIFO", func() evictionPolicy[int, struct{}] { return newS3FIFO[int, struct{}](capacity) }},
		{"2Q", func() evictionPolicy[int, struct{}] { return newTwoQ[int, struct{}](capacity) }},
		{"ARC", func() evictionPolicy[int, struct{}] { return newARC[int, struct{}](capacity) }},
		{
			"TinyLFU",
			func() evictionPolicy[int, struct{}] {
				return newTinyLFU[int, struct{}](capacity, func(k int) uint64 { return uint64(k) })
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rate := hitRateUnderZipf(tc.make(), capacity, ops)
			t.Logf("%s hit rate: %.3f", tc.name, rate)
			if rate < minRate {
				t.Errorf("%s hit rate %.3f below %.2f sanity floor",
					tc.name, rate, minRate)
			}
		})
	}
}

// TestS3FIFOBeatsLRUOnZipf is the spec's headline guarantee: S3-FIFO
// should outperform LRU on Zipfian workloads. This test runs both
// policies against the same trace and asserts S3-FIFO ≥ LRU. The
// margin is small at this synthetic scale (capacity=64, 20k ops),
// so we only assert the inequality, not a percentage gap.
func TestS3FIFOBeatsLRUOnZipf(t *testing.T) {
	const (
		capacity = 128
		ops      = 50000
	)
	lru := hitRateUnderZipf(newLRU[int, struct{}](), capacity, ops)
	s3 := hitRateUnderZipf(newS3FIFO[int, struct{}](capacity), capacity, ops)
	t.Logf("LRU=%.3f  S3FIFO=%.3f", lru, s3)
	if s3+0.001 < lru {
		t.Errorf("S3-FIFO (%.3f) should match or beat LRU (%.3f) on Zipf",
			s3, lru)
	}
}

// TestNewPolicyFallback verifies that an unknown Policy value falls
// back to LRU rather than returning nil.
func TestNewPolicyFallback(t *testing.T) {
	p := newPolicy[string, int](Policy(255), policyConfig[string]{budget: 4})
	if p == nil {
		t.Fatal("newPolicy returned nil for unknown policy")
	}
	if _, ok := p.(*lruPolicy[string, int]); !ok {
		t.Errorf("fallback policy = %T, want *lruPolicy", p)
	}
}

// TestNewPolicyDispatch verifies each Policy enum value produces the
// expected concrete type.
func TestNewPolicyDispatch(t *testing.T) {
	cfg := policyConfig[string]{budget: 8, hasher: func(string) uint64 { return 0 }}
	cases := []struct {
		policy Policy
		check  func(evictionPolicy[string, int]) bool
	}{
		{PolicyLRU, func(p evictionPolicy[string, int]) bool { _, ok := p.(*lruPolicy[string, int]); return ok }},
		{PolicyFIFO, func(p evictionPolicy[string, int]) bool { _, ok := p.(*fifoPolicy[string, int]); return ok }},
		{PolicyS3FIFO, func(p evictionPolicy[string, int]) bool { _, ok := p.(*s3fifoPolicy[string, int]); return ok }},
		{PolicyLFU, func(p evictionPolicy[string, int]) bool { _, ok := p.(*lfuPolicy[string, int]); return ok }},
		{PolicyTinyLFU, func(p evictionPolicy[string, int]) bool { _, ok := p.(*tinyLFUPolicy[string, int]); return ok }},
		{Policy2Q, func(p evictionPolicy[string, int]) bool { _, ok := p.(*twoQPolicy[string, int]); return ok }},
		{PolicyARC, func(p evictionPolicy[string, int]) bool { _, ok := p.(*arcPolicy[string, int]); return ok }},
	}
	for _, tc := range cases {
		t.Run(tc.policy.String(), func(t *testing.T) {
			p := newPolicy[string, int](tc.policy, cfg)
			if !tc.check(p) {
				t.Errorf("policy %s dispatched to wrong type %T", tc.policy, p)
			}
		})
	}
}
