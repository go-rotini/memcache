package memcache

import (
	"testing"
	"time"
)

func TestPolicyString(t *testing.T) {
	cases := map[Policy]string{
		PolicyS3FIFO:  "s3fifo",
		PolicyLRU:     "lru",
		PolicyLFU:     "lfu",
		PolicyTinyLFU: "tinylfu",
		PolicyFIFO:    "fifo",
		PolicyARC:     "arc",
		Policy2Q:      "2q",
	}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("Policy(%d).String() = %q, want %q", p, got, want)
		}
	}
	if got := Policy(99).String(); got != "unknown" {
		t.Errorf("unknown Policy.String() = %q, want %q", got, "unknown")
	}
}

func TestEvictionReasonString(t *testing.T) {
	cases := map[EvictionReason]string{
		EvictReasonCapacity:      "capacity",
		EvictReasonExpired:       "expired",
		EvictReasonExpireFunc:    "expire-func",
		EvictReasonReplaced:      "replaced",
		EvictReasonDeleted:       "deleted",
		EvictReasonDeletedPrefix: "deleted-prefix",
		EvictReasonDeletedWhere:  "deleted-where",
		EvictReasonTag:           "tag",
		EvictReasonRemote:        "remote",
		EvictReasonResize:        "resize",
		EvictReasonComputed:      "computed",
		EvictReasonClear:         "clear",
		EvictReasonClose:         "close",
		EvictReasonLoadError:     "load-error",
	}
	for r, want := range cases {
		if got := r.String(); got != want {
			t.Errorf("Reason(%d).String() = %q, want %q", r, got, want)
		}
	}
	if got := EvictionReason(255).String(); got != "unknown" {
		t.Errorf("unknown reason.String() = %q, want %q", got, "unknown")
	}
}

func TestNumEvictionReasonsMatchesEnum(t *testing.T) {
	// numEvictionReasons should equal the count of reason constants.
	// If a new reason is added without updating numEvictionReasons, the
	// per-reason counters in Stats will under-allocate.
	last := EvictReasonLoadError
	if int(last)+1 != numEvictionReasons {
		t.Errorf("numEvictionReasons=%d but last reason index is %d (want %d)",
			numEvictionReasons, last, int(last)+1)
	}
}

func TestEventKindString(t *testing.T) {
	cases := map[EventKind]string{
		EventInsert:          "insert",
		EventUpdate:          "update",
		EventEvict:           "evict",
		EventExpire:          "expire",
		EventLoad:            "load",
		EventLoadError:       "load-error",
		EventLoadTimeout:     "load-timeout",
		EventLoadRateLimited: "load-rate-limited",
		EventInvalidateTag:   "invalidate-tag",
		EventResize:          "resize",
		EventSnapshot:        "snapshot",
		EventCompute:         "compute",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Errorf("EventKind(%d).String() = %q, want %q", k, got, want)
		}
	}
	if got := EventKind(255).String(); got != "unknown" {
		t.Errorf("unknown EventKind.String() = %q, want %q", got, "unknown")
	}
}

func TestComputeActionString(t *testing.T) {
	cases := map[ComputeAction]string{
		ComputeStore:  "store",
		ComputeDelete: "delete",
		ComputeNoOp:   "no-op",
	}
	for a, want := range cases {
		if got := a.String(); got != want {
			t.Errorf("ComputeAction(%d).String() = %q, want %q", a, got, want)
		}
	}
	if got := ComputeAction(99).String(); got != "unknown" {
		t.Errorf("unknown ComputeAction.String() = %q, want %q", got, "unknown")
	}
}

func TestItemZeroValueIsValid(t *testing.T) {
	var it Item[string]
	if it.Hits != 0 || it.Weight != 0 || !it.Expiry.IsZero() {
		t.Error("zero Item should have all zero fields")
	}
}

func TestKeyedItemEmbedding(t *testing.T) {
	ki := KeyedItem[string, int]{
		Key: "k",
		Item: Item[int]{
			Value:    42,
			Inserted: time.Unix(1, 0),
		},
	}
	if ki.Key != "k" || ki.Value != 42 {
		t.Error("KeyedItem failed to expose embedded fields")
	}
}

func TestLoadResultZero(t *testing.T) {
	var r LoadResult[int]
	if r.Value != 0 || r.TTL != 0 || r.Err != nil {
		t.Error("zero LoadResult should be empty")
	}
}
