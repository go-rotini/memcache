package memcache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// drainEvents pulls all events currently buffered in ch up to a
// brief deadline and returns them. Used to assert event content
// without flaking on goroutine timing.
func drainEvents[K comparable, V any](ch <-chan Event[K, V]) []Event[K, V] {
	deadline := time.After(100 * time.Millisecond)
	var out []Event[K, V]
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, e)
		case <-deadline:
			return out
		}
	}
}

func TestSubscribeReceivesInsertEvent(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	ch, cancel := c.Subscribe(8, EventInsert)
	defer cancel()

	_ = c.Set("k", 1)
	events := drainEvents(ch)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1: %+v", len(events), events)
	}
	got := events[0]
	if got.Kind != EventInsert || got.Key != "k" || got.Value != 1 {
		t.Errorf("event = %+v, want EventInsert k=1", got)
	}
}

func TestSubscribeKindFilter(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	ch, cancel := c.Subscribe(8, EventEvict) // only evictions
	defer cancel()

	_ = c.Set("k", 1) // produces EventInsert — should NOT arrive
	c.Delete("k")     // produces EventEvict (reason=deleted)

	events := drainEvents(ch)
	if len(events) != 1 || events[0].Kind != EventEvict {
		t.Errorf("filter failed; got %+v", events)
	}
}

func TestSubscribeNoFilterReceivesAll(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	ch, cancel := c.Subscribe(16) // no filter
	defer cancel()

	_ = c.Set("k", 1) // EventInsert
	_ = c.Set("k", 2) // EventUpdate
	c.Delete("k")     // EventEvict

	events := drainEvents(ch)
	kinds := make(map[EventKind]int)
	for _, e := range events {
		kinds[e.Kind]++
	}
	if kinds[EventInsert] != 1 || kinds[EventUpdate] != 1 || kinds[EventEvict] != 1 {
		t.Errorf("kinds = %v, want each of Insert/Update/Evict once", kinds)
	}
}

func TestSubscribeFullBufferDrops(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(64))
	defer c.Close()
	ch, cancel := c.Subscribe(2, EventInsert) // tiny buffer
	defer cancel()

	for i := range 10 {
		_ = c.Set(itoaSimple(i), i)
	}
	// Don't drain; let some events buffer & rest drop.
	st := c.Stats()
	if st.EventsDropped == 0 {
		t.Errorf("EventsDropped = 0, want > 0 with full buffer")
	}
	_ = ch
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	ch, cancel := c.Subscribe(8)
	cancel()

	// Channel should be closed; receive returns immediately.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not close channel")
	}
}

func TestCloseClosesAllSubscribers(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	ch, _ := c.Subscribe(8)
	_ = c.Close()
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after Cache.Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Cache.Close did not close subscriber channel")
	}
}

func TestEventOnExpire(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(4), WithShards(1), WithClock(clk),
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()
	ch, cancel := c.Subscribe(8, EventExpire)
	defer cancel()

	_ = c.SetWithTTL("k", 99, time.Second)
	clk.Advance(2 * time.Second)
	_, _ = c.Get("k") // lazy expiration; produces EventExpire

	events := drainEvents(ch)
	if len(events) != 1 || events[0].Kind != EventExpire {
		t.Fatalf("expected EventExpire, got %+v", events)
	}
	if events[0].Key != "k" || events[0].Value != 99 {
		t.Errorf("expire event = %+v", events[0])
	}
	if events[0].Reason != EvictReasonExpired {
		t.Errorf("expire event reason = %v, want %v", events[0].Reason, EvictReasonExpired)
	}
}

func TestEventOnEvictReasonRecorded(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	ch, cancel := c.Subscribe(8, EventEvict)
	defer cancel()
	_ = c.Set("k", 1)
	c.Delete("k")
	events := drainEvents(ch)
	if len(events) != 1 || events[0].Reason != EvictReasonDeleted {
		t.Errorf("expected Evict(reason=Deleted), got %+v", events)
	}
}

func TestEventOnInvalidateTag(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	ch, cancel := c.Subscribe(8, EventInvalidateTag)
	defer cancel()
	_ = c.SetWithTags("k", 1, "g")
	_ = c.InvalidateTag("g")

	events := drainEvents(ch)
	if len(events) != 1 || events[0].Kind != EventInvalidateTag {
		t.Errorf("expected EventInvalidateTag, got %+v", events)
	}
}

func TestEventOnLoadSuccess(t *testing.T) {
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		return 42, 0, nil
	})
	c, _ := New[string, int](WithMaxEntries(4), WithLoader[string, int](loader))
	defer c.Close()
	ch, cancel := c.Subscribe(4, EventLoad, EventLoadError)
	defer cancel()
	_, _ = c.GetOrLoad(context.Background(), "k")

	events := drainEvents(ch)
	if len(events) != 1 || events[0].Kind != EventLoad || events[0].Value != 42 {
		t.Errorf("expected EventLoad value=42, got %+v", events)
	}
}

func TestEventOnLoadError(t *testing.T) {
	boom := errors.New("upstream broke")
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		return 0, 0, boom
	})
	c, _ := New[string, int](WithMaxEntries(4), WithLoader[string, int](loader))
	defer c.Close()
	ch, cancel := c.Subscribe(4, EventLoadError)
	defer cancel()
	_, _ = c.GetOrLoad(context.Background(), "k")

	events := drainEvents(ch)
	if len(events) != 1 || events[0].Kind != EventLoadError {
		t.Fatalf("expected EventLoadError, got %+v", events)
	}
	if !errors.Is(events[0].Err, boom) {
		t.Errorf("event.Err = %v, want %v", events[0].Err, boom)
	}
}

func TestEventOnLoadTimeout(t *testing.T) {
	loader := LoaderFunc[string, int](func(ctx context.Context, _ string) (int, time.Duration, error) {
		<-ctx.Done()
		return 0, 0, ctx.Err()
	})
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader[string, int](loader),
		WithLoaderTimeout(20*time.Millisecond),
	)
	defer c.Close()
	ch, cancel := c.Subscribe(4, EventLoadTimeout, EventLoadError)
	defer cancel()
	_, _ = c.GetOrLoad(context.Background(), "k")

	events := drainEvents(ch)
	if len(events) != 1 || events[0].Kind != EventLoadTimeout {
		t.Fatalf("expected EventLoadTimeout, got %+v", events)
	}
}

func TestEventOnResize(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	ch, cancel := c.Subscribe(4, EventResize)
	defer cancel()
	c.Resize(16)
	events := drainEvents(ch)
	if len(events) != 1 || events[0].Kind != EventResize {
		t.Errorf("expected EventResize, got %+v", events)
	}
}

func TestHookOnHitFires(t *testing.T) {
	hits := atomic.Int64{}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithOnHit(func(string, int) { hits.Add(1) }),
	)
	defer c.Close()
	_ = c.Set("k", 1)
	_, _ = c.Get("k") // hit
	_, _ = c.Get("k") // hit
	if hits.Load() != 2 {
		t.Errorf("OnHit fired %d times, want 2", hits.Load())
	}
}

func TestHookOnMissFires(t *testing.T) {
	misses := atomic.Int64{}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithOnMiss[string](func(string) { misses.Add(1) }),
	)
	defer c.Close()
	_, _ = c.Get("k") // miss
	if misses.Load() != 1 {
		t.Errorf("OnMiss fired %d times, want 1", misses.Load())
	}
}

func TestHookOnEvictFires(t *testing.T) {
	type record struct {
		key    string
		value  int
		reason EvictionReason
	}
	got := atomic.Pointer[record]{}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithOnEvict(func(k string, v int, r EvictionReason) {
			got.Store(&record{k, v, r})
		}),
	)
	defer c.Close()
	_ = c.Set("k", 5)
	c.Delete("k")
	r := got.Load()
	if r == nil || r.key != "k" || r.value != 5 || r.reason != EvictReasonDeleted {
		t.Errorf("OnEvict record = %+v, want (k, 5, deleted)", r)
	}
}

func TestHookOnExpireFires(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	expired := atomic.Int64{}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithClock(clk),
		WithJanitorInterval(time.Hour),
		WithOnExpire(func(string, int) { expired.Add(1) }),
	)
	defer c.Close()
	_ = c.SetWithTTL("k", 1, time.Second)
	clk.Advance(2 * time.Second)
	_, _ = c.Get("k") // lazy expiration

	if expired.Load() != 1 {
		t.Errorf("OnExpire fired %d times, want 1", expired.Load())
	}
}

func TestHookOnLoadFires(t *testing.T) {
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		return 7, 0, nil
	})
	loaded := atomic.Int64{}
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithLoader[string, int](loader),
		WithOnLoad(func(_ string, v int, _ time.Duration, err error) {
			if err == nil && v == 7 {
				loaded.Add(1)
			}
		}),
	)
	defer c.Close()
	_, _ = c.GetOrLoad(context.Background(), "k")
	if loaded.Load() != 1 {
		t.Errorf("OnLoad fired %d times with v=7, want 1", loaded.Load())
	}
}

func TestHookTypeMismatchRejected(t *testing.T) {
	// Hook over (string, string) won't satisfy a Cache[string, int].
	_, err := New[string, int](
		WithMaxEntries(4),
		WithOnHit(func(string, string) {}),
	)
	var ce *ConfigError
	if !errors.As(err, &ce) || ce.Field != "OnHit" {
		t.Errorf("expected ConfigError on OnHit mismatch; got %v", err)
	}
}

func TestSubscribeAfterCloseClosesImmediately(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	_ = c.Close()
	ch, cancel := c.Subscribe(8)
	defer cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel from post-Close Subscribe should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("post-Close subscribe channel never closed")
	}
}

func TestNegativeTombstoneSuppressesEvictEvent(t *testing.T) {
	loader := LoaderFunc[string, int](func(_ context.Context, _ string) (int, time.Duration, error) {
		return 0, 0, ErrNotFound
	})
	clk := NewFakeClock(time.Unix(0, 0))
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithClock(clk),
		WithLoader[string, int](loader),
		WithNegativeCache(time.Second),
		WithJanitorInterval(time.Hour),
	)
	defer c.Close()
	ch, cancel := c.Subscribe(8, EventEvict, EventExpire)
	defer cancel()

	// Populate negative tombstone.
	_, _ = c.GetOrLoad(context.Background(), "k")
	// Expire it via TTL + lazy Get.
	clk.Advance(2 * time.Second)
	_, _ = c.Get("k")

	// We should NOT see Evict/Expire for the tombstone (it's
	// internal bookkeeping, not user-visible state).
	events := drainEvents(ch)
	if len(events) != 0 {
		t.Errorf("expected no Evict/Expire events for negative tombstone; got %+v", events)
	}
}
