package memcache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNoopTracerStart(t *testing.T) {
	var tr noopTracer
	ctx, span := tr.Start(context.Background(), "memcache.load",
		Attr{Key: "k", Value: "v"},
	)
	if ctx == nil {
		t.Fatal("noopTracer.Start returned nil context")
	}
	if span == nil {
		t.Fatal("noopTracer.Start returned nil span")
	}
}

func TestNoopSpanEndAndSetAttr(t *testing.T) {
	var s noopSpan
	// Both methods are no-ops, but their bodies still need execution
	// to be counted in coverage.
	s.SetAttr("hit", true)
	s.SetAttr("hit", false)
	s.End(nil)
	s.End(errors.New("synthetic"))
}

func TestNoopTracerThroughInterface(t *testing.T) {
	// Confirm the interface satisfaction so future impls can be
	// swapped in without breaking call sites.
	var tr Tracer = noopTracer{}
	ctx, span := tr.Start(context.Background(), "memcache.snapshot.save")
	if ctx == nil || span == nil {
		t.Fatal("interface call returned nil ctx/span")
	}
	span.SetAttr("path", "/tmp/x")
	span.End(nil)
}

type recordingTracer struct{ ended chan error }

func (r recordingTracer) Start(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	return ctx, recordingSpan(r)
}

type recordingSpan recordingTracer

func (r recordingSpan) End(err error)     { r.ended <- err }
func (recordingSpan) SetAttr(string, any) {}

func TestLoadSpanRecordsLoadError(t *testing.T) {
	boom := errors.New("boom")
	tr := recordingTracer{ended: make(chan error, 1)}
	c, _ := New[string, int](WithMaxEntries(8), WithTracer(tr))
	defer c.Close()
	_, err := c.GetOrLoadFn(context.Background(), "k", func(context.Context, string) (int, time.Duration, error) {
		return 0, 0, boom
	})
	if got := <-tr.ended; !errors.Is(got, boom) || !errors.Is(err, boom) {
		t.Fatalf("span ended with %v, call returned %v; want both to wrap %v", got, err, boom)
	}
}
