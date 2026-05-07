package memcache

import (
	"context"
	"errors"
	"testing"
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
