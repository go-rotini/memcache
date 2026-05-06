package memcache

import "context"

// Tracer emits observability spans for cache operations. The
// interface is deliberately minimal — start/end with named
// attributes — so the package can stay free of any direct
// dependency on `go.opentelemetry.io/otel` or another tracing
// SDK. Adapters in the README show how to wire common tracers
// (OTel, OpenCensus, custom) behind this interface.
//
// Span names emitted by the package: `memcache.get`, `memcache.set`,
// `memcache.load`, `memcache.evict`, `memcache.snapshot.save`,
// `memcache.snapshot.load`. Attribute keys include `key` (when
// safely stringable), `hit` (bool), `reason` (eviction reason),
// and `error` (when the span ended with an error).
//
// Implementations are responsible for thread safety. Slow
// implementations slow the cache; for high-throughput workloads
// prefer non-blocking samplers / batched exporters.
type Tracer interface {
	// Start begins a span for op. The returned context carries
	// span data so child spans can chain via ctx; the returned
	// Span MUST be closed via End when the operation finishes.
	Start(ctx context.Context, op string, attrs ...Attr) (context.Context, Span)
}

// Span is one in-flight tracing scope.
type Span interface {
	// End closes the span. err annotates whether the operation
	// succeeded; pass nil on success.
	End(err error)
	// SetAttr attaches a key/value attribute mid-span. Useful
	// when an attribute (e.g., hit/miss) only becomes known
	// partway through the operation.
	SetAttr(key string, value any)
}

// Attr is one tracing attribute.
type Attr struct {
	Key   string
	Value any
}

// noopTracer is the default Tracer when [WithTracer] is not set.
// All methods are no-ops; the cache still calls them on the hot
// path so removing the option does not require code changes
// elsewhere.
type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End(error)           {}
func (noopSpan) SetAttr(string, any) {}
