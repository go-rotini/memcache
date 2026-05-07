package memcache

import "context"

// Tracer emits observability spans for cache operations. The
// interface is deliberately minimal — start/end with named
// attributes — so the package can stay free of any direct
// dependency on `go.opentelemetry.io/otel` or another tracing
// SDK. Adapters in the README show how to wire common tracers
// (OTel, OpenCensus, custom) behind this interface.
//
// Spans emitted by the package today:
//   - `memcache.load` — fires when [Cache.GetOrLoad] runs the
//     configured Loader. Attributes: `key` (the load key). The
//     span ends with an error attribute when the loader fails.
//   - `memcache.snapshot.save` — fires when [Cache.Save] /
//     [Cache.SaveFile] persist the cache. Attributes: `path`
//     when SaveFile is used.
//   - `memcache.snapshot.load` — fires when [Cache.Load] /
//     [Cache.LoadFile] reads a snapshot. Attributes: `path`
//     when LoadFile is used.
//
// `Get`/`Set`/`Delete` do NOT emit spans by design: per-call
// tracing on a 60-ns Get path would dominate the cost. Hook
// observability ([WithOnHit]/[WithOnMiss]/[WithOnEvict]/
// [WithOnExpire]) covers per-call signals at lower overhead.
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
