package memcache

import "context"

// Tracer emits observability spans. Spans: memcache.load (every
// [Cache.GetOrLoad] and [Cache.GetOrLoadFn] call, hit or miss),
// memcache.snapshot.save and memcache.snapshot.load. Get, Set and
// Delete emit no spans. Implementations must be safe for concurrent use.
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

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End(error)           {}
func (noopSpan) SetAttr(string, any) {}
