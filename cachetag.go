package memcache

import (
	"reflect"
	"strings"
	"sync"
)

// cacheStructTag is the struct-tag key we read for snapshot
// filtering. Format: `cache:"name,opt1,opt2,..."` matching the
// spec's §13.2 shape.
const cacheStructTag = "cache"

// cacheTagOptions captures the parsed `cache:"..."` options that
// affect a single struct field.
type cacheTagOptions struct {
	skip   bool // field is `cache:"-"` — drop entirely
	secret bool // field is marked `secret` — zero during snapshot
	// rename, omitempty, versioned, tag-template are recognized in
	// parseCacheTag but the struct walker below only acts on skip
	// and secret in v0; the rest is documentation-only.
}

// parseCacheTag extracts options from a struct-tag string.
//
// Tag values follow `name,opt1,opt2`. The first token is the
// field's display name (`-` to skip the field entirely); each
// subsequent token is an option keyword. Unknown tokens are
// ignored so future spec additions don't break parsing.
func parseCacheTag(tag string) cacheTagOptions {
	var opts cacheTagOptions
	if tag == "" {
		return opts
	}
	parts := strings.Split(tag, ",")
	if len(parts) > 0 && parts[0] == "-" {
		opts.skip = true
	}
	for _, part := range parts[1:] {
		if part == "secret" {
			opts.secret = true
		}
	}
	return opts
}

// cacheTypeMeta is the per-V-type metadata derived once and cached
// for the lifetime of the process. `secretFields` lists the indices
// (recursive into anonymous embeds) of fields that need zeroing
// before snapshot encoding.
type cacheTypeMeta struct {
	hasSecret     bool
	secretIndices [][]int // each is a reflect.Value.FieldByIndex path
}

// cacheTypeMetaCache memoizes [cacheTypeMeta] keyed on
// reflect.Type. The first walk per type is O(field count); every
// subsequent lookup is a `sync.Map.Load`.
var cacheTypeMetaCache sync.Map // map[reflect.Type]*cacheTypeMeta

// metaFor returns the cached metadata for t, computing it on first
// access. Non-struct types yield a metadata struct with
// hasSecret=false; the cache hot-path checks this flag and
// short-circuits.
func metaFor(t reflect.Type) *cacheTypeMeta {
	if t == nil {
		return &cacheTypeMeta{}
	}
	// Dereference pointer types so a `Cache[K, *User]` and
	// `Cache[K, User]` share the same metadata.
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return &cacheTypeMeta{}
	}
	if cached, ok := cacheTypeMetaCache.Load(t); ok {
		if m, ok := cached.(*cacheTypeMeta); ok {
			return m
		}
	}
	meta := buildMeta(t)
	cacheTypeMetaCache.Store(t, meta)
	return meta
}

// buildMeta walks a struct's fields, recursing into anonymous
// embedded structs, and records the index path of every field
// tagged `secret` or `-`.
func buildMeta(t reflect.Type) *cacheTypeMeta {
	meta := &cacheTypeMeta{}
	walkStructFields(t, nil, func(path []int, opts cacheTagOptions) {
		if opts.skip || opts.secret {
			indexCopy := append([]int(nil), path...)
			meta.secretIndices = append(meta.secretIndices, indexCopy)
			meta.hasSecret = true
		}
	})
	return meta
}

// walkStructFields invokes visit for every field in t (including
// promoted fields from anonymous struct embeds), passing the index
// path and the parsed `cache:"..."` options.
func walkStructFields(t reflect.Type, prefix []int, visit func(path []int, opts cacheTagOptions)) {
	for i := range t.NumField() {
		field := t.Field(i)
		path := append(append([]int(nil), prefix...), i)
		opts := parseCacheTag(field.Tag.Get(cacheStructTag))
		visit(path, opts)
		// Recurse into anonymous struct embeds so users can
		// inherit secret tags from base types.
		if field.Anonymous {
			ft := field.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				walkStructFields(ft, path, visit)
			}
		}
	}
}

// applySnapshotFilter returns a copy of v with every `secret` (or
// `-`) field zeroed out. Returns the original v unchanged when
// V has no such fields (the common case).
//
// The copy is made via reflect.Value.Set on a fresh
// reflect.New(elem) target so the caller's original value is
// never mutated. For pointer V types the copy is a freshly
// allocated *V pointing at a copy of the underlying struct.
func applySnapshotFilter[V any](v V) V {
	t := reflect.TypeOf(v)
	if t == nil {
		return v
	}
	meta := metaFor(t)
	if !meta.hasSecret {
		return v
	}
	src := reflect.ValueOf(v)
	// Resolve to the underlying struct, both for value- and
	// pointer-typed V.
	if src.Kind() == reflect.Pointer {
		if src.IsNil() {
			return v
		}
		clone := reflect.New(src.Elem().Type())
		clone.Elem().Set(src.Elem())
		zeroSecretFields(clone.Elem(), meta.secretIndices)
		out, _ := clone.Interface().(V)
		return out
	}
	if src.Kind() != reflect.Struct {
		return v
	}
	clone := reflect.New(t).Elem()
	clone.Set(src)
	zeroSecretFields(clone, meta.secretIndices)
	out, _ := clone.Interface().(V)
	return out
}

// zeroSecretFields zeros every field whose index path appears in
// indices. Caller must have already cloned the struct into the
// passed reflect.Value.
func zeroSecretFields(v reflect.Value, indices [][]int) {
	for _, idx := range indices {
		f := v.FieldByIndex(idx)
		if !f.CanSet() {
			continue
		}
		f.Set(reflect.Zero(f.Type()))
	}
}
