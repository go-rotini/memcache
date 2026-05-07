package memcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// cacheStructTag is the struct-tag key for snapshot filtering. Format:
// `cache:"name,opt1,opt2,..."`.
const cacheStructTag = "cache"

type cacheTagOptions struct {
	skip        bool   // cache:"-" drops the field
	secret      bool   // zero during snapshot
	omitempty   bool   // skip from snapshot when zero-valued
	versioned   bool   // include in schema fingerprint
	tagTemplate string // tag=<template> auto-tags during Set
}

// parseCacheTag extracts options from a struct-tag string of the form
// "name,opt1,opt2". Unknown tokens are ignored.
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
		switch {
		case part == "secret":
			opts.secret = true
		case part == "omitempty":
			opts.omitempty = true
		case part == "versioned":
			opts.versioned = true
		case strings.HasPrefix(part, "tag="):
			opts.tagTemplate = strings.TrimPrefix(part, "tag=")
		}
	}
	return opts
}

type fieldFilter struct {
	path      []int
	zero      bool // skip || secret: always zero
	omitempty bool // zero only when current value is the type's zero
}

type tagTemplateBinding struct {
	template string
	fields   map[string][]int // name -> index path
}

type cacheTypeMeta struct {
	hasFilters   bool
	filters      []fieldFilter
	versioned    bool
	versionHash  string // sha256 fingerprint of versioned-tagged schema
	tagTemplates []tagTemplateBinding
	hasTemplates bool
	hasOmitEmpty bool
}

// cacheTypeMetaCache memoizes cacheTypeMeta by reflect.Type.
var cacheTypeMetaCache sync.Map // map[reflect.Type]*cacheTypeMeta

func metaFor(t reflect.Type) *cacheTypeMeta {
	if t == nil {
		return &cacheTypeMeta{}
	}
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

func buildMeta(t reflect.Type) *cacheTypeMeta {
	meta := &cacheTypeMeta{}
	fieldsByName := map[string][]int{}
	walkStructFields(t, nil, func(path []int, name string, opts cacheTagOptions) {
		indexCopy := append([]int(nil), path...)
		fieldsByName[name] = indexCopy
		switch {
		case opts.skip || opts.secret:
			meta.filters = append(meta.filters, fieldFilter{path: indexCopy, zero: true})
			meta.hasFilters = true
		case opts.omitempty:
			meta.filters = append(meta.filters, fieldFilter{path: indexCopy, omitempty: true})
			meta.hasFilters = true
			meta.hasOmitEmpty = true
		}
		if opts.versioned {
			meta.versioned = true
		}
		if opts.tagTemplate != "" {
			meta.tagTemplates = append(meta.tagTemplates, tagTemplateBinding{
				template: opts.tagTemplate,
			})
			meta.hasTemplates = true
		}
	})
	for i := range meta.tagTemplates {
		meta.tagTemplates[i].fields = fieldsByName
	}
	if meta.versioned {
		meta.versionHash = computeVersionHash(t)
	}
	return meta
}

func walkStructFields(t reflect.Type, prefix []int, visit func(path []int, name string, opts cacheTagOptions)) {
	for i := range t.NumField() {
		field := t.Field(i)
		path := append(append([]int(nil), prefix...), i)
		opts := parseCacheTag(field.Tag.Get(cacheStructTag))
		visit(path, field.Name, opts)
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

// computeVersionHash returns a stable fingerprint of t's exported schema:
// sorted "Name:Type" pairs, sha256-hashed and hex-encoded.
func computeVersionHash(t reflect.Type) string {
	var lines []string
	walkStructFields(t, nil, func(_ []int, name string, _ cacheTagOptions) {
		lines = append(lines, name+":"+typeSignature(t, name))
	})
	for i := 1; i < len(lines); i++ {
		for j := i; j > 0 && lines[j-1] > lines[j]; j-- {
			lines[j-1], lines[j] = lines[j], lines[j-1]
		}
	}
	h := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(h[:])
}

func typeSignature(t reflect.Type, name string) string {
	f, ok := t.FieldByName(name)
	if !ok {
		return "?"
	}
	return f.Type.String()
}

// applySnapshotFilter returns a copy of v with secret/skip fields zeroed.
// Returns v unchanged when V has no filtered fields.
func applySnapshotFilter[V any](v V) V {
	t := reflect.TypeOf(v)
	if t == nil {
		return v
	}
	meta := metaFor(t)
	if !meta.hasFilters {
		return v
	}
	src := reflect.ValueOf(v)
	if src.Kind() == reflect.Pointer {
		if src.IsNil() {
			return v
		}
		clone := reflect.New(src.Elem().Type())
		clone.Elem().Set(src.Elem())
		applyFilters(clone.Elem(), meta.filters)
		out, _ := clone.Interface().(V)
		return out
	}
	if src.Kind() != reflect.Struct {
		return v
	}
	clone := reflect.New(t).Elem()
	clone.Set(src)
	applyFilters(clone, meta.filters)
	out, _ := clone.Interface().(V)
	return out
}

func applyFilters(v reflect.Value, filters []fieldFilter) {
	for _, f := range filters {
		fv := v.FieldByIndex(f.path)
		if !fv.CanSet() {
			continue
		}
		switch {
		case f.zero:
			fv.Set(reflect.Zero(fv.Type()))
		case f.omitempty:
			// omitempty is declarative; gob cannot elide zero values,
			// so non-zero values are preserved through the snapshot.
			// Use cache:"-" to guarantee non-persistence.
			continue
		}
	}
}

// extractTemplateTags returns one tag string per tag=<template>
// declaration on V's type. Templates support {FieldName} placeholders.
// Returns nil for non-struct V or types with no templates.
func extractTemplateTags[V any](v V) []string {
	t := reflect.TypeOf(v)
	if t == nil {
		return nil
	}
	meta := metaFor(t)
	if !meta.hasTemplates {
		return nil
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil
	}
	tags := make([]string, 0, len(meta.tagTemplates))
	for _, tpl := range meta.tagTemplates {
		tags = append(tags, expandTemplate(tpl.template, rv, tpl.fields))
	}
	return tags
}

// expandTemplate substitutes {FieldName} placeholders in template.
// Unknown names render as the literal placeholder for debuggability.
func expandTemplate(template string, rv reflect.Value, fields map[string][]int) string {
	var b strings.Builder
	i := 0
	for i < len(template) {
		open := strings.IndexByte(template[i:], '{')
		if open < 0 {
			b.WriteString(template[i:])
			break
		}
		b.WriteString(template[i : i+open])
		i += open + 1 // past the '{'
		closeIdx := strings.IndexByte(template[i:], '}')
		if closeIdx < 0 {
			// Unterminated placeholder; emit verbatim.
			b.WriteByte('{')
			b.WriteString(template[i:])
			break
		}
		name := template[i : i+closeIdx]
		i += closeIdx + 1 // past the '}'
		path, ok := fields[name]
		if !ok {
			b.WriteByte('{')
			b.WriteString(name)
			b.WriteByte('}')
			continue
		}
		fv := rv.FieldByIndex(path)
		fmt.Fprintf(&b, "%v", fv.Interface())
	}
	return b.String()
}

// schemaVersion returns v's fingerprint when V opts into versioning via
// the `versioned` tag, or "" otherwise.
func schemaVersion[V any](v V) string {
	t := reflect.TypeOf(v)
	if t == nil {
		return ""
	}
	return metaFor(t).versionHash
}
