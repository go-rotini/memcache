package memcache

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"
)

func TestCompressedCodecRoundTrip(t *testing.T) {
	codec := NewCompressedCodec(GobCodec{}, gzip.DefaultCompression)
	type val struct{ Name, Notes string }
	in := val{Name: "alice", Notes: strings.Repeat("highly compressible ", 100)}

	raw, err := codec.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(raw) >= 200 {
		t.Errorf("compressed size = %d, expected substantial compression", len(raw))
	}
	var out val
	if err := codec.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("round-trip drift: got %+v, want %+v", out, in)
	}
}

func TestCompressedCodecNameIncludesGzip(t *testing.T) {
	codec := NewCompressedCodec(GobCodec{}, gzip.BestSpeed)
	if want := "gob+gzip"; codec.Name() != want {
		t.Errorf("Name = %q, want %q", codec.Name(), want)
	}
	codec2 := NewCompressedCodec(JSONCodec{}, gzip.BestSpeed)
	if want := "json+gzip"; codec2.Name() != want {
		t.Errorf("Name = %q, want %q", codec2.Name(), want)
	}
}

func TestCompressedCodecCacheRoundTrip(t *testing.T) {
	c, _ := New[string, string](
		WithMaxEntries(8),
		WithCompressedCodec(GobCodec{}, gzip.BestSpeed),
	)
	defer c.Close()
	_ = c.Set("k", strings.Repeat("ab", 1024))

	var buf bytes.Buffer
	if err := c.Save(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := New[string, string](
		WithMaxEntries(8),
		WithCompressedCodec(GobCodec{}, gzip.BestSpeed),
	)
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatal(err)
	}
	got, ok := dst.Get("k")
	if !ok || len(got) != 2048 {
		t.Errorf("after compressed round-trip: ok=%v len=%d", ok, len(got))
	}
}

func TestCompressedCodecCodecMismatchRejected(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(8))
	defer src.Close()
	_ = src.Set("k", 1)

	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}

	// Loading a gob snapshot into a gob+gzip cache must surface
	// codec mismatch.
	dst, _ := New[string, int](
		WithMaxEntries(8),
		WithCompressedCodec(GobCodec{}, gzip.BestSpeed),
	)
	defer dst.Close()
	if _, err := dst.Load(&buf); err == nil {
		t.Fatal("expected codec mismatch error, got nil")
	}
}
