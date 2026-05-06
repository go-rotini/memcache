package memcache

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

const (
	codecOpMarshal   = "marshal"
	codecOpUnmarshal = "unmarshal"
)

// CompressedCodec wraps a base [Codec] with gzip compression. The
// resulting codec marshals through the base codec first, then runs
// the bytes through gzip; Unmarshal reverses the order. The Name
// returned is `<base>+gzip` so snapshot codec-mismatch checks catch
// the difference between a compressed and an uncompressed snapshot
// of the same payload.
//
// Use this when snapshots cross a network boundary, when on-disk
// space matters more than save/load CPU, or when the cache holds
// highly compressible data (parsed config, repeated tags, ASCII).
//
// Constructed via [WithCompressedCodec] or directly with
// [NewCompressedCodec]; either form composes with [WithCodec].
type CompressedCodec struct {
	base  Codec
	level int // gzip compression level
}

// NewCompressedCodec wraps base with gzip at the given compression
// level. Pass [gzip.DefaultCompression] for the standard tradeoff
// (level 6); [gzip.BestSpeed] (1) for tighter latency budgets;
// [gzip.BestCompression] (9) for the smallest snapshots. Invalid
// levels fall back to [gzip.DefaultCompression] at Marshal time.
func NewCompressedCodec(base Codec, level int) CompressedCodec {
	return CompressedCodec{base: base, level: level}
}

// Marshal encodes v through the base codec and then gzip-compresses
// the result.
func (c CompressedCodec) Marshal(v any) ([]byte, error) {
	raw, err := c.base.Marshal(v)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	level := c.level
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		level = gzip.DefaultCompression
	}
	gw, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, &CodecError{Op: codecOpMarshal, Codec: c.Name(), Err: err}
	}
	if _, err := gw.Write(raw); err != nil {
		_ = gw.Close()
		return nil, &CodecError{Op: codecOpMarshal, Codec: c.Name(), Err: err}
	}
	if err := gw.Close(); err != nil {
		return nil, &CodecError{Op: codecOpMarshal, Codec: c.Name(), Err: err}
	}
	return buf.Bytes(), nil
}

// Unmarshal gzip-decompresses data and decodes the result through
// the base codec.
func (c CompressedCodec) Unmarshal(data []byte, v any) error {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return &CodecError{Op: codecOpUnmarshal, Codec: c.Name(), Err: err}
	}
	defer gr.Close()
	raw, err := io.ReadAll(gr)
	if err != nil {
		return &CodecError{Op: codecOpUnmarshal, Codec: c.Name(), Err: err}
	}
	return c.base.Unmarshal(raw, v)
}

// Name reports `<base>+gzip` (e.g. `gob+gzip`, `json+gzip`).
func (c CompressedCodec) Name() string {
	return fmt.Sprintf("%s+gzip", c.base.Name())
}

// WithCompressedCodec installs [NewCompressedCodec](base, level) as
// the cache's snapshot codec. Convenience wrapper around
// [WithCodec] for the common gzip case.
func WithCompressedCodec(base Codec, level int) Option {
	return WithCodec(NewCompressedCodec(base, level))
}
