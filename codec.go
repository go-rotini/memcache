package memcache

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
)

// errRawCodecNotBytes is returned by RawCodec.Marshal when the input is
// not a []byte. errRawCodecNotBytesPtr is returned by RawCodec.Unmarshal
// when the destination is not a *[]byte.
var (
	errRawCodecNotBytes    = errors.New("raw codec only supports []byte values")
	errRawCodecNotBytesPtr = errors.New("raw codec only supports *[]byte destinations")
)

// Codec is the abstraction the snapshot subsystem uses to serialize
// keys and values to bytes. The package ships gob, JSON, and raw
// implementations; users may supply their own via WithCodec.
type Codec interface {
	// Marshal encodes v to bytes.
	Marshal(v any) ([]byte, error)

	// Unmarshal decodes data into v (which must be a non-nil pointer).
	Unmarshal(data []byte, v any) error

	// Name returns the codec's stable identifier (used in snapshot
	// headers to detect codec mismatches on Load).
	Name() string
}

// GobCodec is the default snapshot codec. Compact, fast, Go-only. Each
// snapshot Save/Load round trip includes type schema in the byte
// stream; types whose fields change between writes/reads handle the
// drift gracefully (gob's documented behavior for missing/extra fields).
type GobCodec struct{}

// Marshal encodes v with encoding/gob.
func (GobCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, &CodecError{Op: "marshal", Codec: "gob", Err: err}
	}
	return buf.Bytes(), nil
}

// Unmarshal decodes data into v.
func (GobCodec) Unmarshal(data []byte, v any) error {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		return &CodecError{Op: "unmarshal", Codec: "gob", Err: err}
	}
	return nil
}

// Name reports "gob".
func (GobCodec) Name() string { return "gob" }

// JSONCodec is a JSON snapshot codec. Larger and slower than gob, but
// portable across languages and `jq`-able. Requires V (and K when used
// as a map key in the snapshot format) to be JSON-marshalable.
type JSONCodec struct{}

// Marshal encodes v with encoding/json.
func (JSONCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, &CodecError{Op: "marshal", Codec: "json", Err: err}
	}
	return b, nil
}

// Unmarshal decodes data into v.
func (JSONCodec) Unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return &CodecError{Op: "unmarshal", Codec: "json", Err: err}
	}
	return nil
}

// Name reports "json".
func (JSONCodec) Name() string { return "json" }

// RawCodec passes byte slices through without encoding. It is only valid
// for caches whose value type is []byte. Marshal returns the input
// directly (a defensive copy is made so subsequent mutations of the
// caller's slice don't affect the snapshot stream); Unmarshal copies
// the input into the destination *[]byte.
type RawCodec struct{}

// Marshal copies and returns v's bytes. Returns an error if v is not
// []byte.
func (RawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, &CodecError{
			Op: "marshal", Codec: "raw",
			Err: fmt.Errorf("%w: got %T", errRawCodecNotBytes, v),
		}
	}
	dup := make([]byte, len(b))
	copy(dup, b)
	return dup, nil
}

// Unmarshal copies data into the *[]byte at v. Returns an error if v is
// not *[]byte.
func (RawCodec) Unmarshal(data []byte, v any) error {
	dst, ok := v.(*[]byte)
	if !ok {
		return &CodecError{
			Op: "unmarshal", Codec: "raw",
			Err: fmt.Errorf("%w: got %T", errRawCodecNotBytesPtr, v),
		}
	}
	dup := make([]byte, len(data))
	copy(dup, data)
	*dst = dup
	return nil
}

// Name reports "raw".
func (RawCodec) Name() string { return "raw" }
