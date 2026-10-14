package memcache

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// errRawCodecNotBytes is returned by RawCodec.Marshal when the input is
// not a []byte. errRawCodecNotBytesPtr is returned by RawCodec.Unmarshal
// when the destination is not a *[]byte.
var (
	errRawCodecNotBytes    = errors.New("raw codec only supports []byte values")
	errRawCodecNotBytesPtr = errors.New("raw codec only supports *[]byte destinations")
)

// Codec serializes snapshot keys and values to bytes. The package ships
// [GobCodec], [JSONCodec] and [RawCodec], plus the [CompressedCodec] and
// [EncryptedCodec] wrappers; install one with [WithCodec].
type Codec interface {
	// Marshal encodes v to bytes.
	Marshal(v any) ([]byte, error)

	// Unmarshal decodes data into v (which must be a non-nil pointer).
	Unmarshal(data []byte, v any) error

	// Name returns the codec's stable identifier (used in snapshot
	// headers to detect codec mismatches on Load).
	Name() string
}

// GobCodec is the default snapshot codec, using encoding/gob. Each
// encoded key and value carries its own gob type description, so struct
// fields added or removed between save and load are handled by gob's
// usual rules.
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

// JSONCodec is a snapshot codec using encoding/json. Larger and slower
// than gob, but readable by other tools. K and V must be
// JSON-marshalable.
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

// RawCodec passes byte slices through without encoding: Marshal accepts
// only []byte and Unmarshal only *[]byte, and both copy the bytes.
//
// In snapshots, values must be []byte (or implement [SnapshotMarshaler]).
// Keys can't be []byte, so a snapshot written with RawCodec (alone or
// wrapped by [CompressedCodec] or [EncryptedCodec]) stores a key of
// string kind as its bytes and a key of integer kind as a varint; any
// other key type fails [Cache.Save] with a [*CodecError].
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

// errRawCodecKey is returned when a snapshot key can't be written as raw
// bytes; errRawCodecKeyInvalid when stored key bytes don't decode.
var (
	errRawCodecKey        = errors.New("raw codec only supports string and integer keys")
	errRawCodecKeyInvalid = errors.New("raw codec key is not a valid varint")
)

// isRawCodec reports whether c is [RawCodec], directly or inside
// [CompressedCodec] or [EncryptedCodec].
func isRawCodec(c Codec) bool {
	switch v := c.(type) {
	case RawCodec, *RawCodec:
		return true
	case CompressedCodec:
		return isRawCodec(v.base)
	case *CompressedCodec:
		return v != nil && isRawCodec(v.base)
	case *EncryptedCodec:
		return v != nil && isRawCodec(v.base)
	}
	return false
}

// rawKeyBytes encodes a snapshot key for a raw codec: a string kind as
// its bytes, an integer kind as a varint.
func rawKeyBytes(key any) ([]byte, error) {
	rv := reflect.ValueOf(key)
	switch rv.Kind() {
	case reflect.String:
		return []byte(rv.String()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return binary.AppendVarint(nil, rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return binary.AppendUvarint(nil, rv.Uint()), nil
	}
	return nil, &CodecError{
		Op: codecOpMarshal, Codec: "raw",
		Err: fmt.Errorf("%w: got key type %T", errRawCodecKey, key),
	}
}

// setRawKey decodes b, written by [rawKeyBytes], into the key at dst.
func setRawKey(b []byte, dst any) error {
	rv := reflect.ValueOf(dst).Elem()
	bad := func() error {
		return &CodecError{
			Op: codecOpUnmarshal, Codec: "raw",
			Err: fmt.Errorf("%w for %s", errRawCodecKeyInvalid, rv.Type()),
		}
	}
	switch rv.Kind() {
	case reflect.String:
		rv.SetString(string(b))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, size := binary.Varint(b)
		if size <= 0 || size != len(b) || rv.OverflowInt(n) {
			return bad()
		}
		rv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, size := binary.Uvarint(b)
		if size <= 0 || size != len(b) || rv.OverflowUint(n) {
			return bad()
		}
		rv.SetUint(n)
	default:
		return &CodecError{
			Op: codecOpUnmarshal, Codec: "raw",
			Err: fmt.Errorf("%w: got key type %s", errRawCodecKey, rv.Type()),
		}
	}
	return nil
}
