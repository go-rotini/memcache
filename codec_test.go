package memcache

import (
	"errors"
	"testing"
)

func TestGobCodecRoundTrip(t *testing.T) {
	c := GobCodec{}
	type payload struct {
		Name  string
		Count int
	}
	in := payload{Name: "x", Count: 42}
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out payload
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if in != out {
		t.Errorf("round-trip mismatch: in=%+v out=%+v", in, out)
	}
	if c.Name() != "gob" {
		t.Errorf("Name() = %q, want %q", c.Name(), "gob")
	}
}

func TestJSONCodecRoundTrip(t *testing.T) {
	c := JSONCodec{}
	type payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	in := payload{Name: "x", Count: 42}
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out payload
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if in != out {
		t.Errorf("round-trip mismatch: in=%+v out=%+v", in, out)
	}
	if c.Name() != "json" {
		t.Errorf("Name() = %q, want %q", c.Name(), "json")
	}
}

func TestRawCodecRoundTrip(t *testing.T) {
	c := RawCodec{}
	in := []byte("hello, raw")
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != string(in) {
		t.Errorf("Marshal returned %q, want %q", b, in)
	}
	// Defensive copy: mutating in should not affect b.
	in[0] = '!'
	if b[0] == '!' {
		t.Error("RawCodec.Marshal should defensive-copy")
	}

	var out []byte
	if err := c.Unmarshal([]byte("world"), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(out) != "world" {
		t.Errorf("Unmarshal: got %q, want %q", out, "world")
	}
	if c.Name() != "raw" {
		t.Errorf("Name() = %q, want %q", c.Name(), "raw")
	}
}

func TestRawCodecRejectsWrongTypes(t *testing.T) {
	c := RawCodec{}
	if _, err := c.Marshal("not-bytes"); err == nil {
		t.Error("Marshal should reject non-[]byte")
	} else if !errors.Is(err, &CodecError{}) {
		t.Errorf("error should be a CodecError, got %T", err)
	}

	var dst string
	if err := c.Unmarshal([]byte{1, 2, 3}, &dst); err == nil {
		t.Error("Unmarshal should reject non-*[]byte destination")
	}
}

func TestGobCodecMarshalErrorWraps(t *testing.T) {
	c := GobCodec{}
	// channels are not gob-encodable
	_, err := c.Marshal(make(chan int))
	if err == nil {
		t.Fatal("expected marshal error for channel value")
	}
	if !errors.Is(err, &CodecError{}) {
		t.Errorf("error should be a CodecError, got %T", err)
	}
}
