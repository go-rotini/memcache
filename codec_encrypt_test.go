package memcache

import (
	"bytes"
	"testing"
)

func TestEncryptedCodecRejectsBadKeyLength(t *testing.T) {
	for _, n := range []int{0, 16, 31, 33, 48} {
		key := make([]byte, n)
		if _, err := NewEncryptedCodec(GobCodec{}, key); err == nil {
			t.Errorf("expected error for key length %d, got nil", n)
		}
	}
}

func TestEncryptedCodecRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte("0123456789abcdef"), 2) // 32 bytes
	codec, err := NewEncryptedCodec(GobCodec{}, key)
	if err != nil {
		t.Fatal(err)
	}
	in := struct{ Name string }{Name: "alice"}
	raw, err := codec.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	// Plaintext "alice" should NOT appear verbatim in the ciphertext.
	if bytes.Contains(raw, []byte("alice")) {
		t.Errorf("ciphertext contains plaintext substring; encryption appears broken")
	}
	var out struct{ Name string }
	if err := codec.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.Name != in.Name {
		t.Errorf("round-trip drift: got %q, want %q", out.Name, in.Name)
	}
}

func TestEncryptedCodecWrongKeyFailsAuth(t *testing.T) {
	a := bytes.Repeat([]byte("0123456789abcdef"), 2)
	b := bytes.Repeat([]byte("fedcba9876543210"), 2)
	codecA, _ := NewEncryptedCodec(GobCodec{}, a)
	codecB, _ := NewEncryptedCodec(GobCodec{}, b)
	in := struct{ V int }{V: 7}
	raw, _ := codecA.Marshal(in)
	var out struct{ V int }
	if err := codecB.Unmarshal(raw, &out); err == nil {
		t.Error("decrypt with wrong key should fail authentication")
	}
}

func TestEncryptedCodecCacheRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	c, err := New[string, int](
		WithMaxEntries(8),
		WithEncryptedCodec(GobCodec{}, key),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.Set("k", 99)

	var buf bytes.Buffer
	if err := c.Save(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := New[string, int](
		WithMaxEntries(8),
		WithEncryptedCodec(GobCodec{}, key),
	)
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatal(err)
	}
	if v, _ := dst.Get("k"); v != 99 {
		t.Errorf("after encrypted round-trip Get = %d, want 99", v)
	}
}

func TestEncryptedCodecBadKeyLengthSurfacesAtNew(t *testing.T) {
	_, err := New[string, int](
		WithMaxEntries(8),
		WithEncryptedCodec(GobCodec{}, []byte("too short")),
	)
	if err == nil {
		t.Error("expected ConfigError for bad key length")
	}
}
