package memcache

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// errEncryptShortCiphertext fires when an encrypted blob is shorter
// than the AES-GCM nonce (12 bytes); the format is malformed.
var errEncryptShortCiphertext = errors.New("memcache: encrypted snapshot ciphertext shorter than nonce")

// errEncryptKeyLength fires when [NewEncryptedCodec] is called with
// a key that is not exactly 32 bytes (AES-256).
var errEncryptKeyLength = errors.New("memcache: encrypted codec key must be 32 bytes (AES-256)")

// EncryptedCodec wraps a base [Codec] with AES-256-GCM authenticated
// encryption. Wire format: 12-byte nonce followed by GCM-sealed
// ciphertext. Construct via [NewEncryptedCodec]; install via [WithCodec]
// or [WithEncryptedCodec]. The package does not manage key rotation.
type EncryptedCodec struct {
	base Codec
	gcm  cipher.AEAD
}

// NewEncryptedCodec wraps base with AES-256-GCM keyed by key.
// key must be exactly 32 bytes; shorter or longer keys return a
// configuration error at construction time so the failure is loud
// rather than masked as a runtime decode error later.
func NewEncryptedCodec(base Codec, key []byte) (*EncryptedCodec, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("%w: got %d", errEncryptKeyLength, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("memcache: AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("memcache: GCM mode: %w", err)
	}
	return &EncryptedCodec{base: base, gcm: gcm}, nil
}

// Marshal encodes v through the base codec, then encrypts the
// resulting bytes with a fresh 96-bit nonce. The output layout is
// nonce ‖ ciphertext.
func (c *EncryptedCodec) Marshal(v any) ([]byte, error) {
	plain, err := c.base.Marshal(v)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, &CodecError{Op: codecOpMarshal, Codec: c.Name(), Err: err}
	}
	out := c.gcm.Seal(nonce, nonce, plain, nil)
	return out, nil
}

// Unmarshal splits the leading nonce off data, decrypts the
// remainder, and feeds the recovered plaintext to the base codec.
// Returns an error when the ciphertext fails authentication
// (wrong key, truncation, tampering).
func (c *EncryptedCodec) Unmarshal(data []byte, v any) error {
	ns := c.gcm.NonceSize()
	if len(data) < ns {
		return &CodecError{Op: codecOpUnmarshal, Codec: c.Name(), Err: errEncryptShortCiphertext}
	}
	nonce, ciphertext := data[:ns], data[ns:]
	plain, err := c.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return &CodecError{Op: codecOpUnmarshal, Codec: c.Name(), Err: err}
	}
	return c.base.Unmarshal(plain, v)
}

// Name reports `<base>+aes-256-gcm` so snapshot codec-mismatch
// detection catches the difference between encrypted and
// plaintext snapshots of the same payload.
func (c *EncryptedCodec) Name() string {
	return fmt.Sprintf("%s+aes-256-gcm", c.base.Name())
}

// WithEncryptedCodec installs [NewEncryptedCodec](base, key) as the
// cache's snapshot codec. Construction errors (e.g., wrong key
// length) propagate through New as a [*ConfigError].
func WithEncryptedCodec(base Codec, key []byte) Option {
	return func(c *config) {
		codec, err := NewEncryptedCodec(base, key)
		if err != nil {
			c.codecCtorErr = err
			return
		}
		c.codec = codec
	}
}
