package memcache

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
)

// errEncryptTooLarge fires when a value is too large to seal: above
// AES-GCM's per-message limit, or too large to hold with its nonce and
// tag in one slice.
var errEncryptTooLarge = errors.New("memcache: value too large to encrypt")

// maxGCMPlaintext is AES-GCM's per-message plaintext limit.
const maxGCMPlaintext = (1<<32 - 2) * aes.BlockSize

// errEncryptShortCiphertext fires when an encrypted blob is shorter
// than the AES-GCM nonce (12 bytes); the format is malformed.
var errEncryptShortCiphertext = errors.New("memcache: encrypted snapshot ciphertext shorter than nonce")

// errEncryptKeyLength fires when [NewEncryptedCodec] is called with
// a key that is not exactly 32 bytes (AES-256).
var errEncryptKeyLength = errors.New("memcache: encrypted codec key must be 32 bytes (AES-256)")

// EncryptedCodec wraps a base [Codec] with AES-256-GCM authenticated
// encryption. Marshal's output is a 12-byte nonce followed by the
// GCM-sealed ciphertext. Construct via [NewEncryptedCodec]; install via
// [WithCodec] or [WithEncryptedCodec]. The package does not manage key
// rotation.
//
// As a snapshot codec it encrypts every record as one sealed block bound
// to the snapshot header; the header itself (codec and cache names,
// [WithSnapshotMetadata] entries, save time and record count) is
// authenticated but not encrypted. See [Cache.Save].
type EncryptedCodec struct {
	base Codec
	gcm  cipher.AEAD
}

// NewEncryptedCodec wraps base with AES-256-GCM keyed by key.
// key must be exactly 32 bytes; any other length returns an error.
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
	return c.seal(plain, nil)
}

// Unmarshal splits the leading nonce off data, decrypts the
// remainder, and feeds the recovered plaintext to the base codec.
// Returns an error when the ciphertext fails authentication
// (wrong key, truncation, tampering).
func (c *EncryptedCodec) Unmarshal(data []byte, v any) error {
	plain, err := c.openField(data)
	if err != nil {
		return err
	}
	return c.base.Unmarshal(plain, v)
}

// openField decrypts one nonce ‖ ciphertext value written by Marshal.
func (c *EncryptedCodec) openField(data []byte) ([]byte, error) {
	plain, err := c.open(data, nil)
	if err != nil {
		return nil, &CodecError{Op: codecOpUnmarshal, Codec: c.Name(), Err: err}
	}
	return plain, nil
}

// seal encrypts plain with a fresh nonce, authenticating ad with it,
// and returns nonce ‖ ciphertext.
func (c *EncryptedCodec) seal(plain, ad []byte) ([]byte, error) {
	ns, overhead := c.gcm.NonceSize(), c.gcm.Overhead()
	if uint64(len(plain)) > maxGCMPlaintext || len(plain) > math.MaxInt-ns-overhead {
		return nil, &CodecError{Op: codecOpMarshal, Codec: c.Name(), Err: errEncryptTooLarge}
	}
	nonce := make([]byte, ns, ns+len(plain)+overhead)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, &CodecError{Op: codecOpMarshal, Codec: c.Name(), Err: err}
	}
	return c.gcm.Seal(nonce, nonce, plain, ad), nil
}

// open reverses seal; ad must match the value passed to seal.
func (c *EncryptedCodec) open(data, ad []byte) ([]byte, error) {
	ns := c.gcm.NonceSize()
	if len(data) < ns {
		return nil, errEncryptShortCiphertext
	}
	return c.gcm.Open(nil, data[:ns], data[ns:], ad) //nolint:wrapcheck // callers wrap
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
