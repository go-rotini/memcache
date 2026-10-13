package memcache

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"time"
)

// snapshotMagic is the 4-byte file signature ("RTNI"). Readers reject
// non-matching files with [ErrSnapshotIncompatible].
const snapshotMagic = "RTNI"

// snapshotVersion is the on-disk format version written by Save. v2
// added a metadata map after `name`. v3 adds each record's sliding TTL
// and, with [EncryptedCodec], encrypts all records as one unit bound to
// the header. Load reads v2 and v3.
const (
	snapshotVersion   uint8 = 3
	snapshotVersionV2 uint8 = 2
)

// defaultMaxSnapshotBytes caps Load input when [WithMaxSnapshotBytes] is
// not set, and SaveFile output to the [WithAutoLoad] path. A variable so
// tests can lower it.
var defaultMaxSnapshotBytes = int64(256 << 20) // 256 MiB

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// errEncryptedLegacy is the Err of a load refused because the
	// snapshot is an encrypted one in an older format.
	errEncryptedLegacy error = encryptedLegacyError{}
	errFieldTooBig           = errors.New("memcache: snapshot field exceeds 2 GiB")
	errTooManyTags           = errors.New("memcache: more than 255 tags per entry")
	errFieldPastEnd          = errors.New("field length runs past the end of the snapshot or its size limit")
	errInflateTooLarge       = errors.New("decompressed data exceeds the decompression limit")
)

// encryptedLegacyError is [ErrSnapshotIncompatible], told apart so
// auto-load can skip the file.
type encryptedLegacyError struct{}

func (encryptedLegacyError) Error() string { return ErrSnapshotIncompatible.Error() }
func (encryptedLegacyError) Unwrap() error { return ErrSnapshotIncompatible }

// snapshotSchemaVersionKey is the reserved metadata key for versioned
// struct fingerprints; Load refuses mismatches with [ErrSnapshotIncompatible].
const snapshotSchemaVersionKey = "__memcache_schema_version"

type entrySnapshot[K comparable, V any] struct {
	key        K
	value      V
	expireAt   int64
	weight     int64
	inserted   int64
	hits       uint32
	flags      uint16
	slidingTTL int64
	tags       []string
}

// Save writes a snapshot of the cache to w using the cache's configured
// [Codec]: a magic-prefixed header, length-prefixed records, and a
// trailing CRC32-Castagnoli. Acquires each shard's read lock in turn;
// writers to a shard being snapshotted block until its pass completes.
//
// With [EncryptedCodec], the header (codec name, cache name,
// [WithSnapshotMetadata] entries, save time and record count) stays
// readable, so [InspectSnapshot] works without the key; the records
// (keys, values, expiry, weights, flags and tags) are encrypted and
// authenticated as one unit bound to that header, so neither can be
// changed, reordered or swapped without Load failing.
//
// With [WithMaxSnapshotBytes], Save fails with [ErrSnapshotTooLarge]
// rather than write a snapshot that a Load with the same limit would
// refuse; without it, Save has no size limit. A failed Save may leave
// partial output in w.
func (c *Cache[K, V]) Save(w io.Writer) (err error) {
	if c.closed.Load() {
		return ErrClosed
	}
	_, span := c.tracer.Start(context.Background(), "memcache.snapshot.save")
	defer func() { span.End(err) }()
	return c.saveTo(w, c.cfg.maxSnapshotBytes)
}

// saveTo writes a snapshot to w without checking for Close, so Close's
// final auto-save works. A limit <= 0 means no size limit.
func (c *Cache[K, V]) saveTo(w io.Writer, limit int64) error {
	staged := c.snapshotEntries()
	recCodec, enc := splitEncryption(c.cfg.codec)

	var header bytes.Buffer
	if err := c.writeHeader(&header, int64(len(staged))); err != nil {
		return err
	}

	// Refuse to write what a Load with this limit would refuse to read.
	if limit <= 0 {
		limit = math.MaxInt64
	}
	ss := &saveState{inflateLeft: inflateLimit(limit)}
	lw := &limitWriter{w: w, left: limit}
	crc := crc32.New(crcTable)
	cw := io.MultiWriter(lw, crc)
	if _, err := cw.Write(header.Bytes()); err != nil {
		return wrapSaveErr("header", err)
	}
	if enc == nil {
		for i := range staged {
			if err := c.writeRecord(cw, ss, recCodec, &staged[i]); err != nil {
				return err
			}
		}
	} else {
		var plain bytes.Buffer
		body := &limitWriter{w: &plain, left: limit}
		for i := range staged {
			if err := c.writeRecord(body, ss, recCodec, &staged[i]); err != nil {
				return err
			}
		}
		sealed, err := enc.seal(plain.Bytes(), header.Bytes())
		if err != nil {
			return wrapSaveErr("records", err)
		}
		if err := binary.Write(cw, binary.LittleEndian, uint64(len(sealed))); err != nil {
			return wrapSaveErr("records length", err)
		}
		if _, err := cw.Write(sealed); err != nil {
			return wrapSaveErr("records", err)
		}
	}
	// CRC trailer is written without updating the CRC accumulator.
	if err := binary.Write(lw, binary.LittleEndian, crc.Sum32()); err != nil {
		return wrapSaveErr("crc", err)
	}
	c.counters.stampSnapshot(c.cfg.clock.Now())
	c.publishEvent(Event[K, V]{Kind: EventSnapshot, At: c.cfg.clock.Now()})
	return nil
}

// SaveFile writes a snapshot to path atomically via temp file + fsync +
// rename. Readers see either the prior or new snapshot, never torn. It
// applies the size limit [Cache.Save] does, and when path is the
// [WithAutoLoad] path, the default limit auto-load reads with, so a
// failed SaveFile keeps the old file.
func (c *Cache[K, V]) SaveFile(path string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	return c.saveFileTo(path)
}

// saveFileTo is the closed-check-free SaveFile implementation.
func (c *Cache[K, V]) saveFileTo(path string) (err error) {
	_, span := c.tracer.Start(context.Background(), "memcache.snapshot.save",
		Attr{Key: "path", Value: path})
	defer func() { span.End(err) }()
	dir := filepath.Dir(path)
	tmp, terr := os.CreateTemp(dir, ".memcache-*.tmp")
	if terr != nil {
		return &SnapshotError{Op: "save", Path: path, Message: "create temp", Err: terr}
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	bw := bufio.NewWriter(tmp)
	if err := c.saveTo(bw, c.saveFileLimit(path)); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = tmp.Close()
		cleanup()
		return &SnapshotError{Op: "save", Path: path, Message: "flush", Err: err}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return &SnapshotError{Op: "save", Path: path, Message: "fsync", Err: err}
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return &SnapshotError{Op: "save", Path: path, Message: "close", Err: err}
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return &SnapshotError{Op: "save", Path: path, Message: "rename", Err: err}
	}
	return nil
}

// Load replaces the cache contents with the snapshot read from r and
// returns the number of entries inserted.
//
// The whole snapshot is read, decoded and checked against its CRC
// before the cache is touched, so on any error the cache is left as it
// was. Read and decode failures are [*SnapshotError] values; see
// [ErrSnapshotIncompatible] and [ErrSnapshotCorrupt].
//
// Records are checked against the cache's own limits as if they were
// being set: records past their TTL, and records whose key, weight or
// tags break [WithMaxKeySize], [WithMaxValueWeight] or
// [WithMaxTagsPerEntry] or [WithMaxTagsTotal], are skipped. With a [Weigher], each value is
// weighed again rather than trusting the weight in the snapshot. The
// cache's entry and byte bounds apply to the loaded total. Load reads
// snapshots written by this and the previous format version, except
// encrypted ones in the previous format (written by memcache v1.0.2 or
// earlier), which fail with [ErrSnapshotIncompatible]: their records
// can't be checked against tampering.
func (c *Cache[K, V]) Load(r io.Reader) (n int, err error) {
	_, span := c.tracer.Start(context.Background(), "memcache.snapshot.load")
	defer func() { span.End(err) }()
	return c.loadInto(r, true)
}

// Merge is [Cache.Load] without the reset: the snapshot's entries
// overwrite existing keys and other keys are kept. It returns the number
// of entries written.
func (c *Cache[K, V]) Merge(r io.Reader) (int, error) {
	return c.loadInto(r, false)
}

// saveFileLimit returns SaveFile's size limit for path: the
// [WithMaxSnapshotBytes] limit, else the default one when the cache
// auto-loads path, else none (0).
func (c *Cache[K, V]) saveFileLimit(path string) int64 {
	if c.cfg.maxSnapshotBytes > 0 {
		return c.cfg.maxSnapshotBytes
	}
	if c.cfg.autoLoadPath != "" && filepath.Clean(path) == filepath.Clean(c.cfg.autoLoadPath) {
		return defaultMaxSnapshotBytes
	}
	return 0
}

// snapshotLimit returns the [WithMaxSnapshotBytes] limit, or the default.
func (c *Cache[K, V]) snapshotLimit() int64 {
	if limit := c.cfg.maxSnapshotBytes; limit > 0 {
		return limit
	}
	return defaultMaxSnapshotBytes
}

func (c *Cache[K, V]) loadInto(r io.Reader, reset bool) (int, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	staged, err := c.readSnapshot(r)
	if err != nil {
		return 0, err
	}
	if reset {
		c.Reset()
	}
	now := c.cfg.clock.Now().UnixNano()
	loaded := 0
	for i := range staged {
		snap := &staged[i]
		if snap.expireAt != 0 && snap.expireAt <= now {
			continue
		}
		if !c.admitSnapshotRecord(snap) {
			continue
		}
		c.applySnapshot(snap)
		loaded++
	}
	c.counters.stampSnapshot(c.cfg.clock.Now())
	c.publishEvent(Event[K, V]{Kind: EventSnapshot, At: c.cfg.clock.Now()})
	return loaded, nil
}

// readSnapshot reads, decodes and CRC-checks a whole snapshot without
// touching the cache.
func (c *Cache[K, V]) readSnapshot(r io.Reader) ([]entrySnapshot[K, V], error) {
	limit := c.snapshotLimit()
	crc := crc32.New(crcTable)
	var header bytes.Buffer
	sr := newSnapReader(bufio.NewReader(r), crc, limit)
	sr.capture = &header

	info, err := readSnapshotHeader(sr)
	if err != nil {
		return nil, err
	}
	sr.capture = nil

	if info.Codec != c.cfg.codec.Name() {
		return nil, &SnapshotError{
			Op:      "load",
			Message: fmt.Sprintf("codec mismatch: snapshot=%q cache=%q", info.Codec, c.cfg.codec.Name()),
			Err:     ErrSnapshotIncompatible,
		}
	}
	if want := c.schemaVersion(); want != "" {
		got := info.Metadata[snapshotSchemaVersionKey]
		if got != "" && got != want {
			return nil, &SnapshotError{
				Op:      "load",
				Message: fmt.Sprintf("schema version mismatch: snapshot=%s cache=%s", got, want),
				Err:     ErrSnapshotIncompatible,
			}
		}
	}

	ls := &loadState{inflateLeft: inflateLimit(limit), version: info.Version}
	recCodec, enc := splitEncryption(c.cfg.codec)
	if info.Version < snapshotVersion {
		if enc != nil {
			// Older snapshots seal each field on its own, so fields can
			// be swapped between records undetected.
			return nil, &SnapshotError{
				Op:      "load",
				Message: fmt.Sprintf("encrypted version %d snapshots are not supported", info.Version),
				Err:     errEncryptedLegacy,
			}
		}
		recCodec, enc = c.cfg.codec, nil
	}
	ls.codec = recCodec

	var staged []entrySnapshot[K, V]
	if enc == nil {
		staged, err = c.readRecords(sr, ls, info.Count)
	} else {
		staged, err = c.readSealedRecords(sr, ls, enc, header.Bytes(), info.Count)
	}
	if err != nil {
		return nil, err
	}

	// The trailing CRC is read without updating the running CRC.
	sr.h = nil
	stored, err := sr.u32()
	if err != nil {
		return nil, sr.fail("reading trailing CRC", err)
	}
	if stored != crc.Sum32() {
		return nil, sr.fail("CRC mismatch", ErrSnapshotCorrupt)
	}
	return staged, nil
}

// readRecords reads count records from sr.
func (c *Cache[K, V]) readRecords(sr *snapReader, ls *loadState, count int64) ([]entrySnapshot[K, V], error) {
	var staged []entrySnapshot[K, V]
	for range count {
		snap, err := c.readRecord(sr, ls)
		if err != nil {
			return nil, err
		}
		staged = append(staged, snap)
	}
	return staged, nil
}

// readSealedRecords reads the encrypted block of a v3 snapshot, opens it
// with header as the associated data, and reads count records from it.
func (c *Cache[K, V]) readSealedRecords(
	sr *snapReader, ls *loadState, enc *EncryptedCodec, header []byte, count int64,
) ([]entrySnapshot[K, V], error) {
	n, err := sr.u64()
	if err != nil {
		return nil, sr.fail("reading records length", err)
	}
	sealed, err := sr.take(n)
	if err != nil {
		return nil, sr.fail("reading records", err)
	}
	at := sr.off
	plain, err := enc.open(sealed, header)
	if err != nil {
		return nil, &SnapshotError{Op: "load", Offset: at, Message: "decrypting records",
			Err: errors.Join(ErrSnapshotCorrupt, err)}
	}
	inner := newSnapReader(bytes.NewReader(plain), nil, int64(len(plain)))
	inner.pinOffset(at)
	staged, err := c.readRecords(inner, ls, count)
	if err != nil {
		return nil, err
	}
	if inner.left != 0 {
		return nil, inner.fail("trailing bytes after records", ErrSnapshotCorrupt)
	}
	return staged, nil
}

// LoadFile loads a snapshot from path; convenience wrapper for
// [Cache.Load] that opens and closes the file.
func (c *Cache[K, V]) LoadFile(path string) (n int, err error) {
	_, span := c.tracer.Start(context.Background(), "memcache.snapshot.load",
		Attr{Key: "path", Value: path})
	defer func() { span.End(err) }()
	f, oerr := os.Open(path)
	if oerr != nil {
		return 0, &SnapshotError{Op: "load", Path: path, Message: "open", Err: oerr}
	}
	defer f.Close()
	n, err = c.loadInto(f, true)
	if se, ok := errors.AsType[*SnapshotError](err); ok && se.Path == "" {
		se.Path = path
	}
	return n, err
}

// InspectSnapshot reads only the header of a snapshot file and returns
// its metadata, skipping records and CRC. Useful for compatibility
// checks before a full Load. It reads no more than the file holds.
func InspectSnapshot(path string) (*SnapshotInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, &SnapshotError{Op: "load", Path: path, Message: "open", Err: err}
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, &SnapshotError{Op: "load", Path: path, Message: "stat", Err: err}
	}
	info, err := readSnapshotHeader(newSnapReader(bufio.NewReader(f), nil, fi.Size()))
	if err != nil {
		if se, ok := errors.AsType[*SnapshotError](err); ok {
			se.Path = path
		}
		return nil, err
	}
	info.SizeBytes = fi.Size()
	return info, nil
}

func (c *Cache[K, V]) snapshotEntries() []entrySnapshot[K, V] {
	now := c.cfg.clock.Now().UnixNano()
	var out []entrySnapshot[K, V]
	for _, s := range c.shards {
		s.mu.RLock()
		s.storage.each(func(e *entry[K, V]) bool {
			// Skip TTL-expired and negative tombstones; neither
			// carries useful warm-restart state.
			if e.expired(now) || e.flags.has(flagNegative) {
				return true
			}
			snap := entrySnapshot[K, V]{
				key:      e.key,
				value:    e.loadValue(),
				expireAt: e.expireAt.Load(),
				weight:   e.weight,
				inserted: e.inserted,
				hits:     e.hits.Load(),
				flags:    uint16(e.flags.load() & flagSliding),
			}
			if e.flags.has(flagSliding) {
				snap.slidingTTL = e.slidingTTL
			}
			if len(e.tags) > 0 {
				snap.tags = append([]string(nil), e.tags...)
			}
			out = append(out, snap)
			return true
		})
		s.mu.RUnlock()
	}
	return out
}

// admitSnapshotRecord applies the cache's key, weight and tag limits to
// a decoded record, recomputing its weight with the cache's [Weigher]
// when one is configured. It reports whether the record may be loaded.
func (c *Cache[K, V]) admitSnapshotRecord(snap *entrySnapshot[K, V]) bool {
	if c.checkKeySize(snap.key) != nil {
		return false
	}
	if c.weigher != nil {
		snap.weight = c.weighOf(snap.value)
	} else {
		snap.weight = clampWeight(snap.weight)
	}
	if c.cfg.maxValueWeight > 0 && snap.weight > c.cfg.maxValueWeight {
		return false
	}
	if len(snap.tags) > 0 && c.validateTagLimits(snap.tags) != nil {
		return false
	}
	return true
}

// applySnapshot inserts (or overwrites) one snapshotted record while
// preserving the snapshot's expireAt, inserted and hits values.
// Policy state initializes fresh.
func (c *Cache[K, V]) applySnapshot(snap *entrySnapshot[K, V]) {
	s := c.shardFor(snap.key)
	s.mu.Lock()
	defer c.unlockShard(s)

	s.supersedeFlight(snap.key)
	if c.sealed.Load() {
		return
	}
	if existing, ok := s.storage.get(snap.key); ok {
		c.removeLocked(s, existing, EvictReasonReplaced)
	}

	e := s.pool.get()
	e.key = snap.key
	e.storeValue(snap.value)
	e.weight = snap.weight
	e.inserted = snap.inserted
	e.lastAccess.Store(snap.inserted)
	e.expireAt.Store(snap.expireAt)
	e.hits.Store(snap.hits)
	if entryFlags(snap.flags).has(flagSliding) {
		e.flags.store(flagSliding)
		e.slidingTTL = snap.slidingTTL
	}
	if len(snap.tags) > 0 {
		e.tags = append(e.tags[:0], snap.tags...)
	}
	s.storage.set(snap.key, e)
	s.markAmended()
	s.expiryAdd(e)
	s.policy.OnInsert(e)
	c.retagLocked(snap.key, nil, snap.tags)
	c.counters.entries.Add(1)
	c.counters.bytes.Add(snap.weight)
	if snap.expireAt > 0 {
		c.startJanitorLocked(s)
	}
	c.evictOverTotalLocked(s)
}

// evictOverTotalLocked evicts from s while the cache holds more entries
// than [WithMaxEntries] allows. Loading bounds the total rather than each
// shard: keys hash with a per-cache random seed, so a snapshot's entries
// spread over shards differently than in the cache that saved them, and
// per-shard budgets would drop entries that fit the cache. A shard left
// over its share trims on its next write. The byte budget, and any
// remaining entry overage, is shed across shards when s is unlocked.
// Caller MUST hold s.mu.
func (c *Cache[K, V]) evictOverTotalLocked(s *shard[K, V]) {
	limit := c.maxEntries.Load()
	for limit > 0 && c.counters.entries.Load() > limit && s.storage.length() > 0 {
		victim := shedVictim(s.policy, false)
		if victim == nil {
			return
		}
		c.removeLocked(s, victim, EvictReasonCapacity)
		c.counters.evictions.Add(1)
	}
}

func (c *Cache[K, V]) writeHeader(w io.Writer, count int64) error {
	if _, err := io.WriteString(w, snapshotMagic); err != nil {
		return wrapSaveErr("magic", err)
	}
	if err := binary.Write(w, binary.LittleEndian, snapshotVersion); err != nil {
		return wrapSaveErr("version", err)
	}
	if err := writeString(w, c.cfg.codec.Name()); err != nil {
		return wrapSaveErr("codec", err)
	}
	if err := writeString(w, c.cfg.name); err != nil {
		return wrapSaveErr("name", err)
	}
	if err := writeMetadata(w, c.effectiveSnapshotMetadata()); err != nil {
		return wrapSaveErr("metadata", err)
	}
	if err := binary.Write(w, binary.LittleEndian, c.cfg.clock.Now().UnixNano()); err != nil {
		return wrapSaveErr("saveTime", err)
	}
	if err := binary.Write(w, binary.LittleEndian, count); err != nil {
		return wrapSaveErr("count", err)
	}
	return nil
}

// effectiveSnapshotMetadata returns user metadata merged with reserved
// entries (e.g. schema version). Reserved keys win over user values.
func (c *Cache[K, V]) effectiveSnapshotMetadata() map[string]string {
	version := c.schemaVersion()
	user := c.cfg.snapshotMetadata
	if version == "" {
		return user
	}
	out := make(map[string]string, len(user)+1)
	maps.Copy(out, user)
	out[snapshotSchemaVersionKey] = version
	return out
}

func (c *Cache[K, V]) schemaVersion() string {
	var zero V
	return schemaVersion(zero)
}

// writeMetadata emits a uint16 count then sorted (key, value) string
// pairs so saves are deterministic.
func writeMetadata(w io.Writer, meta map[string]string) error {
	if len(meta) > 65535 {
		return errFieldTooBig
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(len(meta))); err != nil {
		return err
	}
	if len(meta) == 0 {
		return nil
	}
	keys := make([]string, 0, len(meta))
	for k := range meta {
		keys = append(keys, k)
	}
	sortStrings(keys)
	for _, k := range keys {
		if err := writeString(w, k); err != nil {
			return err
		}
		if err := writeString(w, meta[k]); err != nil {
			return err
		}
	}
	return nil
}

// readMetadata inverts writeMetadata. Returns a non-nil empty map when
// the snapshot has no metadata.
func readMetadata(sr *snapReader) (map[string]string, error) {
	n, err := sr.u16()
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, min(int64(n), sr.left/8))
	for range int(n) {
		k, err := sr.str()
		if err != nil {
			return nil, err
		}
		v, err := sr.str()
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

// sortStrings avoids a sort import; insertion sort is fine for typical
// metadata sizes (<=16 entries).
func sortStrings(a []string) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1] > a[j]; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}

func (c *Cache[K, V]) writeRecord(w io.Writer, ss *saveState, codec Codec, snap *entrySnapshot[K, V]) error {
	keyBytes, err := marshalKey(ss, codec, snap.key)
	if err != nil {
		return wrapSaveErr("key marshal", err)
	}
	valBytes, err := marshalValue(ss, codec, snap.value)
	if err != nil {
		return wrapSaveErr("value marshal", err)
	}
	if err := writeBytes(w, keyBytes); err != nil {
		return wrapSaveErr("key", err)
	}
	if err := writeBytes(w, valBytes); err != nil {
		return wrapSaveErr("value", err)
	}
	for _, field := range []struct {
		name string
		v    any
	}{
		{"expireAt", snap.expireAt},
		{"weight", snap.weight},
		{"inserted", snap.inserted},
		{"hits", snap.hits},
		{"flags", snap.flags},
		{"slidingTTL", snap.slidingTTL},
	} {
		if err := binary.Write(w, binary.LittleEndian, field.v); err != nil {
			return wrapSaveErr(field.name, err)
		}
	}
	if len(snap.tags) > 255 {
		return wrapSaveErr("tags", errTooManyTags)
	}
	if err := binary.Write(w, binary.LittleEndian, uint8(len(snap.tags))); err != nil {
		return wrapSaveErr("tagCount", err)
	}
	for _, tag := range snap.tags {
		if err := writeString(w, tag); err != nil {
			return wrapSaveErr("tag", err)
		}
	}
	return nil
}

// marshalValue applies the cache:"..." filter, then tries
// [SnapshotMarshaler] (V and *V), then falls back to codec.
func marshalValue[V any](ss *saveState, codec Codec, value V) ([]byte, error) {
	value = applySnapshotFilter(value)
	if m, ok := any(value).(SnapshotMarshaler); ok {
		return m.SnapshotMarshal()
	}
	if m, ok := any(&value).(SnapshotMarshaler); ok {
		return m.SnapshotMarshal()
	}
	return ss.encode(codec, value)
}

// unmarshalValue runs snapshot decoding. dst MUST be a pointer to V.
func unmarshalValue[V any](ls *loadState, b []byte, dst *V) error {
	if u, ok := any(*dst).(SnapshotUnmarshaler); ok {
		return u.SnapshotUnmarshal(b)
	}
	if u, ok := any(dst).(SnapshotUnmarshaler); ok {
		return u.SnapshotUnmarshal(b)
	}
	return ls.decode(ls.codec, b, dst)
}

// readSnapshotHeader parses the header.
func readSnapshotHeader(sr *snapReader) (*SnapshotInfo, error) {
	magic, err := sr.take(uint64(len(snapshotMagic)))
	if err != nil {
		return nil, sr.fail("reading magic", err)
	}
	if string(magic) != snapshotMagic {
		return nil, &SnapshotError{
			Op:      "load",
			Message: fmt.Sprintf("bad magic: %q (want %q)", magic, snapshotMagic),
			Err:     ErrSnapshotIncompatible,
		}
	}
	version, err := sr.u8()
	if err != nil {
		return nil, sr.fail("reading version", err)
	}
	if version != snapshotVersion && version != snapshotVersionV2 {
		return nil, &SnapshotError{
			Op:      "load",
			Message: fmt.Sprintf("unsupported version %d (want %d or %d)", version, snapshotVersionV2, snapshotVersion),
			Err:     ErrSnapshotIncompatible,
		}
	}
	codec, err := sr.str()
	if err != nil {
		return nil, sr.fail("reading codec", err)
	}
	name, err := sr.str()
	if err != nil {
		return nil, sr.fail("reading name", err)
	}
	metadata, err := readMetadata(sr)
	if err != nil {
		return nil, sr.fail("reading metadata", err)
	}
	saveTime, err := sr.i64()
	if err != nil {
		return nil, sr.fail("reading saveTime", err)
	}
	count, err := sr.i64()
	if err != nil {
		return nil, sr.fail("reading count", err)
	}
	if count < 0 {
		return nil, sr.fail("negative count", ErrSnapshotCorrupt)
	}
	info := &SnapshotInfo{
		Version:  version,
		Codec:    codec,
		Name:     name,
		Count:    count,
		Metadata: metadata,
	}
	if saveTime != 0 {
		info.SaveTime = time.Unix(0, saveTime)
	}
	return info, nil
}

func (c *Cache[K, V]) readRecord(sr *snapReader, ls *loadState) (entrySnapshot[K, V], error) {
	var snap entrySnapshot[K, V]

	keyBytes, err := sr.bytes()
	if err != nil {
		return snap, sr.fail("reading key bytes", err)
	}
	valBytes, err := sr.bytes()
	if err != nil {
		return snap, sr.fail("reading value bytes", err)
	}
	if err := unmarshalKey(ls, keyBytes, &snap.key); err != nil {
		return snap, sr.fail("key unmarshal", err)
	}
	if err := unmarshalValue(ls, valBytes, &snap.value); err != nil {
		return snap, sr.fail("value unmarshal", err)
	}

	if snap.expireAt, err = sr.i64(); err != nil {
		return snap, sr.fail("reading expireAt", err)
	}
	if snap.weight, err = sr.i64(); err != nil {
		return snap, sr.fail("reading weight", err)
	}
	if snap.inserted, err = sr.i64(); err != nil {
		return snap, sr.fail("reading inserted", err)
	}
	if snap.hits, err = sr.u32(); err != nil {
		return snap, sr.fail("reading hits", err)
	}
	if snap.flags, err = sr.u16(); err != nil {
		return snap, sr.fail("reading flags", err)
	}
	if ls.version >= snapshotVersion {
		if snap.slidingTTL, err = sr.i64(); err != nil {
			return snap, sr.fail("reading slidingTTL", err)
		}
	}
	tagCount, err := sr.u8()
	if err != nil {
		return snap, sr.fail("reading tagCount", err)
	}
	if tagCount > 0 {
		snap.tags = make([]string, tagCount)
		for i := range snap.tags {
			tag, err := sr.str()
			if err != nil {
				return snap, sr.fail("reading tag", err)
			}
			snap.tags[i] = tag
		}
	}

	// Save writes a weight of at least 1 and no flag but sliding.
	if snap.weight < 1 {
		return snap, sr.fail(fmt.Sprintf("invalid weight %d", snap.weight), ErrSnapshotCorrupt)
	}
	if entryFlags(snap.flags)&^flagSliding != 0 || snap.flags > math.MaxUint8 {
		return snap, sr.fail(fmt.Sprintf("invalid flags %#x", snap.flags), ErrSnapshotCorrupt)
	}
	if entryFlags(snap.flags).has(flagSliding) {
		switch {
		case ls.version < snapshotVersion:
			// The sliding TTL wasn't saved; the entry keeps its
			// absolute expiry.
			snap.flags = 0
		case snap.slidingTTL <= 0:
			return snap, sr.fail("sliding entry without a sliding TTL", ErrSnapshotCorrupt)
		}
	}
	return snap, nil
}

// writeBytes prefixes b with its uint32 length and writes the payload.
func writeBytes(w io.Writer, b []byte) error {
	if uint64(len(b)) > 1<<31-1 {
		return errFieldTooBig
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	_, err := w.Write(b)
	return err
}

// writeString is writeBytes specialized to string.
func writeString(w io.Writer, s string) error {
	if uint64(len(s)) > 1<<31-1 {
		return errFieldTooBig
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(s))); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	if _, err := io.WriteString(w, s); err != nil {
		return fmt.Errorf("write string payload: %w", err)
	}
	return nil
}

func wrapSaveErr(field string, err error) error {
	return &SnapshotError{Op: "save", Message: "writing " + field, Err: err}
}

// snapReader reads snapshot fields. It never reads, or allocates for,
// more than left bytes: a length prefix that runs past what remains
// fails with [ErrSnapshotCorrupt] before anything is allocated. Bytes
// read are added to h (when non-nil) and capture (when non-nil).
type snapReader struct {
	r       io.Reader
	h       hash.Hash32
	capture *bytes.Buffer
	left    int64
	off     int64
	pinned  bool // off is fixed (fields read from decrypted records)
	buf     [8]byte
}

func newSnapReader(r io.Reader, h hash.Hash32, limit int64) *snapReader {
	return &snapReader{r: r, h: h, left: limit}
}

// pinOffset makes errors from sr report offset at.
func (sr *snapReader) pinOffset(at int64) {
	sr.off = at
	sr.pinned = true
}

// fail returns a load [*SnapshotError] at the current offset.
func (sr *snapReader) fail(msg string, err error) error {
	switch {
	case errors.Is(err, errInflateTooLarge):
		err = errors.Join(ErrSnapshotTooLarge, err)
	case errors.Is(err, errFieldPastEnd):
		err = errors.Join(ErrSnapshotCorrupt, err)
	}
	return &SnapshotError{Op: "load", Offset: sr.off, Message: msg, Err: err}
}

// read fills p, which the caller sized within sr.left.
func (sr *snapReader) read(p []byte) error {
	if int64(len(p)) > sr.left {
		return errFieldPastEnd
	}
	n, err := io.ReadFull(sr.r, p)
	if sr.h != nil {
		_, _ = sr.h.Write(p[:n])
	}
	if sr.capture != nil {
		_, _ = sr.capture.Write(p[:n])
	}
	sr.left -= int64(n)
	if !sr.pinned {
		sr.off += int64(n)
	}
	return err //nolint:wrapcheck // io.ReadFull errors are wrapped by fail
}

// take reads n bytes into a new slice.
func (sr *snapReader) take(n uint64) ([]byte, error) {
	if n > uint64(sr.left) {
		return nil, errFieldPastEnd
	}
	b := make([]byte, n)
	if err := sr.read(b); err != nil {
		return nil, err
	}
	return b, nil
}

func (sr *snapReader) u8() (uint8, error) {
	err := sr.read(sr.buf[:1])
	return sr.buf[0], err
}

func (sr *snapReader) u16() (uint16, error) {
	err := sr.read(sr.buf[:2])
	return binary.LittleEndian.Uint16(sr.buf[:2]), err
}

func (sr *snapReader) u32() (uint32, error) {
	err := sr.read(sr.buf[:4])
	return binary.LittleEndian.Uint32(sr.buf[:4]), err
}

func (sr *snapReader) u64() (uint64, error) {
	err := sr.read(sr.buf[:8])
	return binary.LittleEndian.Uint64(sr.buf[:8]), err
}

func (sr *snapReader) i64() (int64, error) {
	v, err := sr.u64()
	return int64(v), err
}

// bytes reads a uint32-length-prefixed field; empty fields are nil.
func (sr *snapReader) bytes() ([]byte, error) {
	n, err := sr.u32()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	return sr.take(uint64(n))
}

func (sr *snapReader) str() (string, error) {
	b, err := sr.bytes()
	return string(b), err
}

// snapshotInflateFactor bounds how far [CompressedCodec] fields may
// decompress: to this many times the snapshot size limit in total.
const snapshotInflateFactor = 16

// inflateLimit returns the decompression budget for a snapshot size
// limit.
func inflateLimit(limit int64) int64 {
	if limit > math.MaxInt64/snapshotInflateFactor {
		return math.MaxInt64
	}
	return limit * snapshotInflateFactor
}

// limitWriter passes writes to w until left bytes are used up, then
// fails with [ErrSnapshotTooLarge].
type limitWriter struct {
	w    io.Writer
	left int64
}

func (lw *limitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > lw.left {
		return 0, ErrSnapshotTooLarge
	}
	n, err := lw.w.Write(p)
	lw.left -= int64(n)
	return n, err
}

// saveState carries per-save encoding state.
type saveState struct {
	// inflateLeft is how many more bytes [CompressedCodec] layers may
	// compress during this save, so Save fails where Load would.
	inflateLeft int64
}

// encode marshals v through codec, counting the bytes that
// [CompressedCodec] layers compress against ss.inflateLeft. It mirrors
// [loadState.decode].
func (ss *saveState) encode(codec Codec, v any) ([]byte, error) {
	switch cc := codec.(type) {
	case CompressedCodec:
		return ss.encodeCompressed(cc, v)
	case *CompressedCodec:
		if cc != nil {
			return ss.encodeCompressed(*cc, v)
		}
	case *EncryptedCodec:
		if cc != nil {
			plain, err := ss.encode(cc.base, v)
			if err != nil {
				return nil, err
			}
			return cc.seal(plain, nil)
		}
	}
	return codec.Marshal(v)
}

func (ss *saveState) encodeCompressed(cc CompressedCodec, v any) ([]byte, error) {
	raw, err := ss.encode(cc.base, v)
	if err != nil {
		return nil, err
	}
	if ss.inflateLeft -= int64(len(raw)); ss.inflateLeft < 0 {
		return nil, fmt.Errorf("%w: compressed fields exceed the decompression limit", ErrSnapshotTooLarge)
	}
	return cc.compress(raw)
}

// loadState carries per-load decoding state.
type loadState struct {
	// codec decodes record keys and values.
	codec Codec
	// version is the snapshot's format version.
	version uint8
	// inflateLeft is how many more bytes [CompressedCodec] layers may
	// inflate to during this load; it starts at [inflateLimit].
	inflateLeft int64
}

// decode unmarshals data into dst through codec, inflating
// [CompressedCodec] layers within ls.inflateLeft.
func (ls *loadState) decode(codec Codec, data []byte, dst any) error {
	switch cc := codec.(type) {
	case CompressedCodec:
		return ls.decodeCompressed(cc, data, dst)
	case *CompressedCodec:
		if cc != nil {
			return ls.decodeCompressed(*cc, data, dst)
		}
	case *EncryptedCodec:
		if cc != nil {
			plain, err := cc.openField(data)
			if err != nil {
				return err
			}
			return ls.decode(cc.base, plain, dst)
		}
	}
	return codec.Unmarshal(data, dst)
}

func (ls *loadState) decodeCompressed(cc CompressedCodec, data []byte, dst any) error {
	raw, err := inflate(data, ls.inflateLeft)
	if err != nil {
		return &CodecError{Op: codecOpUnmarshal, Codec: cc.Name(), Err: err}
	}
	ls.inflateLeft -= int64(len(raw))
	return ls.decode(cc.base, raw, dst)
}

// splitEncryption finds an [EncryptedCodec] layer in codec and returns
// codec without it, plus the layer; enc is nil when there is none.
func splitEncryption(codec Codec) (rec Codec, enc *EncryptedCodec) {
	switch v := codec.(type) {
	case *EncryptedCodec:
		if v != nil {
			return v.base, v
		}
	case CompressedCodec:
		if base, e := splitEncryption(v.base); e != nil {
			return CompressedCodec{base: base, level: v.level}, e
		}
	case *CompressedCodec:
		if v != nil {
			if base, e := splitEncryption(v.base); e != nil {
				return CompressedCodec{base: base, level: v.level}, e
			}
		}
	}
	return codec, nil
}

// marshalKey encodes a snapshot key with codec. A raw codec takes only
// bytes, so string and integer keys are first turned into bytes (see
// rawKeyBytes).
func marshalKey[K comparable](ss *saveState, codec Codec, key K) ([]byte, error) {
	if !isRawCodec(codec) {
		return ss.encode(codec, key)
	}
	b, err := rawKeyBytes(key)
	if err != nil {
		return nil, err
	}
	return ss.encode(codec, b)
}

// unmarshalKey reverses [marshalKey].
func unmarshalKey[K comparable](ls *loadState, data []byte, dst *K) error {
	if !isRawCodec(ls.codec) {
		return ls.decode(ls.codec, data, dst)
	}
	var b []byte
	if err := ls.decode(ls.codec, data, &b); err != nil {
		return err
	}
	return setRawKey(b, dst)
}
