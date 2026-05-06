package memcache

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

// snapshotMagic is the 4-byte file signature prefix. The bytes
// spell "RTNI" (rotini); a reader that doesn't see these refuses
// the file with [ErrSnapshotIncompatible].
const snapshotMagic = "RTNI"

// snapshotVersion is the on-disk format version. Writers stamp
// this; readers reject anything they don't understand.
const snapshotVersion uint8 = 1

// defaultMaxSnapshotBytes caps Load input when [WithMaxSnapshotBytes]
// is not set — protects against OOM on a corrupt or hostile file.
const defaultMaxSnapshotBytes = int64(256 << 20) // 256 MiB

// crcTable is the precomputed CRC32-Castagnoli polynomial table used
// for snapshot integrity checks.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// errFieldTooBig fires when a snapshot field exceeds the 2 GiB
// limit imposed by the uint32 length prefix. Wrapped by call
// sites for context.
var errFieldTooBig = errors.New("memcache: snapshot field exceeds 2 GiB")

// errTooManyTags fires when an entry carries more than 255 tags;
// the on-disk format reserves a single byte for the count.
var errTooManyTags = errors.New("memcache: more than 255 tags per entry")

// entrySnapshot is the per-entry record extracted from a live cache
// at Save time and reconstructed during Load.
type entrySnapshot[K comparable, V any] struct {
	key      K
	value    V
	expireAt int64
	weight   int64
	inserted int64
	hits     uint32
	flags    uint16
	tags     []string
}

// Save writes a snapshot of the cache to w using the cache's
// configured [Codec]. The format is described in §9.2 of the
// requirements: a magic-prefixed header, length-prefixed records,
// and a trailing CRC32-Castagnoli over every preceding byte.
//
// Save acquires each shard's read lock in turn — concurrent reads
// are unaffected, but writers to a shard being snapshotted block
// until that shard's pass completes.
func (c *Cache[K, V]) Save(w io.Writer) error {
	if c.closed.Load() {
		return ErrClosed
	}
	return c.saveTo(w)
}

// saveTo is the closed-check-free Save implementation. Close calls
// this directly so the final auto-save runs even though `closed`
// has already been flipped.
func (c *Cache[K, V]) saveTo(w io.Writer) error {
	// Materialize a stable view of every live entry under per-shard
	// read locks so subsequent encoding can run lock-free.
	staged := c.snapshotEntries()

	crc := crc32.New(crcTable)
	cw := io.MultiWriter(w, crc)

	if err := c.writeHeader(cw, int64(len(staged))); err != nil {
		return err
	}
	for i := range staged {
		if err := c.writeRecord(cw, &staged[i]); err != nil {
			return err
		}
	}
	// CRC trailer is written WITHOUT updating the CRC accumulator.
	if err := binary.Write(w, binary.LittleEndian, crc.Sum32()); err != nil {
		return wrapSaveErr("crc", err)
	}
	c.publishEvent(Event[K, V]{Kind: EventSnapshot, At: c.cfg.clock.Now()})
	return nil
}

// SaveFile writes a snapshot to path atomically: the contents are
// first written to a sibling temp file, fsync'd, then renamed into
// place. Readers either see the prior snapshot or the new one,
// never a torn write.
func (c *Cache[K, V]) SaveFile(path string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	return c.saveFileTo(path)
}

// saveFileTo is the closed-check-free SaveFile implementation.
// Close calls this directly so the final auto-save runs even
// though `closed` has already been flipped.
func (c *Cache[K, V]) saveFileTo(path string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".memcache-*.tmp")
	if err != nil {
		return &SnapshotError{Op: "save", Path: path, Message: "create temp", Err: err}
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	bw := bufio.NewWriter(tmp)
	if err := c.saveTo(bw); err != nil {
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

// Load replaces the cache contents with the snapshot read from r.
// Returns the number of entries actually inserted (records past
// their TTL are skipped — they would expire instantly anyway).
//
// On any read or validation error the cache is left in a partially
// loaded state; callers concerned about atomicity should use
// [Cache.Merge] in concert with their own rollback strategy, or
// load into a fresh cache via [Cache.Clone] semantics.
func (c *Cache[K, V]) Load(r io.Reader) (int, error) {
	return c.loadInto(r, true)
}

// Merge reads a snapshot and folds its entries into the cache,
// overwriting existing keys. Existing entries that are NOT in the
// snapshot are left as-is. Returns the number of entries inserted
// or replaced.
func (c *Cache[K, V]) Merge(r io.Reader) (int, error) {
	return c.loadInto(r, false)
}

// loadInto is the shared Load/Merge implementation. reset chooses
// between the two semantics — true wipes the cache before
// inserting (Load), false preserves untouched keys (Merge).
func (c *Cache[K, V]) loadInto(r io.Reader, reset bool) (int, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}

	limit := c.cfg.maxSnapshotBytes
	if limit <= 0 {
		limit = defaultMaxSnapshotBytes
	}
	// +1 so we can detect "exceeded the limit" rather than silently
	// truncating at the limit.
	r = io.LimitReader(r, limit+1)

	br := bufio.NewReader(r)
	crc := crc32.New(crcTable)

	info, err := readSnapshotHeader(br, crc)
	if err != nil {
		return 0, err
	}

	if info.Codec != c.cfg.codec.Name() {
		return 0, &SnapshotError{
			Op:      "load",
			Message: fmt.Sprintf("codec mismatch: snapshot=%q cache=%q", info.Codec, c.cfg.codec.Name()),
			Err:     ErrSnapshotIncompatible,
		}
	}

	if reset {
		c.Reset()
	}

	now := c.cfg.clock.Now().UnixNano()
	loaded := 0
	for range info.Count {
		snap, err := c.readRecord(br, crc)
		if err != nil {
			return loaded, err
		}
		// Skip records past their TTL.
		if snap.expireAt != 0 && snap.expireAt <= now {
			continue
		}
		c.applySnapshot(snap)
		loaded++
	}

	// Trailing CRC is read directly (NOT through the tee) so the
	// computed CRC does not mix with the stored one.
	var stored uint32
	if err := binary.Read(br, binary.LittleEndian, &stored); err != nil {
		return loaded, &SnapshotError{Op: "load", Message: "reading trailing CRC", Err: err}
	}
	if stored != crc.Sum32() {
		return loaded, &SnapshotError{Op: "load", Message: "CRC mismatch", Err: ErrSnapshotCorrupt}
	}
	c.publishEvent(Event[K, V]{Kind: EventSnapshot, At: c.cfg.clock.Now()})
	return loaded, nil
}

// LoadFile loads a snapshot from path. Convenience wrapper around
// [Cache.Load] that handles file open/close.
func (c *Cache[K, V]) LoadFile(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, &SnapshotError{Op: "load", Path: path, Message: "open", Err: err}
	}
	defer f.Close()
	return c.Load(f)
}

// InspectSnapshot reads only the header of a snapshot file and
// returns its metadata without touching the records or running CRC
// validation. Useful for compatibility checks before committing to
// a full Load (e.g. "is this snapshot the right format/version?").
func InspectSnapshot(path string) (*SnapshotInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, &SnapshotError{Op: "load", Path: path, Message: "open", Err: err}
	}
	defer f.Close()
	br := bufio.NewReader(f)
	info, err := readSnapshotHeader(br, nil)
	if err != nil {
		return nil, err
	}
	if fi, statErr := f.Stat(); statErr == nil {
		info.SizeBytes = fi.Size()
	}
	return info, nil
}

// snapshotEntries returns a slice of entry records suitable for
// serialization. Per-shard read locks bound concurrent writers
// only for the shard being staged.
func (c *Cache[K, V]) snapshotEntries() []entrySnapshot[K, V] {
	now := c.cfg.clock.Now().UnixNano()
	var out []entrySnapshot[K, V]
	for _, s := range c.shards {
		s.mu.RLock()
		for _, e := range s.entries {
			// Skip TTL-expired and negative-cache tombstones —
			// neither carries useful warm-restart state.
			if e.expired(now) || e.flags.has(flagNegative) {
				continue
			}
			snap := entrySnapshot[K, V]{
				key:      e.key,
				value:    e.value,
				expireAt: e.expireAt.Load(),
				weight:   e.weight,
				inserted: e.inserted,
				hits:     e.hits.Load(),
				flags:    uint16(e.flags),
			}
			if len(e.tags) > 0 {
				snap.tags = append([]string(nil), e.tags...)
			}
			out = append(out, snap)
		}
		s.mu.RUnlock()
	}
	return out
}

// applySnapshot inserts (or overwrites) one snapshotted record
// into the cache while preserving the snapshot's expireAt and
// inserted timestamps. Hits / generation reset to zero per spec —
// the policy state is freshly initialized on Load.
func (c *Cache[K, V]) applySnapshot(snap entrySnapshot[K, V]) {
	s := c.shardFor(snap.key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.entries[snap.key]; ok {
		// Overwrite path (Merge semantics, or Load-after-non-empty
		// edge case).
		c.removeLocked(s, existing, EvictReasonReplaced)
	}

	e := s.pool.get()
	e.key = snap.key
	e.value = snap.value
	e.weight = snap.weight
	e.inserted = snap.inserted
	e.lastAccess.Store(snap.inserted)
	e.expireAt.Store(snap.expireAt)
	e.hits.Store(snap.hits)
	e.flags = entryFlags(snap.flags)
	if len(snap.tags) > 0 {
		e.tags = append(e.tags[:0], snap.tags...)
	}
	s.entries[snap.key] = e
	s.expiryAdd(e)
	s.policy.OnInsert(e)
	c.retagLocked(snap.key, nil, snap.tags)
	c.counters.entries.Add(1)
	c.counters.bytes.Add(snap.weight)
	if snap.expireAt > 0 {
		c.startJanitorLocked(s)
	}
	c.evictWhileOverBudgetLocked(s)
}

// writeHeader emits the fixed-shape file header.
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
	if err := binary.Write(w, binary.LittleEndian, c.cfg.clock.Now().UnixNano()); err != nil {
		return wrapSaveErr("saveTime", err)
	}
	if err := binary.Write(w, binary.LittleEndian, count); err != nil {
		return wrapSaveErr("count", err)
	}
	return nil
}

// writeRecord emits one entry's serialized form. Key and value go
// through the cache's [Codec]; the rest of the metadata is fixed
// width.
func (c *Cache[K, V]) writeRecord(w io.Writer, snap *entrySnapshot[K, V]) error {
	keyBytes, err := c.cfg.codec.Marshal(snap.key)
	if err != nil {
		return wrapSaveErr("key marshal", err)
	}
	valBytes, err := c.cfg.codec.Marshal(snap.value)
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

// readSnapshotHeader parses magic/version/codec/name/saveTime/count
// from r. Updates h with every byte consumed when h is non-nil
// (used by Load's CRC accumulator; nil for InspectSnapshot).
func readSnapshotHeader(r io.Reader, h hash.Hash32) (*SnapshotInfo, error) {
	tee := r
	if h != nil {
		tee = io.TeeReader(r, h)
	}
	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(tee, magic); err != nil {
		return nil, &SnapshotError{Op: "load", Message: "reading magic", Err: err}
	}
	if string(magic) != snapshotMagic {
		return nil, &SnapshotError{
			Op:      "load",
			Message: fmt.Sprintf("bad magic: %q (want %q)", magic, snapshotMagic),
			Err:     ErrSnapshotIncompatible,
		}
	}
	var version uint8
	if err := binary.Read(tee, binary.LittleEndian, &version); err != nil {
		return nil, &SnapshotError{Op: "load", Message: "reading version", Err: err}
	}
	if version != snapshotVersion {
		return nil, &SnapshotError{
			Op:      "load",
			Message: fmt.Sprintf("unsupported version %d (want %d)", version, snapshotVersion),
			Err:     ErrSnapshotIncompatible,
		}
	}
	codec, err := readString(tee)
	if err != nil {
		return nil, &SnapshotError{Op: "load", Message: "reading codec", Err: err}
	}
	name, err := readString(tee)
	if err != nil {
		return nil, &SnapshotError{Op: "load", Message: "reading name", Err: err}
	}
	var saveTime, count int64
	if err := binary.Read(tee, binary.LittleEndian, &saveTime); err != nil {
		return nil, &SnapshotError{Op: "load", Message: "reading saveTime", Err: err}
	}
	if err := binary.Read(tee, binary.LittleEndian, &count); err != nil {
		return nil, &SnapshotError{Op: "load", Message: "reading count", Err: err}
	}
	if count < 0 {
		return nil, &SnapshotError{Op: "load", Message: "negative count", Err: ErrSnapshotCorrupt}
	}
	info := &SnapshotInfo{
		Version: version,
		Codec:   codec,
		Name:    name,
		Count:   count,
	}
	if saveTime != 0 {
		info.SaveTime = time.Unix(0, saveTime)
	}
	return info, nil
}

// readRecord reads one entry's serialized form. Updates h with
// every byte consumed.
func (c *Cache[K, V]) readRecord(r io.Reader, h hash.Hash32) (entrySnapshot[K, V], error) {
	var snap entrySnapshot[K, V]
	tee := io.TeeReader(r, h)

	keyBytes, err := readBytes(tee)
	if err != nil {
		return snap, &SnapshotError{Op: "load", Message: "reading key bytes", Err: err}
	}
	valBytes, err := readBytes(tee)
	if err != nil {
		return snap, &SnapshotError{Op: "load", Message: "reading value bytes", Err: err}
	}

	if err := c.cfg.codec.Unmarshal(keyBytes, &snap.key); err != nil {
		return snap, &SnapshotError{Op: "load", Message: "key unmarshal", Err: err}
	}
	if err := c.cfg.codec.Unmarshal(valBytes, &snap.value); err != nil {
		return snap, &SnapshotError{Op: "load", Message: "value unmarshal", Err: err}
	}

	for _, target := range []struct {
		name string
		v    any
	}{
		{"expireAt", &snap.expireAt},
		{"weight", &snap.weight},
		{"inserted", &snap.inserted},
		{"hits", &snap.hits},
		{"flags", &snap.flags},
	} {
		if err := binary.Read(tee, binary.LittleEndian, target.v); err != nil {
			return snap, &SnapshotError{Op: "load", Message: "reading " + target.name, Err: err}
		}
	}
	var tagCount uint8
	if err := binary.Read(tee, binary.LittleEndian, &tagCount); err != nil {
		return snap, &SnapshotError{Op: "load", Message: "reading tagCount", Err: err}
	}
	if tagCount > 0 {
		snap.tags = make([]string, tagCount)
		for i := range snap.tags {
			tag, err := readString(tee)
			if err != nil {
				return snap, &SnapshotError{Op: "load", Message: "reading tag", Err: err}
			}
			snap.tags[i] = tag
		}
	}
	return snap, nil
}

// writeBytes prefixes b with its uint32 length and writes the
// concatenation. Used for keys, values, and string slices.
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

// writeString is writeBytes specialized to string. Avoids an
// allocation by going through io.WriteString for the payload.
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

// readBytes is the inverse of writeBytes.
func readBytes(r io.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, fmt.Errorf("read field payload: %w", err)
	}
	return b, nil
}

// readString is the inverse of writeString.
func readString(r io.Reader) (string, error) {
	b, err := readBytes(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// wrapSaveErr packages an inner error into a [*SnapshotError]
// keyed on the field that failed.
func wrapSaveErr(field string, err error) error {
	return &SnapshotError{Op: "save", Message: "writing " + field, Err: err}
}
