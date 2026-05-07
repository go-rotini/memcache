package memcache

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path/filepath"
	"time"
)

// snapshotMagic is the 4-byte file signature ("RTNI"). Readers reject
// non-matching files with [ErrSnapshotIncompatible].
const snapshotMagic = "RTNI"

// snapshotVersion is the on-disk format version. v2 adds a metadata
// map after `name` for [WithSnapshotMetadata] / [InspectSnapshot].
const snapshotVersion uint8 = 2

// defaultMaxSnapshotBytes caps Load input when [WithMaxSnapshotBytes] is
// not set.
const defaultMaxSnapshotBytes = int64(256 << 20) // 256 MiB

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var errFieldTooBig = errors.New("memcache: snapshot field exceeds 2 GiB")
var errTooManyTags = errors.New("memcache: more than 255 tags per entry")

// snapshotSchemaVersionKey is the reserved metadata key for versioned
// struct fingerprints; Load refuses mismatches with [ErrSnapshotIncompatible].
const snapshotSchemaVersionKey = "__memcache_schema_version"

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

// Save writes a snapshot of the cache to w using the cache's configured
// [Codec]: magic-prefixed header, length-prefixed records, and a
// trailing CRC32-Castagnoli. Acquires each shard's read lock in turn;
// writers to a shard being snapshotted block until its pass completes.
func (c *Cache[K, V]) Save(w io.Writer) (err error) {
	if c.closed.Load() {
		return ErrClosed
	}
	_, span := c.tracer.Start(context.Background(), "memcache.snapshot.save")
	defer func() { span.End(err) }()
	return c.saveTo(w)
}

// saveTo is the closed-check-free Save implementation. Called by Close
// so the final auto-save runs even with closed already flipped.
func (c *Cache[K, V]) saveTo(w io.Writer) error {
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
	// CRC trailer is written without updating the CRC accumulator.
	if err := binary.Write(w, binary.LittleEndian, crc.Sum32()); err != nil {
		return wrapSaveErr("crc", err)
	}
	c.counters.stampSnapshot(c.cfg.clock.Now())
	c.publishEvent(Event[K, V]{Kind: EventSnapshot, At: c.cfg.clock.Now()})
	return nil
}

// SaveFile writes a snapshot to path atomically via temp file + fsync +
// rename. Readers see either the prior or new snapshot, never torn.
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

// Load replaces the cache contents with the snapshot read from r and
// returns the number of entries inserted. Records past their TTL are
// skipped. On error the cache may be partially loaded.
func (c *Cache[K, V]) Load(r io.Reader) (n int, err error) {
	_, span := c.tracer.Start(context.Background(), "memcache.snapshot.load")
	defer func() { span.End(err) }()
	return c.loadInto(r, true)
}

// Merge folds the snapshot's entries into the cache, overwriting existing
// keys. Untouched keys are preserved. Returns inserts plus replacements.
func (c *Cache[K, V]) Merge(r io.Reader) (int, error) {
	return c.loadInto(r, false)
}

func (c *Cache[K, V]) loadInto(r io.Reader, reset bool) (int, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}

	limit := c.cfg.maxSnapshotBytes
	if limit <= 0 {
		limit = defaultMaxSnapshotBytes
	}
	// +1 so we detect exceeding the limit rather than silent truncation.
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
	if want := c.schemaVersion(); want != "" {
		got := info.Metadata[snapshotSchemaVersionKey]
		if got != "" && got != want {
			return 0, &SnapshotError{
				Op:      "load",
				Message: fmt.Sprintf("schema version mismatch: snapshot=%s cache=%s", got, want),
				Err:     ErrSnapshotIncompatible,
			}
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
		if snap.expireAt != 0 && snap.expireAt <= now {
			continue
		}
		c.applySnapshot(snap)
		loaded++
	}

	// Trailing CRC is read directly (not through the tee) so the
	// computed CRC does not mix with the stored one.
	var stored uint32
	if err := binary.Read(br, binary.LittleEndian, &stored); err != nil {
		return loaded, &SnapshotError{Op: "load", Message: "reading trailing CRC", Err: err}
	}
	if stored != crc.Sum32() {
		return loaded, &SnapshotError{Op: "load", Message: "CRC mismatch", Err: ErrSnapshotCorrupt}
	}
	c.counters.stampSnapshot(c.cfg.clock.Now())
	c.publishEvent(Event[K, V]{Kind: EventSnapshot, At: c.cfg.clock.Now()})
	return loaded, nil
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
	return c.loadInto(f, true)
}

// InspectSnapshot reads only the header of a snapshot file and returns
// its metadata, skipping records and CRC. Useful for compatibility
// checks before a full Load.
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
				flags:    uint16(e.flags),
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

// applySnapshot inserts (or overwrites) one snapshotted record while
// preserving the snapshot's expireAt and inserted timestamps. Hits and
// generation reset to zero; policy state initializes fresh on Load.
func (c *Cache[K, V]) applySnapshot(snap entrySnapshot[K, V]) {
	s := c.shardFor(snap.key)
	s.mu.Lock()
	defer c.unlockShard(s)

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
	e.flags = entryFlags(snap.flags)
	if len(snap.tags) > 0 {
		e.tags = append(e.tags[:0], snap.tags...)
	}
	s.storage.set(snap.key, e)
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
func readMetadata(r io.Reader) (map[string]string, error) {
	var n uint16
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	out := make(map[string]string, n)
	for range int(n) {
		k, err := readString(r)
		if err != nil {
			return nil, err
		}
		v, err := readString(r)
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

func (c *Cache[K, V]) writeRecord(w io.Writer, snap *entrySnapshot[K, V]) error {
	keyBytes, err := c.cfg.codec.Marshal(snap.key)
	if err != nil {
		return wrapSaveErr("key marshal", err)
	}
	valBytes, err := c.marshalValue(snap.value)
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

// marshalValue applies the cache:"..." filter, then tries
// [SnapshotMarshaler] (V and *V), then falls back to the configured
// [Codec].
func (c *Cache[K, V]) marshalValue(value V) ([]byte, error) {
	value = applySnapshotFilter(value)
	if m, ok := any(value).(SnapshotMarshaler); ok {
		return m.SnapshotMarshal()
	}
	if m, ok := any(&value).(SnapshotMarshaler); ok {
		return m.SnapshotMarshal()
	}
	return c.cfg.codec.Marshal(value)
}

// unmarshalValue runs snapshot decoding. dst MUST be a pointer to V.
func (c *Cache[K, V]) unmarshalValue(b []byte, dst *V) error {
	if u, ok := any(*dst).(SnapshotUnmarshaler); ok {
		return u.SnapshotUnmarshal(b)
	}
	if u, ok := any(dst).(SnapshotUnmarshaler); ok {
		return u.SnapshotUnmarshal(b)
	}
	return c.cfg.codec.Unmarshal(b, dst)
}

// readSnapshotHeader parses the header. h, when non-nil, accumulates
// every byte consumed for CRC checking.
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
	metadata, err := readMetadata(tee)
	if err != nil {
		return nil, &SnapshotError{Op: "load", Message: "reading metadata", Err: err}
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
	if err := c.unmarshalValue(valBytes, &snap.value); err != nil {
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

func readString(r io.Reader) (string, error) {
	b, err := readBytes(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func wrapSaveErr(field string, err error) error {
	return &SnapshotError{Op: "save", Message: "writing " + field, Err: err}
}
