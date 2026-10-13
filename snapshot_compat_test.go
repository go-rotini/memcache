package memcache

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// goldenRec is the value type of the testdata/v2 snapshots, which the
// previous release wrote.
type goldenRec struct {
	Name string
	N    []int
}

var goldenKey = bytes.Repeat([]byte{7}, 32)

func goldenCodecs(tb testing.TB) map[string]Codec {
	tb.Helper()
	enc, err := NewEncryptedCodec(GobCodec{}, goldenKey)
	if err != nil {
		tb.Fatal(err)
	}
	gzEnc, err := NewEncryptedCodec(NewCompressedCodec(GobCodec{}, 1), goldenKey)
	if err != nil {
		tb.Fatal(err)
	}
	return map[string]Codec{
		"gob":    GobCodec{},
		"json":   JSONCodec{},
		"gz":     NewCompressedCodec(JSONCodec{}, 1),
		"enc":    enc,
		"gz-enc": gzEnc,
	}
}

func TestLoadGoldenV2Snapshots(t *testing.T) {
	for name, codec := range goldenCodecs(t) {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("testdata", "v2", name+".v2")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := InspectSnapshot(path)
			if err != nil || info.Version != snapshotVersionV2 || info.Metadata["app"] != "x" {
				t.Fatalf("InspectSnapshot = %+v, %v", info, err)
			}
			// Load at the save time, so the TTL'd records are live.
			c, _ := New[string, goldenRec](WithMaxEntries(100), WithCodec(codec), WithClock(NewFakeClock(info.SaveTime)))
			defer c.Close()
			n, err := c.Load(bytes.NewReader(data))
			if _, encrypted := codec.(*EncryptedCodec); encrypted {
				if !errors.Is(err, ErrSnapshotIncompatible) {
					t.Fatalf("encrypted v2 Load = %d, %v; want ErrSnapshotIncompatible", n, err)
				}
				return
			}
			if err != nil || n != 4 {
				t.Fatalf("Load = %d, %v", n, err)
			}
			p, _ := c.Peek("plain")
			ttl, _ := c.TTL("ttl")
			if p.Name != "p" || len(p.N) != 3 || len(c.Tags("tagged")) != 2 || ttl <= 0 {
				t.Fatalf("plain=%+v tags=%v ttl=%v", p, c.Tags("tagged"), ttl)
			}
			if m, _ := c.ItemMetadata("slide"); m.Sliding {
				t.Fatal("a v2 sliding entry should load with an absolute expiry")
			}
			var buf bytes.Buffer
			if err := c.Save(&buf); err != nil {
				t.Fatal(err)
			}
			if n, err := c.Load(&buf); err != nil || n != 4 {
				t.Fatalf("v3 round trip = %d, %v", n, err)
			}
		})
	}
}

func TestEncryptedV2SnapshotWithSwappedValuesIsRejected(t *testing.T) {
	enc, err := NewEncryptedCodec(JSONCodec{}, goldenKey)
	if err != nil {
		t.Fatal(err)
	}
	sealed := func(v any) string {
		b, err := enc.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	// Each v2 field is sealed on its own, so an attacker can swap values
	// between records and fix the CRC.
	recs := []entrySnapshot[string, string]{
		{key: "alice", value: "role=user", weight: 1},
		{key: "bob", value: "role=admin", weight: 1},
	}
	data := craftSnapshot(t, rawSealedCodec{enc, sealed}, snapshotVersionV2, recs)
	c, _ := New[string, string](WithMaxEntries(10), WithCodec(enc))
	defer c.Close()
	_ = c.Set("keep", "x")
	if _, err := c.Load(bytes.NewReader(data)); !errors.Is(err, ErrSnapshotIncompatible) {
		t.Fatalf("Load = %v, want ErrSnapshotIncompatible", err)
	}
	if v, _ := c.Get("keep"); v != "x" {
		t.Fatal("a refused Load changed the cache")
	}
}

// rawSealedCodec writes fields as enc would, under enc's name, for
// craftSnapshot.
type rawSealedCodec struct {
	enc  *EncryptedCodec
	seal func(any) string
}

func (r rawSealedCodec) Marshal(v any) ([]byte, error) { return []byte(r.seal(v)), nil }
func (r rawSealedCodec) Unmarshal([]byte, any) error   { return errors.New("unused") }
func (r rawSealedCodec) Name() string                  { return r.enc.Name() }

func TestSnapshotWeightsAreClamped(t *testing.T) {
	recs := []entrySnapshot[string, string]{
		{key: "a", value: "1", weight: math.MaxInt64},
		{key: "b", value: "2", weight: math.MaxInt64},
	}
	c, _ := New[string, string](WithMaxEntries(10))
	defer c.Close()
	if n, err := c.Load(bytes.NewReader(craftSnapshot(t, GobCodec{}, snapshotVersion, recs))); err != nil || n != 2 {
		t.Fatalf("Load = %d, %v", n, err)
	}
	if got := c.Bytes(); got != 2*maxEntryWeight {
		t.Fatalf("Bytes = %d, want %d", got, 2*maxEntryWeight)
	}
	_ = c.SetWithOptions("c", "3", SetWeight(math.MaxInt64))
	if got := c.Bytes(); got <= 0 {
		t.Fatalf("Bytes overflowed to %d", got)
	}
}

// An encrypted v2 snapshot at the auto-load path, as v1.0.2 left it, is
// skipped with a warning: New starts empty and the next save replaces it.
func TestAutoLoadSkipsAnEncryptedV2Snapshot(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "v2", "enc.v2"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cache.snap")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	codec := goldenCodecs(t)["enc"]
	c, err := New[string, goldenRec](WithMaxEntries(100), WithCodec(codec),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
		WithAutoLoad(path), WithAutoSave(path, time.Hour))
	if err != nil {
		t.Fatalf("New = %v; want an encrypted v2 snapshot skipped", err)
	}
	if c.Len() != 0 || !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("Len = %d, log %q; want an empty cache and a warning", c.Len(), logs.String())
	}
	_ = c.Set("k", goldenRec{Name: "n"})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := InspectSnapshot(path); err != nil || info.Version != snapshotVersion {
		t.Fatalf("after Close the file is %+v, %v; want a v%d snapshot", info, err, snapshotVersion)
	}
	c2, err := New[string, goldenRec](WithMaxEntries(100), WithCodec(codec), WithAutoLoad(path))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if v, ok := c2.Get("k"); !ok || v.Name != "n" {
		t.Fatalf("restart Get = %+v, %v", v, ok)
	}
	// Other load errors still fail New.
	if err := os.WriteFile(path, []byte("not a snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New[string, goldenRec](WithMaxEntries(100), WithCodec(codec), WithAutoLoad(path)); err == nil {
		t.Fatal("New with a corrupt auto-load file succeeded")
	}
}

func TestSaveSizeLimitAppliesOnlyWhenAsked(t *testing.T) {
	defer func(old int64) { defaultMaxSnapshotBytes = old }(defaultMaxSnapshotBytes)
	defaultMaxSnapshotBytes = 1 << 10
	dir := t.TempDir()
	autoPath := filepath.Join(dir, "auto.snap")
	fill := func(c *Cache[int, []byte]) {
		for i := range 8 {
			_ = c.Set(i, bytes.Repeat([]byte{'x'}, 512))
		}
	}
	c, _ := New[int, []byte](WithMaxEntries(100), WithCodec(RawCodec{}), WithAutoLoad(autoPath))
	defer c.Close()
	fill(c)
	var buf bytes.Buffer
	if err := c.Save(&buf); err != nil || buf.Len() <= 1<<10 {
		t.Fatalf("Save past the default limit = %d bytes, %v; want no limit", buf.Len(), err)
	}
	if err := c.SaveFile(filepath.Join(dir, "other.snap")); err != nil {
		t.Fatalf("SaveFile to another path = %v; want no limit", err)
	}
	if err := os.WriteFile(autoPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.SaveFile(autoPath); !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("SaveFile to the auto-load path = %v; want ErrSnapshotTooLarge", err)
	}
	if b, _ := os.ReadFile(autoPath); string(b) != "old" {
		t.Fatal("a failed SaveFile replaced the old file")
	}

	limited, _ := New[int, []byte](WithMaxEntries(100), WithCodec(RawCodec{}), WithMaxSnapshotBytes(2<<10))
	defer limited.Close()
	fill(limited)
	if err := limited.Save(io.Discard); !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("Save past WithMaxSnapshotBytes = %v; want ErrSnapshotTooLarge", err)
	}
	if err := limited.SaveFile(filepath.Join(dir, "limited.snap")); !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("SaveFile past WithMaxSnapshotBytes = %v; want ErrSnapshotTooLarge", err)
	}
}
