package memcache

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
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
