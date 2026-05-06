package memcache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotRoundTrip(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(8))
	defer src.Close()
	for k, v := range map[string]int{"alice": 1, "bob": 2, "carol": 3} {
		_ = src.Set(k, v)
	}
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatalf("Save: %v", err)
	}

	dst, _ := New[string, int](WithMaxEntries(8))
	defer dst.Close()
	n, err := dst.Load(&buf)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 3 || dst.Len() != 3 {
		t.Errorf("loaded %d entries, dst.Len=%d, want 3/3", n, dst.Len())
	}
	if v, _ := dst.Get("bob"); v != 2 {
		t.Errorf("after Load, Get(bob) = %d, want 2", v)
	}
}

func TestSaveFileLoadFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.gob")

	src, _ := New[string, int](WithMaxEntries(8))
	defer src.Close()
	for i := range 5 {
		_ = src.Set(itoaSimple(i), i*10)
	}
	if err := src.SaveFile(path); err != nil {
		t.Fatalf("SaveFile: %v", err)
	}

	// File exists and is non-empty.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("snapshot file is empty")
	}

	dst, _ := New[string, int](WithMaxEntries(8))
	defer dst.Close()
	n, err := dst.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if n != 5 {
		t.Errorf("loaded %d, want 5", n)
	}
	for i := range 5 {
		if v, _ := dst.Get(itoaSimple(i)); v != i*10 {
			t.Errorf("Get(%d) = %d, want %d", i, v, i*10)
		}
	}
}

func TestSaveFileAtomicityLeavesNoTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.gob")
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_ = c.Set("k", 1)
	if err := c.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	// Directory must contain the snapshot ONLY — no leftover .tmp file.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "snap.gob" {
		t.Errorf("directory contents = %v, want only snap.gob", entries)
	}
}

func TestLoadDetectsCRCCorruption(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(4))
	defer src.Close()
	_ = src.Set("k", 1)
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	// Flip the trailing CRC bytes specifically so the rest of the
	// snapshot still parses cleanly and the CRC check is what
	// triggers ErrSnapshotCorrupt.
	corrupted := buf.Bytes()
	if len(corrupted) < 8 {
		t.Fatal("snapshot too small for the test")
	}
	corrupted[len(corrupted)-1] ^= 0xff

	dst, _ := New[string, int](WithMaxEntries(4))
	defer dst.Close()
	_, err := dst.Load(bytes.NewReader(corrupted))
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Errorf("expected ErrSnapshotCorrupt, got %v", err)
	}
}

func TestLoadDetectsBadMagic(t *testing.T) {
	junk := []byte("XXXX\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_, err := c.Load(bytes.NewReader(junk))
	if !errors.Is(err, ErrSnapshotIncompatible) {
		t.Errorf("expected ErrSnapshotIncompatible, got %v", err)
	}
}

func TestLoadDetectsBadVersion(t *testing.T) {
	// "RTNI" + version=99
	bad := append([]byte{}, snapshotMagic...)
	bad = append(bad, 99) // unsupported version
	c, _ := New[string, int](WithMaxEntries(4))
	defer c.Close()
	_, err := c.Load(bytes.NewReader(bad))
	if !errors.Is(err, ErrSnapshotIncompatible) {
		t.Errorf("expected ErrSnapshotIncompatible, got %v", err)
	}
}

func TestLoadDetectsCodecMismatch(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(4)) // gob codec
	defer src.Close()
	_ = src.Set("k", 1)
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}

	dst, _ := New[string, int](WithMaxEntries(4), WithCodec(JSONCodec{}))
	defer dst.Close()
	_, err := dst.Load(bytes.NewReader(buf.Bytes()))
	if !errors.Is(err, ErrSnapshotIncompatible) {
		t.Errorf("expected ErrSnapshotIncompatible on codec mismatch, got %v", err)
	}
}

func TestLoadSkipsExpiredEntries(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	src, _ := New[string, int](WithMaxEntries(8), WithClock(clk), WithJanitorInterval(time.Hour))
	defer src.Close()
	_ = src.SetWithTTL("alive", 1, time.Hour)
	_ = src.SetWithTTL("dead", 2, time.Second)

	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}

	clk.Advance(2 * time.Second) // dead's TTL has elapsed
	dst, _ := New[string, int](WithMaxEntries(8), WithClock(clk), WithJanitorInterval(time.Hour))
	defer dst.Close()
	n, err := dst.Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || !dst.Has("alive") || dst.Has("dead") {
		t.Errorf("expected only 'alive' to load; got count=%d alive=%v dead=%v",
			n, dst.Has("alive"), dst.Has("dead"))
	}
}

func TestLoadResetsBeforeReading(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("preexisting", 99)

	src, _ := New[string, int](WithMaxEntries(8))
	defer src.Close()
	_ = src.Set("loaded", 1)
	var buf bytes.Buffer
	_ = src.Save(&buf)

	if _, err := c.Load(&buf); err != nil {
		t.Fatal(err)
	}
	if c.Has("preexisting") {
		t.Error("Load should Reset before populating")
	}
	if !c.Has("loaded") {
		t.Error("Load should populate snapshotted keys")
	}
}

func TestMergePreservesUntouched(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("preexisting", 99)
	_ = c.Set("overwrite-me", 1)

	src, _ := New[string, int](WithMaxEntries(8))
	defer src.Close()
	_ = src.Set("overwrite-me", 2)
	_ = src.Set("new", 3)
	var buf bytes.Buffer
	_ = src.Save(&buf)

	if _, err := c.Merge(&buf); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Get("preexisting"); v != 99 {
		t.Errorf("Merge dropped preexisting key; got %d", v)
	}
	if v, _ := c.Get("overwrite-me"); v != 2 {
		t.Errorf("Merge should overwrite; got %d", v)
	}
	if v, _ := c.Get("new"); v != 3 {
		t.Errorf("Merge should insert new; got %d", v)
	}
}

func TestSnapshotPreservesTags(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(4))
	defer src.Close()
	_ = src.SetWithTags("k", 1, "user-42", "team-a")
	var buf bytes.Buffer
	_ = src.Save(&buf)

	dst, _ := New[string, int](WithMaxEntries(4))
	defer dst.Close()
	if _, err := dst.Load(&buf); err != nil {
		t.Fatal(err)
	}
	got := dst.Tags("k")
	if len(got) != 2 {
		t.Errorf("loaded tags = %v, want 2", got)
	}
	// Tag index should also work end-to-end.
	if n := dst.InvalidateTag("user-42"); n != 1 {
		t.Errorf("InvalidateTag after Load = %d, want 1", n)
	}
}

func TestSnapshotMetadataRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.gob")
	src, _ := New[string, int](
		WithMaxEntries(4),
		WithSnapshotMetadata(map[string]string{
			"app_version": "1.4.2",
			"git_sha":     "abc123",
		}),
	)
	defer src.Close()
	_ = src.Set("k", 1)
	if err := src.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	info, err := InspectSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Metadata["app_version"] != "1.4.2" {
		t.Errorf("Metadata[app_version] = %q, want 1.4.2", info.Metadata["app_version"])
	}
	if info.Metadata["git_sha"] != "abc123" {
		t.Errorf("Metadata[git_sha] = %q, want abc123", info.Metadata["git_sha"])
	}
}

func TestInspectSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.gob")
	src, _ := New[string, int](WithMaxEntries(4), WithName("test-cache"))
	defer src.Close()
	_ = src.Set("a", 1)
	_ = src.Set("b", 2)
	if err := src.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	info, err := InspectSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Version != snapshotVersion {
		t.Errorf("info.Version = %d, want %d", info.Version, snapshotVersion)
	}
	if info.Codec != "gob" {
		t.Errorf("info.Codec = %q, want %q", info.Codec, "gob")
	}
	if info.Name != "test-cache" {
		t.Errorf("info.Name = %q, want %q", info.Name, "test-cache")
	}
	if info.Count != 2 {
		t.Errorf("info.Count = %d, want 2", info.Count)
	}
	if info.SizeBytes <= 0 {
		t.Errorf("info.SizeBytes = %d, want > 0", info.SizeBytes)
	}
}

func TestInspectSnapshotMissingFile(t *testing.T) {
	_, err := InspectSnapshot(filepath.Join(t.TempDir(), "no-such-file"))
	if err == nil {
		t.Error("InspectSnapshot on missing file should error")
	}
}

func TestSaveOnClosedReturnsErrClosed(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	_ = c.Close()
	if err := c.Save(&bytes.Buffer{}); !errors.Is(err, ErrClosed) {
		t.Errorf("Save on closed = %v, want ErrClosed", err)
	}
}

func TestLoadOnClosedReturnsErrClosed(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(4))
	_ = c.Close()
	_, err := c.Load(bytes.NewReader([]byte{}))
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Load on closed = %v, want ErrClosed", err)
	}
}

func TestEmptyCacheRoundTrip(t *testing.T) {
	src, _ := New[string, int](WithMaxEntries(4))
	defer src.Close()
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := New[string, int](WithMaxEntries(4))
	defer dst.Close()
	n, err := dst.Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 || dst.Len() != 0 {
		t.Errorf("empty round-trip n=%d Len=%d", n, dst.Len())
	}
}

func TestWithMaxSnapshotBytesRejectsOversized(t *testing.T) {
	src, _ := New[string, []byte](
		WithMaxBytes(1<<20),
		WithWeigher[[]byte](BytesWeigher()),
	)
	defer src.Close()
	// 1 KiB value
	_ = src.Set("k", make([]byte, 1024))
	var buf bytes.Buffer
	if err := src.Save(&buf); err != nil {
		t.Fatal(err)
	}

	dst, _ := New[string, []byte](
		WithMaxBytes(1<<20),
		WithWeigher[[]byte](BytesWeigher()),
		WithMaxSnapshotBytes(64), // far below the snapshot's actual size
	)
	defer dst.Close()
	_, err := dst.Load(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Error("expected error from undersized MaxSnapshotBytes; got nil")
	}
}

func TestAutoLoadMissingFileIsNoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent.gob")
	c, err := New[string, int](
		WithMaxEntries(4),
		WithAutoLoad(path),
	)
	if err != nil {
		t.Fatalf("New with non-existent auto-load path should not error: %v", err)
	}
	defer c.Close()
	if c.Len() != 0 {
		t.Errorf("auto-load with missing file should leave cache empty; Len=%d", c.Len())
	}
}

func TestAutoLoadPopulates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.gob")

	// First cache: write a snapshot.
	src, _ := New[string, int](WithMaxEntries(4))
	_ = src.Set("k", 42)
	if err := src.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	src.Close()

	// Second cache with WithAutoLoad reads it during New.
	c, err := New[string, int](
		WithMaxEntries(4),
		WithAutoLoad(path),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	if v, _ := c.Get("k"); v != 42 {
		t.Errorf("auto-load: Get(k) = %d, want 42", v)
	}
}

func TestAutoLoadCorruptedFailsByDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.gob")
	if err := os.WriteFile(path, []byte("not a snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := New[string, int](
		WithMaxEntries(4),
		WithAutoLoad(path),
	)
	if err == nil {
		t.Error("New with corrupt auto-load should error by default")
	}
}

func TestAutoLoadCorruptedIgnoredWhenOptedIn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.gob")
	if err := os.WriteFile(path, []byte("not a snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := New[string, int](
		WithMaxEntries(4),
		WithAutoLoad(path),
		WithAutoLoadIgnoreErrors(true),
	)
	if err != nil {
		t.Errorf("New with WithAutoLoadIgnoreErrors should succeed; got %v", err)
	}
	if c != nil {
		defer c.Close()
		if c.Len() != 0 {
			t.Errorf("expected empty cache on ignored auto-load failure; Len=%d", c.Len())
		}
	}
}

func TestCloseWritesFinalAutoSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "final.gob")
	c, _ := New[string, int](
		WithMaxEntries(4),
		WithAutoSave(path, 0), // disable periodic; only final-save on Close
	)
	_ = c.Set("k", 99)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Close should have written %q: %v", path, err)
	}
	// Verify by loading.
	c2, _ := New[string, int](WithMaxEntries(4))
	defer c2.Close()
	_, err := c2.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := c2.Get("k"); v != 99 {
		t.Errorf("final auto-save round trip: Get(k) = %d, want 99", v)
	}
}
