package memcache

import (
	"log/slog"
	"testing"
	"time"
)

// applyOpts builds a fresh defaultConfig and applies opts in order,
// returning the resulting config for inspection.
func applyOpts(opts ...Option) *config {
	c := defaultConfig()
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	return c
}

func TestDefaultConfig(t *testing.T) {
	c := defaultConfig()
	if c.policy != PolicyS3FIFO {
		t.Errorf("default policy = %v, want %v", c.policy, PolicyS3FIFO)
	}
	if c.shards < 1 {
		t.Errorf("default shards = %d, want ≥ 1", c.shards)
	}
	if !c.statsEnabled {
		t.Error("stats should default on")
	}
	if c.clock == nil {
		t.Error("default clock must not be nil")
	}
	if c.codec == nil {
		t.Error("default codec must not be nil")
	}
	if c.logger == nil {
		t.Error("default logger must not be nil")
	}
}

func TestWithMaxEntries(t *testing.T) {
	c := applyOpts(WithMaxEntries(123))
	if c.maxEntries != 123 {
		t.Errorf("maxEntries = %d, want 123", c.maxEntries)
	}
}

func TestWithMaxBytes(t *testing.T) {
	c := applyOpts(WithMaxBytes(4096))
	if c.maxBytes != 4096 {
		t.Errorf("maxBytes = %d, want 4096", c.maxBytes)
	}
}

func TestWithDefaultTTL(t *testing.T) {
	c := applyOpts(WithDefaultTTL(7 * time.Second))
	if c.defaultTTL != 7*time.Second {
		t.Errorf("defaultTTL = %v, want 7s", c.defaultTTL)
	}
}

func TestWithSlidingTTL(t *testing.T) {
	c := applyOpts(WithSlidingTTL(true))
	if !c.slidingTTL {
		t.Error("slidingTTL should be true")
	}
}

func TestWithTTLJitter(t *testing.T) {
	c := applyOpts(WithTTLJitter(50 * time.Millisecond))
	if c.ttlJitter != 50*time.Millisecond {
		t.Errorf("ttlJitter = %v, want 50ms", c.ttlJitter)
	}
}

func TestWithJanitorInterval(t *testing.T) {
	c := applyOpts(WithJanitorInterval(2 * time.Second))
	if c.janitorInterval != 2*time.Second {
		t.Errorf("janitorInterval = %v, want 2s", c.janitorInterval)
	}
}

func TestWithPolicy(t *testing.T) {
	c := applyOpts(WithPolicy(PolicyLFU))
	if c.policy != PolicyLFU {
		t.Errorf("policy = %v, want %v", c.policy, PolicyLFU)
	}
}

func TestWithShardsRoundsUpToPowerOfTwo(t *testing.T) {
	c := applyOpts(WithShards(7))
	if c.shards != 8 {
		t.Errorf("shards = %d, want 8 (next power of 2)", c.shards)
	}
}

func TestWithShardsMinimum(t *testing.T) {
	c := applyOpts(WithShards(0))
	if c.shards != 1 {
		t.Errorf("shards = %d, want 1 (clamped)", c.shards)
	}
	c = applyOpts(WithShards(-5))
	if c.shards != 1 {
		t.Errorf("shards (neg) = %d, want 1", c.shards)
	}
}

func TestWithMaxKeySize(t *testing.T) {
	c := applyOpts(WithMaxKeySize(64))
	if c.maxKeySize != 64 {
		t.Errorf("maxKeySize = %d, want 64", c.maxKeySize)
	}
}

func TestWithMaxValueWeight(t *testing.T) {
	c := applyOpts(WithMaxValueWeight(1024))
	if c.maxValueWeight != 1024 {
		t.Errorf("maxValueWeight = %d, want 1024", c.maxValueWeight)
	}
}

func TestWithName(t *testing.T) {
	c := applyOpts(WithName("snowflake"))
	if c.name != "snowflake" {
		t.Errorf("name = %q, want %q", c.name, "snowflake")
	}
}

func TestWithClockIgnoresNil(t *testing.T) {
	c := defaultConfig()
	original := c.clock
	WithClock(nil)(c)
	if c.clock != original {
		t.Error("WithClock(nil) should not overwrite default clock")
	}
}

func TestWithClockOverride(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	c := applyOpts(WithClock(clk))
	if c.clock != clk {
		t.Error("WithClock did not install custom clock")
	}
}

func TestWithCodecIgnoresNil(t *testing.T) {
	c := defaultConfig()
	original := c.codec
	WithCodec(nil)(c)
	if c.codec != original {
		t.Error("WithCodec(nil) should not overwrite default codec")
	}
}

func TestWithCodecOverride(t *testing.T) {
	c := applyOpts(WithCodec(JSONCodec{}))
	if _, ok := c.codec.(JSONCodec); !ok {
		t.Errorf("codec = %T, want JSONCodec", c.codec)
	}
}

func TestWithLoggerIgnoresNil(t *testing.T) {
	c := defaultConfig()
	original := c.logger
	WithLogger(nil)(c)
	if c.logger != original {
		t.Error("WithLogger(nil) should not overwrite default logger")
	}
}

func TestWithLoggerOverride(t *testing.T) {
	custom := slog.New(slog.NewTextHandler(nil, nil))
	c := applyOpts(WithLogger(custom))
	if c.logger != custom {
		t.Error("WithLogger did not install custom logger")
	}
}

func TestWithStatsEnabled(t *testing.T) {
	c := applyOpts(WithStatsEnabled(false))
	if c.statsEnabled {
		t.Error("WithStatsEnabled(false) should disable stats")
	}
}

func TestWithWeigher(t *testing.T) {
	c := applyOpts(WithWeigher(StringWeigher()))
	if c.weigher == nil {
		t.Error("WithWeigher should populate weigher field")
	}
}

func TestWithHasher(t *testing.T) {
	hasher := func(string) uint64 { return 42 }
	c := applyOpts(WithHasher(hasher))
	if c.hasher == nil {
		t.Error("WithHasher should populate hasher field")
	}
}

func TestWithLoaderIgnoresNil(t *testing.T) {
	c := defaultConfig()
	var nilLoader Loader[string, int]
	WithLoader(nilLoader)(c)
	if c.loader != nil {
		t.Error("WithLoader(nil) should be a no-op")
	}
}

func TestOptionsLastWriteWins(t *testing.T) {
	c := applyOpts(
		WithMaxEntries(100),
		WithMaxEntries(200),
	)
	if c.maxEntries != 200 {
		t.Errorf("expected last-write-wins (200); got %d", c.maxEntries)
	}
}

func TestNilOptionTolerated(t *testing.T) {
	// New must accept a nil Option without panicking.
	c, err := New[string, int](WithMaxEntries(8), nil, WithDefaultTTL(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
}

func TestNewRejectsNegativeMaxEntries(t *testing.T) {
	_, err := New[string, int](WithMaxEntries(-1))
	var ce *ConfigError
	if !asError(err, &ce) || ce.Field != "MaxEntries" {
		t.Errorf("expected ConfigError on MaxEntries; got %v", err)
	}
}

func TestNewRejectsNegativeMaxBytes(t *testing.T) {
	_, err := New[string, int](WithMaxBytes(-1))
	var ce *ConfigError
	if !asError(err, &ce) || ce.Field != "MaxBytes" {
		t.Errorf("expected ConfigError on MaxBytes; got %v", err)
	}
}

func TestNewRejectsTypeMismatchedWeigher(t *testing.T) {
	// Weigher[int] paired with V=string is a type mismatch.
	_, err := New[string, string](
		WithMaxBytes(1024),
		WithWeigher[int](func(int) int64 { return 1 }),
	)
	var ce *ConfigError
	if !asError(err, &ce) || ce.Field != "Weigher" {
		t.Errorf("expected ConfigError on Weigher mismatch; got %v", err)
	}
}

func TestNewRejectsTypeMismatchedHasher(t *testing.T) {
	_, err := New[string, int](
		WithMaxEntries(8),
		WithHasher(func(int) uint64 { return 0 }),
	)
	var ce *ConfigError
	if !asError(err, &ce) || ce.Field != "Hasher" {
		t.Errorf("expected ConfigError on Hasher mismatch; got %v", err)
	}
}

// asError mirrors errors.As without importing it solely for this
// helper at the top of the file (keeps the test file lean).
func asError[T error](err error, target *T) bool {
	if err == nil {
		return false
	}
	for {
		if t, ok := err.(T); ok {
			*target = t
			return true
		}
		type wrapper interface{ Unwrap() error }
		w, ok := err.(wrapper)
		if !ok {
			return false
		}
		err = w.Unwrap()
		if err == nil {
			return false
		}
	}
}
