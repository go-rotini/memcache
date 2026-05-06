package memcache

import (
	"context"
	"errors"
	"testing"
)

func TestGetCtxRespectsCancellation(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("k", 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, ok, err := c.GetCtx(ctx, "k")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("GetCtx err = %v, want context.Canceled", err)
	}
	if ok || v != 0 {
		t.Errorf("canceled GetCtx = (%d, %v), want (0, false)", v, ok)
	}
}

func TestGetCtxNormalDelegate(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("k", 42)
	v, ok, err := c.GetCtx(context.Background(), "k")
	if err != nil || !ok || v != 42 {
		t.Errorf("GetCtx = (%d, %v, %v); want (42, true, nil)", v, ok, err)
	}
}

func TestSetCtxRespectsCancellation(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.SetCtx(ctx, "k", 7); !errors.Is(err, context.Canceled) {
		t.Errorf("SetCtx err = %v, want context.Canceled", err)
	}
	if c.Has("k") {
		t.Error("canceled SetCtx must not store the value")
	}
}

func TestDeleteCtxRespectsCancellation(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	_ = c.Set("k", 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	removed, err := c.DeleteCtx(ctx, "k")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("DeleteCtx err = %v, want context.Canceled", err)
	}
	if removed {
		t.Error("canceled DeleteCtx must not remove the entry")
	}
	if !c.Has("k") {
		t.Error("entry should still be present after canceled DeleteCtx")
	}
}

func TestDeleteCtxNormalReturnsFalseOnMiss(t *testing.T) {
	c, _ := New[string, int](WithMaxEntries(8))
	defer c.Close()
	removed, err := c.DeleteCtx(context.Background(), "missing")
	if err != nil || removed {
		t.Errorf("DeleteCtx miss = (%v, %v), want (false, nil)", removed, err)
	}
}
