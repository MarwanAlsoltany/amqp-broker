package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	t.Run("Consistency", func(t *testing.T) {
		h1 := hash(nil)
		h2 := hash(nil)
		h3 := hash(nil)
		assert.Equal(t, h1, h2)
		assert.Equal(t, h2, h3)
	})

	t.Run("Length", func(t *testing.T) {
		h := hash("test")
		assert.Len(t, h, 32) // MD5 hex is 32 chars
	})

	t.Run("SameValue", func(t *testing.T) {
		e1 := Exchange{Name: "test", Type: "direct"}
		e2 := Exchange{Name: "test", Type: "direct"}
		h1 := hash(e1)
		h2 := hash(e2)
		assert.Equal(t, h1, h2)
	})

	t.Run("DifferentValue", func(t *testing.T) {
		e1 := Exchange{Name: "test1", Type: "direct"}
		e2 := Exchange{Name: "test2", Type: "direct"}
		h1 := hash(e1)
		h2 := hash(e2)
		assert.NotEqual(t, h1, h2)
	})

	t.Run("DifferentTypes", func(t *testing.T) {
		e := Exchange{Name: "test", Type: "direct"}
		q := Queue{Name: "test"}
		b := Binding{Source: "test", Destination: "test"}

		he := hash(e)
		hq := hash(q)
		hb := hash(b)

		// all should produce valid hashes
		assert.Len(t, he, 32)
		assert.Len(t, hq, 32)
		assert.Len(t, hb, 32)

		// all should be different
		assert.NotEqual(t, he, hq)
		assert.NotEqual(t, hq, hb)
		assert.NotEqual(t, he, hb)
	})
}

func TestContextWithAnyCancel(t *testing.T) {
	t.Run("CancelByFunc", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		other, otherCancel := context.WithCancel(t.Context())
		defer otherCancel()
		ctx, cancel := contextWithAnyCancel(parent, other)
		cancel()
		select {
		case <-ctx.Done():
			// expected
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by cancel()")
		}
	})

	t.Run("CancelByParent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		other, otherCancel := context.WithCancel(t.Context())
		defer otherCancel()
		ctx, cancel := contextWithAnyCancel(parent, other)
		parentCancel()
		select {
		case <-ctx.Done():
			// expected
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by parent")
		}
		cancel()
	})

	t.Run("CancelByOther", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		other, otherCancel := context.WithCancel(t.Context())
		defer otherCancel()
		ctx, cancel := contextWithAnyCancel(parent, other)
		otherCancel()
		select {
		case <-ctx.Done():
			// expected
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by other")
		}
		cancel()
	})

	t.Run("WhenOtherNil", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		ctx, cancel := contextWithAnyCancel(parent, nil)
		parentCancel()
		select {
		case <-ctx.Done():
			// expected
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by parent (nil other)")
		}
		cancel()
	})
}
