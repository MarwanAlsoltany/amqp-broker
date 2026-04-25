package handler

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// seenCache is a simple map-backed cache safe for single-goroutine use in benchmarks.
type seenCache struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newSeenCache() *seenCache {
	return &seenCache{m: make(map[string]struct{})}
}

func (c *seenCache) Seen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.m[id]; ok {
		return false // duplicate
	}
	c.m[id] = struct{}{}
	return true // first time
}

func BenchmarkDeduplicationMiddleware(b *testing.B) {
	base := ActionHandler(ActionAck)

	b.Run("Miss", func(b *testing.B) {
		b.ReportAllocs()
		cache := newSeenCache()
		h := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{Cache: cache})(base)
		ctx := b.Context()
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			msg := message.New(nil)
			msg.MessageID = string(rune('a'+i%26)) + string(rune('0'+i/26%10)) // unique IDs
			_, _ = h(ctx, &msg)
		}
	})

	b.Run("Hit", func(b *testing.B) {
		b.ReportAllocs()
		cache := newSeenCache()
		h := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{Cache: cache})(base)
		ctx := b.Context()
		// pre-seed so every call is a duplicate
		msg := message.New(nil)
		msg.MessageID = "dup-id"
		cache.Seen("dup-id") // mark as seen
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}

func BenchmarkValidationMiddleware(b *testing.B) {
	base := ActionHandler(ActionAck)
	msg := message.New([]byte(`{"key":"value"}`))
	msg.ContentType = "application/json"

	b.Run("Pass", func(b *testing.B) {
		b.ReportAllocs()
		h := ValidationMiddleware(&ValidationMiddlewareConfig{
			Validate: func(m *message.Message) error {
				if m.ContentType == "" {
					return errors.New("missing content type")
				}
				return nil
			},
		})(base)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("Fail", func(b *testing.B) {
		b.ReportAllocs()
		validationErr := errors.New("invalid")
		h := ValidationMiddleware(&ValidationMiddlewareConfig{
			Validate: func(_ *message.Message) error { return validationErr },
		})(base)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}

func BenchmarkTransformMiddleware(b *testing.B) {
	base := ActionHandler(ActionAck)
	smallBody := bytes.Repeat([]byte("x"), 256)
	largeBody := bytes.Repeat([]byte("x"), 64*1024)

	b.Run("SmallBody", func(b *testing.B) {
		b.ReportAllocs()
		h := TransformMiddleware(&TransformMiddlewareConfig{
			Transform: func(_ context.Context, body []byte) ([]byte, error) {
				out := make([]byte, len(body))
				copy(out, body)
				return out, nil
			},
		})(base)
		ctx := b.Context()
		msg := message.New(smallBody)
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("LargeBody", func(b *testing.B) {
		b.ReportAllocs()
		h := TransformMiddleware(&TransformMiddlewareConfig{
			Transform: func(_ context.Context, body []byte) ([]byte, error) {
				out := make([]byte, len(body))
				copy(out, body)
				return out, nil
			},
		})(base)
		ctx := b.Context()
		msg := message.New(largeBody)
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}

func BenchmarkDeadlineMiddleware(b *testing.B) {
	base := ActionHandler(ActionAck)

	b.Run("NotExpired", func(b *testing.B) {
		b.ReportAllocs()
		h := DeadlineMiddleware(nil)(base)
		ctx := b.Context()
		future := time.Now().Add(time.Hour)
		msg := message.New(nil)
		msg.Headers = message.Arguments{"x-deadline": future}
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("Expired", func(b *testing.B) {
		b.ReportAllocs()
		h := DeadlineMiddleware(nil)(base)
		ctx := b.Context()
		past := time.Now().Add(-time.Hour)
		msg := message.New(nil)
		msg.Headers = message.Arguments{"x-deadline": past}
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("NoHeader", func(b *testing.B) {
		b.ReportAllocs()
		h := DeadlineMiddleware(nil)(base)
		ctx := b.Context()
		msg := message.New(nil)
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}

func BenchmarkTimeoutMiddleware(b *testing.B) {
	// NOTE: FastHandler > SlowHandler in ns/op is expected and not a bug:
	// FastHandler (Timeout: 1s) pays the full goroutine-to-channel round-trip cost:
	// the handler runs to completion in the spawned goroutine and the caller blocks
	// on resultCh until the goroutine sends; SlowHandler (Timeout: 1ns) skips that
	// rendezvous entirely, the timeout fires before the select even evaluates
	// resultCh, so the caller returns immediately on ctx.Done() while the goroutine
	// keeps sleeping in the background, the goroutine is load-bearing: without it a
	// blocking handler could not be interrupted by a timeout at all

	b.Run("FastHandler", func(b *testing.B) {
		b.ReportAllocs()
		h := TimeoutMiddleware(&TimeoutMiddlewareConfig{Timeout: time.Second})(ActionHandler(ActionAck))
		ctx := b.Context()
		msg := message.New(nil)
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("SlowHandler", func(b *testing.B) {
		b.ReportAllocs()
		// timeout shorter than handler sleep -> timeout fires
		h := TimeoutMiddleware(&TimeoutMiddlewareConfig{Timeout: time.Nanosecond})(
			testSleepHandler(ActionAck, time.Millisecond),
		)
		ctx := b.Context()
		msg := message.New(nil)
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}
