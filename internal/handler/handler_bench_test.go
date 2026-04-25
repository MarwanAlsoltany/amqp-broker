package handler

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// Package-level sinks prevent the compiler from eliminating benchmark calls
// whose return values are otherwise unused (dead-code elimination).
var (
	benchSinkAction Action
	benchSinkError  error
)

// passCountMiddleware returns a middleware that increments a heap-allocated atomic
// counter on each invocation.  The counter escapes to the heap, preventing the
// compiler from collapsing chains of these middlewares into a single call.
func passCountMiddleware() Middleware {
	n := new(atomic.Int64)
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			n.Add(1)
			return next(ctx, msg)
		}
	}
}

func BenchmarkWrap(b *testing.B) {
	mw := passCountMiddleware()

	msg := message.New([]byte("test"))

	b.Run("NoMiddleware", func(b *testing.B) {
		b.ReportAllocs()
		h := ActionHandler(ActionAck)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("1Middleware", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(ActionHandler(ActionAck), mw)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("5Middleware", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(ActionHandler(ActionAck), mw, mw, mw, mw, mw)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("10Middleware", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(ActionHandler(ActionAck), mw, mw, mw, mw, mw, mw, mw, mw, mw, mw)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("15Middleware", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(ActionHandler(ActionAck),
			mw, mw, mw, mw, mw,
			mw, mw, mw, mw, mw,
			mw, mw, mw, mw, mw,
		)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(ActionHandler(ActionAck), mw, mw, mw, mw, mw)
		ctx := b.Context()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkAction, benchSinkError = h(ctx, &msg)
			}
		})
	})
}
