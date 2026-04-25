package handler

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

func BenchmarkRecoveryMiddleware(b *testing.B) {
	// discard logger to measure middleware overhead, not I/O
	logger := slog.New(slog.DiscardHandler)

	msg := message.New([]byte("test"))

	b.Run("NoPanic", func(b *testing.B) {
		b.ReportAllocs()
		cfg := &RecoveryMiddlewareConfig{Logger: logger}
		h := RecoveryMiddleware(cfg)(ActionHandler(ActionAck))
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("Panic", func(b *testing.B) {
		b.ReportAllocs()
		panicHandler := Handler(func(_ context.Context, _ *message.Message) (Action, error) {
			panic("bench panic")
		})
		cfg := &RecoveryMiddlewareConfig{Logger: logger}
		h := RecoveryMiddleware(cfg)(panicHandler)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}

func BenchmarkFallbackMiddleware(b *testing.B) {
	msg := message.New([]byte("test"))
	fallback := ActionHandler(ActionNackDiscard)

	b.Run("NoFallback", func(b *testing.B) {
		b.ReportAllocs()
		// handler succeeds -> fallback never called
		h := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
		})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("Fallback", func(b *testing.B) {
		b.ReportAllocs()
		// handler fails -> fallback invoked every time
		h := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
		})(testErrorHandler(ActionNackRequeue, errors.New("primary failed")))
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}
