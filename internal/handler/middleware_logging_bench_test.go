package handler

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

func BenchmarkLoggingMiddleware(b *testing.B) {
	// discard logger so we measure middleware overhead, not I/O
	logger := slog.New(slog.DiscardHandler)

	base := ActionHandler(ActionAck)
	msg := message.New([]byte("test"))
	msg.MessageID = "bench-id"

	b.Run("NoFields", func(b *testing.B) {
		b.ReportAllocs()
		h := LoggingMiddleware(&LoggingMiddlewareConfig{Logger: logger})(base)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("WithFields", func(b *testing.B) {
		b.ReportAllocs()
		h := LoggingMiddleware(&LoggingMiddlewareConfig{
			Logger: logger,
			Fields: func(m *message.Message) []any {
				return []any{slog.String("tenant", "acme"), slog.String("region", "us-east")}
			},
		})(base)
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}

func BenchmarkMetricsMiddleware(b *testing.B) {
	// discard logger so we measure middleware overhead, not I/O
	logger := slog.New(slog.DiscardHandler)

	b.ReportAllocs()
	base := ActionHandler(ActionAck)
	h := MetricsMiddleware(&MetricsMiddlewareConfig{
		Logger: logger,
		Record: func(_ context.Context, _ *message.Message, _ Action, _ error, _ time.Duration) {},
	})(base)
	ctx := b.Context()
	msg := message.New([]byte("test"))
	b.ResetTimer()
	for b.Loop() {
		benchSinkAction, benchSinkError = h(ctx, &msg)
	}
}

func BenchmarkDebugMiddleware(b *testing.B) {
	// discard logger so we measure middleware overhead, not I/O
	logger := slog.New(slog.DiscardHandler)

	base := ActionHandler(ActionAck)

	smallBody := bytes.Repeat([]byte("x"), 512)
	largeBody := bytes.Repeat([]byte("x"), 64*1024)

	b.Run("SmallBody", func(b *testing.B) {
		b.ReportAllocs()
		h := DebugMiddleware(&DebugMiddlewareConfig{Logger: logger})(base)
		ctx := b.Context()
		msg := message.New(smallBody)
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("LargeBody", func(b *testing.B) {
		b.ReportAllocs()
		h := DebugMiddleware(&DebugMiddlewareConfig{Logger: logger})(base)
		ctx := b.Context()
		msg := message.New(largeBody)
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}
