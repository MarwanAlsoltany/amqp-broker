package handler

import (
	"runtime"
	"testing"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

func BenchmarkConcurrencyMiddleware(b *testing.B) {
	msg := message.New([]byte("test"))

	b.Run("UnderLimit", func(b *testing.B) {
		b.ReportAllocs()
		// max=100: slots always available for sequential bench
		h := ConcurrencyMiddleware(&ConcurrencyMiddlewareConfig{Max: 100})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("AtSaturation", func(b *testing.B) {
		b.ReportAllocs()
		// max=1: each call immediately acquires and releases, but next call must acquire again
		h := ConcurrencyMiddleware(&ConcurrencyMiddlewareConfig{Max: 1})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("ParallelUnderLimit", func(b *testing.B) {
		b.ReportAllocs()
		h := ConcurrencyMiddleware(&ConcurrencyMiddlewareConfig{Max: 1000})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkAction, benchSinkError = h(ctx, &msg)
			}
		})
	})

	b.Run("ParallelAtSaturation", func(b *testing.B) {
		b.ReportAllocs()
		// max matches GOMAXPROCS, all goroutines compete for the same slots
		h := ConcurrencyMiddleware(&ConcurrencyMiddlewareConfig{Max: runtime.GOMAXPROCS(0)})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkAction, benchSinkError = h(ctx, &msg)
			}
		})
	})
}

func BenchmarkRateLimitMiddleware(b *testing.B) {
	msg := message.New([]byte("test"))

	b.Run("UnderRate", func(b *testing.B) {
		b.ReportAllocs()
		// very high rate: token always available
		h := RateLimitMiddleware(b.Context(), &RateLimitMiddlewareConfig{RPS: 1000000})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		h := RateLimitMiddleware(b.Context(), &RateLimitMiddlewareConfig{RPS: 1000000})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkAction, benchSinkError = h(ctx, &msg)
			}
		})
	})
}
