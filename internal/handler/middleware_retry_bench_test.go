package handler

import (
	"errors"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

func BenchmarkRetryMiddleware(b *testing.B) {
	msg := message.New([]byte("test"))

	// NOTE: sub-benchmarks with actual retries are omitted because they measure
	// backoff sleep time (MinBackoff/MaxBackoff), not middleware implementation cost
	b.Run("NoRetry", func(b *testing.B) {
		b.ReportAllocs()
		// handler succeeds on first attempt
		h := RetryMiddleware(&RetryMiddlewareConfig{
			MaxAttempts: 3,
			MinBackoff:  time.Millisecond,
		})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})
}

func BenchmarkCircuitBreakerMiddleware(b *testing.B) {
	msg := message.New([]byte("test"))

	b.Run("Closed", func(b *testing.B) {
		b.ReportAllocs()
		h := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold: 100, // very high: won't trip during bench
		})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("Open", func(b *testing.B) {
		b.ReportAllocs()
		// trip the circuit before measuring
		cfg := &CircuitBreakerMiddlewareConfig{
			Threshold: 2,
			Cooldown:  time.Hour, // won't recover during bench
		}
		h := CircuitBreakerMiddleware(cfg)(testErrorHandler(ActionNackRequeue, errors.New("err")))
		ctx := b.Context()
		// trip it
		benchSinkAction, benchSinkError = h(ctx, &msg)
		benchSinkAction, benchSinkError = h(ctx, &msg)
		benchSinkAction, benchSinkError = h(ctx, &msg)
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("HalfOpen", func(b *testing.B) {
		b.ReportAllocs()
		// start in open state with a zero cooldown so it transitions to half-open immediately
		cfg := &CircuitBreakerMiddlewareConfig{
			Threshold:    2,
			Cooldown:     time.Nanosecond,
			MinSuccesses: 100, // won't close during bench
			MaxProbes:    1,
		}
		h := CircuitBreakerMiddleware(cfg)(ActionHandler(ActionAck))
		ctx := b.Context()
		// trip it
		fail := CircuitBreakerMiddleware(cfg)(testErrorHandler(ActionNackRequeue, errors.New("err")))
		_, _ = fail(ctx, &msg)
		_, _ = fail(ctx, &msg)
		_, _ = fail(ctx, &msg)
		time.Sleep(2 * time.Nanosecond) // allow cooldown -> half-open
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(ctx, &msg)
		}
	})

	b.Run("Contention", func(b *testing.B) {
		b.ReportAllocs()
		h := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold: 1000000,
		})(ActionHandler(ActionAck))
		ctx := b.Context()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkAction, benchSinkError = h(ctx, &msg)
			}
		})
	})
}
