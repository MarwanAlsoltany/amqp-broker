package handler

import (
	"context"
	"iter"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// batchHandlerAck is a BatchHandler that immediately acks all messages.
func batchHandlerAck(_ context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
	for range msgs {
		// drain ...
	}
	return ActionAck, nil
}

func BenchmarkBatchMiddlewareSync(b *testing.B) {
	base := ActionHandler(ActionNoAction)

	// synchronous batch requires all Size goroutines to be simultaneously blocked
	// inside the middleware before the batch flushes. RunParallel cannot guarantee
	// that invariant at benchmark teardown: when pb.Next() starts returning false
	// some goroutines exit, leaving the remainder blocked forever with no timeout;
	// size-varying sub-benchmarks are therefore omitted
	//
	// what can be benchmarked:
	// - parallelism: concurrent callers with a short flush timeout so individual
	//                calls return promptly even when the batch isn't full
	// - flush timeout: sequential baseline; measures timeout-driven flush overhead

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(base, BatchMiddleware(b.Context(), batchHandlerAck, &BatchConfig{
			Size:         10,
			FlushTimeout: time.Millisecond,
		}))
		msg := message.New([]byte("test"))
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkAction, benchSinkError = h(b.Context(), &msg)
			}
		})
	})

	b.Run("TimeoutFlush", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(base, BatchMiddleware(b.Context(), batchHandlerAck, &BatchConfig{
			Size:         1000,             // large batch size, won't fill
			FlushTimeout: time.Microsecond, // fires almost immediately
		}))
		msg := message.New([]byte("test"))
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(b.Context(), &msg)
		}
	})
}

func BenchmarkBatchMiddlewareAsync(b *testing.B) {
	base := ActionHandler(ActionNoAction)

	runAsync := func(b *testing.B, size int) {
		b.ReportAllocs()
		h := Wrap(base, BatchMiddleware(b.Context(), batchHandlerAck, &BatchConfig{
			Async:        true,
			Size:         size,
			BufferSize:   size * 10,
			FlushTimeout: time.Hour,
		}))
		msg := message.New([]byte("test"))
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(b.Context(), &msg)
		}
	}

	b.Run("Size10", func(b *testing.B) { runAsync(b, 10) })
	b.Run("Size50", func(b *testing.B) { runAsync(b, 50) })
	b.Run("Size100", func(b *testing.B) { runAsync(b, 100) })

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(base, BatchMiddleware(b.Context(), batchHandlerAck, &BatchConfig{
			Async:        true,
			Size:         10,
			BufferSize:   10000,
			FlushTimeout: time.Hour,
		}))
		msg := message.New([]byte("test"))
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkAction, benchSinkError = h(b.Context(), &msg)
			}
		})
	})

	b.Run("TimeoutFlush", func(b *testing.B) {
		b.ReportAllocs()
		h := Wrap(base, BatchMiddleware(b.Context(), batchHandlerAck, &BatchConfig{
			Async:        true,
			Size:         1000,
			BufferSize:   10000,
			FlushTimeout: time.Microsecond,
		}))
		msg := message.New([]byte("test"))
		b.ResetTimer()
		for b.Loop() {
			benchSinkAction, benchSinkError = h(b.Context(), &msg)
		}
	})
}
