// Package handler provides the [Handler] and [Middleware] types, [Action] constants,
// and a rich set of middleware for consuming AMQP messages.
//
// This package is internal to the broker module and not intended for direct use.
// All types and functionality are exposed through top-level broker package re-exports.
//
// # Handler
//
// [Handler] is the function type called for each consumed message. It returns an
// [Action] to control message acknowledgment:
//
//	handler := func(ctx context.Context, msg *message.Message) (Action, error) {
//	    // process the message
//	    return ActionAck, nil
//	}
//
// [ActionHandler] creates a handler that returns a fixed action, useful for testing
// or as the base handler for terminal middleware like [BatchMiddleware].
//
// # Middleware
//
// [Middleware] wraps handlers to add cross-cutting concerns. Use [Wrap]
// to compose multiple middlewares:
//
//	handler := Wrap(
//	    myHandler,
//	    RecoveryMiddleware(&RecoveryMiddlewareConfig{}),
//	    LoggingMiddleware(&LoggingMiddlewareConfig{}),
//	    TimeoutMiddleware(&TimeoutMiddlewareConfig{Timeout: 30*time.Second}),
//	    RetryMiddleware(&RetryMiddlewareConfig{MaxAttempts: 3, MinBackoff: time.Second}),
//	)
//
// Available middleware:
//   - Recovery & Fallback: [RecoveryMiddleware], [FallbackMiddleware]
//   - Logging & Observability: [LoggingMiddleware], [MetricsMiddleware], [DebugMiddleware]
//   - Retry & Circuit Breaking: [RetryMiddleware], [CircuitBreakerMiddleware]
//   - Message Processing: [DeduplicationMiddleware], [ValidationMiddleware], [TransformMiddleware]
//   - Timeouts: [DeadlineMiddleware], [TimeoutMiddleware]
//   - Flow Control: [ConcurrencyMiddleware], [RateLimitMiddleware]
//   - Batch Processing: [BatchMiddleware]
//
// # Batch Processing
//
// [BatchMiddleware] accumulates messages into batches and processes them with a [BatchHandler].
// It supports both synchronous (blocking) and asynchronous (non-blocking) modes:
//
//	// Synchronous mode (blocking, at-least-once):
//	handler := Wrap(NoActionHandler,
//	    BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
//	            for _, msg := range msgs {
//	                process(msg)
//	            }
//	            return ActionAck, nil
//	        },
//	        &BatchConfig{Size: 50, FlushTimeout: 200 * time.Millisecond},
//	    ),
//	)
//
//	// Asynchronous mode (non-blocking, at-most-once by default):
//	handler := Wrap(NoActionHandler,
//	    BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
//	            for _, msg := range msgs {
//	                analytics.Record(msg)
//	            }
//	            return ActionAck, nil
//	        },
//	        &BatchConfig{Async: true, Size: 100, FlushTimeout: 500 * time.Millisecond},
//	    ),
//	)
//
// For async at-least-once guarantees, set EnqueueAction to [ActionNoAction] and ensure
// PrefetchCount >= BufferSize.
//
// # Error Handling
//
// All middleware errors are rooted at [ErrMiddleware], which wraps the base broker error.
// Use errors.Is for error classification:
//
//	if errors.Is(err, handler.ErrMiddleware) { ... }
package handler
