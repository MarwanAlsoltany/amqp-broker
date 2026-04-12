package broker

import (
	"context"

	"github.com/MarwanAlsoltany/amqp-broker/internal/handler"
)

var (
	// ErrHandler is the root sentinel for all handler operations.
	// All handler-related errors wrap this error.
	ErrHandler = handler.ErrHandler

	// ErrMiddleware is the base error for handler middleware operations.
	// All errors returned by middlewares wrap this error for consistent error handling.
	ErrMiddleware = handler.ErrMiddleware
)

type (
	// Handler is the function type called for each consumed message.
	// It processes the message and returns an action to control acknowledgment.
	//
	// The context is cancelled when the consumer is stopped, allowing
	// handlers to gracefully abort long-running operations.
	//
	// Example:
	//
	//	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
	//		// process the message
	//		if err := process(msg.Body); err != nil {
	//			return HandlerActionNackRequeue, err // retry later
	//		}
	//		return HandlerActionAck, nil // success
	//	}
	Handler = handler.Handler

	// HandlerAction controls how a consumed message is acknowledged.
	HandlerAction = handler.Action

	// HandlerMiddleware wraps a [Handler] to add cross-cutting concerns.
	// Middlewares execute in order: outermost first, innermost last.
	//
	// Use [WrapHandler] to compose multiple middlewares:
	//
	//	handler := WrapHandler(myHandler,
	//		RecoveryMiddleware(...),  // outermost
	//		LoggingMiddleware(...),
	//		TimeoutMiddleware(...),   // innermost
	//	)
	HandlerMiddleware = handler.Middleware
)

const (
	// HandlerActionAck acknowledges the message, removing it from the queue.
	// Use this when the message was processed successfully.
	HandlerActionAck = handler.ActionAck

	// HandlerActionNackRequeue negatively acknowledges and requeues the message.
	// The message will be redelivered, allowing retry of transient failures.
	// Use this for temporary errors like network timeouts or busy resources.
	HandlerActionNackRequeue = handler.ActionNackRequeue

	// HandlerActionNackDiscard negatively acknowledges without requeuing.
	// The message is discarded (or sent to a dead-letter exchange if configured).
	// Use this for permanent errors like invalid message format.
	HandlerActionNackDiscard = handler.ActionNackDiscard

	// HandlerActionNoAction performs no acknowledgment.
	// The message remains unacknowledged until manually ack'd or the consumer closes.
	// Use this when the handler delegates acknowledgment to another component.
	HandlerActionNoAction = handler.ActionNoAction
)

// ActionHandler returns a Handler that always returns the specified action.
// Useful for creating simple handlers or testing.
//
// Example:
//
//	handler := ActionHandler(HandlerActionAck) // always acks
func ActionHandler(action HandlerAction) Handler {
	return handler.ActionHandler(action)
}

// WrapHandler wraps a handler with multiple middlewares.
// Middlewares are applied in the order provided: the first middleware
// in the list becomes the outermost wrapper.
//
// Execution order:
//  1. First middleware's pre-handler logic
//  2. Second middleware's pre-handler logic
//  3. ... (additional middlewares)
//  4. Innermost handler execution
//  5. ... (middleware post-handler logic in reverse)
//  6. Second middleware's post-handler logic
//  7. First middleware's post-handler logic
//
// Example:
//
//	handler := WrapHandler(
//		myHandler,
//		RecoveryMiddleware(&RecoveryMiddlewareConfig{}), // catches panics (outermost)
//		LoggingMiddleware(&LoggingMiddlewareConfig{}),   // logs execution
//		TimeoutMiddleware(&TimeoutMiddlewareConfig{      // enforces timeout (innermost)
//			Timeout: 30 * time.Second,
//		}),
//	)
func WrapHandler(h Handler, mw ...HandlerMiddleware) Handler {
	return handler.Wrap(h, mw...)
}

type (
	// BatchHandler processes a batch of messages delivered as a lazy indexed iterator.
	//
	// Two acknowledgment patterns are supported:
	//
	//  1. Uniform action: return [HandlerActionAck], [HandlerActionNackRequeue], or [HandlerActionNackDiscard].
	//     The consumer applies that action to every message in the batch.
	//     Do NOT call msg.Ack/Nack/Reject() on any message in this mode.
	//
	//  2. Per-message acking: call [Message.Ack], [Message.Nack], or
	//     [Message.Reject] on each visited message, then return [HandlerActionNoAction].
	//     The consumer will not touch acknowledgments.
	//     Messages skipped via an early break from the range loop are automatically
	//     handled according to [BatchConfig.ErrorAction].
	BatchHandler = handler.BatchHandler

	// BatchConfig configures [BatchMiddleware].
	BatchConfig = handler.BatchConfig
)

// BatchMiddleware accumulates messages into batches and processes them with a [BatchHandler].
// It is a terminal middleware: the next handler is never called. Use ActionHandler(HandlerActionNoAction) as the base handler.
//
// The ctx parameter controls the async background goroutine's lifetime (Async=true only).
// When cancelled, the goroutine drains its buffer and flushes a final batch before exiting.
// Pass the consumer's lifecycle context.
//
// Example (sync, blocking: transactional DB writes):
//
//	WrapHandler(ActionHandler(HandlerActionNoAction),
//	    BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *Message]) (HandlerAction, error) {
//	            batch := &pgx.Batch{}
//	            for _, msg := range msgs {
//	                batch.Queue("INSERT INTO events ...", msg.Body)
//	            }
//	            if err := pool.SendBatch(ctx, batch).Close(); err != nil {
//	                return HandlerActionNackRequeue, err
//	            }
//	            return HandlerActionAck, nil
//	        },
//	        // PrefetchCount must be >= BatchMiddlewareConfig.Size
//	        &BatchMiddlewareConfig{Size: 50, FlushTimeout: 200 * time.Millisecond},
//	    ),
//	),
//
// Example (async, at-most-once - fire-and-forget - non-durable pipeline):
//
//	WrapHandler(ActionHandler(HandlerActionNoAction),
//	    BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *Message]) (HandlerAction, error) {
//	            for _, msg := range msgs {
//	                analytics.Record(msg)
//	            }
//	            return HandlerActionAck, nil
//	        },
//	        &BatchMiddlewareConfig{Async: true, Size: 100, FlushTimeout: 500 * time.Millisecond},
//	    ),
//	),
//
// Example (async, at-least-once - durable pipeline):
//
//	WrapHandler(ActionHandler(HandlerActionNoAction),
//	    BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *Message]) (HandlerAction, error) {
//	            batch := &pgx.Batch{}
//	            for i, msg := range msgs {
//	                var row Row
//	                if err := json.Unmarshal(msg.Body, &row); err != nil {
//	                    return HandlerActionNackRequeue, fmt.Errorf("msg %d: %w", i, err)
//	                }
//	                batch.Queue("INSERT INTO events ...", row.Fields()...)
//	            }
//	            return HandlerActionAck, pool.SendBatch(ctx, batch).Close()
//	        },
//	        &BatchMiddlewareConfig{
//	            Async:         true,
//	            EnqueueAction: HandlerActionNoAction,
//	            ErrorAction:   HandlerActionNackRequeue,
//	            Size:          50, // BufferSize defaults to 10*Size=500; PrefetchCount must be >= BufferSize
//	            FlushTimeout:  200 * time.Millisecond,
//	            OnError: func(ctx context.Context, err error, count int) {
//	                log.Printf("batch of %d failed: %v", count, err)
//	            },
//	        },
//	    ),
//	),
func BatchMiddleware(ctx context.Context, bh BatchHandler, cfg *BatchConfig) HandlerMiddleware {
	return handler.BatchMiddleware(ctx, bh, cfg)
}

type (
	// RetryMiddlewareConfig configures automatic retry behavior for failed handlers.
	// Failed handlers are retried with exponential backoff up to MaxAttempts.
	RetryMiddlewareConfig = handler.RetryMiddlewareConfig

	// LoggingMiddlewareConfig configures structured logging of message lifecycle events.
	// Logs include message start, completion, errors, and custom fields.
	LoggingMiddlewareConfig = handler.LoggingMiddlewareConfig

	// MetricsMiddlewareConfig configures metrics collection for message processing.
	// Tracks processing duration, success/failure counts, and custom metrics.
	MetricsMiddlewareConfig = handler.MetricsMiddlewareConfig

	// RecoveryMiddlewareConfig configures panic recovery for handlers.
	// Catches panics in handlers and converts them to errors.
	RecoveryMiddlewareConfig = handler.RecoveryMiddlewareConfig

	// TimeoutMiddlewareConfig configures per-message processing timeouts.
	// Operations exceeding the timeout are cancelled via context.
	TimeoutMiddlewareConfig = handler.TimeoutMiddlewareConfig

	// RateLimitMiddlewareConfig configures rate limiting for message processing.
	// Limits the rate at which handlers are invoked using a token bucket algorithm.
	RateLimitMiddlewareConfig = handler.RateLimitMiddlewareConfig

	// ConcurrencyMiddlewareConfig configures concurrency limiting for handlers.
	// Limits the number of handlers executing concurrently using a semaphore.
	ConcurrencyMiddlewareConfig = handler.ConcurrencyMiddlewareConfig

	// CircuitBreakerMiddlewareConfig configures circuit breaker behavior.
	// Opens the circuit after consecutive failures, preventing handler execution.
	CircuitBreakerMiddlewareConfig = handler.CircuitBreakerMiddlewareConfig

	// ValidationMiddlewareConfig configures message validation before handler execution.
	// Invalid messages are rejected without calling the handler.
	ValidationMiddlewareConfig = handler.ValidationMiddlewareConfig

	// DeduplicationMiddlewareConfig configures duplicate message detection.
	// Detected duplicates are acknowledged without calling the handler.
	DeduplicationMiddlewareConfig = handler.DeduplicationMiddlewareConfig

	// DeadlineMiddlewareConfig configures message deadline checking.
	// Messages past their deadline are discarded without processing.
	DeadlineMiddlewareConfig = handler.DeadlineMiddlewareConfig

	// FallbackMiddlewareConfig configures fallback handler for failures.
	// When the primary handler fails, the fallback handler is invoked.
	FallbackMiddlewareConfig = handler.FallbackMiddlewareConfig

	// DebugMiddlewareConfig configures debug logging for message details.
	// Logs complete message structure including headers and body.
	DebugMiddlewareConfig = handler.DebugMiddlewareConfig

	// TransformMiddlewareConfig configures message body transformation.
	// Transforms messages before passing to the handler (e.g. decompression, decryption, decoding).
	TransformMiddlewareConfig = handler.TransformMiddlewareConfig
)

// LoggingMiddleware creates a middleware that logs message lifecycle events.
// It logs when a handler starts, succeeds, or fails, along with configurable fields.
//
// Default behavior:
//   - Uses slog.Default() for logging
//   - Logs at INFO level for start, success, and errors (zero value of [slog.Level])
//   - Includes message ID, action, and error (if any)
//
// Example:
//
//	mw := LoggingMiddleware(&LoggingMiddlewareConfig{
//		Logger:       myLogger,
//		StartLevel:   slog.LevelDebug,
//		SuccessLevel: slog.LevelInfo,
//		ErrorLevel:   slog.LevelError,
//	})
func LoggingMiddleware(cfg *LoggingMiddlewareConfig) HandlerMiddleware {
	return handler.LoggingMiddleware(cfg)
}

// MetricsMiddleware creates a middleware that tracks processing metrics.
// It is a post-handler middleware: measures duration and records metrics after the handler returns.
// It measures handler execution duration and invokes callbacks for custom metrics.
//
// Use the Record callback to export metrics to your monitoring system:
//
// Example:
//
//	mw := MetricsMiddleware(&MetricsMiddlewareConfig{
//		Record: func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration) {
//			metrics.RecordDuration(duration)
//			if err != nil {
//				metrics.IncrementErrors()
//			}
//		},
//	})
func MetricsMiddleware(cfg *MetricsMiddlewareConfig) HandlerMiddleware {
	return handler.MetricsMiddleware(cfg)
}

// RecoveryMiddleware creates a middleware that recovers from panics in handlers.
// It is a post-handler middleware: intercepts panics after the handler returns (via defer).
// It catches panics, logs them, and returns an error instead of crashing.
//
// The recovered panic is converted to an error and the handler returns
// [HandlerActionNackRequeue] by default.
//
// Example:
//
//	mw := RecoveryMiddleware(&RecoveryMiddlewareConfig{
//		OnPanic: func(ctx context.Context, msg *Message, recovered any) {
//			log.Printf("Handler panicked: %v", recovered)
//		},
//	})
func RecoveryMiddleware(cfg *RecoveryMiddlewareConfig) HandlerMiddleware {
	return handler.RecoveryMiddleware(cfg)
}

// TimeoutMiddleware creates a middleware that enforces per-message timeouts.
// It is a pre+post-handler middleware: sets a deadline context before calling the handler, monitors the outcome after.
// It wraps the handler context with a timeout, cancelling long-running operations.
//
// The timeout is enforced via context cancellation. Handlers should respect
// context.Done() to allow graceful termination.
//
// Example:
//
//	mw := TimeoutMiddleware(&TimeoutMiddlewareConfig{
//		Timeout: 30 * time.Second,
//	})
func TimeoutMiddleware(cfg *TimeoutMiddlewareConfig) HandlerMiddleware {
	return handler.TimeoutMiddleware(cfg)
}

// RetryMiddleware creates a middleware that retries failed handlers.
// It is a post-handler middleware: evaluates the handler result and retries on retriable errors.
// It implements exponential backoff between retry attempts.
//
// The middleware only retries if the handler returns [HandlerActionNackRequeue].
// Other actions (Ack, NackDiscard, NoAction) are returned immediately.
//
// Example:
//
//	mw := RetryMiddleware(&RetryMiddlewareConfig{
//		MaxAttempts: 3,
//		MinBackoff: 100 * time.Millisecond, // initial
//		MaxBackoff: 10 * time.Second,
//		ShouldRetry: func(err error) bool {
//			// only retry transient errors
//			return isTransientError(err)
//		},
//	})
func RetryMiddleware(cfg *RetryMiddlewareConfig) HandlerMiddleware {
	return handler.RetryMiddleware(cfg)
}

// RateLimitMiddleware creates a middleware that limits message processing rate.
// It is a pre-handler middleware: waits for a rate token before calling the handler.
// It uses a token bucket algorithm to control the rate of handler invocations.
//
// The context parameter is used to stop the rate limiter when the consumer is closed.
// The rate and burst parameters control throughput:
//   - Rate: tokens added per second (messages/second)
//   - Burst: maximum tokens in bucket (burst capacity)
//
// Example:
//
//	mw := RateLimitMiddleware(ctx, &RateLimitMiddlewareConfig{
//		Rate:  10.0,  // 10 messages per second
//		Burst: 20,    // Allow bursts up to 20 messages
//	})
func RateLimitMiddleware(ctx context.Context, cfg *RateLimitMiddlewareConfig) HandlerMiddleware {
	return handler.RateLimitMiddleware(ctx, cfg)
}

// ConcurrencyMiddleware creates a middleware that limits concurrent handlers.
// It is a pre+post-handler middleware: acquires a concurrency slot before calling the handler, releases it after.
// It uses a semaphore to restrict the number of handlers executing simultaneously.
//
// This is useful for:
//   - Limiting resource usage (DB connections, file handles, etc.)
//   - Controlling memory consumption during message processing
//   - Preventing thundering herd scenarios
//
// Example:
//
//	mw := ConcurrencyMiddleware(&ConcurrencyMiddlewareConfig{
//		Max: 10,  // Max 10 concurrent handlers
//	})
func ConcurrencyMiddleware(cfg *ConcurrencyMiddlewareConfig) HandlerMiddleware {
	return handler.ConcurrencyMiddleware(cfg)
}

// CircuitBreakerMiddleware creates a middleware that implements circuit breaker pattern.
// It is a pre+post-handler middleware: checks the circuit state before calling the handler, updates it after.
// It opens the circuit after consecutive failures, preventing handler execution.
//
// Circuit states:
//   - Closed: Normal operation, all messages are processed
//   - Open: Circuit is open, messages are rejected immediately
//   - Half-Open: Testing recovery, at most MaxProbes concurrent messages processed
//
// Circuit state transitions:
//   - Closed -> Open: after Threshold consecutive failures
//   - Open -> HalfOpen: after Cooldown period
//   - HalfOpen -> Closed: after MinSuccesses consecutive successes
//   - HalfOpen -> Open: on any failure
//
// In half-open state the circuit acts as a controlled gate: only MaxProbes
// messages are forwarded to the handler concurrently. All other arriving messages are
// rejected with the configured Action until a slot is freed. This prevents a burst of
// traffic from swamping a backend that is still recovering.
//
// Example:
//
//	mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
//		Threshold:    5,                // open after 5 consecutive failures
//		Cooldown:     30 * time.Second, // stay open for 30s
//		MinSuccesses: 2,                // require 2 successes to close
//		MaxProbes:    1,                // allow 1 probe at a time (default)
//	})
func CircuitBreakerMiddleware(cfg *CircuitBreakerMiddlewareConfig) HandlerMiddleware {
	return handler.CircuitBreakerMiddleware(cfg)
}

// ValidationMiddleware creates a middleware that validates messages before processing.
// It is a pre-handler middleware: validates the message before calling the handler.
// Invalid messages are rejected without invoking the handler.
//
// Use this to enforce message schema, required headers, or business rules
// before passing messages to the handler (i.e. fail fast on invalid messages).
//
// Example:
//
//	mw := ValidationMiddleware(&ValidationMiddlewareConfig{
//		Validate: func(msg *Message) error {
//			if msg.ContentType != "application/json" {
//				return fmt.Errorf("invalid content type: %s", msg.ContentType)
//			}
//			return nil
//		},
//	})
func ValidationMiddleware(cfg *ValidationMiddlewareConfig) HandlerMiddleware {
	return handler.ValidationMiddleware(cfg)
}

// DeduplicationMiddleware creates a middleware that filters duplicate messages.
// It is a pre-handler middleware: checks for duplicates before calling the handler.
// Duplicate messages are acknowledged without invoking the handler.
//
// Deduplication is based on message IDs tracked in the provided cache.
// The cache must be thread-safe for concurrent access.
//
// Example:
//
//	cache := NewInMemoryCache(100000, 5*time.Minute)
//	mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
//		Cache: cache,
//		Identify: func(msg *Message) string {
//			// use message ID for deduplication
//			// (or an other strategy, e.g. body hash)
//			return msg.MessageID
//		},
//	})
func DeduplicationMiddleware(cfg *DeduplicationMiddlewareConfig) HandlerMiddleware {
	return handler.DeduplicationMiddleware(cfg)
}

// DeadlineMiddleware creates a middleware that checks message deadlines.
// It is a pre-handler middleware: checks the message deadline before calling the handler.
// Messages past their deadline are discarded without processing.
//
// The deadline is extracted from message metadata (e.g., timestamp + TTL,
// or a custom header). Messages past their deadline are rejected.
//
// Example:
//
//	mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{
//		Timestamp: func(msg *Message) (time.Time, bool) {
//			// extract deadline from custom header
//			if val, ok := msg.Headers["x-deadline"].(time.Time); ok {
//				return val, true
//			}
//			return time.Time{}, false
//		},
//		Action: HandlerActionNackDiscard,
//	})
func DeadlineMiddleware(cfg *DeadlineMiddlewareConfig) HandlerMiddleware {
	return handler.DeadlineMiddleware(cfg)
}

// FallbackMiddleware creates a middleware that provides fallback handlers.
// It is a post-handler middleware: invokes the fallback handler after the primary handler fails.
// When the primary handler fails, the fallback handler is invoked.
//
// This is useful for:
//   - Dead-letter queue routing
//   - Alternative processing paths
//   - Error notification and logging
//
// Example:
//
//	mw := FallbackMiddleware(&FallbackMiddlewareConfig{
//		Fallback: func(ctx context.Context, msg *Message) (HandlerAction, error) {
//			// send to dead-letter queue or log the error
//			log.Printf("Primary handler failed, message: %s", string(msg.Body))
//			return HandlerActionNackDiscard, nil
//		},
//	})
func FallbackMiddleware(cfg *FallbackMiddlewareConfig) HandlerMiddleware {
	return handler.FallbackMiddleware(cfg)
}

// DebugMiddleware creates a middleware that logs detailed message information.
// It is a pre-handler middleware: logs message details before calling the handler.
// It logs the complete message structure including headers, properties, and body.
//
// Useful during development and troubleshooting to inspect message contents.
// The body is truncated to 1KB by default to prevent excessive log output.
//
// Example:
//
//	mw := DebugMiddleware(&DebugMiddlewareConfig{
//		Logger: myLogger,
//		Level:  slog.LevelDebug,
//		Fields: func(msg *Message) []any {
//			return []any{slog.String("id", msg.MessageID), slog.String("type", msg.ContentType)}
//			// or []any{"id", msg.MessageID, "type", msg.ContentType}
//		},
//	})
func DebugMiddleware(cfg *DebugMiddlewareConfig) HandlerMiddleware {
	return handler.DebugMiddleware(cfg)
}

// TransformMiddleware creates a middleware that transforms message bodies.
// It is a pre-handler middleware: rewrites the message body before calling the handler.
// It applies transformations before passing the message to the handler.
//
// Common use cases:
//   - Decompression (gzip, zlib)
//   - Decoding (base64, hex)
//   - Deserialization (JSON, protobuf, msgpack)
//   - Encryption/decryption
//
// Example:
//
//	mw := TransformMiddleware(&TransformMiddlewareConfig{
//		Transform: func(ctx context.Context, body []byte) ([]byte, error) {
//			// decompress gzip body
//			return gzipDecompress(body)
//		},
//	})
func TransformMiddleware(cfg *TransformMiddlewareConfig) HandlerMiddleware {
	return handler.TransformMiddleware(cfg)
}
