package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// DeduplicationMiddlewareConfig configures duplicate message detection.
// Detected duplicates are acknowledged without calling the handler.
type DeduplicationMiddlewareConfig struct {
	// Cache tracks seen message IDs. Must be safe for concurrent use.
	// If nil, deduplication is disabled (messages pass through unchanged).
	Cache interface {
		// Seen returns true if the message ID has not been seen before, recording it for
		// future calls. Returns false if the ID was already recorded (duplicate).
		Seen(id string) bool
	}
	// Identify extracts the deduplication ID from a message.
	// If nil, uses msg.MessageID. Common alternatives: hash of body, custom header value.
	Identify func(*message.Message) string
	// Action determines the action when a duplicate is detected (default: [ActionAck]).
	// Common alternatives: [ActionNackDiscard] (dead-letter), [ActionNoAction] (manual handling).
	Action Action
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
//		Identify: func(msg *message.Message) string {
//			// use message ID for deduplication
//			// (or an other strategy, e.g. body hash)
//			return msg.MessageID
//		},
//	})
func DeduplicationMiddleware(cfg *DeduplicationMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &DeduplicationMiddlewareConfig{}
	}

	if cfg.Cache == nil {
		// no cache: deduplication disabled, pass through
		return noopMiddleware()
	}

	identify := cfg.Identify
	if identify == nil {
		// default: use message ID
		identify = func(msg *message.Message) string { return msg.MessageID }
	}

	dupAction := cfg.Action
	if dupAction == 0 {
		// default: silently acknowledge duplicates
		dupAction = ActionAck
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			id := identify(msg)
			if !cfg.Cache.Seen(id) {
				// duplicate detected
				return dupAction, nil
			}
			// first time seeing this message
			return next(ctx, msg)
		}
	}
}

// ValidationMiddlewareConfig configures message validation before handler execution.
// Invalid messages are rejected without calling the handler.
type ValidationMiddlewareConfig struct {
	// Validate is the validation function to apply to each message.
	// If nil, all messages pass through unchanged (no-op).
	Validate func(*message.Message) error
	// Action determines the handler action when validation fails (default: [ActionNackDiscard]).
	// Common alternatives: [ActionNackRequeue] (retry later), [ActionNoAction] (manual handling).
	Action Action
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
//		Validate: func(msg *message.Message) error {
//			if msg.ContentType != "application/json" {
//				return fmt.Errorf("invalid content type: %s", msg.ContentType)
//			}
//			return nil
//		},
//	})
func ValidationMiddleware(cfg *ValidationMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &ValidationMiddlewareConfig{}
	}

	if cfg.Validate == nil {
		// default: no validation, pass through unchanged
		return noopMiddleware()
	}

	errAction := cfg.Action
	if errAction == 0 {
		// default: dead-letter invalid messages
		errAction = ActionNackDiscard
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			if err := cfg.Validate(msg); err != nil {
				return errAction, fmt.Errorf("%w: validation failed: %w", ErrMiddleware, err)
			}
			return next(ctx, msg)
		}
	}
}

// TransformMiddlewareConfig configures message body transformation.
// Transforms messages before passing to the handler (e.g. decompression, decryption, decoding).
type TransformMiddlewareConfig struct {
	// Transform applies a transformation to the message body.
	// If nil, messages pass through unchanged (no-op).
	// Receives context for cancellation/tracing support.
	Transform func(context.Context, []byte) ([]byte, error)
	// Action determines the handler action when transformation fails (default: [ActionNackDiscard]).
	// Common alternatives: [ActionNackRequeue] (retry later), [ActionNoAction] (manual handling).
	Action Action
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
func TransformMiddleware(cfg *TransformMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &TransformMiddlewareConfig{}
	}

	if cfg.Transform == nil {
		// no transform: pass through unchanged
		return noopMiddleware()
	}

	errAction := cfg.Action
	if errAction == 0 {
		// default: dead-letter messages that can't be transformed
		errAction = ActionNackDiscard
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			transformed, err := cfg.Transform(ctx, msg.Body)
			if err != nil {
				return errAction, fmt.Errorf("%w: body transform failed: %w", ErrMiddleware, err)
			}
			msg.Body = transformed
			return next(ctx, msg)
		}
	}
}

// DeadlineMiddlewareConfig configures message deadline checking.
// Messages past their deadline are discarded without processing.
type DeadlineMiddlewareConfig struct {
	// Timestamp extracts the deadline from a message.
	// If nil, uses msg.Headers["x-deadline"] as time.Time.
	// Common alternatives: parse Unix timestamp, RFC3339 string, TTL from timestamp.
	Timestamp func(*message.Message) (time.Time, bool)
	// Action determines the action when deadline has passed (default: [ActionNackDiscard]).
	// Common alternatives: [ActionNackRequeue] (retry later), [ActionNoAction] (manual handling).
	Action Action
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
//		Timestamp: func(msg *message.Message) (time.Time, bool) {
//			// extract deadline from custom header
//			if val, ok := msg.Headers["x-deadline"].(time.Time); ok {
//				return val, true
//			}
//			return time.Time{}, false
//		},
//		Action: ActionNackDiscard,
//	})
func DeadlineMiddleware(cfg *DeadlineMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &DeadlineMiddlewareConfig{}
	}

	timestamp := cfg.Timestamp
	if timestamp == nil {
		// default: extract time.Time from x-deadline header
		timestamp = func(msg *message.Message) (time.Time, bool) {
			if raw, ok := msg.Headers["x-deadline"]; ok {
				if deadline, ok := raw.(time.Time); ok {
					return deadline, true
				}
			}
			return time.Time{}, false
		}
	}

	expAction := cfg.Action
	if expAction == 0 {
		// default: dead-letter expired messages
		expAction = ActionNackDiscard
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			if deadline, ok := timestamp(msg); ok {
				if time.Now().After(deadline) {
					return expAction, fmt.Errorf("%w: message expired at %v", ErrMiddleware, deadline)
				}
			}
			// no deadline or deadline in future
			return next(ctx, msg)
		}
	}
}

// TimeoutMiddlewareConfig configures per-message processing timeouts.
// Operations exceeding the timeout are cancelled via context.
type TimeoutMiddlewareConfig struct {
	// Timeout is the maximum duration for handler execution.
	// If <= 0, no timeout is applied (no-op).
	Timeout time.Duration
	// Action determines the handler action when timeout occurs (default: [ActionNackRequeue]).
	// Common alternatives: [ActionNackDiscard] (dead-letter), [ActionNoAction] (manual handling).
	Action Action
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
func TimeoutMiddleware(cfg *TimeoutMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &TimeoutMiddlewareConfig{}
	}

	if cfg.Timeout <= 0 {
		// no timeout: pass through unchanged
		return noopMiddleware()
	}

	timeoutAction := cfg.Action
	if timeoutAction == 0 {
		// default: requeue for retry (timeout might be transient)
		timeoutAction = ActionNackRequeue
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()

			type result struct {
				action Action
				err    error
			}

			resultCh := make(chan result, 1)
			go func() {
				action, err := next(ctx, msg)
				resultCh <- result{action, err}
			}()

			select {
			case result := <-resultCh:
				return result.action, result.err
			case <-ctx.Done():
				return timeoutAction, fmt.Errorf("%w: message processing timeout after %vms", ErrMiddleware, cfg.Timeout.Milliseconds())
			}
		}
	}
}
