package handler

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// RecoveryMiddlewareConfig configures panic recovery for handlers.
// Catches panics in handlers and converts them to errors.
type RecoveryMiddlewareConfig struct {
	// Logger to use for panic reports. If nil, uses slog.Default().
	Logger *slog.Logger
	// Level is the log level for panic reports (default: [slog.LevelInfo]).
	// Pass an explicit value (e.g. slog.LevelError) to override.
	Level slog.Level
	// Action determines the handler action when a panic is recovered (default: [ActionNackRequeue]).
	// Common alternatives: [ActionNackDiscard] (dead-letter), [ActionNoAction] (manual handling).
	Action Action
	// OnPanic is called after logging but before returning, allowing custom panic handling.
	// If nil, only logging and action return occurs.
	OnPanic func(ctx context.Context, msg *message.Message, recovered any)
}

// RecoveryMiddleware creates a middleware that recovers from panics in handlers.
// It is a post-handler middleware: intercepts panics after the handler returns (via defer).
// It catches panics, logs them, and returns an error instead of crashing.
//
// The recovered panic is converted to an error and the handler returns
// [ActionNackRequeue] by default.
//
// Example:
//
//	mw := RecoveryMiddleware(&RecoveryMiddlewareConfig{
//		OnPanic: func(ctx context.Context, msg *message.Message, recovered any) {
//			log.Printf("Handler panicked: %v", recovered)
//		},
//	})
func RecoveryMiddleware(cfg *RecoveryMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &RecoveryMiddlewareConfig{}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	panicAction := cfg.Action
	if panicAction == 0 {
		// default: requeue for retry (panic might be transient)
		panicAction = ActionNackRequeue
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (action Action, err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Log(ctx, cfg.Level, "handler.panic",
						slog.String(logFields["id"], msg.MessageID),
						slog.Any(logFields["panic"], r),
					)

					if cfg.OnPanic != nil {
						cfg.OnPanic(ctx, msg, r)
					}

					action = panicAction
					err = fmt.Errorf("%w: recovered panic: %v", ErrMiddleware, r)
				}
			}()
			return next(ctx, msg)
		}
	}
}

// FallbackMiddlewareConfig configures fallback handler for failures.
// When the primary handler fails, the fallback handler is invoked.
type FallbackMiddlewareConfig struct {
	// Fallback is the handler called when ShouldFallback returns true.
	// If nil, fallback is disabled (messages pass through unchanged).
	Fallback Handler
	// ShouldFallback determines whether to call the fallback handler.
	// If nil, defaults to triggering on any error (err != nil).
	// Common alternatives: trigger only on specific error types, specific actions.
	ShouldFallback func(error, Action) bool
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
//		Fallback: func(ctx context.Context, msg *message.Message) (Action, error) {
//			// send to dead-letter queue or log the error
//			log.Printf("Primary handler failed, message: %s", string(msg.Body))
//			return ActionNackDiscard, nil
//		},
//	})
func FallbackMiddleware(cfg *FallbackMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &FallbackMiddlewareConfig{}
	}

	if cfg.Fallback == nil {
		// no fallback: pass through unchanged
		return noopMiddleware()
	}

	shouldFallback := cfg.ShouldFallback
	if shouldFallback == nil {
		// default: fallback on any error
		shouldFallback = func(err error, _ Action) bool { return err != nil }
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			action, err := next(ctx, msg)

			if shouldFallback(err, action) {
				fbAction, fbErr := cfg.Fallback(ctx, msg)
				// always preserve original error context if both errors exist
				if err != nil && fbErr != nil {
					return fbAction, fmt.Errorf("%w (original: %w)", fbErr, err)
				}
				return fbAction, fbErr
			}

			return action, err
		}
	}
}
