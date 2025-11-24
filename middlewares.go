package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Middleware wraps a Handler to provide additional functionality.
type Middleware func(Handler) Handler

// MiddlewareChain applies middlewares in order (first middleware wraps the last).
//
// Example usage:
//
//	handler := MiddlewareChain(
//	    myHandler,
//	    RecoveryMiddleware(logger),
//	    LoggingMiddleware(logger),
//	    MetricsMiddleware(logger),
//	    TimeoutMiddleware(30*time.Second),
//	    RetryMiddleware(RetryConfig{MaxAttempts: 3, Delay: time.Second * 3}),
//	)
func MiddlewareChain(handler Handler, middlewares ...Middleware) Handler {
	// Apply in reverse so first middleware is outermost
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// LoggingMiddleware logs message processing with structured logging.
func LoggingMiddleware(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (AckAction, error) {
			logger.Info("message received",
				"messageID", msg.MessageID,
			)

			action, err := next(ctx, msg)

			if err != nil {
				logger.Error("message processing failed",
					"messageID", msg.MessageID,
					"error", err,
					"action", action.String(),
				)
			} else {
				logger.Info("message processed",
					"messageID", msg.MessageID,
					"action", action.String(),
				)
			}

			return action, err
		}
	}
}

// MetricsMiddleware tracks processing duration.
func MetricsMiddleware(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (AckAction, error) {
			start := time.Now()
			action, err := next(ctx, msg)
			duration := time.Since(start)

			logger.Debug("message timing",
				"messageID", msg.MessageID,
				"durationMS", duration.Milliseconds(),
			)

			return action, err
		}
	}
}

// RecoveryMiddleware recovers from panics in handlers.
func RecoveryMiddleware(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (action AckAction, err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("panic recovered",
						"messageID", msg.MessageID,
						"panic", r,
					)
					action = AckActionNackRequeue
					err = fmt.Errorf("panic: %v", r)
				}
			}()
			return next(ctx, msg)
		}
	}
}

// TimeoutMiddleware adds a timeout to message processing.
func TimeoutMiddleware(timeout time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (AckAction, error) {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			type result struct {
				action AckAction
				err    error
			}

			done := make(chan result, 1)
			go func() {
				action, err := next(ctx, msg)
				done <- result{action, err}
			}()

			select {
			case res := <-done:
				return res.action, res.err
			case <-ctx.Done():
				return AckActionNackRequeue, fmt.Errorf("message processing timeout after %v", timeout)
			}
		}
	}
}

// RetryMiddlewareConfig configures retry behavior.
type RetryMiddlewareConfig struct {
	MaxAttempts int           // Maximum retry attempts (0 = no retries)
	Delay       time.Duration // Delay between retries
	Backoff     bool          // Use exponential backoff
	MaxDelay    time.Duration // Maximum delay for backoff
	// ErrOnly retries only on handler errors, not on nack actions
	ErrOnly bool
}

// RetryMiddleware retries failed message processing.
func RetryMiddleware(cfg RetryMiddlewareConfig) Middleware {
	if cfg.MaxAttempts < 0 {
		cfg.MaxAttempts = 0
	}
	if cfg.Delay <= 0 {
		cfg.Delay = time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 30 * time.Second
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (AckAction, error) {
			var action AckAction
			var err error
			delay := cfg.Delay

			for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
				// Check context before retry
				select {
				case <-ctx.Done():
					return AckActionNackRequeue, ctx.Err()
				default:
				}

				action, err = next(ctx, msg)

				// Success - return immediately
				if err == nil && (cfg.ErrOnly || action == AckActionAck) {
					return action, nil
				}

				// Last attempt - return as-is
				if attempt == cfg.MaxAttempts {
					break
				}

				// Wait before retry (with context cancellation check)
				select {
				case <-ctx.Done():
					return AckActionNackRequeue, ctx.Err()
				case <-time.After(delay):
				}

				// Exponential backoff
				if cfg.Backoff {
					delay *= 2
					if delay > cfg.MaxDelay {
						delay = cfg.MaxDelay
					}
				}
			}

			return action, err
		}
	}
}
