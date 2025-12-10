package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// HandlerAction specifies how a message should be acknowledged.
type HandlerAction int

func (a HandlerAction) String() string {
	switch a {
	case HandlerActionAck:
		return "ack"
	case HandlerActionNackRequeue:
		return "nack.requeue"
	case HandlerActionNackDiscard:
		return "nack.discard"
	case HandlerActionNoAction:
		return ""
	default:
		return "unknown"
	}
}

const (
	// HandlerActionAck acknowledges the message successfully.
	HandlerActionAck HandlerAction = iota
	// HandlerActionNackRequeue rejects the message and requeues it.
	HandlerActionNackRequeue
	// HandlerActionNackDiscard rejects the message without requeuing.
	HandlerActionNackDiscard
	// HandlerActionNoAction indicates the handler already called Ack/Nack/Reject manually.
	HandlerActionNoAction
)

// Handler is called for each consumed message.
// It returns an HandlerAction to control message acknowledgment.
type Handler func(ctx context.Context, msg *Message) (HandlerAction, error)

// HandleMiddleware wraps a Handler to provide additional functionality.
type HandleMiddleware func(Handler) Handler

// HandlerMiddlewareChain applies middlewares in order (first middleware wraps the last).
//
// Example usage:
//
//	handler := HandlerMiddlewareChain(
//	    myHandler,
//	    RecoveryMiddleware(logger),
//	    LoggingMiddleware(logger),
//	    MetricsMiddleware(logger),
//	    TimeoutMiddleware(30*time.Second),
//	    RetryMiddleware(RetryConfig{MaxAttempts: 3, Delay: time.Second * 3}),
//	)
func HandlerMiddlewareChain(handler Handler, middlewares ...HandleMiddleware) Handler {
	// apply in reverse so first middleware is outermost
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// LoggingMiddleware logs message processing with structured logging.
func LoggingMiddleware(logger *slog.Logger) HandleMiddleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (HandlerAction, error) {
			logger.Info("handler.start", slog.String("message", msg.MessageID))

			action, err := next(ctx, msg)

			if err != nil {
				logger.Error(
					"handler.error",
					slog.String("message", msg.MessageID),
					slog.String("action", action.String()),
					slog.Any("error", err),
				)
			} else {
				logger.Info(
					"handler.success",
					slog.String("message", msg.MessageID),
					slog.String("action", action.String()),
				)
			}

			return action, err
		}
	}
}

// MetricsMiddleware tracks processing duration.
func MetricsMiddleware(logger *slog.Logger) HandleMiddleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (HandlerAction, error) {
			start := time.Now()
			action, err := next(ctx, msg)
			duration := time.Since(start)

			logger.Info(
				"handler.metrics",
				slog.String("message", msg.MessageID),
				slog.String("action", action.String()),
				slog.Int64("duration", duration.Milliseconds()),
			)

			return action, err
		}
	}
}

// RecoveryMiddleware recovers from panics in handlers.
func RecoveryMiddleware(logger *slog.Logger) HandleMiddleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (action HandlerAction, err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error(
						"handler.panic",
						slog.String("message", msg.MessageID),
						slog.Any("panic", r),
					)
					action = HandlerActionNackRequeue
					err = fmt.Errorf("%w: panic: %v", ErrConsumerHandlerMiddleware, r)
				}
			}()
			return next(ctx, msg)
		}
	}
}

// TimeoutMiddleware adds a timeout to message processing.
func TimeoutMiddleware(timeout time.Duration) HandleMiddleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (HandlerAction, error) {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			type result struct {
				action HandlerAction
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
				return HandlerActionNackRequeue, fmt.Errorf("%w: message processing timeout after %vms", ErrConsumerHandlerMiddleware, timeout.Milliseconds())
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
func RetryMiddleware(cfg RetryMiddlewareConfig) HandleMiddleware {
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
		return func(ctx context.Context, msg *Message) (HandlerAction, error) {
			var action HandlerAction
			var err error
			delay := cfg.Delay

			for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
				// check if context is already cancelled
				select {
				case <-ctx.Done():
					return HandlerActionNackRequeue, ctx.Err()
				default:
				}

				action, err = next(ctx, msg)

				// success: return immediately
				if err == nil && (cfg.ErrOnly || action == HandlerActionAck) {
					return action, nil
				}

				// last attempt: return as-is
				if attempt == cfg.MaxAttempts {
					break
				}

				// wait before retry (with context cancellation check)
				select {
				case <-ctx.Done():
					return HandlerActionNackRequeue, ctx.Err()
				case <-time.After(delay):
				}

				// exponential backoff
				if cfg.Backoff {
					delay = min(delay*2, cfg.MaxDelay)
				}
			}

			return action, err
		}
	}
}
