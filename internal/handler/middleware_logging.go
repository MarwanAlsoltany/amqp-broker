package handler

import (
	"context"
	"log/slog"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// LoggingMiddlewareConfig configures structured logging of message lifecycle events.
// Logs include message start, completion, errors, and custom fields.
type LoggingMiddlewareConfig struct {
	// Logger to use for output. If nil, uses slog.Default().
	Logger *slog.Logger
	// StartLevel is the log level for handler start (default: [slog.LevelInfo]).
	// Pass an explicit value (e.g. slog.LevelDebug) to override.
	StartLevel slog.Level
	// SuccessLevel is the log level for successful completion (default: [slog.LevelInfo]).
	// Pass an explicit value (e.g. slog.LevelDebug) to override.
	SuccessLevel slog.Level
	// ErrorLevel is the log level for errors (default: [slog.LevelInfo]).
	// Pass an explicit value (e.g. slog.LevelError) to override.
	ErrorLevel slog.Level
	// NoStart disables handler start logging (default: false = logging enabled).
	NoStart bool
	// NoSuccess disables successful completion logging (default: false = logging enabled).
	NoSuccess bool
	// NoError disables handler error logging (default: false = logging enabled).
	NoError bool
	// Fields extracts additional log attributes from a message.
	// If nil, no extra fields are added (only message ID, action, and error are logged).
	// Use this to add correlation IDs, tenant info, or other context.
	Fields func(*message.Message) []any
}

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
func LoggingMiddleware(cfg *LoggingMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &LoggingMiddlewareConfig{}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			if !cfg.NoStart {
				args := []any{slog.String(logFields["id"], msg.MessageID)}
				if cfg.Fields != nil {
					args = append(args, cfg.Fields(msg)...)
				}
				logger.Log(ctx, cfg.StartLevel, "handler.start", args...)
			}

			action, err := next(ctx, msg)

			if err != nil {
				if !cfg.NoError {
					args := []any{
						slog.String(logFields["id"], msg.MessageID),
						slog.String(logFields["action"], action.String()),
						slog.Any(logFields["error"], err),
					}
					if cfg.Fields != nil {
						args = append(args, cfg.Fields(msg)...)
					}
					logger.Log(ctx, cfg.ErrorLevel, "handler.error", args...)
				}
			} else if !cfg.NoSuccess {
				args := []any{
					slog.String(logFields["id"], msg.MessageID),
					slog.String(logFields["action"], action.String()),
				}
				if cfg.Fields != nil {
					args = append(args, cfg.Fields(msg)...)
				}
				logger.Log(ctx, cfg.SuccessLevel, "handler.success", args...)
			}

			return action, err
		}
	}
}

// MetricsMiddlewareConfig configures metrics collection for message processing.
// Tracks processing duration, success/failure counts, and custom metrics.
type MetricsMiddlewareConfig struct {
	// Logger to use for metrics output. If nil, uses slog.New(slog.DiscardHandler).
	// For semantic lifecycle logging (start/success/error), use [LoggingMiddleware].
	Logger *slog.Logger
	// Level is the log level for metrics output (default: [slog.LevelInfo]).
	// Pass an explicit value (e.g. slog.LevelDebug) to override.
	Level slog.Level
	// Record is an optional callback for custom metrics collection (e.g., Prometheus, StatsD).
	// Called after handler execution with timing and result information.
	// Check err != nil to distinguish error cases from successful completion.
	Record func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration)
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
func MetricsMiddleware(cfg *MetricsMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &MetricsMiddlewareConfig{}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			start := time.Now()
			action, err := next(ctx, msg)
			duration := time.Since(start)

			// log metrics
			logger.Log(ctx, cfg.Level, "handler.metrics",
				slog.String(logFields["id"], msg.MessageID),
				slog.String(logFields["action"], action.String()),
				slog.Int64(logFields["duration"], duration.Milliseconds()),
			)

			// optional custom metrics callback
			if cfg.Record != nil {
				cfg.Record(ctx, msg, action, err, duration)
			}

			return action, err
		}
	}
}

// DebugMiddlewareConfig configures debug logging for message details.
// Logs complete message structure including headers and body.
type DebugMiddlewareConfig struct {
	// Logger to use for debug output. If nil, uses slog.Default().
	Logger *slog.Logger
	// Level is the log level to use (default: [slog.LevelInfo], i.e. the zero value).
	// Pass an explicit value (e.g. slog.LevelDebug) to override.
	Level slog.Level
	// Fields extracts log attributes from a message.
	// If nil, logs id, content_type, headers, and body (up to 1KB).
	// Use this to customize what gets logged or exclude sensitive data.
	Fields func(*message.Message) []any
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
//		Fields: func(msg *message.Message) []any {
//			return []any{slog.String("id", msg.MessageID), slog.String("type", msg.ContentType)}
//			// or []any{"id", msg.MessageID, "type", msg.ContentType}
//		},
//	})
func DebugMiddleware(cfg *DebugMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &DebugMiddlewareConfig{}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	fields := cfg.Fields
	if fields == nil {
		// default: log all fields, truncate body at 1KB
		fields = func(msg *message.Message) []any {
			body := string(msg.Body)
			if len(body) > 1024 {
				body = body[:1024] + "... (truncated)"
			}
			return []any{
				slog.String(logFields["id"], msg.MessageID),
				slog.String(logFields["contentType"], msg.ContentType),
				slog.Any(logFields["headers"], msg.Headers),
				slog.String(logFields["body"], body),
			}
		}
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			attrs := fields(msg)
			logger.Log(ctx, cfg.Level, "handler.debug", attrs...)
			return next(ctx, msg)
		}
	}
}
