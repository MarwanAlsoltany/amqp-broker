package handler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoggingMiddleware(t *testing.T) {
	t.Run("LogsInfoAndSuccess", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{Logger: logger}
		mw := LoggingMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "test-message-123"

		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.Contains(t, output, "handler.start")
		assert.Contains(t, output, "handler.success")
		assert.Contains(t, output, "test-message-123")
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := LoggingMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithDefaultLogger", func(t *testing.T) {
		cfg := &LoggingMiddlewareConfig{}
		mw := LoggingMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("LogsErrorAtInfoLevel", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{
			Logger: logger,
		}
		mw := LoggingMiddleware(cfg)

		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		msg.MessageID = "test-message-456"

		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, handlerErr, err)
		assert.Equal(t, ActionNackRequeue, action)
		output := buf.String()
		assert.Contains(t, output, "handler.error")
		assert.Contains(t, output, "test-message-456")
		assert.Contains(t, output, "handler error")
	})

	t.Run("WithCustomLevels", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		cfg := &LoggingMiddlewareConfig{
			Logger:       logger,
			StartLevel:   slog.LevelDebug,
			SuccessLevel: slog.LevelWarn,
		}
		mw := LoggingMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.Contains(t, output, "level=DEBUG")
		assert.Contains(t, output, "level=WARN")
	})

	t.Run("WithCustomErrorLevel", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		cfg := &LoggingMiddlewareConfig{
			Logger:     logger,
			ErrorLevel: slog.LevelError,
		}
		mw := LoggingMiddleware(cfg)

		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "handler.error")
		assert.Contains(t, output, "level=ERROR")
	})

	t.Run("DisableStartLogging", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{
			Logger:  logger,
			NoStart: true,
		}
		mw := LoggingMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.NotContains(t, output, "handler.start")
		assert.Contains(t, output, "handler.success")
	})

	t.Run("DisableSuccessLogging", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{
			Logger:    logger,
			NoSuccess: true,
		}
		mw := LoggingMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.Contains(t, output, "handler.start")
		assert.NotContains(t, output, "handler.success")
	})

	t.Run("ErrorLoggedEvenWhenSuccessDisabled", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{
			Logger:    logger,
			NoSuccess: true,
		}
		mw := LoggingMiddleware(cfg)

		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "handler.error")
	})

	t.Run("DisableErrorLogging", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{
			Logger:  logger,
			NoError: true,
		}
		mw := LoggingMiddleware(cfg)

		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		output := buf.String()
		assert.NotContains(t, output, "handler.error")
	})

	t.Run("PassesContextToLogger", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{Logger: logger}
		mw := LoggingMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		type ctxKey string
		ctx := context.WithValue(context.Background(), ctxKey("ctx-key"), "ctx-value")
		msg := message.New([]byte("test"))
		_, _ = wrapped(ctx, &msg)

		// just verify logging works with context
		output := buf.String()
		assert.Contains(t, output, "handler.start")
	})

	t.Run("HandlerErrorStillPropagates", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{Logger: logger}
		mw := LoggingMiddleware(cfg)

		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, handlerErr)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("WithCustomFields", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{
			Logger: logger,
			Fields: func(msg *message.Message) []any {
				return []any{
					slog.String("correlationID", "abc-123"),
					slog.String("tenant", "test-tenant"),
				}
			},
		}
		mw := LoggingMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "test-message-fields"

		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.Contains(t, output, "handler.start")
		assert.Contains(t, output, "handler.success")
		assert.Contains(t, output, "correlationID=abc-123")
		assert.Contains(t, output, "tenant=test-tenant")
	})

	t.Run("WithCustomFieldsOnError", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{
			Logger: logger,
			Fields: func(msg *message.Message) []any {
				return []any{slog.String("traceID", "xyz-789")}
			},
		}
		mw := LoggingMiddleware(cfg)

		handlerErr := errors.New("test error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "handler.error")
		assert.Contains(t, output, "traceID=xyz-789")
	})

	t.Run("WithNilFields", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &LoggingMiddlewareConfig{
			Logger: logger,
			Fields: nil, // explicit nil
		}
		mw := LoggingMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.Contains(t, output, "handler.start")
		assert.Contains(t, output, "handler.success")
	})
}

func TestMetricsMiddleware(t *testing.T) {
	t.Run("LogsMetricsWithLogger", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &MetricsMiddlewareConfig{Logger: logger}
		mw := MetricsMiddleware(cfg)

		handler := testSleepHandler(ActionAck, 10*time.Millisecond)
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		msg.MessageID = "test-message-789"

		start := time.Now()
		action, err := wrapped(t.Context(), &msg)
		duration := time.Since(start)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.GreaterOrEqual(t, duration, 10*time.Millisecond)
		output := buf.String()
		assert.Contains(t, output, "handler.metrics")
		assert.Contains(t, output, "test-message-789")
	})

	t.Run("UsesDiscardHandlerByDefault", func(t *testing.T) {
		mw := MetricsMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithRecordCallback", func(t *testing.T) {
		var recorded bool
		var recordedDuration time.Duration
		var recordedAction Action

		cfg := &MetricsMiddlewareConfig{
			Record: func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration) {
				recorded = true
				recordedDuration = duration
				recordedAction = action
			},
		}
		mw := MetricsMiddleware(cfg)

		handler := testSleepHandler(ActionAck, 10*time.Millisecond)
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.True(t, recorded)
		assert.GreaterOrEqual(t, recordedDuration, 10*time.Millisecond)
		assert.Equal(t, ActionAck, recordedAction)
	})

	t.Run("WithLoggerAndRecord", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		var recordCalled bool

		cfg := &MetricsMiddlewareConfig{
			Logger: logger,
			Record: func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration) {
				recordCalled = true
			},
		}
		mw := MetricsMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		// both logger and record callback should be called
		assert.Contains(t, buf.String(), "handler.metrics")
		assert.True(t, recordCalled)
	})

	t.Run("RecordsErrorInformation", func(t *testing.T) {
		var recordedErr error
		var recordedAction Action

		cfg := &MetricsMiddlewareConfig{
			Record: func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration) {
				recordedErr = err
				recordedAction = action
			},
		}
		mw := MetricsMiddleware(cfg)

		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, handlerErr)
		assert.Equal(t, ActionNackRequeue, action)
		assert.ErrorIs(t, recordedErr, handlerErr)
		assert.Equal(t, ActionNackRequeue, recordedAction)
	})

	t.Run("PassesMessageToRecord", func(t *testing.T) {
		var recordedMsg *message.Message

		cfg := &MetricsMiddlewareConfig{
			Record: func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration) {
				recordedMsg = msg
			},
		}
		mw := MetricsMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "test-message-789"
		_, _ = wrapped(t.Context(), &msg)

		assert.Equal(t, "test-message-789", recordedMsg.MessageID)
	})

	t.Run("PassesContextToRecord", func(t *testing.T) {
		var receivedCtx context.Context
		type ctxKey string
		key := ctxKey("ctx-key")
		expectedValue := "ctx-value"

		cfg := &MetricsMiddlewareConfig{
			Record: func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration) {
				receivedCtx = ctx
			},
		}
		mw := MetricsMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		ctx := context.WithValue(context.Background(), key, expectedValue)
		msg := message.New([]byte("test"))
		_, _ = wrapped(ctx, &msg)

		assert.NotNil(t, receivedCtx)
		assert.Equal(t, expectedValue, receivedCtx.Value(key))
	})

	t.Run("DurationIsAccurate", func(t *testing.T) {
		var recordedDuration time.Duration

		cfg := &MetricsMiddlewareConfig{
			Record: func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration) {
				recordedDuration = duration
			},
		}
		mw := MetricsMiddleware(cfg)

		sleepDuration := 20 * time.Millisecond
		handler := testSleepHandler(ActionAck, sleepDuration)
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		assert.GreaterOrEqual(t, recordedDuration, sleepDuration)
	})

	t.Run("HandlerErrorStillPropagates", func(t *testing.T) {
		cfg := &MetricsMiddlewareConfig{
			Record: func(ctx context.Context, msg *message.Message, action Action, err error, duration time.Duration) {
			},
		}
		mw := MetricsMiddleware(cfg)

		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, handlerErr)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("WithCustomLevel", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

		cfg := &MetricsMiddlewareConfig{Logger: logger, Level: slog.LevelWarn}
		mw := MetricsMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "handler.metrics")
		assert.Contains(t, output, "WARN")
	})

	t.Run("WithZeroLevelUsesInfo", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		cfg := &MetricsMiddlewareConfig{Logger: logger, Level: 0}
		mw := MetricsMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "handler.metrics")
		assert.Contains(t, output, "INFO")
	})
}

func TestDebugMiddleware(t *testing.T) {
	t.Run("LogsWithDefaultConfig", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("hello"))
		msg.MessageID = "debug-msg-1"
		msg.ContentType = "text/plain"
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.Contains(t, output, "debug-msg-1")
		assert.Contains(t, output, "text/plain")
		assert.Contains(t, output, "hello")
		assert.Contains(t, output, "handler.debug")
	})

	t.Run("UsesDefaultLogger", func(t *testing.T) {
		// this test verifies slog.Default() is used when logger is nil
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: nil, // should use slog.Default()
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		// should use all defaults: slog.Default(), LevelDebug, default fields
		mw := DebugMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("TruncatesLargeBody", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
		})
		wrapped := mw(testHandler(ActionAck))

		// create a body larger than 1KB
		largeBody := make([]byte, 2048)
		for i := range largeBody {
			largeBody[i] = 'A'
		}
		msg := message.New(largeBody)
		wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "truncated")
		// should not contain all 2048 As
		assert.Less(t, len(output), 3000)
	})

	t.Run("WithCustomFields", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
			Fields: func(msg *message.Message) []any {
				// only log ID, no body/headers for security
				return []any{
					slog.String("id", msg.MessageID),
				}
			},
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("sensitive-data"))
		msg.MessageID = "msg-123"
		msg.Headers = make(message.Arguments)
		msg.Headers["secret"] = "should-not-appear"
		wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "msg-123")
		assert.NotContains(t, output, "sensitive-data")
		assert.NotContains(t, output, "should-not-appear")
	})

	t.Run("WithCustomLevel", func(t *testing.T) {
		var buf bytes.Buffer
		// use Debug level handler so we can see the log
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
			Level:  slog.LevelWarn, // use Warn level
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "warn-msg"
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.Contains(t, output, "warn-msg")
		assert.Contains(t, output, "WARN")
	})

	t.Run("WithZeroLevelUsesInfo", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
			// Level zero value == slog.LevelInfo
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "level=INFO")
	})

	t.Run("PassesContextToLogger", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
		})
		wrapped := mw(testHandler(ActionAck))

		ctx := context.WithValue(t.Context(), "ctx-key", "ctx-value")
		msg := message.New([]byte("test"))
		wrapped(ctx, &msg)

		// the context is passed to logger.Log, though it may not appear in output
		// this test just ensures no panic
	})

	t.Run("FieldsCanReturnEmptySlice", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
			Fields: func(msg *message.Message) []any {
				return []any{} // no fields
			},
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		output := buf.String()
		assert.Contains(t, output, "handler.debug")
	})

	t.Run("DoesNotModifyMessage", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
		})
		wrapped := mw(testHandler(ActionAck))

		originalBody := []byte("original")
		msg := message.New(originalBody)
		msg.MessageID = "test-id"
		wrapped(t.Context(), &msg)

		// message should be unchanged
		assert.Equal(t, "test-id", msg.MessageID)
		assert.Equal(t, originalBody, msg.Body)
	})

	t.Run("HandlerErrorStillPropagates", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		mw := DebugMiddleware(&DebugMiddlewareConfig{
			Logger: logger,
		})

		handlerErr := errors.New("handler failed")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, handlerErr)
		assert.Equal(t, ActionNackRequeue, action)
		// should still have logged
		assert.Contains(t, buf.String(), "handler.debug")
	})
}
