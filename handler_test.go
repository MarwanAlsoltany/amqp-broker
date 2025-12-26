package broker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerActionString(t *testing.T) {
	tests := []struct {
		action   HandlerAction
		expected string
	}{
		{HandlerActionAck, "ack"},
		{HandlerActionNackRequeue, "nack.requeue"},
		{HandlerActionNackDiscard, "nack.discard"},
		{HandlerActionNoAction, ""},
		{HandlerAction(999), "unknown"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.action.String())
	}
}

func TestHandlerMiddlewareChain(t *testing.T) {
	callOrder := []string{}

	middleware1 := func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (HandlerAction, error) {
			callOrder = append(callOrder, "m1-before")
			action, err := next(ctx, msg)
			callOrder = append(callOrder, "m1-after")
			return action, err
		}
	}

	middleware2 := func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (HandlerAction, error) {
			callOrder = append(callOrder, "m2-before")
			action, err := next(ctx, msg)
			callOrder = append(callOrder, "m2-after")
			return action, err
		}
	}

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		callOrder = append(callOrder, "handler")
		return HandlerActionAck, nil
	}

	wrapped := HandlerMiddlewareChain(handler, middleware1, middleware2)

	ctx := t.Context()
	msg := NewMessage([]byte("test"))
	action, err := wrapped(ctx, &msg)

	require.NoError(t, err)
	assert.Equal(t, HandlerActionAck, action)
	assert.Equal(t, []string{"m1-before", "m2-before", "handler", "m2-after", "m1-after"}, callOrder)

	t.Run("WithComplexChain", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		wrapped := HandlerMiddlewareChain(
			testHandler(HandlerActionAck),
			RecoveryMiddleware(logger),
			LoggingMiddleware(logger),
			MetricsMiddleware(logger),
			TimeoutMiddleware(1*time.Second),
			RetryMiddleware(RetryMiddlewareConfig{MaxAttempts: 3, Delay: 10 * time.Millisecond}),
		)

		ctx := t.Context()
		msg := NewMessage([]byte("test"))
		msg.MessageID = "complex-test"

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
		// assert that something was logged
		output := buf.String()
		assert.NotEmpty(t, output)
		assert.Contains(t, output, "complex-test")
	})
}

func TestLoggingMiddleware(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	middleware := LoggingMiddleware(logger)

	wrapped := middleware(testHandler(HandlerActionAck))

	ctx := t.Context()
	msg := NewMessage([]byte("test"))
	msg.MessageID = "test-message-123"

	action, err := wrapped(ctx, &msg)

	require.NoError(t, err)
	assert.Equal(t, HandlerActionAck, action)
	// assert that something was logged
	output := buf.String()
	assert.NotEmpty(t, output)
	assert.Contains(t, output, "test-message-123")

	t.Run("WithNoLogger", func(t *testing.T) {
		middleware := LoggingMiddleware(nil)

		wrapped := middleware(testHandler(HandlerActionAck))

		ctx := t.Context()
		msg := NewMessage([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
	})

	t.Run("WithError", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		middleware := LoggingMiddleware(logger)

		handlerErr := errors.New("handler error")
		handler := testErrorHandler(HandlerActionNackRequeue, handlerErr)

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := NewMessage([]byte("test"))
		msg.MessageID = "test-message-456"

		action, err := wrapped(ctx, &msg)

		assert.Error(t, err)
		assert.Equal(t, handlerErr, err)
		assert.Equal(t, HandlerActionNackRequeue, action)
		// assert that something was logged
		output := buf.String()
		assert.NotEmpty(t, output)
		assert.Contains(t, output, "test-message-456")
	})
}

func TestMetricsMiddleware(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	middleware := MetricsMiddleware(logger)

	handler := testSleepHandler(HandlerActionAck, 10*time.Millisecond)

	wrapped := middleware(handler)

	ctx := t.Context()
	msg := NewMessage([]byte("test"))
	msg.MessageID = "test-message-789"

	start := time.Now()
	action, err := wrapped(ctx, &msg)
	duration := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, HandlerActionAck, action)
	assert.GreaterOrEqual(t, duration, 10*time.Millisecond)
	// assert that something was logged
	output := buf.String()
	assert.NotEmpty(t, output)
	assert.Contains(t, output, "test-message-789")

	t.Run("WithNoLogger", func(t *testing.T) {
		middleware := MetricsMiddleware(nil)

		wrapped := middleware(testHandler(HandlerActionAck))

		ctx := t.Context()
		msg := NewMessage([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
	})
}

func TestRecoveryMiddleware(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	middleware := RecoveryMiddleware(logger)

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		panic("something went wrong")
	}

	wrapped := middleware(handler)

	ctx := t.Context()
	msg := NewMessage([]byte("test"))
	msg.MessageID = "test-message-panic"

	action, err := wrapped(ctx, &msg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panic")
	assert.Equal(t, HandlerActionNackRequeue, action)
	// assert that something was logged
	output := buf.String()
	assert.NotEmpty(t, output)
	assert.Contains(t, output, "test-message-panic")

	t.Run("WithNoLogger", func(t *testing.T) {
		middleware := RecoveryMiddleware(nil)

		wrapped := middleware(testHandler(HandlerActionAck))

		ctx := t.Context()
		msg := NewMessage([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
	})

	t.Run("WithNoPanic", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		middleware := RecoveryMiddleware(logger)

		wrapped := middleware(testHandler(HandlerActionAck))

		ctx := t.Context()
		msg := NewMessage([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
		// assert that nothing was logged
		output := buf.String()
		assert.Empty(t, output)
	})
}

func TestTimeoutMiddleware(t *testing.T) {
	middleware := TimeoutMiddleware(50 * time.Millisecond)

	handler := testSleepHandler(HandlerActionAck, 100*time.Millisecond)

	wrapped := middleware(handler)

	ctx := t.Context()
	msg := NewMessage([]byte("test"))

	action, err := wrapped(ctx, &msg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.Equal(t, HandlerActionNackRequeue, action)

	t.Run("CompletesInTime", func(t *testing.T) {
		middleware := TimeoutMiddleware(100 * time.Millisecond)

		handler := testSleepHandler(HandlerActionAck, 10*time.Millisecond)

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := NewMessage([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
	})
}

func TestRetryMiddleware(t *testing.T) {
	attempts := 0

	config := RetryMiddlewareConfig{
		MaxAttempts: 3,
		Delay:       10 * time.Millisecond,
	}
	middleware := RetryMiddleware(config)

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		attempts++
		if attempts < 3 {
			return HandlerActionNackRequeue, errors.New("temporary error")
		}
		return HandlerActionAck, nil
	}

	wrapped := middleware(handler)

	ctx := t.Context()
	msg := NewMessage([]byte("test"))

	action, err := wrapped(ctx, &msg)

	require.NoError(t, err)
	assert.Equal(t, HandlerActionAck, action)
	assert.Equal(t, 3, attempts)

	t.Run("WithBadConfig", func(t *testing.T) {
		config := RetryMiddlewareConfig{
			MaxAttempts: -1,
			Delay:       -5 * time.Millisecond,
			MaxDelay:    -10 * time.Millisecond,
		}
		middleware := RetryMiddleware(config)

		handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
			return HandlerActionAck, nil
		}

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := NewMessage([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
	})

	t.Run("ExhaustsRetries", func(t *testing.T) {
		attempts := 0

		config := RetryMiddlewareConfig{
			MaxAttempts: 3,
			Delay:       10 * time.Millisecond,
		}
		middleware := RetryMiddleware(config)

		handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
			attempts++
			return HandlerActionNackRequeue, errors.New("persistent error")
		}

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := NewMessage([]byte("test"))

		action, err := wrapped(ctx, &msg)

		assert.Error(t, err)
		assert.Equal(t, HandlerActionNackRequeue, action)
		assert.Equal(t, 4, attempts) // MaxAttempts + 1 initial attempt
	})

	t.Run("DoesNotRetryOnAck", func(t *testing.T) {
		attempts := 0

		config := RetryMiddlewareConfig{
			MaxAttempts: 3,
			Delay:       10 * time.Millisecond,
		}
		middleware := RetryMiddleware(config)

		handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
			attempts++
			return HandlerActionAck, nil
		}

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := NewMessage([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
		assert.Equal(t, 1, attempts)
	})

	t.Run("WhenContextCancelled", func(t *testing.T) {
		config := RetryMiddlewareConfig{
			MaxAttempts: 5,
			Delay:       50 * time.Millisecond,
			MaxDelay:    250 * time.Millisecond,
			Backoff:     true,
		}
		middleware := RetryMiddleware(config)

		attempts := 0
		handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
			attempts++
			return HandlerActionNackRequeue, errors.New("temporary error")
		}

		wrapped := middleware(handler)

		t.Run("BeforeOperation", func(t *testing.T) {
			attempts = 0
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			action, err = wrapped(ctx, &msg)

			assert.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
			assert.Contains(t, err.Error(), "context canceled")
			assert.Equal(t, HandlerActionNackRequeue, action)
			assert.Equal(t, attempts, 0) // should have stopped immediately
		})

		t.Run("DuringOperation", func(t *testing.T) {
			attempts = 0
			ctx, cancel := context.WithCancel(t.Context())
			msg := NewMessage([]byte("test"))

			// cancel the context after a short delay
			go func() {
				time.Sleep(70 * time.Millisecond)
				cancel()
			}()

			action, err := wrapped(ctx, &msg)

			assert.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
			assert.Contains(t, err.Error(), "context canceled")
			assert.Equal(t, HandlerActionNackRequeue, action)
			assert.Less(t, attempts, 6) // should have stopped before exhausting all retries
		})
	})
}
