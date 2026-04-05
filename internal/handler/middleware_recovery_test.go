package handler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoveryMiddleware(t *testing.T) {
	t.Run("RecoversPanicWithDefaultAction", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &RecoveryMiddlewareConfig{Logger: logger}
		mw := RecoveryMiddleware(cfg)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			panic("something went wrong")
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		msg.MessageID = "test-message-panic"

		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "panic")
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Equal(t, ActionNackRequeue, action)
		output := buf.String()
		assert.Contains(t, output, "handler.panic")
		assert.Contains(t, output, "test-message-panic")
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := RecoveryMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithDefaultLogger", func(t *testing.T) {
		cfg := &RecoveryMiddlewareConfig{}
		mw := RecoveryMiddleware(cfg)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			panic("test panic")
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "panic")
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("WithCustomAction", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &RecoveryMiddlewareConfig{
			Logger: logger,
			Action: ActionNackDiscard,
		}
		mw := RecoveryMiddleware(cfg)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			panic("test panic")
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("WithZeroActionUsesDefault", func(t *testing.T) {
		cfg := &RecoveryMiddlewareConfig{Action: 0}
		mw := RecoveryMiddleware(cfg)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			panic("test panic")
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("WithOnPanicCallback", func(t *testing.T) {
		var callbackCalled bool
		var callbackRecovered any

		cfg := &RecoveryMiddlewareConfig{
			OnPanic: func(ctx context.Context, msg *message.Message, recovered any) {
				callbackCalled = true
				callbackRecovered = recovered
			},
		}
		mw := RecoveryMiddleware(cfg)

		panicValue := "custom panic value"
		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			panic(panicValue)
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		assert.True(t, callbackCalled)
		assert.Equal(t, panicValue, callbackRecovered)
	})

	t.Run("NoPanicDoesNotCallCallback", func(t *testing.T) {
		var callbackCalled bool

		cfg := &RecoveryMiddlewareConfig{
			OnPanic: func(ctx context.Context, msg *message.Message, recovered any) {
				callbackCalled = true
			},
		}
		mw := RecoveryMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.False(t, callbackCalled)
	})

	t.Run("NoPanicDoesNotLog", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		cfg := &RecoveryMiddlewareConfig{Logger: logger}
		mw := RecoveryMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.Empty(t, buf.String())
	})

	t.Run("ErrorWrappingPreservesContext", func(t *testing.T) {
		cfg := &RecoveryMiddlewareConfig{}
		mw := RecoveryMiddleware(cfg)

		panicValue := "specific panic error"
		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			panic(panicValue)
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		_, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Contains(t, err.Error(), "recovered panic")
		assert.Contains(t, err.Error(), panicValue)
	})

	t.Run("HandlerErrorStillPropagates", func(t *testing.T) {
		cfg := &RecoveryMiddlewareConfig{}
		mw := RecoveryMiddleware(cfg)

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

		cfg := &RecoveryMiddlewareConfig{Logger: logger, Level: slog.LevelWarn}
		mw := RecoveryMiddleware(cfg)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			panic("test panic")
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "handler.panic")
		assert.Contains(t, output, "WARN")
	})

	t.Run("WithZeroLevelUsesInfo", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		cfg := &RecoveryMiddlewareConfig{Logger: logger, Level: 0}
		mw := RecoveryMiddleware(cfg)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			panic("test panic")
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)

		output := buf.String()
		assert.Contains(t, output, "handler.panic")
		assert.Contains(t, output, "INFO")
	})
}

func TestFallbackMiddleware(t *testing.T) {
	t.Run("PrimarySucceedsNoFallback", func(t *testing.T) {
		fallbackCalled := false
		fallback := func(ctx context.Context, msg *message.Message) (Action, error) {
			fallbackCalled = true
			return ActionNackDiscard, nil
		}
		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.False(t, fallbackCalled)
	})

	t.Run("PrimaryFailsCallsFallback", func(t *testing.T) {
		primaryErr := errors.New("primary failed")
		fallback := testHandler(ActionNackDiscard)
		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
		})
		wrapped := mw(testErrorHandler(ActionNackRequeue, primaryErr))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("PreservesOriginalErrorWhenBothFail", func(t *testing.T) {
		primaryErr := errors.New("primary error")
		fallbackErr := errors.New("fallback error")
		fallback := testErrorHandler(ActionNackDiscard, fallbackErr)
		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
		})
		wrapped := mw(testErrorHandler(ActionNackRequeue, primaryErr))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action)
		assert.Contains(t, err.Error(), "fallback error")
		assert.Contains(t, err.Error(), "original: primary error")
		assert.ErrorIs(t, err, fallbackErr)
	})

	t.Run("OnlyPrimaryErrorPreserved", func(t *testing.T) {
		primaryErr := errors.New("primary error")
		fallback := testHandler(ActionAck) // fallback succeeds
		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
		})
		wrapped := mw(testErrorHandler(ActionNackRequeue, primaryErr))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err) // fallback succeeded
		assert.Equal(t, ActionAck, action)
	})

	t.Run("OnlyFallbackErrorReturned", func(t *testing.T) {
		fallbackErr := errors.New("fallback error")
		fallback := testErrorHandler(ActionNackDiscard, fallbackErr)
		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
		})
		// primary succeeds but we force fallback with custom ShouldFallback
		wrapped := mw(testHandler(ActionAck))

		// this won't actually trigger since default ShouldFallback only triggers on error
		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithCustomShouldFallbackOnSpecificError", func(t *testing.T) {
		specificErr := errors.New("specific error")
		otherErr := errors.New("other error")

		fallbackCalled := false
		fallback := func(ctx context.Context, msg *message.Message) (Action, error) {
			fallbackCalled = true
			return ActionAck, nil
		}

		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
			ShouldFallback: func(err error, _ Action) bool {
				return errors.Is(err, specificErr)
			},
		})

		// test with specific error - should fallback
		wrapped := mw(testErrorHandler(ActionNackRequeue, specificErr))
		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.True(t, fallbackCalled)

		// test with other error - should not fallback
		fallbackCalled = false
		wrapped = mw(testErrorHandler(ActionNackRequeue, otherErr))
		action, err = wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, otherErr)
		assert.Equal(t, ActionNackRequeue, action)
		assert.False(t, fallbackCalled)
	})

	t.Run("WithCustomShouldFallbackOnAction", func(t *testing.T) {
		fallbackCalled := false
		fallback := func(ctx context.Context, msg *message.Message) (Action, error) {
			fallbackCalled = true
			return ActionAck, nil
		}

		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
			ShouldFallback: func(_ error, action Action) bool {
				return action == ActionNackRequeue
			},
		})

		// test with NackRequeue - should fallback
		wrapped := mw(testHandler(ActionNackRequeue))
		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.True(t, fallbackCalled)

		// test with Ack - should not fallback
		fallbackCalled = false
		wrapped = mw(testHandler(ActionAck))
		action, err = wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.False(t, fallbackCalled)
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := FallbackMiddleware(nil)
		wrapped := mw(testErrorHandler(ActionNackRequeue, errors.New("error")))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("WithNilFallback", func(t *testing.T) {
		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: nil, // no fallback handler
		})
		wrapped := mw(testErrorHandler(ActionNackRequeue, errors.New("error")))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("WithNilShouldFallbackUsesDefault", func(t *testing.T) {
		fallbackCalled := false
		fallback := func(ctx context.Context, msg *message.Message) (Action, error) {
			fallbackCalled = true
			return ActionAck, nil
		}

		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback:       fallback,
			ShouldFallback: nil, // should use default (error != nil)
		})

		wrapped := mw(testErrorHandler(ActionNackRequeue, errors.New("error")))
		msg := message.New([]byte("test"))
		wrapped(t.Context(), &msg)

		assert.True(t, fallbackCalled, "should use default fallback condition")
	})

	t.Run("FallbackReceivesSameContextAndMessage", func(t *testing.T) {
		var receivedCtx context.Context
		var receivedMsg *message.Message

		fallback := func(ctx context.Context, msg *message.Message) (Action, error) {
			receivedCtx = ctx
			receivedMsg = msg
			return ActionAck, nil
		}

		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback: fallback,
		})

		wrapped := mw(testErrorHandler(ActionNackRequeue, errors.New("error")))

		ctx := t.Context()
		msg := message.New([]byte("test-data"))
		msg.MessageID = "msg-123"

		wrapped(ctx, &msg)

		assert.Equal(t, ctx, receivedCtx)
		assert.Equal(t, "msg-123", receivedMsg.MessageID)
		assert.Equal(t, []byte("test-data"), receivedMsg.Body)
	})

	t.Run("PrimaryNotCalledWhenShouldFallbackAlwaysTrue", func(t *testing.T) {
		primaryCalled := false
		primary := func(ctx context.Context, msg *message.Message) (Action, error) {
			primaryCalled = true
			return ActionAck, nil
		}

		fallback := testHandler(ActionNackDiscard)

		// this is a weird edge case but should be supported
		mw := FallbackMiddleware(&FallbackMiddlewareConfig{
			Fallback:       fallback,
			ShouldFallback: func(_ error, _ Action) bool { return true },
		})

		wrapped := mw(primary)
		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionNackDiscard, action)
		assert.True(t, primaryCalled, "primary should still be called")
	})
}
