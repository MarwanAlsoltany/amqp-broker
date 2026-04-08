package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler(t *testing.T) {
	t.Run("ActionHandler", func(t *testing.T) {
		handler := ActionHandler(HandlerActionAck)

		action, err := handler(t.Context(), &Message{})

		require.NoError(t, err)
		assert.Equal(t, HandlerActionAck, action)
	})

	t.Run("WrapHandler", func(t *testing.T) {
		called := false
		base := func(_ context.Context, _ *Message) (HandlerAction, error) {
			called = true
			return HandlerActionAck, nil
		}
		wrapped := WrapHandler(base)

		_, err := wrapped(t.Context(), &Message{})

		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("Middlewares", func(t *testing.T) {
		ctx := t.Context()

		assert.NotNil(t, BatchMiddleware(ctx, nil, &BatchConfig{}))
		assert.NotNil(t, LoggingMiddleware(&LoggingMiddlewareConfig{}))
		assert.NotNil(t, MetricsMiddleware(&MetricsMiddlewareConfig{}))
		assert.NotNil(t, DebugMiddleware(&DebugMiddlewareConfig{}))
		assert.NotNil(t, RecoveryMiddleware(&RecoveryMiddlewareConfig{}))
		assert.NotNil(t, FallbackMiddleware(&FallbackMiddlewareConfig{}))
		assert.NotNil(t, RetryMiddleware(&RetryMiddlewareConfig{}))
		assert.NotNil(t, CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{Threshold: 5}))
		assert.NotNil(t, ConcurrencyMiddleware(&ConcurrencyMiddlewareConfig{Max: 5}))
		assert.NotNil(t, RateLimitMiddleware(ctx, &RateLimitMiddlewareConfig{RPS: 10}))
		assert.NotNil(t, DeduplicationMiddleware(&DeduplicationMiddlewareConfig{}))
		assert.NotNil(t, ValidationMiddleware(&ValidationMiddlewareConfig{}))
		assert.NotNil(t, TransformMiddleware(&TransformMiddlewareConfig{}))
		assert.NotNil(t, DeadlineMiddleware(&DeadlineMiddlewareConfig{}))
		assert.NotNil(t, TimeoutMiddleware(&TimeoutMiddlewareConfig{Timeout: time.Second}))
	})
}
