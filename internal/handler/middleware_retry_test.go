package handler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryMiddleware(t *testing.T) {
	attempts := 0

	config := &RetryMiddlewareConfig{
		MaxAttempts: 3,
		MinBackoff:  10 * time.Millisecond,
	}
	middleware := RetryMiddleware(config)

	handler := func(ctx context.Context, msg *message.Message) (Action, error) {
		attempts++
		if attempts < 3 {
			return ActionNackRequeue, errors.New("temporary error")
		}
		return ActionAck, nil
	}

	wrapped := middleware(handler)

	ctx := t.Context()
	msg := message.New([]byte("test"))

	action, err := wrapped(ctx, &msg)

	require.NoError(t, err)
	assert.Equal(t, ActionAck, action)
	assert.Equal(t, 3, attempts)

	t.Run("WithNilConfig", func(t *testing.T) {
		attempts := 0
		middleware := RetryMiddleware(nil)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			attempts++
			if attempts < 3 {
				return ActionNackRequeue, errors.New("temporary error")
			}
			return ActionAck, nil
		}

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := message.New([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.GreaterOrEqual(t, attempts, 3) // Uses default MaxAttempts (3)
	})

	t.Run("WithBadConfig", func(t *testing.T) {
		config := &RetryMiddlewareConfig{
			MaxAttempts: -1,
			MinBackoff:  -5 * time.Millisecond,
			MaxBackoff:  -10 * time.Millisecond,
		}
		middleware := RetryMiddleware(config)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			return ActionAck, nil
		}

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := message.New([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("ExhaustsRetries", func(t *testing.T) {
		attempts := 0

		config := &RetryMiddlewareConfig{
			MaxAttempts: 3,
			MinBackoff:  10 * time.Millisecond,
		}
		middleware := RetryMiddleware(config)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			attempts++
			return ActionNackRequeue, errors.New("persistent error")
		}

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := message.New([]byte("test"))

		action, err := wrapped(ctx, &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)
		assert.Equal(t, 4, attempts) // MaxAttempts + 1 initial attempt
	})

	t.Run("DoesNotRetryOnAck", func(t *testing.T) {
		attempts := 0

		config := &RetryMiddlewareConfig{
			MaxAttempts: 3,
			MinBackoff:  10 * time.Millisecond,
		}
		middleware := RetryMiddleware(config)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			attempts++
			return ActionAck, nil
		}

		wrapped := middleware(handler)

		ctx := t.Context()
		msg := message.New([]byte("test"))

		action, err := wrapped(ctx, &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.Equal(t, 1, attempts)
	})

	t.Run("WhenContextCancelled", func(t *testing.T) {
		config := &RetryMiddlewareConfig{
			MaxAttempts:       5,
			MinBackoff:        50 * time.Millisecond,
			MaxBackoff:        250 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}
		middleware := RetryMiddleware(config)

		attempts := 0
		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			attempts++
			return ActionNackRequeue, errors.New("temporary error")
		}

		wrapped := middleware(handler)
		msg := message.New([]byte("test"))

		t.Run("BeforeOperation", func(t *testing.T) {
			attempts = 0
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			action, err := wrapped(ctx, &msg)

			assert.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
			assert.Contains(t, err.Error(), "context canceled")
			assert.Equal(t, ActionNackRequeue, action)
			assert.Equal(t, 0, attempts) // should have stopped immediately
		})

		t.Run("DuringOperation", func(t *testing.T) {
			attempts = 0
			ctx, cancel := context.WithCancel(t.Context())
			msg := message.New([]byte("test"))

			// cancel the context after a short delay
			go func() {
				time.Sleep(70 * time.Millisecond)
				cancel()
			}()

			action, err := wrapped(ctx, &msg)

			assert.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
			assert.Contains(t, err.Error(), "context canceled")
			assert.Equal(t, ActionNackRequeue, action)
			assert.Less(t, attempts, 6) // should have stopped before exhausting all retries
		})
	})

	t.Run("WithBackoffMultiplierLessThanOne", func(t *testing.T) {
		// BackoffMultiplier in (0, 1) should be clamped to 1.0 (constant delay)
		attempts := 0
		config := &RetryMiddlewareConfig{
			MaxAttempts:       2,
			MinBackoff:        10 * time.Millisecond,
			BackoffMultiplier: 0.5, // < 1, should be clamped to 1.0
		}
		middleware := RetryMiddleware(config)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			attempts++
			return ActionNackRequeue, errors.New("test error")
		}

		wrapped := middleware(handler)
		msg := message.New([]byte("test"))

		action, err := wrapped(t.Context(), &msg)

		// should exhaust retries with constant 10ms delay
		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)
		assert.Equal(t, 3, attempts) // 1 initial + 2 retries
	})

	t.Run("WithCustomShouldRetry", func(t *testing.T) {
		attempts := 0
		config := &RetryMiddlewareConfig{
			MaxAttempts: 3,
			MinBackoff:  1 * time.Millisecond,
			ShouldRetry: func(err error, msg *message.Message, action Action) bool {
				// only retry on specific action
				return action == ActionNackRequeue
			},
		}
		middleware := RetryMiddleware(config)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			attempts++
			if attempts == 1 {
				return ActionNackDiscard, errors.New("don't retry this")
			}
			return ActionAck, nil
		}

		wrapped := middleware(handler)
		msg := message.New([]byte("test"))

		action, err := wrapped(t.Context(), &msg)

		// should not retry NackDiscard
		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action)
		assert.Equal(t, 1, attempts) // no retries
	})
}

func TestCircuitBreakerMiddleware(t *testing.T) {
	t.Run("ClosedStatePassesThrough", func(t *testing.T) {
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold: 3,
			Cooldown:  time.Hour,
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("ClosedToOpenOpensAfterThreshold", func(t *testing.T) {
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold: 2,
			Cooldown:  time.Hour,
		})
		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		// first two failures reach threshold
		wrapped(t.Context(), &msg)
		action, err := wrapped(t.Context(), &msg)

		// circuit opens on threshold failure
		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)

		// next request is immediately rejected
		action, err = wrapped(t.Context(), &msg)
		assert.Equal(t, ActionNackDiscard, action) // cfg.Action defaults to NackDiscard
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Contains(t, err.Error(), "circuit open")
	})

	t.Run("ClosedSuccessResetsFailureCount", func(t *testing.T) {
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold: 3,
			Cooldown:  time.Hour,
		})
		handlerErr := errors.New("handler error")
		msg := message.New([]byte("test"))

		// fail twice (below threshold)
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))
		wrapped(t.Context(), &msg)
		wrapped(t.Context(), &msg)

		// succeed, resets failure count
		wrappedOk := mw(testHandler(ActionAck))
		wrappedOk(t.Context(), &msg)

		// fail again × 3, should open now (count was reset, not continued from 2)
		wrapped(t.Context(), &msg)
		wrapped(t.Context(), &msg)
		wrapped(t.Context(), &msg)

		// circuit should now be open
		action, err := wrapped(t.Context(), &msg)

		assert.Equal(t, ActionNackDiscard, action) // cfg.Action defaults to NackDiscard
		assert.Contains(t, err.Error(), "circuit open")
	})

	t.Run("OpenToHalfOpenAfterCooldown", func(t *testing.T) {
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    1,
			Cooldown:     20 * time.Millisecond,
			MinSuccesses: 2,
		})
		handlerErr := errors.New("handler error")
		msg := message.New([]byte("test"))

		// open the circuit
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))
		wrapped(t.Context(), &msg)

		// circuit is open
		action, err := wrapped(t.Context(), &msg)
		assert.Contains(t, err.Error(), "circuit open")
		assert.Equal(t, ActionNackDiscard, action) // cfg.Action defaults to NackDiscard

		// wait for cooldown to elapse
		time.Sleep(30 * time.Millisecond)

		// first request after cooldown enters half-open state
		// (still fails because handler returns error)
		action, err = wrapped(t.Context(), &msg)
		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)

		// circuit is open again (failure in half-open)
		action, err = wrapped(t.Context(), &msg)
		assert.Contains(t, err.Error(), "circuit open")
	})

	t.Run("HalfOpenToClosedAfterMinSuccesses", func(t *testing.T) {
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    1,
			Cooldown:     20 * time.Millisecond,
			MinSuccesses: 2,
		})
		handlerErr := errors.New("handler error")
		msg := message.New([]byte("test"))

		// open the circuit
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))
		wrapped(t.Context(), &msg)

		// verify circuit is open
		action, err := wrapped(t.Context(), &msg)
		assert.Contains(t, err.Error(), "circuit open")

		// wait for cooldown -> half-open
		time.Sleep(30 * time.Millisecond)

		// succeed twice to close circuit from half-open
		wrappedOk := mw(testHandler(ActionAck))
		action, err = wrappedOk(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)

		action, err = wrappedOk(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)

		// circuit is now closed, should handle many requests
		for i := 0; i < 10; i++ {
			action, err = wrappedOk(t.Context(), &msg)
			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
		}
	})

	t.Run("HalfOpenToOpenOnFailure", func(t *testing.T) {
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    1,
			Cooldown:     20 * time.Millisecond,
			MinSuccesses: 2,
		})
		handlerErr := errors.New("handler error")
		msg := message.New([]byte("test"))

		// open the circuit
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))
		wrapped(t.Context(), &msg)

		// wait for cooldown -> half-open
		time.Sleep(30 * time.Millisecond)

		// first success in half-open
		wrappedOk := mw(testHandler(ActionAck))
		wrappedOk(t.Context(), &msg)

		// second request fails -> back to open
		action, err := wrapped(t.Context(), &msg)
		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)

		// circuit is open again
		action, err = wrapped(t.Context(), &msg)
		assert.Contains(t, err.Error(), "circuit open")
	})

	t.Run("WithCustomShouldCount", func(t *testing.T) {
		validationErr := errors.New("validation error")
		serviceErr := errors.New("service error")

		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold: 2,
			Cooldown:  time.Hour,
			ShouldCount: func(err error) bool {
				// only count service errors, ignore validation
				return err != nil && !errors.Is(err, validationErr)
			},
		})
		msg := message.New([]byte("test"))

		// validation errors don't count
		wrappedValidation := mw(testErrorHandler(ActionNackDiscard, validationErr))
		for i := 0; i < 10; i++ {
			wrappedValidation(t.Context(), &msg)
		}

		// circuit should still be closed (validation errors ignored)
		wrappedOk := mw(testHandler(ActionAck))
		action, err := wrappedOk(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)

		// service errors do count
		wrappedService := mw(testErrorHandler(ActionNackRequeue, serviceErr))
		wrappedService(t.Context(), &msg)
		action, err = wrappedService(t.Context(), &msg)

		// circuit opens on second service error
		assert.Error(t, err)

		// verify circuit is open (next request rejected immediately)
		action, err = wrappedService(t.Context(), &msg)
		assert.Contains(t, err.Error(), "circuit open")
	})

	t.Run("HalfOpenGatesRequestsBeyondMax", func(t *testing.T) {
		// MaxProbes=1: a concurrent second request while the first is still
		// executing must be rejected with the configured Action
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    1,
			Cooldown:     20 * time.Millisecond,
			MinSuccesses: 10, // high: stay in half-open throughout the test
			MaxProbes:    1,
		})
		msg := message.New([]byte("test"))

		// open the circuit
		wrapped := mw(testErrorHandler(ActionNackRequeue, errors.New("err")))
		wrapped(t.Context(), &msg)

		// wait for cooldown -> half-open on next call
		time.Sleep(30 * time.Millisecond)

		// block the handler so we can verify the gate while it's occupied
		ready := make(chan struct{})
		done := make(chan struct{})
		blockingHandler := mw(func(ctx context.Context, m *message.Message) (Action, error) {
			close(ready)
			<-done
			return ActionAck, nil
		})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			blockingHandler(t.Context(), &msg)
		}()

		// wait until the goroutine is inside the handler (slot taken)
		<-ready

		// second request arrives while slot is occupied, must be rejected
		action, err := blockingHandler(t.Context(), &msg)
		assert.Equal(t, ActionNackDiscard, action) // cfg.Action default
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Contains(t, err.Error(), "circuit half-open")

		close(done)
		wg.Wait()
	})

	t.Run("HalfOpenSlotReleasedAfterHandler", func(t *testing.T) {
		// after a half-open handler returns (success), the slot must be freed so that
		// the next sequential request can enter without being rejected.
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    1,
			Cooldown:     20 * time.Millisecond,
			MinSuccesses: 3, // require 3 successes, stays half-open for first 2
			MaxProbes:    1,
		})
		msg := message.New([]byte("test"))

		// open the circuit
		wrapped := mw(testErrorHandler(ActionNackRequeue, errors.New("err")))
		wrapped(t.Context(), &msg)

		time.Sleep(30 * time.Millisecond)

		wrappedOk := mw(testHandler(ActionAck))

		// first probe: success (successes=1, circuit still half-open)
		action, err := wrappedOk(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)

		// second probe: slot was released, should proceed (successes=2, still half-open)
		action, err = wrappedOk(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)

		// third probe: closes the circuit (successes=3 >= MinSuccesses)
		action, err = wrappedOk(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)

		// now closed: normal operation, no gating
		action, err = wrappedOk(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("HalfOpenMaxRequestsGreaterThanOne", func(t *testing.T) {
		// MaxProbes=2: two concurrent probes are allowed, the third is rejected.
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    1,
			Cooldown:     20 * time.Millisecond,
			MinSuccesses: 10, // stay in half-open
			MaxProbes:    2,
		})
		msg := message.New([]byte("test"))

		// open the circuit
		wrapped := mw(testErrorHandler(ActionNackRequeue, errors.New("err")))
		wrapped(t.Context(), &msg)

		time.Sleep(30 * time.Millisecond)

		// block both goroutines inside the handler
		var entered sync.WaitGroup
		entered.Add(2)
		done := make(chan struct{})
		blockingHandler := mw(func(ctx context.Context, m *message.Message) (Action, error) {
			entered.Done()
			<-done
			return ActionAck, nil
		})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); blockingHandler(t.Context(), &msg) }()
		go func() { defer wg.Done(); blockingHandler(t.Context(), &msg) }()

		// wait for both goroutines to be inside the handler (both slots taken)
		entered.Wait()

		// third request: both slots occupied, must be rejected
		action, err := blockingHandler(t.Context(), &msg)
		assert.Equal(t, ActionNackDiscard, action)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Contains(t, err.Error(), "circuit half-open")

		close(done)
		wg.Wait()
	})

	t.Run("HalfOpenDefaultMaxIsOne", func(t *testing.T) {
		// without setting MaxProbes the default of 1 must be applied.
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    1,
			Cooldown:     20 * time.Millisecond,
			MinSuccesses: 10, // stay in half-open
			// MaxProbes intentionally omitted, must default to 1
		})
		msg := message.New([]byte("test"))

		wrapped := mw(testErrorHandler(ActionNackRequeue, errors.New("err")))
		wrapped(t.Context(), &msg)

		time.Sleep(30 * time.Millisecond)

		ready := make(chan struct{})
		done := make(chan struct{})
		blockingHandler := mw(func(ctx context.Context, m *message.Message) (Action, error) {
			close(ready)
			<-done
			return ActionAck, nil
		})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			blockingHandler(t.Context(), &msg)
		}()

		<-ready // first goroutine holds the sole slot

		// second request must be rejected because the default gate size is 1
		action, err := blockingHandler(t.Context(), &msg)
		assert.Equal(t, ActionNackDiscard, action)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Contains(t, err.Error(), "circuit half-open")

		close(done)
		wg.Wait()
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		// nil config should use all defaults
		mw := CircuitBreakerMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithBadConfig", func(t *testing.T) {
		// zero/negative values should be normalized to defaults
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    0,
			Cooldown:     0,
			MinSuccesses: -1,
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("HalfOpenMultipleSuccessesRequired", func(t *testing.T) {
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold:    1,
			Cooldown:     20 * time.Millisecond,
			MinSuccesses: 3, // require 3 consecutive successes
		})
		handlerErr := errors.New("handler error")
		msg := message.New([]byte("test"))

		// open the circuit
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))
		wrapped(t.Context(), &msg)

		// wait for cooldown
		time.Sleep(30 * time.Millisecond)

		// 2 successes (not enough)
		wrappedOk := mw(testHandler(ActionAck))
		wrappedOk(t.Context(), &msg)
		wrappedOk(t.Context(), &msg)

		// fail on 3rd -> back to open
		_, err := wrapped(t.Context(), &msg)
		assert.Error(t, err)

		// circuit is open
		_, err = wrapped(t.Context(), &msg)
		assert.Contains(t, err.Error(), "circuit open")
	})

	t.Run("WithCustomAction", func(t *testing.T) {
		// explicitly set Action to NackRequeue
		mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
			Threshold: 1,
			Cooldown:  time.Hour,
			Action:    ActionNackRequeue,
		})
		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackDiscard, handlerErr))

		msg := message.New([]byte("test"))
		// open the circuit
		wrapped(t.Context(), &msg)

		// circuit is open: custom action must be returned
		action, err := wrapped(t.Context(), &msg)
		assert.Equal(t, ActionNackRequeue, action)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Contains(t, err.Error(), "circuit open")
	})
}
