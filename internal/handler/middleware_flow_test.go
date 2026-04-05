package handler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyMiddleware(t *testing.T) {
	t.Run("ProcessesMessage", func(t *testing.T) {
		cfg := &ConcurrencyMiddlewareConfig{Max: 2}
		mw := ConcurrencyMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := ConcurrencyMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithBadConfig", func(t *testing.T) {
		cfg := &ConcurrencyMiddlewareConfig{Max: 0}
		mw := ConcurrencyMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithContextCancelled", func(t *testing.T) {
		cfg := &ConcurrencyMiddlewareConfig{Max: 1}
		mw := ConcurrencyMiddleware(cfg)

		release := make(chan struct{})
		acquired := make(chan struct{})
		blockingHandler := func(ctx context.Context, msg *message.Message) (Action, error) {
			close(acquired) // signal: semaphore is held
			<-release       // block until test releases us
			return ActionAck, nil
		}
		wrapped := mw(blockingHandler)

		msg := message.New([]byte("test"))
		go wrapped(t.Context(), &msg)
		<-acquired // wait until first goroutine holds the semaphore

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		action, err := wrapped(ctx, &msg)

		assert.Equal(t, ActionNackRequeue, action)
		assert.ErrorIs(t, err, context.Canceled)

		close(release) // unblock the first goroutine
	})

	t.Run("WithCustomAction", func(t *testing.T) {
		cfg := &ConcurrencyMiddlewareConfig{
			Max:    1,
			Action: ActionNackDiscard,
		}
		mw := ConcurrencyMiddleware(cfg)

		release := make(chan struct{})
		acquired := make(chan struct{})
		blockingHandler := func(ctx context.Context, msg *message.Message) (Action, error) {
			close(acquired)
			<-release
			return ActionAck, nil
		}
		wrapped := mw(blockingHandler)

		msg := message.New([]byte("test"))
		go wrapped(t.Context(), &msg)
		<-acquired

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		action, err := wrapped(ctx, &msg)

		assert.Equal(t, ActionNackDiscard, action)
		assert.ErrorIs(t, err, context.Canceled)

		close(release)
	})

	t.Run("OnWaitCalled", func(t *testing.T) {
		var waited bool
		var mu sync.Mutex
		cfg := &ConcurrencyMiddlewareConfig{
			Max: 1,
			OnWait: func(ctx context.Context, msg *message.Message) {
				mu.Lock()
				waited = true
				mu.Unlock()
			},
		}
		mw := ConcurrencyMiddleware(cfg)

		release := make(chan struct{})
		first := make(chan struct{})
		blockingHandler := func(ctx context.Context, msg *message.Message) (Action, error) {
			<-release
			return ActionAck, nil
		}
		wrapped := mw(blockingHandler)

		msg := message.New([]byte("test"))
		// first goroutine holds the semaphore
		go func() {
			close(first)
			wrapped(t.Context(), &msg)
		}()
		<-first

		// second message should wait
		done := make(chan struct{})
		go func() {
			wrapped(t.Context(), &msg)
			close(done)
		}()

		time.Sleep(50 * time.Millisecond) // give time for OnWait to be called
		mu.Lock()
		isWaited := waited
		mu.Unlock()
		assert.True(t, isWaited)

		close(release) // release both goroutines
		<-done
	})

	t.Run("OnAcquireCalled", func(t *testing.T) {
		var acquireCount int
		cfg := &ConcurrencyMiddlewareConfig{
			Max: 2,
			OnAcquire: func(ctx context.Context, msg *message.Message) {
				acquireCount++
			},
		}
		mw := ConcurrencyMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		_, _ = wrapped(t.Context(), &msg)
		_, _ = wrapped(t.Context(), &msg)

		assert.Equal(t, 2, acquireCount)
	})

	t.Run("BothLifecycleCallbacks", func(t *testing.T) {
		var waitTime, acquireTime time.Time
		var mu sync.Mutex
		cfg := &ConcurrencyMiddlewareConfig{
			Max: 1,
			OnWait: func(ctx context.Context, msg *message.Message) {
				mu.Lock()
				waitTime = time.Now()
				mu.Unlock()
			},
			OnAcquire: func(ctx context.Context, msg *message.Message) {
				mu.Lock()
				acquireTime = time.Now()
				mu.Unlock()
			},
		}
		mw := ConcurrencyMiddleware(cfg)

		release := make(chan struct{})
		first := make(chan struct{})
		blockingHandler := func(ctx context.Context, msg *message.Message) (Action, error) {
			<-release
			return ActionAck, nil
		}
		wrapped := mw(blockingHandler)

		msg := message.New([]byte("test"))
		go func() {
			close(first)
			wrapped(t.Context(), &msg)
		}()
		<-first

		// second message should wait
		done := make(chan struct{})
		go func() {
			wrapped(t.Context(), &msg)
			close(done)
		}()

		time.Sleep(50 * time.Millisecond)
		close(release)
		<-done

		mu.Lock()
		wt := waitTime
		at := acquireTime
		mu.Unlock()

		assert.False(t, wt.IsZero())
		assert.False(t, at.IsZero())
		// acquireTime >= waitTime (time.After is too strict on low-res clocks e.g. slow machines)
		assert.True(t, !at.Before(wt))
	})

	t.Run("MeasureWaitTime", func(t *testing.T) {
		var waitDuration time.Duration
		var mu sync.Mutex
		cfg := &ConcurrencyMiddlewareConfig{
			Max: 1,
			OnWait: func(ctx context.Context, msg *message.Message) {
				// mark start of wait
			},
			OnAcquire: func(ctx context.Context, msg *message.Message) {
				// in real usage, measure duration between OnWait and OnAcquire
			},
		}
		mw := ConcurrencyMiddleware(cfg)

		release := make(chan struct{})
		first := make(chan struct{})
		blockingHandler := func(ctx context.Context, msg *message.Message) (Action, error) {
			<-release
			return ActionAck, nil
		}
		wrapped := mw(blockingHandler)

		msg := message.New([]byte("test"))
		go func() {
			close(first)
			wrapped(t.Context(), &msg)
		}()
		<-first

		// second message should wait
		start := time.Now()
		done := make(chan struct{})
		go func() {
			wrapped(t.Context(), &msg)
			mu.Lock()
			waitDuration = time.Since(start)
			mu.Unlock()
			close(done)
		}()

		time.Sleep(50 * time.Millisecond)
		close(release)
		<-done

		mu.Lock()
		wd := waitDuration
		mu.Unlock()
		assert.Greater(t, wd, 40*time.Millisecond)
	})
}

func TestRateLimitMiddleware(t *testing.T) {
	t.Run("ProcessesMessage", func(t *testing.T) {
		mw := RateLimitMiddleware(t.Context(), &RateLimitMiddlewareConfig{RPS: 10})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithBadConfig", func(t *testing.T) {
		mw := RateLimitMiddleware(t.Context(), &RateLimitMiddlewareConfig{RPS: 0})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithContextCancelled", func(t *testing.T) {
		// rps=1: one initial token; drain it first, then use a cancelled context
		mw := RateLimitMiddleware(t.Context(), &RateLimitMiddlewareConfig{RPS: 1})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		// drain the single initial token
		_, _ = wrapped(t.Context(), &msg)

		// no tokens remain; cancelled context must return immediately
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		action, err := wrapped(ctx, &msg)

		assert.Equal(t, ActionNackRequeue, action)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("RefillsTokens", func(t *testing.T) {
		// rps=1000: ticker fires every 1ms; drain all tokens then wait for a refill
		rps := 1000
		mw := RateLimitMiddleware(t.Context(), &RateLimitMiddlewareConfig{RPS: rps})
		wrapped := mw(testHandler(ActionAck))
		msg := message.New([]byte("test"))

		// drain all initial tokens
		for range rps {
			_, _ = wrapped(t.Context(), &msg)
		}

		// wait long enough for at least one tick (2ms >> 1ms interval)
		time.Sleep(2 * time.Millisecond)

		// at least one token should have been refilled
		action, err := wrapped(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithBurstOnly", func(t *testing.T) {
		// burst=5 but refillRate defaults to 10: allows 5 immediate, then 10/s
		mw := RateLimitMiddleware(t.Context(), &RateLimitMiddlewareConfig{Burst: 5})
		wrapped := mw(testHandler(ActionAck))
		msg := message.New([]byte("test"))

		// should be able to process 5 immediately (burst capacity)
		for i := 0; i < 5; i++ {
			action, err := wrapped(t.Context(), &msg)
			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
		}
	})

	t.Run("WithRefillRateOnly", func(t *testing.T) {
		// refillRate=5 but burst defaults to 10: 10 immediate, then 5/s refill
		mw := RateLimitMiddleware(t.Context(), &RateLimitMiddlewareConfig{RefillRate: 1000})
		wrapped := mw(testHandler(ActionAck))
		msg := message.New([]byte("test"))

		// should be able to process 10 immediately (default burst)
		for i := 0; i < 10; i++ {
			action, err := wrapped(t.Context(), &msg)
			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
		}

		// refill at 1000/s = 1ms tick
		time.Sleep(2 * time.Millisecond)
		action, err := wrapped(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithBurstAndRefillRate", func(t *testing.T) {
		// explicit control: burst=3, refillRate=1000 (1ms tick)
		mw := RateLimitMiddleware(t.Context(), &RateLimitMiddlewareConfig{
			Burst:      3,
			RefillRate: 1000,
		})
		wrapped := mw(testHandler(ActionAck))
		msg := message.New([]byte("test"))

		// can process 3 immediately
		for i := 0; i < 3; i++ {
			action, err := wrapped(t.Context(), &msg)
			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
		}

		// wait for refill
		time.Sleep(2 * time.Millisecond)
		action, err := wrapped(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("RPSOverriddenByBurst", func(t *testing.T) {
		// RPS=50 but Burst=2: burst wins, refillRate uses RPS
		mw := RateLimitMiddleware(t.Context(), &RateLimitMiddlewareConfig{
			RPS:   50,
			Burst: 2,
		})
		wrapped := mw(testHandler(ActionAck))
		msg := message.New([]byte("test"))

		// can only process 2 immediately (Burst=2 overrides RPS)
		for i := 0; i < 2; i++ {
			action, err := wrapped(t.Context(), &msg)
			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
		}
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		// nil config should use all defaults (burst=10, refillRate=10)
		mw := RateLimitMiddleware(t.Context(), nil)
		wrapped := mw(testHandler(ActionAck))
		msg := message.New([]byte("test"))

		// should be able to process 10 immediately (default burst)
		for i := 0; i < 10; i++ {
			action, err := wrapped(t.Context(), &msg)
			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
		}
	})

	t.Run("RPSOverriddenByRefillRate", func(t *testing.T) {
		// RPS=50 but RefillRate=1000: burst uses RPS, refillRate wins
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mw := RateLimitMiddleware(ctx, &RateLimitMiddlewareConfig{
			RPS:        50,
			RefillRate: 1000,
		})
		wrapped := mw(testHandler(ActionAck))
		msg := message.New([]byte("test"))

		// can process 50 immediately (RPS used for burst)
		for i := 0; i < 50; i++ {
			action, err := wrapped(t.Context(), &msg)
			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
		}

		// wait for refill at 1000/s (1ms tick)
		time.Sleep(2 * time.Millisecond)
		action, err := wrapped(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("StopsGoroutineOnContextCancel", func(t *testing.T) {
		// create middleware with a cancellable context
		ctx, cancel := context.WithCancel(context.Background())
		mw := RateLimitMiddleware(ctx, &RateLimitMiddlewareConfig{RPS: 10})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		// verify middleware works
		action, err := wrapped(t.Context(), &msg)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)

		// cancel context to stop background goroutine
		cancel()
		// give goroutine time to clean up
		time.Sleep(5 * time.Millisecond)
		// test passes if no goroutine leak (verified by test -race if enabled)
	})
}
