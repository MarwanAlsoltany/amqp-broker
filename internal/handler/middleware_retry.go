package handler

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// RetryMiddlewareConfig configures automatic retry behavior for failed handlers.
// Failed handlers are retried with exponential backoff up to MaxAttempts.
type RetryMiddlewareConfig struct {
	// MaxAttempts is the maximum number of retry attempts (default: 3).
	MaxAttempts int
	// MinBackoff is the initial backoff duration between retries (default: 1s).
	// A random jitter (0-25%) is added to each delay to prevent thundering herd issues.
	MinBackoff time.Duration
	// MaxBackoff caps the backoff growth when BackoffMultiplier > 1 (default: 30s).
	MaxBackoff time.Duration
	// BackoffMultiplier grows the delay on each retry (default: 2.0 = exponential backoff).
	// Values in (0, 1) are clamped to 1.0 (constant delay). MaxBackoff caps the maximum.
	BackoffMultiplier float64
	// ShouldRetry determines whether to retry after a handler invocation.
	// If nil, a default is used (retries on any non-nil error, regardless of message/action).
	ShouldRetry func(error, *message.Message, Action) bool
}

// RetryMiddleware creates a middleware that retries failed handlers.
// It is a post-handler middleware: evaluates the handler result and retries on retriable errors.
// It implements exponential backoff between retry attempts.
//
// The middleware only retries if the handler returns [ActionNackRequeue].
// Other actions (Ack, NackDiscard, NoAction) are returned immediately.
//
// Example:
//
//	mw := RetryMiddleware(&RetryMiddlewareConfig{
//		MaxAttempts: 3,
//		MinBackoff: 100 * time.Millisecond, // initial
//		MaxBackoff: 10 * time.Second,
//		ShouldRetry: func(err error) bool {
//			// only retry transient errors
//			return isTransientError(err)
//		},
//	})
func RetryMiddleware(cfg *RetryMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &RetryMiddlewareConfig{}
	}

	if cfg.MaxAttempts <= 0 {
		// default to 3 retries (4 total invocations)
		cfg.MaxAttempts = 3
	}
	if cfg.MinBackoff <= 0 {
		// default 1s: good enough for reasonable recovery time
		cfg.MinBackoff = time.Second
	}
	if cfg.MaxBackoff <= 0 {
		// default to 30s: prevent excessively long waits
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.BackoffMultiplier <= 0 {
		// default to 2.0: standard exponential backoff (doubling), exponential is preferable
		// over constant delay since it adapts better to varying error durations
		cfg.BackoffMultiplier = 2.0
	} else if cfg.BackoffMultiplier < 1 {
		// clamp (0,1) to 1.0: shrinking delays make no sense for retry backoff
		cfg.BackoffMultiplier = 1.0
	}
	if cfg.ShouldRetry == nil {
		// default to retrying on any error, regardless of message/action
		cfg.ShouldRetry = func(err error, _ *message.Message, _ Action) bool { return err != nil }
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			var action Action
			var err error
			delay := cfg.MinBackoff

			for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
				// check if context is already cancelled
				select {
				case <-ctx.Done():
					return ActionNackRequeue, fmt.Errorf("%w: %w", ErrMiddleware, ctx.Err())
				default:
				}

				action, err = next(ctx, msg)

				// success: return immediately
				if !cfg.ShouldRetry(err, msg, action) {
					return action, err
				}

				// last attempt: return as-is
				if attempt == cfg.MaxAttempts {
					break
				}

				// add +0-25% jitter to prevent thundering herd
				// on retries when many messages fail simultaneously
				backoff := delay + time.Duration(rand.Int64N(int64(delay/4)))
				// wait before retry (with context cancellation check)
				select {
				case <-ctx.Done():
					return ActionNackRequeue, fmt.Errorf("%w: %w", ErrMiddleware, ctx.Err())
				case <-time.After(backoff):
				}

				// exponential backoff
				if cfg.BackoffMultiplier > 1 {
					delay = min(time.Duration(float64(delay)*cfg.BackoffMultiplier), cfg.MaxBackoff)
				}
			}

			return action, err
		}
	}
}

// CircuitBreakerMiddlewareConfig configures circuit breaker behavior.
// Opens the circuit after consecutive failures, preventing handler execution.
type CircuitBreakerMiddlewareConfig struct {
	// Threshold is the number of consecutive failures before opening the circuit (default: 5).
	Threshold int
	// Cooldown is how long the circuit stays open before entering half-open state (default: 30s).
	Cooldown time.Duration
	// MinSuccesses is the number of consecutive successes in half-open state required
	// to close the circuit (default: 2).
	MinSuccesses int
	// MaxProbes is the maximum number of requests allowed through concurrently
	// while the circuit is in half-open state (default: 1). Requests beyond this limit are
	// rejected immediately with the configured Action, just like in the open state.
	// This prevents a thundering herd from overwhelming a recovering backend: only a
	// controlled number of probe messages are forwarded to the handler at a time.
	MaxProbes int
	// ShouldCount determines if an error should count as a failure. If nil, all errors count.
	// Use this to ignore client errors (validation, etc.) that shouldn't trip the circuit.
	ShouldCount func(error) bool
	// Action determines the handler action when the circuit is open or half-open gate is full
	// (default: [ActionNackDiscard]).
	// [ActionAck] would acknowledge messages without processing, which may be desirable in
	// some cases but can lead to silent message loss if the circuit is open for an extended period.
	// [ActionNackDiscard] routes messages to the dead-letter queue (if configured).
	// [ActionNackRequeue] requeues, but beware: the circuit may still be open on redelivery.
	Action Action
}

// CircuitBreakerMiddleware creates a middleware that implements circuit breaker pattern.
// It is a pre+post-handler middleware: checks the circuit state before calling the handler, updates it after.
// It opens the circuit after consecutive failures, preventing handler execution.
//
// Circuit states:
//   - Closed: Normal operation, all messages are processed
//   - Open: Circuit is open, messages are rejected immediately
//   - Half-Open: Testing recovery, at most MaxProbes concurrent messages processed
//
// Circuit state transitions:
//   - Closed -> Open: after Threshold consecutive failures
//   - Open -> HalfOpen: after Cooldown period
//   - HalfOpen -> Closed: after MinSuccesses consecutive successes
//   - HalfOpen -> Open: on any failure
//
// In half-open state the circuit acts as a controlled gate: only MaxProbes
// messages are forwarded to the handler concurrently. All other arriving messages are
// rejected with the configured Action until a slot is freed. This prevents a burst of
// traffic from swamping a backend that is still recovering.
//
// Example:
//
//	mw := CircuitBreakerMiddleware(&CircuitBreakerMiddlewareConfig{
//		Threshold:    5,                // open after 5 consecutive failures
//		Cooldown:     30 * time.Second, // stay open for 30s
//		MinSuccesses: 2,                // require 2 successes to close
//		MaxProbes:    1,                // allow 1 probe at a time (default)
//	})
func CircuitBreakerMiddleware(cfg *CircuitBreakerMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &CircuitBreakerMiddlewareConfig{}
	}

	if cfg.Threshold <= 0 {
		// default: 5 consecutive failures opens the circuit
		cfg.Threshold = 5
	}
	if cfg.Cooldown <= 0 {
		// default: 30s cooldown before testing recovery
		cfg.Cooldown = 30 * time.Second
	}
	if cfg.MinSuccesses <= 0 {
		// default: 2 consecutive successes closes the circuit from half-open
		cfg.MinSuccesses = 2
	}
	if cfg.MaxProbes <= 0 {
		// default: 1 concurrent probe at a time in half-open state (classic CB behavior)
		cfg.MaxProbes = 1
	}
	if cfg.ShouldCount == nil {
		// default: all errors count as failures
		cfg.ShouldCount = func(err error) bool { return err != nil }
	}
	if cfg.Action == 0 {
		// default: discard when circuit is open to avoid requeue loops
		cfg.Action = ActionNackDiscard
	}

	type circuitState int

	const (
		circuitClosed   circuitState = iota // normal operation
		circuitOpen                         // rejecting requests
		circuitHalfOpen                     // testing recovery
	)

	var (
		mu               sync.Mutex
		state            circuitState
		failures         int
		successes        int
		halfOpenInFlight int
		openUntil        time.Time
	)

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			mu.Lock()

			tookHalfOpenSlot := false

			switch state {
			case circuitOpen:
				// check if cooldown has elapsed
				if time.Now().After(openUntil) {
					// transition to half-open, reset counters
					state = circuitHalfOpen
					successes = 0
					halfOpenInFlight = 0
					// take the first probe slot (count was just reset so this always succeeds)
					halfOpenInFlight++
					tookHalfOpenSlot = true
					mu.Unlock()
				} else {
					// still open, reject immediately
					mu.Unlock()
					return cfg.Action, fmt.Errorf("%w: circuit open", ErrMiddleware)
				}
			case circuitHalfOpen:
				// gate: only allow MaxProbes concurrent probes through
				if halfOpenInFlight >= cfg.MaxProbes {
					mu.Unlock()
					return cfg.Action, fmt.Errorf("%w: circuit half-open", ErrMiddleware)
				}
				halfOpenInFlight++
				tookHalfOpenSlot = true
				mu.Unlock()
			case circuitClosed:
				// normal operation
				mu.Unlock()
			}

			// execute handler
			action, err := next(ctx, msg)

			// update state based on result
			mu.Lock()
			defer mu.Unlock()

			// release the half-open probe slot before evaluating the result
			if tookHalfOpenSlot {
				halfOpenInFlight--
			}

			if cfg.ShouldCount(err) {
				// failure
				if state == circuitHalfOpen {
					// any failure in half-open -> back to open
					state = circuitOpen
					openUntil = time.Now().Add(cfg.Cooldown)
					failures = 0
					successes = 0
				} else if state == circuitClosed {
					// track consecutive failures in closed state
					failures++
					successes = 0
					if failures >= cfg.Threshold {
						// threshold reached -> open circuit
						state = circuitOpen
						openUntil = time.Now().Add(cfg.Cooldown)
						failures = 0
					}
				}
			} else {
				// success
				failures = 0
				if state == circuitHalfOpen {
					// track consecutive successes in half-open
					successes++
					if successes >= cfg.MinSuccesses {
						// enough successes -> close circuit
						state = circuitClosed
						successes = 0
					}
				} else if state == circuitClosed {
					// just reset success counter in closed state
					successes = 0
				}
			}

			return action, err
		}
	}
}
