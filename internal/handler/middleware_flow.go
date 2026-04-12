package handler

import (
	"context"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// ConcurrencyMiddlewareConfig configures concurrency limiting for handlers.
// Limits the number of handlers executing concurrently using a semaphore.
type ConcurrencyMiddlewareConfig struct {
	// Max concurrent handlers (default: 1).
	Max int
	// Action determines the handler action when context is cancelled while waiting
	// for a semaphore slot (default: [ActionNackRequeue]).
	Action Action
	// OnWait is called before blocking when no semaphore slots are available.
	// Use for metrics/logging when messages must wait for concurrency limit.
	OnWait func(ctx context.Context, msg *message.Message)
	// OnAcquire is called after acquiring a semaphore slot, before handler execution.
	// Called regardless of whether the slot was acquired immediately or after waiting.
	// To measure wait time, record a timestamp in [ConcurrencyMiddlewareConfig.OnWait] and compute elapsed time here.
	OnAcquire func(ctx context.Context, msg *message.Message)
}

// ConcurrencyMiddleware creates a middleware that limits concurrent handlers.
// It is a pre+post-handler middleware: acquires a concurrency slot before calling the handler, releases it after.
// It uses a semaphore to restrict the number of handlers executing simultaneously.
//
// This is useful for:
//   - Limiting resource usage (DB connections, file handles, etc.)
//   - Controlling memory consumption during message processing
//   - Preventing thundering herd scenarios
//
// Example:
//
//	mw := ConcurrencyMiddleware(&ConcurrencyMiddlewareConfig{
//		Max: 10,  // Max 10 concurrent handlers
//	})
func ConcurrencyMiddleware(cfg *ConcurrencyMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &ConcurrencyMiddlewareConfig{}
	}

	max := cfg.Max
	if max <= 0 {
		max = 1
	}

	action := cfg.Action
	if action == 0 {
		action = ActionNackRequeue
	}

	semaphore := make(chan struct{}, max)

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			// check if we need to wait
			select {
			case semaphore <- struct{}{}:
				// acquired immediately, no wait
				if cfg.OnAcquire != nil {
					cfg.OnAcquire(ctx, msg)
				}
			default:
				// need to wait for slot
				if cfg.OnWait != nil {
					cfg.OnWait(ctx, msg)
				}
				select {
				case <-ctx.Done():
					return action, ErrMiddleware.Detailf("%w", ctx.Err())
				case semaphore <- struct{}{}:
					if cfg.OnAcquire != nil {
						cfg.OnAcquire(ctx, msg)
					}
				}
			}
			defer func() { <-semaphore }()
			return next(ctx, msg)
		}
	}
}

// RateLimitMiddlewareConfig configures rate limiting for message processing.
// Limits the rate at which handlers are invoked using a token bucket algorithm.
type RateLimitMiddlewareConfig struct {
	// RPS (requests per second) is a convenience field that sets both Burst and RefillRate
	// to the same value. Explicit Burst/RefillRate override this if provided.
	RPS int
	// Burst is the bucket capacity (max messages that can be processed immediately).
	// Overrides RPS if > 0. Default: 10.
	Burst int
	// RefillRate is how many tokens are added per second (sustained throughput limit).
	// Overrides RPS if > 0. Default: 10.
	RefillRate int
}

// RateLimitMiddleware creates a middleware that limits message processing rate.
// It is a pre-handler middleware: waits for a rate token before calling the handler.
// It uses a token bucket algorithm to control the rate of handler invocations.
//
// The context parameter is used to stop the rate limiter when the consumer is closed.
// The rate and burst parameters control throughput:
//   - Rate: tokens added per second (messages/second)
//   - Burst: maximum tokens in bucket (burst capacity)
//
// Example:
//
//	mw := RateLimitMiddleware(ctx, &RateLimitMiddlewareConfig{
//		Rate:  10.0,  // 10 messages per second
//		Burst: 20,    // Allow bursts up to 20 messages
//	})
func RateLimitMiddleware(ctx context.Context, cfg *RateLimitMiddlewareConfig) Middleware {
	if cfg == nil {
		cfg = &RateLimitMiddlewareConfig{}
	}

	// precedence: start with RPS for both, then apply explicit overrides, then defaults
	burst, refillRate := cfg.RPS, cfg.RPS
	if cfg.Burst > 0 {
		burst = cfg.Burst
	}
	if cfg.RefillRate > 0 {
		refillRate = cfg.RefillRate
	}
	if burst <= 0 {
		// default burst capacity: allows 10 immediate requests
		burst = 10
	}
	if refillRate <= 0 {
		// default refill rate: 10 tokens/second steady-state throughput
		refillRate = 10
	}

	tokens := make(chan struct{}, burst)
	for range burst {
		tokens <- struct{}{}
	}
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(refillRate))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			select {
			case <-ctx.Done():
				return ActionNackRequeue, ErrMiddleware.Detailf("%w", ctx.Err())
			case <-tokens:
			}
			return next(ctx, msg)
		}
	}
}
