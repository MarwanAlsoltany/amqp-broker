package endpoint

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// endpointLifecycle defines the internal lifecycle management methods for an endpoint.
// This interface is implemented by types embedding [endpoint] (e.g. [publisher], [consumer]).
type endpointLifecycle interface {
	// init validates options and launches the connection management loop.
	// It must call [endpoint.start] passing itself as the lifecycle implementation.
	init(ctx context.Context) error

	// connect establishes a connection and opens a channel. It should be idempotent
	// and thread-safe. Modifications to endpoint state must be done under stateMu.
	connect(ctx context.Context) error

	// disconnect closes the channel and releases resources. It should be idempotent
	// and thread-safe. Modifications to endpoint state must be done under stateMu.
	disconnect(ctx context.Context) error

	// monitor blocks until the connection/channel is closed or ctx is cancelled.
	// It should be idempotent and thread-safe.
	monitor(ctx context.Context) error
}

// endpointLifecycleFunc is a function type for endpoint lifecycle operations.
type endpointLifecycleFunc func(context.Context) error

// start launches the endpoint's connection management loop and optionally waits for readiness.
//
// If NoWaitReady is set, start returns immediately after launching the background goroutine.
// Otherwise it blocks until the endpoint is ready, a timeout occurs, or an error is returned.
func (e *endpoint) start(ctx context.Context, el endpointLifecycle, onError func(error)) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// fast-path: already ready
	if e.ready.Load() {
		return nil
	}

	noWaitReady := e.opts.NoWaitReady

	// prepare readiness wait context with optional timeout
	timeout := e.opts.ReadyTimeout
	waitCtx := ctx
	if timeout > 0 {
		var waitCancel context.CancelFunc
		waitCtx, waitCancel = context.WithTimeoutCause(ctx, timeout, ErrEndpointNotReadyTimeout)
		defer waitCancel()
	}

	// prepare cancellable lifecycle context; chain-call previous cancel if any
	lifecycleCtx, lifecycleCancel := context.WithCancel(ctx)
	e.stateMu.Lock()
	oldCancel := e.cancel
	resetCancel := func() {
		e.stateMu.Lock()
		e.cancel = oldCancel
		e.stateMu.Unlock()
	}
	e.cancel = func() {
		if oldCancel != nil {
			oldCancel()
		}
		lifecycleCancel()
	}
	e.stateMu.Unlock()

	if noWaitReady {
		go func() {
			defer resetCancel()

			err := e.run(lifecycleCtx, el.connect, el.disconnect, el.monitor)
			if err != nil && onError != nil {
				onError(err)
			}
		}()
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		defer resetCancel()
		errCh <- e.run(lifecycleCtx, el.connect, el.disconnect, el.monitor)
	}()

	for {
		// fast-path: check ready before blocking
		if e.ready.Load() {
			return nil
		}

		e.stateMu.RLock()
		readyCh := e.readyCh
		e.stateMu.RUnlock()

		// race-catcher: re-check after acquiring readyCh to avoid blocking on an
		// already-notified channel if run() signalled readiness between the checks (TOC/TOU case)
		if e.ready.Load() {
			return nil
		}

		select {
		case <-readyCh:
			return nil
		case err := <-errCh:
			return err
		case <-waitCtx.Done():
			return waitCtx.Err()
		}
	}
}

// run manages the connection lifecycle, handling automatic reconnection and readiness signaling.
//
// It loops calling connect -> mark ready -> monitor -> disconnect, retrying with exponential
// backoff when auto-reconnect is enabled. Context cancellation stops the loop immediately.
func (e *endpoint) run(
	ctx context.Context,
	connect endpointLifecycleFunc,
	disconnect endpointLifecycleFunc,
	monitor endpointLifecycleFunc,
) error {
	autoReconnect := !e.opts.NoAutoReconnect
	reconnectMin := e.opts.ReconnectMin
	reconnectMax := e.opts.ReconnectMax
	if reconnectMin <= 0 {
		reconnectMin = DefaultReconnectMin
	}
	if reconnectMax < reconnectMin {
		reconnectMax = reconnectMin
	}

	attempts := 0
	delay := reconnectMin

	e.stateMu.Lock()
	if e.readyCh == nil {
		e.readyCh = make(chan struct{})
	}
	e.stateMu.Unlock()

	for {
		attempts++

		if e.closed.Load() {
			return ErrEndpointClosed
		}

		err := connect(ctx)

		if err != nil {
			// honor context cancellation without backoff
			if errors.Is(err, context.Canceled) {
				return err
			}

			if !autoReconnect {
				return fmt.Errorf("%w: %w", ErrEndpointNoAutoReconnect, err)
			}

			// fail-fast on first attempt so the initial error surfaces immediately
			// rather than the caller seeing "timeout exceeded"
			if attempts == 1 {
				return err
			}

			backoff := delay + time.Duration(rand.Int64N(int64(delay/4))) // +0-25% jitter

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				delay = min(delay*2, reconnectMax)
				continue
			}
		}

		// mark ready and signal waiters
		e.stateMu.Lock()
		e.ready.Store(true)
		if e.readyCh != nil {
			close(e.readyCh)
			e.readyCh = make(chan struct{}) // recreate for next cycle
		}
		e.stateMu.Unlock()

		delay = reconnectMin // reset backoff on successful connection

		err = monitor(ctx)

		e.ready.Store(false)

		if errors.Is(err, context.Canceled) {
			_ = disconnect(ctx)
			return err
		}

		_ = disconnect(ctx)

		if !autoReconnect {
			if err != nil {
				return fmt.Errorf("%w: %w", ErrEndpointNoAutoReconnect, err)
			}
			return nil
		}

		backoff := delay + time.Duration(rand.Int64N(int64(delay/4))) // +0-25% jitter

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			delay = min(delay*2, reconnectMax)
		}
	}
}
