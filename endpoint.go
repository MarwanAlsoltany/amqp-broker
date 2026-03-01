package broker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Endpoint defines the common interface for AMQP publishers and consumers, providing access
// to the underlying connection, channel, and lifecycle management operations.
//
// Ownership & Lifecycle:
//   - Each Endpoint owns a dedicated connection and channel to the AMQP broker.
//   - Supports automatic reconnection and resource management.
//   - Provides methods to declare exchanges, queues, and bindings on the endpoint's channel.
//   - Use [Endpoint.Close] to stop the endpoint and release resources.
//   - Use [Endpoint.Release] to stop the endpoint and remove it from the broker registry.
//
// Concurrency:
//   - All methods are safe for concurrent use unless otherwise specified.
//   - Connection and channel references may be nil if not connected or closed.
//
// Usage Patterns:
//   - Use [Endpoint.Connection] and [Endpoint.Channel] to access the underlying AMQP resources.
//   - Use [Endpoint.Ready] to check if the endpoint is ready for operations.
//   - Use [Endpoint.Exchange], [Endpoint.Queue], and [Endpoint.Binding] to declare topology as needed.
//   - Use [Endpoint.Close] or [Endpoint.Release] for cleanup and resource management.
//
// See [EndpointOptions] for configuration details shared by publishers and consumers.
type Endpoint interface {
	// Connection returns the current connection (may be nil if not connected).
	Connection() *Connection
	// Channel returns the current channel (may be nil if not connected or closed).
	Channel() *Channel
	// Ready indicates whether the endpoint is ready for operations.
	Ready() bool
	// Exchange declares an exchange on the endpoint's channel.
	Exchange(Exchange) error
	// Queue declares a queue on the endpoint's channel.
	Queue(Queue) error
	// Binding declares a binding on the endpoint's channel.
	Binding(Binding) error
	// Close stops the endpoint and releases resources.
	Close() error
	// Release removes the endpoint from the broker's registry and closes it.
	Release()
}

// EndpointOptions holds common reconnection and readiness configuration
// for endpoints (publishers/consumers).
type EndpointOptions struct {
	// NoAutoDeclare disables automatic declaration of topology (exchanges, queues, and bindings) for an endpoint.
	// This setting is relevant for publishers and consumers that want to manage topology declaration manually
	// by declaring on demand or by relying on external side effects for declaration.
	// Default: false (automatic declaration enabled)
	//
	// Note: If NoAutoDeclare is set to true, the caller must ensure that the necessary exchanges, queues,
	// and bindings are declared for an endpoint before the endpoint attempts to use them. Failure to do so
	// may result in errors during connection (and when publishing or consuming messages) due to missing topology.
	NoAutoDeclare bool
	// NoAutoReconnect disables automatic reconnection on connection failure.
	// Default: false
	NoAutoReconnect bool
	// ReconnectMin is the minimum delay between reconnection attempts.
	// Only used if [EndpointOptions.NoAutoReconnect] is false.
	// Default: defaultReconnectMin (500ms)
	ReconnectMin time.Duration
	// ReconnectMax is the maximum delay between reconnection attempts.
	// Only used if [EndpointOptions.NoAutoReconnect] is false.
	// Default: defaultReconnectMax (30s)
	ReconnectMax time.Duration
	// NoWaitReady prevents new publisher from blocking until is's connected.
	// Default: false
	NoWaitReady bool
	// ReadyTimeout is the maximum time to wait for the publisher to be ready.
	// Only used if [EndpointOptions.NoWaitReady] is false.
	// Default: defaultReadyTimeout (10s)
	ReadyTimeout time.Duration
}

// endpointRole indicates the intended use of a connection.
type endpointRole uint8

const (
	// roleController indicates the connection is for control operations (topology management).
	roleController endpointRole = iota
	// rolePublisher indicates the connection is for publishers.
	rolePublisher
	// roleConsumer indicates the connection is for consumers.
	roleConsumer
)

// endpoint provides common lifecycle management for publishers and consumers.
// It provides a baseline implementation of [Endpoint] interface.
type endpoint struct {
	_ noCopy // prevents accidental copying (go vet)

	id   string
	role endpointRole
	opts EndpointOptions

	// broker reference for proxied operations
	broker *Broker

	// amqp resources
	conn *Connection
	ch   *Channel

	// synchronization mutex for internal state
	stateMu sync.RWMutex
	// readiness state
	// readyCh is closed (in run) to notify waiters that the endpoint became ready,
	// it is recreated immediately after closing, the atomic `ready` is authoritative
	readyCh chan struct{}
	ready   atomic.Bool
	// closed state
	closed atomic.Bool
	// cancel func for the endpoint's long-lived context(s)
	// if reset, the setter has to make sure to chain-call the previous value (if any)
	cancel context.CancelFunc
}

var _ Endpoint = (*endpoint)(nil)

// newEndpoint creates a new endpoint with the given ID, broker, role, and options.
func newEndpoint(id string, b *Broker, role endpointRole, opts EndpointOptions) *endpoint {
	if role != roleController && role != rolePublisher && role != roleConsumer {
		role = roleController // default to control role
	}

	return &endpoint{
		id:      id,
		broker:  b,
		role:    role,
		opts:    opts,
		readyCh: make(chan struct{}),
	}
}

// Connection returns the current connection (may be nil).
func (e *endpoint) Connection() *Connection {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	if e.conn != nil && e.conn.IsClosed() {
		return nil
	}
	return e.conn
}

// Channel returns the current channel (may be nil).
func (e *endpoint) Channel() *Channel {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	if e.ch != nil && e.ch.IsClosed() {
		return nil
	}
	return e.ch
}

// Ready indicates whether the endpoint is ready for operations.
func (e *endpoint) Ready() bool {
	// NOTE: [internal, doesn't affect the public API]
	// calling Ready() concurrently with run() or directly after it, may lead it to
	// not yielding the expected result, this is not a race, and it is not an atomic
	// failure, this is scheduler timing / memory visibility semantics of atomic operations
	return e.ready.Load()
}

// Close stops the endpoint and releases resources.
func (e *endpoint) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}

	// close the channel to stop all operations
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	// cancel lifecycle to stop run() if it's running
	if e.cancel != nil {
		e.cancel()
		e.cancel = nil
	}

	if e.ch != nil {
		err := e.ch.Close()
		e.ch = nil
		if err != nil {
			return fmt.Errorf("%w: close failed: %w", ErrEndpoint, err)
		}
	}

	return nil
}

// Release removes the endpoint from the broker's registry and closes it.
func (e *endpoint) Release() {
	// added to satisfy interface, implementation deferred to embedding types
	panic("not implemented")
}

// Exchange declares an exchange on the endpoint's channel.
func (e *endpoint) Exchange(exchange Exchange) error {
	ch := e.Channel()
	if ch == nil {
		return ErrEndpointNotConnected
	}

	return e.broker.topologyMgr.declare(ch, &Topology{
		Exchanges: []Exchange{exchange},
	})
}

// Queue declares a queue on the endpoint's channel.
func (e *endpoint) Queue(queue Queue) error {
	ch := e.Channel()
	if ch == nil {
		return ErrEndpointNotConnected
	}

	return e.broker.topologyMgr.declare(ch, &Topology{
		Queues: []Queue{queue}},
	)
}

// Binding declares a binding on the endpoint's channel.
func (e *endpoint) Binding(binding Binding) error {
	ch := e.Channel()
	if ch == nil {
		return ErrEndpointNotConnected
	}

	return e.broker.topologyMgr.declare(ch, &Topology{
		Bindings: []Binding{binding}},
	)
}

// endpointLifecycle defines the internal lifecycle management methods for an endpoint.
// This interface must be implemented by types embedding the base endpoint struct and
// implementing the Endpoint interface.
type endpointLifecycle interface {
	// init launches the endpoint's connection management loop and optionally waits for readiness.
	// This method must call [endpoint.start] and pass the [endpointLifecycle] (self) implementation as parameter.
	init(ctx context.Context) error

	// connect establishes a connection to the AMQP broker and opens a channel. It should be idempotent
	// and thread-safe. Modifications to the endpoint's state (fields) must be done under stateMu lock.
	connect(ctx context.Context) error

	// disconnect closes the connection to the AMQP broker and releases resources.
	// It should clean up connection and channel resources safely. It should be idempotent
	// and thread-safe. Modifications to the endpoint's state (fields) must be done under stateMu lock.
	disconnect(ctx context.Context) error

	// monitor monitors the connection to the AMQP broker and handles reconnection.
	// It should block until the connection/channel is closed or context is canceled. It should be idempotent
	// and thread-safe. Modifications to the endpoint's state (fields) must be done under stateMu lock.
	monitor(ctx context.Context) error
}

// endpointLifecycleFunc defines a function type for endpoint lifecycle operations.
type endpointLifecycleFunc func(context.Context) error

// start launches the endpoint's connection management loop and optionally waits for readiness.
//
// This function starts the background reconnection loop by invoking run with the provided
// endpoint connect, disconnect, and monitor functions. If NoWaitReady is set in the endpoint options,
// start returns immediately after launching the background goroutine. Otherwise, it waits for the
// endpoint to become ready (i.e., for a successful connection and channel setup) or for a timeout
// or error to occur.
//
// The readiness wait uses the ReadyTimeout option if specified. If the endpoint becomes ready
// within the timeout, start returns nil. If an error occurs during connection setup, or if the
// timeout is exceeded before readiness, start returns the corresponding error.
//
// Arguments:
//   - ctx: a long-lived context for the endpoint's lifecycle (should not be a timeout context)
//   - el: a type (endpoint, i.e. publisher/consumer) implementing [endpointLifecycle].
//
// Returns: error if the endpoint fails to become ready, the context is canceled, or a connection error occurs.
func (e *endpoint) start(ctx context.Context, el endpointLifecycle, onError func(error)) error {
	// check if context is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// fast-path: check ready state and return immediately
	// this allows start() to be called idempotently
	if e.ready.Load() {
		return nil
	}

	noWaitReady := e.opts.NoWaitReady

	// prepare waitCtx for readiness timeout
	timeout := e.opts.ReadyTimeout
	waitCtx := ctx
	if timeout > 0 {
		var waitCancel context.CancelFunc
		waitCtx, waitCancel = context.WithTimeoutCause(ctx, timeout, ErrEndpointNotReadyTimeout)
		defer waitCancel()
	}

	// prepare cancellable context for the endpoint's lifecycle
	// chain-call previous cancel (if any) to avoid leaving a stale lifecycle running
	lifecycleCtx, lifecycleCancel := context.WithCancel(ctx)
	e.stateMu.Lock()
	oldCancel := e.cancel
	// helper to reset cancel func after run() exits
	resetCancel := func() {
		e.stateMu.Lock()
		e.cancel = oldCancel
		e.stateMu.Unlock()
	}
	// chain-call previous cancel func (if any)
	e.cancel = func() {
		if oldCancel != nil {
			oldCancel()
		}
		lifecycleCancel()
	}
	e.stateMu.Unlock()

	// run in background and ignore errors (signaled via onError callback)
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

	// run in background and record errors to errCh
	errCh := make(chan error, 1)
	go func() {
		defer resetCancel()

		// reconnection loop must use ctx (broker long-lived ctx)
		// so it is not prematurely canceled by the readiness timeout
		errCh <- e.run(lifecycleCtx, el.connect, el.disconnect, el.monitor)
	}()

	// waitable loop that keeps the race-catcher logic together
	for {
		// fast-path: check ready state before blocking
		if e.ready.Load() {
			return nil
		}

		e.stateMu.RLock()
		readyCh := e.readyCh
		e.stateMu.RUnlock()

		// race-catcher: check again before blocking
		// if run() made the endpoint ready between the earlier ready check and now,
		// return immediately to avoid blocking on an already-notified channel,
		// this is needed because readyCh is recreated after being closed in run()
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

// run manages the connection lifecycle for an endpoint, handling automatic reconnection,
// readiness signaling, and resource cleanup.
//
// This function runs a loop that attempts to establish a connection and channel using the provided
// connect function. On success, it marks the endpoint as ready and closes the readyCh to signal readiness.
// It then invokes the monitor function, which should block until the connection or channel is closed
// (either by the server or due to an error). When monitor returns, run calls disconnect to clean up resources.
//
// If auto-reconnect is enabled (the default), run will back off and retry connection attempts
// using exponential backoff between min and max durations. If auto-reconnect is disabled, the function
// returns after the first failure or disconnection.
//
// The function respects context cancellation: if the provided context is canceled, it will clean up
// and return immediately.
//
// Arguments:
//   - ctx: a long-lived context for the endpoint's lifecycle (not a timeout context, e.g. context.Background())
//   - connect: function to establish a connection and channel.
//   - disconnect: function to clean up connection and channel resources.
//   - monitor: function to block until the connection/channel is closed or context is canceled.
//
// Returns: error if the endpoint is closed, context is canceled, or connection fails and auto-reconnect is disabled.
func (e *endpoint) run(
	ctx context.Context,
	connect endpointLifecycleFunc,
	disconnect endpointLifecycleFunc,
	monitor endpointLifecycleFunc,
) error {
	autoReconnect := !e.opts.NoAutoReconnect
	reconnectMin := e.opts.ReconnectMin
	reconnectMax := e.opts.ReconnectMax
	// safeguards for min/max
	if reconnectMin <= 0 {
		reconnectMin = defaultReconnectMin
	}
	if reconnectMax < reconnectMin {
		reconnectMax = reconnectMin
	}

	attempts := 0
	delay := reconnectMin

	e.stateMu.Lock()
	// ensure readyCh is initialized
	if e.readyCh == nil {
		e.readyCh = make(chan struct{})
	}
	e.stateMu.Unlock()

	for {
		attempts++

		if e.closed.Load() {
			return ErrEndpointClosed
		}

		var err error

		// attempt connect
		err = connect(ctx)

		if err != nil {
			// honor context cancellation directly
			if errors.Is(err, context.Canceled) {
				return err
			}

			if !autoReconnect {
				return fmt.Errorf("%w: %w", ErrEndpointNoAutoReconnect, err)
			}

			// fail-fast on first attempt to report the correct error immediately
			// and not simply timeout exceeded when auto-reconnect is enabled
			if attempts == 1 {
				return err
			}

			backoff := delay + time.Duration(rand.Int64N(int64(delay/4))) // +0-25% jitter

			// sleep using backoff or wait ctx done
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				// increase backoff for connection retries
				delay = min(delay*2, reconnectMax)
				continue
			}
		}

		// synchronize ready state
		e.stateMu.Lock()
		// mark ready
		e.ready.Store(true) // added in the lock to avoid race with Ready() calls
		if e.readyCh != nil {
			// signal readiness waiting callers (waiters) to unblock
			close(e.readyCh)
			// recreate for next cycle so readyCh is never nil/closed to avoid panics
			e.readyCh = make(chan struct{})
		}
		e.stateMu.Unlock()

		// reset backoff after successful connection
		delay = reconnectMin

		// block in monitor until disconnect or ctx cancel
		err = monitor(ctx)

		// mark not ready
		e.ready.Store(false)

		// if monitor returned due to ctx cancellation,
		// perform disconnect and propagate cancel error
		// otherwise continue to disconnect and potentially retry
		if errors.Is(err, context.Canceled) {
			_ = disconnect(ctx)
			return err
		}

		// cleanup resources on disconnection
		err = disconnect(ctx)

		if !autoReconnect {
			if err != nil {
				return fmt.Errorf("%w: %w", ErrEndpointNoAutoReconnect, err)
			}
			return ErrEndpointNoAutoReconnect
		}

		backoff := delay + time.Duration(rand.Int64N(int64(delay/4))) // +0-25% jitter

		// wait backoff or ctx done before next attempt
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// increase backoff for next connection retry
			delay = min(delay*2, reconnectMax)
			continue
		}
	}
}
