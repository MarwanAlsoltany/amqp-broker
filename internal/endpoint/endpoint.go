package endpoint

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

var (
	// ErrEndpoint is the base error for endpoint operations.
	// All endpoint-related errors wrap this error.
	ErrEndpoint = &internal.Error{Op: "endpoint"}

	// ErrEndpointClosed indicates the endpoint is closed.
	// This error is returned when operations are attempted on a closed endpoint.
	ErrEndpointClosed = fmt.Errorf("%w: closed", ErrEndpoint)

	// ErrEndpointNotConnected indicates the endpoint is not connected.
	// This error is returned when the endpoint has no active AMQP channel.
	ErrEndpointNotConnected = fmt.Errorf("%w: not connected", ErrEndpoint)

	// ErrEndpointNotReadyTimeout indicates the endpoint did not become ready within the timeout.
	// This occurs when the endpoint fails to establish a connection in time.
	ErrEndpointNotReadyTimeout = fmt.Errorf("%w: not ready within timeout", ErrEndpoint)

	// ErrEndpointNoAutoReconnect indicates the endpoint lost its connection and auto-reconnect is disabled.
	// When auto-reconnect is disabled, connection loss is a terminal error.
	ErrEndpointNoAutoReconnect = fmt.Errorf("%w: connection lost: auto-reconnect is disabled", ErrEndpoint)
)

// Endpoint defines the common interface for AMQP publishers and consumers, providing access
// to the underlying connection, channel, and lifecycle management operations.
//
// Ownership & Lifecycle:
//   - Each Endpoint owns a dedicated AMQP channel and assigns a connection from the pool.
//   - Supports automatic reconnection and resource management.
//   - Provides methods to declare exchanges, queues, and bindings on the channel.
//   - Use [Endpoint.Close] to stop the endpoint and release resources.
//
// Concurrency:
//   - All methods are safe for concurrent use unless otherwise specified.
//
// Endpoints automatically reconnect on connection loss (unless disabled)
// and redeclare topology after reconnection.
//
// See [EndpointOptions] for configuration details shared by publishers and consumers.
type Endpoint interface {
	// Connection returns the current connection (may be nil if not connected).
	Connection() transport.Connection
	// Channel returns the current channel (may be nil if not connected or closed).
	Channel() transport.Channel
	// Ready indicates whether the endpoint is ready for operations.
	Ready() bool
	// Exchange declares an exchange on the endpoint's channel.
	Exchange(topology.Exchange) error
	// Queue declares a queue on the endpoint's channel.
	Queue(topology.Queue) error
	// Binding declares a binding on the endpoint's channel.
	Binding(topology.Binding) error
	// Close stops the endpoint and releases resources.
	Close() error
}

// EndpointOptions holds common reconnection and readiness configuration for endpoints.
//
// Key options:
//   - NoAutoDeclare: Skip automatic topology declaration
//   - NoAutoReconnect: Disable automatic reconnection on connection loss
//   - ReconnectMin/Max: Control reconnection backoff timing
//
// These options apply to both publishers and consumers.
type EndpointOptions struct {
	// NoAutoDeclare disables automatic declaration of topology on connect/reconnect.
	// Default: false (automatic declaration enabled)
	NoAutoDeclare bool
	// NoAutoReconnect disables automatic reconnection on connection failure.
	// When false (default), connection losses after the first successful connect
	// are retried with exponential backoff between [EndpointOptions.ReconnectMin]
	// and [EndpointOptions.ReconnectMax].
	// Note: even when false, the very first connection attempt is fail-fast, if it
	// fails the error is returned immediately to the caller without entering the
	// retry loop. Only subsequent connection losses trigger the backoff/retry cycle.
	// Default: false
	NoAutoReconnect bool
	// ReconnectMin is the minimum delay between reconnection attempts.
	// Only used if [EndpointOptions.NoAutoReconnect] is false.
	// Default: [DefaultReconnectMin] (500ms)
	ReconnectMin time.Duration
	// ReconnectMax is the maximum delay between reconnection attempts.
	// Only used if [EndpointOptions.NoAutoReconnect] is false.
	// Default: [DefaultReconnectMax] (30s)
	ReconnectMax time.Duration
	// NoWaitReady prevents the endpoint from blocking until it is connected.
	// Default: false
	NoWaitReady bool
	// ReadyTimeout is the maximum time to wait for the endpoint to be ready.
	// Only used if [EndpointOptions.NoWaitReady] is false.
	// Default: [DefaultReadyTimeout] (10s)
	ReadyTimeout time.Duration
}

// role indicates the intended use of an endpoint.
type role uint8

const (
	// roleController indicates the connection is for control operations (topology management).
	roleController role = iota
	// rolePublisher indicates the connection is for publishers.
	rolePublisher
	// roleConsumer indicates the connection is for consumers.
	roleConsumer
)

// purpose maps endpointRole to amqp.ConnectionPurpose for connection assignment.
func (r role) purpose() transport.ConnectionPurpose {
	switch r {
	case rolePublisher:
		return transport.ConnectionPurposePublish
	case roleConsumer:
		return transport.ConnectionPurposeConsume
	default:
		return transport.ConnectionPurposeControl
	}
}

// endpoint provides common lifecycle state for publishers and consumers.
// It implements the [Endpoint] interface and is embedded by publisher and consumer.
type endpoint struct {
	_ noCopy

	id   string
	role role
	opts EndpointOptions

	// managers for connection assignment and topology declaration
	connectionMgr *transport.ConnectionManager
	topologyReg   *topology.Registry

	// amqp resources
	conn transport.Connection
	ch   transport.Channel

	// synchronization mutex for internal state
	stateMu sync.RWMutex
	// readyCh is closed to notify waiters that the endpoint became ready;
	// it is recreated immediately after closing so it is never nil/closed at rest
	readyCh chan struct{}
	// ready is the authoritative readiness flag
	ready atomic.Bool
	// closed tracks whether Close has been called
	closed atomic.Bool
	// cancel func for the endpoint's long-lived context
	cancel context.CancelFunc
}

var _ Endpoint = (*endpoint)(nil)

// newEndpoint creates a new base endpoint.
func newEndpoint(id string, role role, connMgr *transport.ConnectionManager, topoReg *topology.Registry, opts EndpointOptions) *endpoint {
	if role != roleController && role != rolePublisher && role != roleConsumer {
		role = roleController
	}
	return &endpoint{
		id:            id,
		role:          role,
		opts:          opts,
		connectionMgr: connMgr,
		topologyReg:   topoReg,
		readyCh:       make(chan struct{}),
	}
}

// Connection returns the current connection (may be nil if not connected or closed).
func (e *endpoint) Connection() transport.Connection {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	if e.conn != nil && e.conn.IsClosed() {
		return nil
	}
	return e.conn
}

// Channel returns the current channel (may be nil if not connected or closed).
func (e *endpoint) Channel() transport.Channel {
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
	// calling Ready() concurrently with endpointLifecycle.run() or directly after it,
	// may lead it to not yielding the expected result, this is not a race, and it is not
	// an atomic failure, this is scheduler timing / memory visibility semantics of atomic operations
	return e.ready.Load()
}

// Close stops the endpoint and releases resources.
func (e *endpoint) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}

	e.stateMu.Lock()
	defer e.stateMu.Unlock()

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

// Exchange declares an exchange on the endpoint's channel.
func (e *endpoint) Exchange(exchange topology.Exchange) error {
	ch := e.Channel()
	if ch == nil {
		return ErrEndpointNotConnected
	}
	return e.topologyReg.Declare(ch, &topology.Topology{
		Exchanges: []topology.Exchange{exchange},
	})
}

// Queue declares a queue on the endpoint's channel.
func (e *endpoint) Queue(queue topology.Queue) error {
	ch := e.Channel()
	if ch == nil {
		return ErrEndpointNotConnected
	}
	return e.topologyReg.Declare(ch, &topology.Topology{
		Queues: []topology.Queue{queue},
	})
}

// Binding declares a binding on the endpoint's channel.
func (e *endpoint) Binding(binding topology.Binding) error {
	ch := e.Channel()
	if ch == nil {
		return ErrEndpointNotConnected
	}
	return e.topologyReg.Declare(ch, &topology.Topology{
		Bindings: []topology.Binding{binding},
	})
}
