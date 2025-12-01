package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Endpoint represents a publisher or consumer with access to its connection and channel.
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
type endpoint struct {
	id     string
	broker *Broker
	role   endpointRole

	// lifecycle state
	conn    *Connection
	ch      *Channel
	mu      sync.RWMutex
	closed  atomic.Bool
	ready   atomic.Bool
	readyCh chan struct{}
}

// newEndpoint creates a new endpoint with the given ID, broker, and role.
func newEndpoint(id string, b *Broker, role endpointRole) *endpoint {
	if role != roleController && role != rolePublisher && role != roleConsumer {
		role = roleController // default to control role
	}
	return &endpoint{
		id:      id,
		broker:  b,
		role:    role,
		readyCh: make(chan struct{}),
	}
}

// Exchange declares an exchange on the endpoint's channel.
func (ep *endpoint) Exchange(e Exchange) error {
	return ep.broker.topologyMgr.declare(ep.Channel(), &Topology{
		Exchanges: []Exchange{e},
	})
}

// Queue declares a queue on the endpoint's channel.
func (ep *endpoint) Queue(q Queue) error {
	return ep.broker.topologyMgr.declare(ep.Channel(), &Topology{
		Queues: []Queue{q}},
	)
}

// Binding declares a binding on the endpoint's channel.
func (ep *endpoint) Binding(b Binding) error {
	return ep.broker.topologyMgr.declare(ep.Channel(), &Topology{
		Bindings: []Binding{b}},
	)
}

// Connection returns the current connection (may be nil).
func (ep *endpoint) Connection() *Connection {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	return ep.conn
}

// Channel returns the current channel (may be nil).
func (ep *endpoint) Channel() *Channel {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	ch := ep.ch
	if ch != nil && ch.IsClosed() {
		return nil
	}
	return ch
}

// Ready indicates whether the endpoint is ready for operations.
func (ep *endpoint) Ready() bool {
	return ep.ready.Load()
}

// Close stops the endpoint and releases resources.
func (ep *endpoint) Close() error {
	ep.closed.Store(true)

	// close the channel to stop all operations
	ep.mu.Lock()
	defer ep.mu.Unlock()

	if ep.ch != nil {
		err := ep.ch.Close()
		ep.ch = nil
		if err != nil {
			return fmt.Errorf("%w: %w", ErrEndpointClose, err)
		}
	}

	return nil
}

// waitReady blocks until the endpoint is ready or context is done.
func (ep *endpoint) waitReady(ctx context.Context) bool {
	// check if context is already cancelled
	select {
	case <-ctx.Done():
		return false
	default:
	}

	if ep.ready.Load() {
		return true
	}
	ep.mu.RLock()
	ch := ep.readyCh
	ep.mu.RUnlock()
	if ch == nil {
		return ep.ready.Load()
	}
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// makeReady handles the reconnection loop for an endpoint.
// The connect function should establish connection/channel and return error.
// The monitor function should wait for disconnection events and return error/nil.
// Returns error if connection fails and auto-reconnect is disabled, or if context is cancelled.
func (ep *endpoint) makeReady(
	ctx context.Context,
	autoReconnect bool,
	reconnectDelay time.Duration,
	connect func(context.Context) error,
	monitor func(context.Context) error,
) error {
	for {
		if ep.closed.Load() {
			return ErrBrokerClosed
		}

		// prepare for connection attempt
		// recreate readyCh for (re)connection attempt
		ep.mu.Lock()
		if ep.readyCh == nil {
			ep.readyCh = make(chan struct{})
		}
		ep.mu.Unlock()

		// establish connection
		if err := connect(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			if !autoReconnect {
				return err
			}
			time.Sleep(reconnectDelay)
			continue
		}

		// mark ready
		ep.ready.Store(true)
		ep.mu.Lock()
		if ep.readyCh != nil {
			close(ep.readyCh)
			ep.readyCh = nil
		}
		ep.mu.Unlock()

		// monitor for disconnection
		if err := monitor(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
		}

		// disconnected? reconnect if enabled
		if !autoReconnect {
			return ErrConnectionClosed
		}
		time.Sleep(reconnectDelay)
	}
}
