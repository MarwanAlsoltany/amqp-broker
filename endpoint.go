package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Endpoint represents a publisher or consumer with access to its connection and channel.
type Endpoint interface {
	// Connection returns the current connection (may be nil if not connected).
	Connection() *amqp.Connection
	// Channel returns the current channel (may be nil if not connected).
	Channel() *amqp.Channel
	// Exchange declares an exchange on the endpoint's channel.
	Exchange(Exchange) error
	// Queue declares a queue on the endpoint's channel.
	Queue(Queue) error
	// Binding declares a binding on the endpoint's channel.
	Binding(Binding) error
	// Close stops the endpoint and releases resources.
	Close() error
	// Unregister removes the endpoint from the broker's registry and closes it.
	Unregister()
}

// endpointRole indicates the intended use of a connection.
type endpointRole int

const (
	// roleControl indicates the connection is for control operations (topology management).
	roleControl endpointRole = iota
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

	// Lifecycle state
	stateMu sync.RWMutex
	conn    *amqp.Connection
	ch      *amqp.Channel
	closed  atomic.Bool
	ready   atomic.Bool
	readyCh chan struct{}
}

// newEndpoint creates a new endpoint with the given ID, broker, and role.
func newEndpoint(id string, b *Broker, role endpointRole) *endpoint {
	return &endpoint{
		id:      id,
		broker:  b,
		role:    role,
		readyCh: make(chan struct{}),
	}
}

// Exchange declares an exchange on the endpoint's channel.
func (e *endpoint) Exchange(ex Exchange) error {
	return e.broker.declareExchange(e.Channel(), ex)
}

// Queue declares a queue on the endpoint's channel.
func (e *endpoint) Queue(q Queue) error {
	return e.broker.declareQueue(e.Channel(), q)
}

// Binding declares a binding on the endpoint's channel.
func (e *endpoint) Binding(b Binding) error {
	return e.broker.declareBinding(e.Channel(), b)
}

// Connection returns the current connection (may be nil).
func (e *endpoint) Connection() *amqp.Connection {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.conn
}

// Channel returns the current channel (may be nil).
func (e *endpoint) Channel() *amqp.Channel {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.ch
}

// Close stops the endpoint and releases resources.
func (e *endpoint) Close() error {
	e.closed.Store(true)
	return nil
}

// waitReady blocks until the endpoint is ready or context is done.
func (e *endpoint) waitReady(ctx context.Context) bool {
	if e.ready.Load() {
		return true
	}
	e.stateMu.RLock()
	ch := e.readyCh
	e.stateMu.RUnlock()
	if ch == nil {
		return e.ready.Load()
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
func (e *endpoint) makeReady(
	ctx context.Context,
	autoReconnect bool,
	reconnectDelay time.Duration,
	connect func(context.Context) error,
	monitor func(context.Context) error,
) {
	for {
		if e.closed.Load() {
			return
		}

		// Prepare for connection attempt
		// recreate readyCh for (re)connection attempt
		e.stateMu.Lock()
		if e.readyCh == nil {
			e.readyCh = make(chan struct{})
		}
		e.stateMu.Unlock()

		// Connect
		if err := connect(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if !autoReconnect {
				return
			}
			time.Sleep(reconnectDelay)
			continue
		}

		// Mark ready
		e.ready.Store(true)
		e.stateMu.Lock()
		if e.readyCh != nil {
			close(e.readyCh)
			e.readyCh = nil
		}
		e.stateMu.Unlock()

		// Monitor for disconnection
		if err := monitor(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
		}

		// Disconnected - reconnect if enabled
		if !autoReconnect {
			return
		}
		time.Sleep(reconnectDelay)
	}
}
