package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher provides methods for publishing messages to AMQP exchanges.
// Publishers own a dedicated connection and channel, and support automatic
// reconnection and publisher confirmations.
type Publisher interface {
	Endpoint
	// Publish sends a message to the specified exchange with the given routing key.
	Publish(ctx context.Context, rk RoutingKey, msgs []Message, opts *PublishOptions) error
}

// PublishOptions configures publishing behavior at the broker/channel level.
// Message-level options (DeliveryMode, Headers, etc.) are set on Message itself.
type PublishOptions struct {
	// Mandatory causes the server to return unroute-able messages to the publisher.
	Mandatory bool
	// Immediate causes messages that cannot be immediately delivered to be returned.
	// Note: RabbitMQ 3.0+ removed support for this flag.
	Immediate bool
	// WaitForConfirm enables publisher confirmation for the message.
	WaitForConfirm bool
	// ConfirmTimeout is the maximum time to wait for confirmation.
	// Default: defaultConfirmTimeout
	ConfirmTimeout time.Duration
}

// PublisherOptions configures Publisher behavior.
type PublisherOptions struct {
	// ConfirmMode enables publisher confirmations (recommended for reliable publishing).
	ConfirmMode bool
	// NoAutoReconnect disables automatic reconnection on connection failure.
	// Default: false
	NoAutoReconnect bool
	// ReconnectDelay is the delay between reconnection attempts.
	// Default: defaultReconnectDelay
	ReconnectDelay time.Duration
	// NoWaitForReady prevents new publisher from blocking until is's connected.
	// Default: false
	NoWaitForReady bool
	// ReadyTimeout is the maximum time to wait for the publisher to be ready.
	// Only used if NoWaitForReady is false.
	// Default: defaultReadyTimeout
	ReadyTimeout time.Duration
}

// publisher manages the lifecycle and state of a Publisher.
type publisher struct {
	id       string
	broker   *Broker
	opts     PublisherOptions
	exchange Exchange
	/* runtime state */
	stateMu     sync.RWMutex
	conn        *amqp.Connection
	relConn     releaseFunc
	ch          *amqp.Channel
	ready       atomic.Bool
	readyCh     chan struct{}
	reconnectCh chan struct{}
	closed      atomic.Bool
	cancelFn    context.CancelFunc
	// serialize publishes to avoid concurrent channel usage
	mu sync.Mutex
	// confirmation channel (if ConfirmMode enabled)
	confCh <-chan amqp.Confirmation
}

func newPublisher(b *Broker, id string, opts PublisherOptions, e Exchange) *publisher {
	return &publisher{
		id:          id,
		broker:      b,
		opts:        opts,
		exchange:    e,
		reconnectCh: make(chan struct{}, 1),
		readyCh:     make(chan struct{}),
	}
}

var _ Publisher = (*publisher)(nil)

// Publish sends a message to the specified exchange with the given routing key.
func (p *publisher) Publish(ctx context.Context, rk RoutingKey, msgs []Message, opts *PublishOptions) error {
	if p.closed.Load() {
		return errors.New("publisher closed")
	}

	if len(msgs) == 0 {
		return nil
	}

	// Serialize publishes to avoid concurrent channel usage
	p.mu.Lock()
	defer p.mu.Unlock()

	ch := p.Channel()
	if ch == nil {
		return errors.New("publisher not connected")
	}

	pubOps := defaultPublishOptions()
	if opts != nil {
		pubOps = *opts
	}

	err := p.broker.ensureExchangeDeclared(ch, p.exchange, true)
	if err != nil {
		return fmt.Errorf("ensure exchange declared: %w", err)
	}

	// Publish all messages
	for i, msg := range msgs {
		pub := messageToPublishing(&msg)

		err := ch.PublishWithContext(
			ctx,
			p.exchange.Name,
			string(rk),
			pubOps.Mandatory,
			pubOps.Immediate,
			pub,
		)
		if err != nil {
			return fmt.Errorf("publish message %d: %w", i, err)
		}
	}

	// Wait for all confirmations if enabled
	if p.opts.ConfirmMode {
		p.stateMu.RLock()
		confCh := p.confCh
		p.stateMu.RUnlock()
		if confCh == nil {
			return errors.New("confirmation channel not available")
		}

		timeout := pubOps.ConfirmTimeout
		if timeout <= 0 {
			timeout = defaultConfirmTimeout
		}

		// Wait for all confirmations
		for i := 0; i < len(msgs); i++ {
			select {
			case c := <-confCh:
				if !c.Ack {
					return fmt.Errorf("message %d not acked by broker", i)
				}
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(timeout):
				return fmt.Errorf("confirmation timeout for message %d", i)
			}
		}
	}

	return nil
}

// Connection returns the current connection (may be nil).
func (p *publisher) Connection() *amqp.Connection {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.conn
}

// Channel returns the current channel (may be nil).
func (p *publisher) Channel() *amqp.Channel {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.ch
}

// Close stops the publisher and releases resources.
func (p *publisher) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	p.stateMu.Lock()
	if p.cancelFn != nil {
		p.cancelFn()
	}
	p.stateMu.Unlock()
	return nil
}

// Unregister removes the publisher from the broker's registry and closes it.
func (p *publisher) Unregister() {
	if p.broker == nil {
		return
	}
	p.broker.publishersMu.Lock()
	delete(p.broker.publishers, p.id)
	p.broker.publishersMu.Unlock()
	_ = p.Close()
}

// run is the main goroutine for the publisher.
func (p *publisher) run() {
	ctx, cancel := context.WithCancel(p.broker.ctx)
	p.stateMu.Lock()
	p.cancelFn = cancel
	p.stateMu.Unlock()

	defer func() {
		p.cleanup()
	}()

	noAutoReconnect := p.opts.NoAutoReconnect
	reconnectDelay := p.opts.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = defaultReconnectDelay
	}

	for {
		if p.closed.Load() {
			return
		}

		// Connect
		if err := p.connect(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if noAutoReconnect {
				return
			}
			time.Sleep(reconnectDelay)
			continue
		}

		// Mark ready
		p.ready.Store(true)
		select {
		case <-p.readyCh:
		default:
			close(p.readyCh)
		}
	}
}

// connect establishes a connection and channel for the publisher.
func (p *publisher) connect(ctx context.Context) error {
	conn, relConn, err := p.broker.connPool.get(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		relConn(true)
		return fmt.Errorf("open channel: %w", err)
	}

	var confCh <-chan amqp.Confirmation
	if p.opts.ConfirmMode {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			relConn(true)
			return fmt.Errorf("enable confirm: %w", err)
		}
		confCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	p.stateMu.Lock()
	p.conn = conn
	p.relConn = relConn
	p.ch = ch
	p.confCh = confCh
	p.stateMu.Unlock()

	return nil
}

// disconnect closes the channel and releases the connection.
func (p *publisher) disconnect() {
	p.ready.Store(false)

	p.stateMu.Lock()
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.relConn != nil {
		p.relConn(false)
		p.relConn = nil
	}
	p.conn = nil
	p.confCh = nil
	p.stateMu.Unlock()
}

// reconnect signals the publisher to reconnect.
func (p *publisher) reconnect() {
	select {
	case p.reconnectCh <- struct{}{}:
	default:
	}
}

// cleanup performs final cleanup when the publisher goroutine exits.
func (p *publisher) cleanup() {
	p.disconnect()
	p.stateMu.Lock()
	if p.cancelFn != nil {
		p.cancelFn()
		p.cancelFn = nil
	}
	p.stateMu.Unlock()
}

// waitReady blocks until the publisher is ready or context is done.
func (p *publisher) waitReady(ctx context.Context) bool {
	if p.ready.Load() {
		return true
	}
	p.stateMu.RLock()
	ch := p.readyCh
	p.stateMu.RUnlock()
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}
