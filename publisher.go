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
	Publish(ctx context.Context, rk RoutingKey, msgs ...Message) error
}

// PublisherOptions configures Publisher behavior.
type PublisherOptions struct {
	// ConfirmMode enables publisher confirmations (recommended for reliable publishing).
	ConfirmMode bool
	// ConfirmTimeout is the maximum time to wait for confirmation.
	// Only used if ConfirmMode is enabled.
	// Default: defaultConfirmTimeout
	ConfirmTimeout time.Duration
	// Mandatory causes the server to return un-routable messages.
	// Returned messages are delivered via OnReturn callback.
	// Default: false
	Mandatory bool
	// Immediate causes messages that cannot be immediately delivered to be returned.
	// Note: RabbitMQ 3.0+ removed support for this flag.
	// Default: false
	Immediate bool
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
	// OnReturn is called when a mandatory or immediate publish is undeliverable.
	// The Return contains the failed Publishing and error details.
	// If nil, returned messages are silently discarded.
	OnReturn func(Message)
	// OnFlow is called when the server sends flow control notifications.
	// active=true means publishing can resume, active=false means publishing is paused.
	// If nil, flow control changes are handled internally without notification.
	OnFlow func(active bool)
	// OnError is called when errors occur in background goroutines that cannot be returned.
	// This includes channel close errors during reconnection monitoring.
	// If nil, errors are silently ignored.
	OnError func(err error)
}

// publisher manages the lifecycle and state of a Publisher.
type publisher struct {
	*endpoint
	opts     PublisherOptions
	exchange Exchange
	// flow control state
	flow   atomic.Bool
	flowCh <-chan bool
	// confirmation channel (if ConfirmMode enabled)
	confirmCh <-chan amqp.Confirmation
	// return channel for undeliverable messages
	returnCh <-chan amqp.Return
	// serialize publishes to avoid concurrent channel usage
	publishMu sync.Mutex
}

var _ Publisher = (*publisher)(nil)

func newPublisher(b *Broker, id string, opts PublisherOptions, e Exchange) *publisher {
	p := &publisher{
		endpoint: newEndpoint(id, b, rolePublisher),
		opts:     opts,
		exchange: e,
	}
	p.flow.Store(true) // Start with flow active
	return p
}

// Publish sends a message to the specified exchange with the given routing key.
func (p *publisher) Publish(ctx context.Context, rk RoutingKey, msgs ...Message) error {
	if p.closed.Load() {
		return errors.New("publisher closed")
	}

	if len(msgs) == 0 {
		return nil
	}

	// Serialize publishes to avoid concurrent channel usage
	p.publishMu.Lock()
	defer p.publishMu.Unlock()

	// Check flow control state
	if !p.flow.Load() {
		return errors.New("publisher flow paused by server")
	}

	ch := p.endpoint.Channel()
	if ch == nil {
		return errors.New("publisher not connected")
	}

	err := p.endpoint.broker.declareExchange(ch, p.exchange)
	if err != nil {
		return fmt.Errorf("ensure exchange declared: %w", err)
	} // Publish all messages
	for i, msg := range msgs {
		pub := publishingFromMessage(&msg)

		err := ch.PublishWithContext(
			ctx,
			p.exchange.Name,
			string(rk),
			p.opts.Mandatory,
			p.opts.Immediate,
			pub,
		)
		if err != nil {
			return fmt.Errorf("publish message %d: %w", i, err)
		}
	}

	// Wait for all confirmations if enabled
	if p.opts.ConfirmMode {
		p.stateMu.RLock()
		confCh := p.confirmCh
		p.stateMu.RUnlock()
		if confCh == nil {
			return errors.New("confirmation channel not available")
		}

		timeout := p.opts.ConfirmTimeout
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
	defer func() {
		cancel()
		p.disconnect()
	}()

	autoReconnect := !p.opts.NoAutoReconnect
	reconnectDelay := p.opts.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = defaultReconnectDelay
	}

	p.makeReady(
		ctx,
		autoReconnect,
		reconnectDelay,
		p.connect,
		p.monitor,
	)
}

// connect establishes a connection and channel for publishing.
func (p *publisher) connect(ctx context.Context) error {
	conn, err := p.broker.connMgr.assign(ctx, p.role)
	if err != nil {
		return fmt.Errorf("assign connection: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	var confCh <-chan amqp.Confirmation
	if p.opts.ConfirmMode {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return fmt.Errorf("enable confirm: %w", err)
		}
		// NotifyPublish is used to preserve strict ordering of acks/nacks.
		// Alternative: NotifyConfirm returns separate ack/nack channels:
		// ackCh, nackCh := ch.NotifyConfirm(make(chan uint64), make(chan uint64))
		// However, NotifyConfirm may lose ordering when acks and nacks are interleaved.
		confCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	// Set up return notification for undeliverable messages
	returnCh := ch.NotifyReturn(make(chan amqp.Return, 100))

	// Set up flow control notification
	flowCh := ch.NotifyFlow(make(chan bool, 1))

	p.stateMu.Lock()
	p.conn = conn
	p.ch = ch
	p.confirmCh = confCh
	p.returnCh = returnCh
	p.flowCh = flowCh
	p.stateMu.Unlock()

	// Start background handlers
	go p.handleReturns(ctx)
	go p.handleFlow(ctx)

	return nil
}

// disconnect closes the channel and clears publisher-specific state.
func (p *publisher) disconnect() {
	p.ready.Store(false)

	p.stateMu.Lock()
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	// Clear publisher-specific channels
	p.confirmCh = nil
	p.returnCh = nil
	p.flowCh = nil
	p.stateMu.Unlock()
}

// monitor watches for channel closures.
func (p *publisher) monitor(ctx context.Context) error {
	ch := p.Channel()
	if ch == nil {
		return errors.New("channel not available")
	}

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-closeCh:
		if err != nil && p.opts.OnError != nil {
			p.opts.OnError(fmt.Errorf("channel closed: %w", err))
		}
		p.disconnect()
		return err
	}
}

// handleReturns processes undeliverable messages returned by the server.
func (p *publisher) handleReturns(ctx context.Context) {
	for {
		p.stateMu.RLock()
		returnCh := p.returnCh
		p.stateMu.RUnlock()

		if returnCh == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case r, ok := <-returnCh:
			if !ok {
				return
			}
			// Call user callback if provided
			if p.opts.OnReturn != nil {
				msg := returnToMessage(&r)
				msg.broker = p.broker
				p.opts.OnReturn(msg)
			}
			// Otherwise silently discard (could log here)
		}
	}
}

// handleFlow processes flow control notifications from the server.
func (p *publisher) handleFlow(ctx context.Context) {
	for {
		p.stateMu.RLock()
		flowCh := p.flowCh
		p.stateMu.RUnlock()

		if flowCh == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case active, ok := <-flowCh:
			if !ok {
				return
			}
			p.flow.Store(active)
			// Notify user if callback is provided
			if p.opts.OnFlow != nil {
				p.opts.OnFlow(active)
			}
		}
	}
}
