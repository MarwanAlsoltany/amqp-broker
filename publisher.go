package broker

import (
	"context"
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
	// OnConfirm is called for each published message with its delivery tag and wait function.
	// Parameters:
	//   - deliveryTag: unique identifier for the message
	//   - wait: function that blocks until confirmation arrives, returns true if acked
	//           Accepts optional context to control timeout/cancellation
	//           If no context provided (nil), waits indefinitely
	// Only called when ConfirmMode is enabled.
	// Providing this callback enables deferred confirmation mode.
	// If not specified (nil), Publish waits for batch confirmations (default behavior).
	// Example: wait(nil) or wait(ctx)
	OnConfirm func(deliveryTag uint64, wait func(context.Context) bool)
	// OnReturn is called when a mandatory or immediate publish is undeliverable.
	// The Return contains the failed Publishing and error details.
	// If not specified (nil), returned messages are silently discarded.
	OnReturn func(Message)
	// OnFlow is called when the server sends flow control notifications.
	// active=true means publishing can resume, active=false means publishing is paused.
	// If not specified (nil), flow control changes are handled internally without notification.
	OnFlow func(active bool)
	// OnError is called when errors occur in background goroutines that cannot be returned.
	// This includes channel close errors during reconnection monitoring.
	// If not specified (nil), errors are silently ignored.
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
	// serialize publishes (one-by-one) to avoid concurrent channel usage
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
	// check if context is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.closed.Load() {
		return ErrPublisherClosed
	}

	if len(msgs) == 0 {
		return nil
	}

	// serialize publishes to avoid concurrent channel usage
	p.publishMu.Lock()
	defer p.publishMu.Unlock()

	// check flow control state
	if !p.flow.Load() {
		return ErrPublisherFlowPaused
	}

	ch := p.Channel()
	if ch == nil {
		return ErrPublisherNotConnected
	}

	// allow for publishing without declaring an exchange (when routing-key == queue name)
	if p.exchange.Name != "" {
		err := p.Exchange(p.exchange)
		if err != nil {
			return wrapError("declare exchange", err)
		}
	}
	// no assumptions are made about declaring the queue automatically
	// the user is responsible for ensuring the destination exists

	// publish all messages
	for i, msg := range msgs {
		pub := amqpPublishingFromMessage(&msg)

		if p.opts.OnConfirm != nil && p.opts.ConfirmMode {
			// deferred confirm mode
			conf, err := ch.PublishWithDeferredConfirmWithContext(
				ctx,
				p.exchange.Name,
				string(rk),
				p.opts.Mandatory,
				p.opts.Immediate,
				pub,
			)
			if err != nil {
				return fmt.Errorf("%w: message %d: %w", ErrPublisherPublishFailed, i, err)
			}
			// invoke callback with delivery tag and wait function
			p.opts.OnConfirm(conf.DeliveryTag, func(ctx context.Context) bool {
				if ctx != nil {
					acked, err := conf.WaitContext(ctx)
					if err != nil {
						p.opts.OnError(err)
					}
					return acked
				}
				return conf.Wait()
			})
		} else {
			// regular publish or batch confirm mode
			err := ch.PublishWithContext(
				ctx,
				p.exchange.Name,
				string(rk),
				p.opts.Mandatory,
				p.opts.Immediate,
				pub,
			)
			if err != nil {
				return fmt.Errorf("%w: message %d: %w", ErrPublisherPublishFailed, i, err)
			}
		}
	}

	// handle confirmations based on mode
	if p.opts.ConfirmMode && p.opts.OnConfirm == nil {
		// batch mode: wait for all confirmations synchronously
		msgLen := len(msgs)

		p.mu.RLock()
		confCh := p.confirmCh
		p.mu.RUnlock()
		if confCh == nil {
			return ErrPublisherConfirmNotAvailable
		}

		timeout := p.opts.ConfirmTimeout
		if timeout <= 0 {
			timeout = defaultConfirmTimeout
		}

		for i := 0; i < msgLen; i++ {
			select {
			case c := <-confCh:
				if !c.Ack {
					return fmt.Errorf("%w: message %d", ErrPublisherPublishConfirm, i)
				}
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(timeout):
				return fmt.Errorf("%w: message %d", ErrPublisherConfirmTimeout, i)
			}
		}
	}

	return nil
}

// Release removes the publisher from the broker's registry and closes it.
func (p *publisher) Release() {
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

	_ = p.makeReady(
		ctx,
		autoReconnect,
		reconnectDelay,
		p.connect,
		p.monitor,
	)
}

// connect establishes a connection and channel for publishing.
func (p *publisher) connect(ctx context.Context) error {
	conn, err := p.broker.connectionMgr.assign(p.role)
	if err != nil {
		return wrapError("assign connection", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return wrapError("open channel", err)
	}

	var confCh <-chan amqp.Confirmation
	if p.opts.ConfirmMode {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return wrapError("enable confirm mode", err)
		}
		// ch.NotifyPublish is used to preserve strict ordering of acks/nacks.
		// alternative: ch.NotifyConfirm returns separate ack/nack channels:
		// ackCh, nackCh := ch.NotifyConfirm(make(chan uint64), make(chan uint64))
		// however, ch.NotifyConfirm may lose ordering when acks and nacks are interleaved.
		// ch.NotifyPublish must be consumed to avoid deadlocks.
		// the channel must have sufficient buffer for outstanding publishes; but for DX,
		// unbuffered channel and consume in background goroutine instead is used instead
		confCh = ch.NotifyPublish(make(chan amqp.Confirmation))
	}

	// set up return notification for undeliverable messages
	returnCh := ch.NotifyReturn(make(chan amqp.Return, 100))

	// set up flow control notification
	flowCh := ch.NotifyFlow(make(chan bool, 1))

	p.mu.Lock()
	p.conn = conn
	p.ch = ch
	p.confirmCh = confCh
	p.returnCh = returnCh
	p.flowCh = flowCh
	p.mu.Unlock()

	// start background handlers
	go p.handleReturns(ctx)
	go p.handleFlow(ctx)
	// when using deferred confirmations, confCh still need to be consumed to avoid deadlocks,
	// even though the confirmations are handled by amqp.DeferredConfirmation objects
	if p.opts.ConfirmMode && p.opts.OnConfirm != nil {
		go p.handleConfirmations(ctx)
	}

	return nil
}

// disconnect closes the channel and clears publisher-specific state.
func (p *publisher) disconnect() {
	p.ready.Store(false)

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	// clear publisher-specific channels
	p.confirmCh = nil
	p.returnCh = nil
	p.flowCh = nil
}

// monitor watches for channel closures.
func (p *publisher) monitor(ctx context.Context) error {
	ch := p.Channel()
	if ch == nil {
		return ErrChannelClosed
	}

	// create a buffered channel to avoid deadlock
	// library sends notification once, then closes the channel
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-closeCh:
		amqpErr := wrapError("channel closed", err)
		if amqpErr != nil && p.opts.OnError != nil {
			p.opts.OnError(amqpErr)
		}
		p.disconnect()
		return amqpErr
	}
}

// handleReturns processes undeliverable messages returned by the server.
func (p *publisher) handleReturns(ctx context.Context) {
	for {
		p.mu.RLock()
		returnCh := p.returnCh
		p.mu.RUnlock()

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
			// call user callback if provided
			if p.opts.OnReturn != nil {
				msg := amqpReturnToMessage(&r)
				msg.broker = p.broker
				p.opts.OnReturn(msg)
			}
			// otherwise silently discard (could log here)
		}
	}
}

// handleFlow processes flow control notifications from the server.
func (p *publisher) handleFlow(ctx context.Context) {
	for {
		p.mu.RLock()
		flowCh := p.flowCh
		p.mu.RUnlock()

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
			// notify user if callback is provided
			if p.opts.OnFlow != nil {
				p.opts.OnFlow(active)
			}
		}
	}
}

// handleConfirmations consumes confirmations from the channel to prevent deadlocks.
// This is needed when using deferred confirmation mode because the channel must be consumed
// even though confirmations are handled by amqp.DeferredConfirmation objects.
func (p *publisher) handleConfirmations(ctx context.Context) {
	for {
		p.mu.RLock()
		confirmCh := p.confirmCh
		p.mu.RUnlock()

		if confirmCh == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case _, ok := <-confirmCh:
			if !ok {
				return
			}
			// simply discard, amqp.DeferredConfirmation handles the actual confirmation
		}
	}
}
