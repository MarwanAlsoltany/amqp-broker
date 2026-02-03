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

// Publisher defines a high-level AMQP exchange publisher with automatic connection management,
// background confirmation handling, and graceful resource cleanup.
//
// Ownership & Lifecycle:
//   - See [Endpoint] for connection and channel ownership details.
//   - Runs in the background upon creation, ready to publish messages as requested by the user (push-based model).
//   - Use [Publisher.Publish] to send messages.
//   - Graceful shutdown ensures all in-flight publish operations and confirmations complete before resources are released.
//
// Concurrency:
//   - [Publisher.Publish] is safe for concurrent use by multiple goroutines.
//   - Confirmation handling may be processed concurrently, depending on configuration.
//
// Usage Patterns:
//   - Call [Publisher.Publish] to send one or more messages to an exchange with a routing key.
//   - If [PublisherOptions.ConfirmMode] is enabled, confirmations are handled in the background or via callbacks.
//
// See [PublisherOptions] for configuration details.
type Publisher interface {
	Endpoint
	// Publish sends a message to the specified exchange with the given routing key.
	Publish(ctx context.Context, rk RoutingKey, msgs ...Message) error
}

// PublisherOptions configures Publisher behavior.
type PublisherOptions struct {
	// EndpointOptions holds reconnection and readiness configuration for the publisher.
	EndpointOptions
	// ConfirmMode enables publisher confirmations
	// (recommended for reliable publishing, but introduces overhead).
	ConfirmMode bool
	// ConfirmTimeout is the maximum time to wait for confirmation.
	// Only used if [PublisherOptions.ConfirmMode] is true.
	// Default: defaultConfirmTimeout
	ConfirmTimeout time.Duration
	// Mandatory causes the server to return un-routable messages.
	// Returned messages are delivered via [PublisherOptions.OnReturn] callback.
	// Default: false
	Mandatory bool
	// Immediate causes messages that cannot be immediately delivered to be returned.
	// Deprecated: RabbitMQ 3.0+ removed support for this flag.
	// Default: false
	Immediate bool
	// OnConfirm is called for each published message with its delivery tag and wait function.
	// Parameters:
	//   - deliveryTag: unique identifier for the message.
	//   - wait: function that blocks until confirmation arrives, returns true if acked.
	//           Accepts optional context to control timeout/cancellation
	//           If no context provided (nil), waits indefinitely.
	// Only called when [PublisherOptions.ConfirmMode] is enabled.
	// Providing this callback enables deferred confirmation mode.
	// If not specified (nil), Publish waits for batch confirmations (default behavior).
	// Example: wait(nil) or wait(ctx)
	OnConfirm func(deliveryTag uint64, wait func(context.Context) bool)
	// OnReturn is called when a mandatory or immediate publish is undeliverable.
	// The [Message.ReturnDetails] returns details about the failure.
	// This callback is invoked asynchronously in a separate goroutine and must be safe for concurrent use.
	// It must return quickly; blocking in this callback may lead to resource leaks or degraded performance.
	// If not specified (nil), returned messages are silently discarded.
	OnReturn func(Message)
	// OnFlow is called when the server sends flow control notifications.
	// active=true means publishing can resume, active=false means publishing is paused.
	// This callback is invoked asynchronously in a separate goroutine and must be safe for concurrent use.
	// It must return quickly; blocking in this callback may lead to resource leaks or degraded performance.
	// If not specified (nil), flow control changes are handled internally without notification.
	OnFlow func(active bool)
	// OnError is called when errors occur in background goroutines that cannot be returned.
	// This includes channel close errors during reconnection monitoring.
	// This callback is invoked asynchronously in a separate goroutine and must be safe for concurrent use.
	// It must return quickly; blocking in this callback may lead to resource leaks or degraded performance.
	// If not specified (nil), errors are silently ignored.
	OnError func(err error)
}

// publisher implements [Publisher] interface and
// manages the lifecycle and state of a Publisher.
type publisher struct {
	_ noCopy

	*endpoint

	opts     PublisherOptions
	exchange Exchange

	// flow control state (true=active, false=paused)
	flow atomic.Bool

	// publisher flow notification channel (to pause/resume publishing)
	flowCh <-chan bool
	// undeliverable messages notification channel
	returnCh <-chan amqp.Return
	// confirmation notification channel (if ConfirmMode=true otherwise nil)
	confirmCh <-chan amqp.Confirmation
	// channel close notification
	closeCh <-chan *amqp.Error

	// serialize publishes (one-by-one) to avoid concurrent channel usage
	publishMu sync.Mutex
}

var _ Publisher = (*publisher)(nil)

func newPublisher(id string, b *Broker, opts PublisherOptions, e Exchange) *publisher {
	ep := newEndpoint(id, b, rolePublisher, opts.EndpointOptions)
	p := &publisher{
		endpoint: ep,
		opts:     opts,
		exchange: e,
	}
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

	ch := p.Channel()
	if ch == nil {
		return ErrPublisherNotConnected
	}

	// check flow control state
	if !p.flow.Load() {
		return fmt.Errorf("%w: %s", ErrPublisher, "flow paused by server")
	}

	// no assumptions are made about declaring the queue automatically
	// the user is responsible for ensuring the destination exists

	// publish all messages
	for i, msg := range msgs {
		pub := amqpPublishingFromMessage(&msg)

		if p.opts.ConfirmMode && p.opts.OnConfirm != nil {
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
				return fmt.Errorf("%w: message %d publish failed: %w", ErrPublisher, i, err)
			}
			// invoke callback with delivery tag and wait function
			p.opts.OnConfirm(conf.DeliveryTag, func(ctx context.Context) bool {
				if ctx != nil {
					acked, err := conf.WaitContext(ctx)
					if err != nil {
						go p.opts.OnError(err)
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
				return fmt.Errorf("%w: message %d publish failed: %w", ErrPublisher, i, err)
			}
		}
	}

	// handle confirmations based on mode
	if p.opts.ConfirmMode && p.opts.OnConfirm == nil {
		// batch mode: wait for all confirmations synchronously
		p.stateMu.RLock()
		confirmCh := p.confirmCh
		p.stateMu.RUnlock()
		if confirmCh == nil {
			return fmt.Errorf("%w: %s", ErrPublisherNotConnected, "confirmation channel not available")
		}

		timeout := p.opts.ConfirmTimeout
		if timeout <= 0 {
			timeout = defaultConfirmTimeout
		}

		for i := range len(msgs) {
			select {
			case c := <-confirmCh:
				if !c.Ack {
					return fmt.Errorf("%w: message %d not confirmed by server", ErrPublisher, i)
				}
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(timeout):
				return fmt.Errorf("%w: message %d confirm timeout", ErrPublisher, i)
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

var _ endpointLifecycle = (*publisher)(nil)

// init initializes the publisher's endpoint lifecycle.
func (p *publisher) init(ctx context.Context) error {
	if err := validatePublisherOptions(p.opts); err != nil {
		return fmt.Errorf("%w: %w", ErrPublisher, err)
	}

	return p.endpoint.start(ctx, p, p.opts.OnError)
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

	// set channel on endpoint before declaring topology
	p.stateMu.Lock()
	p.conn = conn
	p.ch = ch
	p.stateMu.Unlock()

	// declare exchange (or redeclare upon reconnection)
	err = p.Exchange(p.exchange)
	if err != nil {
		// allow bypassing validation error without failing
		// (when routing-key == queue), i.e. using "amq.direct"
		if !errors.Is(err, ErrTopologyExchangeNameEmpty) {
			return err
		}
	}

	// channels are buffered channels to avoid deadlocks, unless otherwise noted,
	// the library sends the notification once then closes the X channel

	// set up channel close notification channel
	var closeCh = ch.NotifyClose(make(chan *amqp.Error, 1))
	// set up return notification channel for undeliverable messages
	// buffer sized relative to expected in-flight messages (arbitrary)
	var returnCh = ch.NotifyReturn(make(chan amqp.Return, 256 /*2^8*/))
	// set up flow control notification channel
	var flowCh = ch.NotifyFlow(make(chan bool, 1))
	// set up confirmation channel if enabled
	var confirmCh <-chan amqp.Confirmation

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
		// an unbuffered channel that is consumed in background goroutine is used instead
		confirmCh = ch.NotifyPublish(make(chan amqp.Confirmation))
	}

	p.stateMu.Lock()
	p.closeCh = closeCh
	p.returnCh = returnCh
	p.flowCh = flowCh
	p.confirmCh = confirmCh
	p.stateMu.Unlock()

	p.flow.Store(true) // start in active state until flow notification is received

	go p.handleReturns(ctx) // start return handler goroutine
	go p.handleFlow(ctx)    // start flow control handler goroutine
	// with deferred confirmations, confirmCh still needs to be drained to avoid deadlocks,
	// even though the confirmations are handled by amqp.DeferredConfirmation objects
	if p.opts.ConfirmMode && p.opts.OnConfirm != nil {
		go p.handleConfirmations(ctx) // start confirmations handler goroutine
	}

	return nil
}

// disconnect closes the channel and clears publisher-specific state.
func (p *publisher) disconnect(_ context.Context) error {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	// clear publisher-specific channels
	p.flowCh = nil
	p.returnCh = nil
	p.confirmCh = nil
	p.closeCh = nil

	if p.ch != nil {
		err := p.ch.Close()
		p.ch = nil

		return wrapError("close channel", err)
	}

	return nil
}

// monitor watches for channel closures.
func (p *publisher) monitor(ctx context.Context) error {
	for {
		p.stateMu.RLock()
		closeCh := p.closeCh
		p.stateMu.RUnlock()

		if closeCh == nil {
			return nil // channel closed, exit monitor
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err, ok := <-closeCh:
			if !ok {
				return nil
			}
			amqpErr := wrapError("channel closed", err)
			if amqpErr != nil && p.opts.OnError != nil {
				go p.opts.OnError(amqpErr)
			}
			return amqpErr
		}
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
			// call user callback if provided
			if p.opts.OnReturn != nil {
				msg := amqpReturnToMessage(&r)
				msg.broker = p.broker
				go p.opts.OnReturn(msg)
			}
			// otherwise silently discard (could log here)
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
			// notify user if callback is provided
			if p.opts.OnFlow != nil {
				go p.opts.OnFlow(active) // prevent blocking
			}
		}
	}
}

// handleConfirmations consumes confirmations from the channel to prevent deadlocks.
// This is needed when using deferred confirmation mode because the channel must be consumed
// even though confirmations are handled by amqp.DeferredConfirmation objects.
func (p *publisher) handleConfirmations(ctx context.Context) {
	for {
		p.stateMu.RLock()
		confirmCh := p.confirmCh
		p.stateMu.RUnlock()

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
