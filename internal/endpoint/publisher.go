package endpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

var (
	// ErrPublisher is the base error for publisher operations.
	// All publisher-specific errors wrap this error.
	ErrPublisher = fmt.Errorf("%w publisher", ErrEndpoint)

	// ErrPublisherClosed indicates the publisher is closed.
	// This error is returned when operations are attempted on a closed publisher.
	ErrPublisherClosed = fmt.Errorf("%w: closed", ErrPublisher)

	// ErrPublisherNotConnected indicates the publisher is not connected.
	// This error is returned when the publisher has no active AMQP channel.
	ErrPublisherNotConnected = fmt.Errorf("%w: not connected", ErrPublisher)
)

// Publisher defines a high-level AMQP publisher with automatic connection management,
// background confirmation handling, and graceful resource cleanup. It supports:
//   - Publisher confirms for guaranteed delivery
//   - Returned message handling for mandatory/immediate flags
//   - Automatic reconnection with topology redeclaration
//
// Ownership & Lifecycle:
//   - See [Endpoint] for connection and channel ownership details.
//   - Runs in the background upon creation, ready to publish messages (push-based model).
//   - Use [Publisher.Publish] to send messages.
//   - Graceful shutdown ensures all in-flight publish operations and confirmations complete.
//
// Concurrency:
//   - [Publisher.Publish] is safe for concurrent use by multiple goroutines.
//
// See [PublisherOptions] for configuration details.
type Publisher interface {
	Endpoint
	// Publish sends one or more messages to the exchange with the given routing key.
	Publish(ctx context.Context, rk topology.RoutingKey, msgs ...message.Message) error
}

// PublisherOptions configures publisher-specific behavior.
//
// Key options:
//   - ConfirmMode: Enable publisher confirms for guaranteed delivery
//   - ConfirmTimeout: Maximum wait time for confirmations
//   - OnReturn: Handle returned (unroutable) messages
type PublisherOptions struct {
	// EndpointOptions holds reconnection and readiness configuration.
	EndpointOptions
	// ConfirmMode enables publisher confirmations
	// (recommended for reliable publishing, but introduces overhead).
	ConfirmMode bool
	// ConfirmTimeout is the maximum time to wait for confirmation.
	// Only used if [PublisherOptions.ConfirmMode] is true.
	// Default: [DefaultConfirmTimeout] (5s)
	ConfirmTimeout time.Duration
	// Mandatory causes the server to return unroutable messages.
	// Default: false
	Mandatory bool
	// Immediate causes the server to return any message that cannot be immediately delivered
	// to at least one consumer. Returned messages are passed to [PublisherOptions.OnReturn].
	// Deprecated: RabbitMQ 3.0+ dropped support for this flag; sending a message with Immediate=true
	// against RabbitMQ results in a channel-level protocol error (540 NOT_IMPLEMENTED).
	// Retained for compatibility with non-RabbitMQ AMQP 0-9-1 brokers.
	// Default: false
	Immediate bool
	// OnConfirm is called for each published message with its delivery tag and wait function.
	// Only called when [PublisherOptions.ConfirmMode] is enabled.
	// Providing this callback enables deferred confirmation mode.
	// If not specified (nil), Publish waits for batch confirmations (default behavior).
	OnConfirm func(deliveryTag uint64, wait func(context.Context) bool)
	// OnReturn is called when a mandatory or immediate publish is undeliverable.
	// Invoked asynchronously; must return quickly and be safe for concurrent use.
	// If not specified (nil), returned messages are silently discarded.
	OnReturn func(message.Message)
	// OnFlow is called when the server sends flow control notifications.
	// Invoked asynchronously; must return quickly and be safe for concurrent use.
	// If not specified (nil), flow control changes are handled internally.
	OnFlow func(active bool)
	// OnError is called when errors occur in background goroutines.
	// Invoked asynchronously; must return quickly and be safe for concurrent use.
	// If not specified (nil), errors are silently ignored.
	OnError func(err error)
}

// publisher implements [Publisher] and manages the lifecycle of an AMQP publisher endpoint.
type publisher struct {
	_ noCopy

	*endpoint

	opts     PublisherOptions
	exchange topology.Exchange

	// flow control state (true=active, false=paused)
	flow atomic.Bool

	// channel notification subscriptions (set in connect, cleared in disconnect)
	flowCh    <-chan bool
	returnCh  <-chan transport.Return
	confirmCh <-chan transport.Confirmation
	closeCh   <-chan *transport.Error

	// serialize publishes to avoid concurrent channel usage
	publishMu sync.Mutex
}

var _ Publisher = (*publisher)(nil)

// NewPublisher creates and starts a Publisher. It calls init internally, so the
// returned publisher is already connected (or connecting if NoWaitReady is set).
// Returns (Publisher, error); the concrete type is not exposed.
func NewPublisher(
	ctx context.Context,
	id string,
	connMgr *transport.ConnectionManager,
	topoReg *topology.Registry,
	opts PublisherOptions,
	e topology.Exchange,
) (Publisher, error) {
	p := newPublisher(id, connMgr, topoReg, opts, e)
	if err := p.init(ctx); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

// newPublisher creates a new publisher. Call init to start its connection loop.
func newPublisher(
	id string,
	connMgr *transport.ConnectionManager,
	topoReg *topology.Registry,
	opts PublisherOptions,
	e topology.Exchange,
) *publisher {
	return &publisher{
		endpoint: newEndpoint(id, rolePublisher, connMgr, topoReg, opts.EndpointOptions),
		opts:     opts,
		exchange: e,
	}
}

// Publish sends one or more messages to the exchange with the given routing key.
func (p *publisher) Publish(ctx context.Context, rk topology.RoutingKey, msgs ...message.Message) error {
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

	if !p.flow.Load() {
		return fmt.Errorf("%w: %s", ErrPublisher, "flow paused by server")
	}

	for i, msg := range msgs {
		pub := messageToPublishing(&msg)

		if p.opts.ConfirmMode && p.opts.OnConfirm != nil {
			// deferred confirm mode: each message gets its own DeferredConfirmation
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
			p.opts.OnConfirm(conf.DeliveryTag, func(ctx context.Context) bool {
				if ctx != nil {
					acked, err := conf.WaitContext(ctx)
					if err != nil && p.opts.OnError != nil {
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

	// batch confirm mode: wait for all confirmations synchronously
	if p.opts.ConfirmMode && p.opts.OnConfirm == nil {
		p.stateMu.RLock()
		confirmCh := p.confirmCh
		p.stateMu.RUnlock()

		if confirmCh == nil {
			return fmt.Errorf("%w: %s", ErrPublisherNotConnected, "confirmation channel not available")
		}

		timeout := p.opts.ConfirmTimeout
		if timeout <= 0 {
			timeout = DefaultConfirmTimeout
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

var _ endpointLifecycle = (*publisher)(nil)

// init validates options and starts the publisher's connection management loop.
func (p *publisher) init(ctx context.Context) error {
	if err := ValidatePublisherOptions(p.opts); err != nil {
		return fmt.Errorf("%w: %w", ErrPublisher, err)
	}
	return p.endpoint.start(ctx, p, p.opts.OnError)
}

// connect establishes a connection and channel for publishing.
func (p *publisher) connect(ctx context.Context) error {
	conn, err := p.connectionMgr.Assign(p.role.purpose())
	if err != nil {
		return internal.Wrap("assign connection", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return internal.Wrap("open channel", err)
	}

	p.stateMu.Lock()
	p.conn = conn
	p.ch = ch
	p.stateMu.Unlock()

	if !p.opts.NoAutoDeclare {
		err = p.Exchange(p.exchange)
		if err != nil {
			// allow empty exchange name (e.g. default exchange "amq.direct")
			if !errors.Is(err, topology.ErrTopologyExchangeNameEmpty) {
				return err
			}
		}
	}

	// channels are buffered to avoid deadlocks;
	// the library sends the notification once then closes the channel

	var closeCh = ch.NotifyClose(make(chan *transport.Error, 1))
	var returnCh = ch.NotifyReturn(make(chan transport.Return, 256 /*2^8*/))
	var flowCh = ch.NotifyFlow(make(chan bool, 1))
	var confirmCh <-chan transport.Confirmation

	if p.opts.ConfirmMode {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return internal.Wrap("enable confirm mode", err)
		}
		// ch.NotifyPublish preserves strict ack/nack ordering;
		// must be consumed to avoid deadlocks
		confirmCh = ch.NotifyPublish(make(chan transport.Confirmation))
	}

	p.stateMu.Lock()
	p.closeCh = closeCh
	p.returnCh = returnCh
	p.flowCh = flowCh
	p.confirmCh = confirmCh
	p.stateMu.Unlock()

	p.flow.Store(true) // start in active state

	go p.handleReturns(ctx)
	go p.handleFlow(ctx)
	// drain confirmCh even in deferred mode to prevent deadlocks
	if p.opts.ConfirmMode && p.opts.OnConfirm != nil {
		go p.handleConfirmations(ctx)
	}

	return nil
}

// disconnect closes the channel and clears publisher-specific state.
func (p *publisher) disconnect(_ context.Context) error {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	p.flowCh = nil
	p.returnCh = nil
	p.confirmCh = nil
	p.closeCh = nil

	if p.ch != nil {
		err := p.ch.Close()
		p.ch = nil
		return internal.Wrap("close channel", err)
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
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err, ok := <-closeCh:
			if !ok {
				return nil
			}
			amqpErr := internal.Wrap("channel closed", err)
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
			if p.opts.OnReturn != nil {
				msg := returnToMessage(&r)
				go p.opts.OnReturn(msg)
			}
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
			if p.opts.OnFlow != nil {
				go p.opts.OnFlow(active)
			}
		}
	}
}

// handleConfirmations drains the confirmation channel to prevent deadlocks.
//
// In deferred confirmation mode the broker still sends one amqp.Confirmation per
// published message into confirmCh, exactly as in batch mode. The amqp091 library
// applies back-pressure to the publisher once this channel's buffer fills: if nobody
// is reading from it, subsequent PublishWithDeferredConfirmWithContext calls will block
// indefinitely, deadlocking the publisher. The actual confirmation result is obtained
// via amqp.DeferredConfirmation.Wait()/WaitContext() by the OnConfirm callback, the
// values arriving on confirmCh are redundant in this mode and are intentionally discarded.
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
			// discard: amqp.DeferredConfirmation handles the actual confirmation
		}
	}
}
