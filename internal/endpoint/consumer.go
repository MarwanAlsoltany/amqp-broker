package endpoint

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/handler"
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

var (
	// ErrConsumer is the base error for consumer operations.
	// All consumer-specific errors wrap this error.
	ErrConsumer = ErrEndpoint.Derive("consumer")

	// ErrConsumerClosed indicates the consumer is closed.
	// This error is returned when operations are attempted on a closed consumer.
	ErrConsumerClosed = ErrConsumer.Derive("closed")

	// ErrConsumerNotConnected indicates the consumer is not connected.
	// This error is returned when the consumer has no active AMQP channel.
	ErrConsumerNotConnected = ErrConsumer.Derive("not connected")
)

// Consumer defines a high-level AMQP consumer with automatic connection management,
// background message delivery, and graceful shutdown. It supports:
//   - Automatic and manual acknowledgment
//   - Configurable concurrency limits
//   - QoS prefetch control
//   - Graceful shutdown with in-flight message handling
//
// Ownership & Lifecycle:
//   - See [Endpoint] for connection and channel ownership details.
//   - Runs in the background upon creation, delivering messages to the handler (push-based model).
//   - Use [Consumer.Consume] to block until cancellation, and [Consumer.Wait] to wait for handlers.
//   - Graceful shutdown ensures all in-flight handlers complete before returning.
//
// Concurrency:
//   - Message handlers may be invoked concurrently, depending on [ConsumerOptions.MaxConcurrentHandlers].
//
// See [ConsumerOptions] for configuration details.
type Consumer interface {
	Endpoint
	// Consume blocks and processes messages until the context is cancelled.
	// Returns ctx.Err() after draining in-flight handlers.
	Consume(ctx context.Context) error
	// Wait blocks until all in-flight message handlers complete.
	// Does not stop the consumer; messages continue to be accepted in the background.
	Wait()
	// Get synchronously fetches one message from the queue (polling).
	// Returns nil, nil if the queue is empty.
	Get() (*message.Message, error)
	// Cancel stops consuming messages without closing the channel.
	// Call Consume() again to restart consumption.
	Cancel() error
}

// ConsumerOptions configures consumer-specific behavior.
//
// Key options:
//   - PrefetchCount: QoS prefetch limit (default: 1)
//   - MaxConcurrentHandlers: Concurrency limit (default: 0, capped by PrefetchCount)
//   - AutoAck: Enable automatic acknowledgment (default: false)
//   - Exclusive: Create exclusive consumer (default: false)
type ConsumerOptions struct {
	// EndpointOptions holds reconnection and readiness configuration.
	EndpointOptions
	// AutoAck enables automatic message acknowledgment.
	// Default: false
	AutoAck bool
	// PrefetchCount sets the channel QoS prefetch limit.
	// Default: [DefaultPrefetchCount] (1)
	PrefetchCount int
	// Exclusive sets the consumer as the sole consumer of the queue.
	// Default: false
	Exclusive bool
	// NoWait starts receiving deliveries immediately without waiting for server confirmation.
	// Default: false
	NoWait bool
	// MaxConcurrentHandlers limits the number of messages processed concurrently.
	//  -1:  unlimited goroutines, one per message (use with caution; [ConsumerOptions.PrefetchCount] provides the only backpressure)
	//   0:  default, capped to the effective [ConsumerOptions.PrefetchCount] (i.e. [DefaultPrefetchCount] if unset)
	//   1:  sequential processing
	//   N:  worker pool with N concurrent handlers
	// Default: 0 (capped by [ConsumerOptions.PrefetchCount])
	MaxConcurrentHandlers int
	// OnCancel is called when the server cancels the consumer.
	// Invoked asynchronously; must return quickly and be safe for concurrent use.
	// If not specified (nil), cancellations trigger automatic reconnection.
	OnCancel func(consumerTag string)
	// OnError is called when errors occur in background goroutines.
	// Invoked asynchronously; must return quickly and be safe for concurrent use.
	// If not specified (nil), errors are silently ignored.
	OnError func(err error)
}

// consumer implements [Consumer] and manages the lifecycle of an AMQP consumer endpoint.
type consumer struct {
	_ noCopy

	*endpoint

	opts    ConsumerOptions
	queue   topology.Queue
	handler handler.Handler

	// consumption cancellation state
	cancelled atomic.Bool
	// in-flight handler count (atomic counter, avoids WaitGroup race with concurrent Add/Wait)
	deliveries atomic.Int64

	// channel notification subscriptions (set in connect, cleared in disconnect)
	cancelCh   <-chan string
	deliveryCh <-chan transport.Delivery
	closeCh    <-chan *transport.Error
}

var _ Consumer = (*consumer)(nil)

// NewConsumer creates and starts a Consumer. It calls init internally, so the
// returned consumer is already connected (or connecting if NoWaitReady is set).
// Returns (Consumer, error); the concrete type is not exposed.
func NewConsumer(
	ctx context.Context,
	id string,
	connMgr *transport.ConnectionManager,
	topoReg *topology.Registry,
	opts ConsumerOptions,
	q topology.Queue,
	h handler.Handler,
) (Consumer, error) {
	c := newConsumer(id, connMgr, topoReg, opts, q, h)
	if err := c.init(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// newConsumer creates a new consumer. Call init to start its connection loop.
func newConsumer(
	id string,
	connMgr *transport.ConnectionManager,
	topoReg *topology.Registry,
	opts ConsumerOptions,
	q topology.Queue,
	h handler.Handler,
) *consumer {
	return &consumer{
		endpoint: newEndpoint(id, roleConsumer, connMgr, topoReg, opts.EndpointOptions),
		opts:     opts,
		queue:    q,
		handler:  h,
	}
}

// Consume blocks until the context is cancelled, then disconnects and waits for in-flight handlers.
func (c *consumer) Consume(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	<-ctx.Done()

	// disconnect tears down the AMQP channel but intentionally does NOT set c.closed,
	// keeping Consume restartable - the expected pattern is:
	// cancel the context to stop consumption, then call Consume again with a new context
	// to resume (or call Cancel() to stop consuming without closing the channel, then Consume again),
	// Close() is the terminal operation that permanently marks the consumer as closed and unusable
	c.disconnect(ctx)
	c.Wait()

	return ctx.Err()
}

// Wait blocks until all in-flight message handlers complete.
func (c *consumer) Wait() {
	// sync.WaitGroup is intentionally not used here, its Add/Wait contract requires
	// that Add() is never called concurrently with a Wait() that has already observed zero;
	// in a streaming consumer that invariant cannot be guaranteed: a new delivery can arrive
	// at the exact moment Wait() observes zero and is about to return, causing a data race;
	// using an atomic counter with a polling ticker avoids that race,
	// the trade-off is up to one tick period (10ms) of extra latency before Wait() returns,
	// which is acceptable for graceful shutdown
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for c.deliveries.Load() > 0 {
		<-ticker.C
	}
}

// Get synchronously fetches one message from the queue (polling).
// Returns nil, nil if the queue is empty.
func (c *consumer) Get() (*message.Message, error) {
	ch := c.Channel()
	if ch == nil {
		return nil, ErrConsumerNotConnected
	}

	delivery, ok, err := ch.Get(c.queue.Name, c.opts.AutoAck)
	if err != nil {
		return nil, ErrConsumer.Detailf("get: %w", err)
	}

	if !ok {
		return nil, nil
	}

	msg := deliveryToMessage(&delivery)
	return &msg, nil
}

// Cancel stops consuming messages without closing the channel.
func (c *consumer) Cancel() error {
	if c.cancelled.Swap(true) {
		return nil // already cancelled
	}

	ch := c.Channel()
	if ch == nil {
		return ErrConsumerNotConnected
	}

	// c.id is the consumer tag (set when ch.Consume is called)
	if err := ch.Cancel(c.id, false); err != nil {
		return ErrConsumer.Detailf("cancel: %w", err)
	}

	return nil
}

var _ endpointLifecycle = (*consumer)(nil)

// init validates options and starts the consumer's connection management loop.
func (c *consumer) init(ctx context.Context) error {
	if err := ValidateConsumerOptions(c.opts); err != nil {
		return ErrConsumer.Detailf("%w", err)
	}
	return c.endpoint.start(ctx, c, c.opts.OnError)
}

// connect establishes a connection, channel, and starts consuming.
func (c *consumer) connect(ctx context.Context) error {
	conn, err := c.connectionMgr.Assign(c.role.purpose())
	if err != nil {
		return ErrConsumer.Detailf("assign connection: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return ErrConsumer.Detailf("open channel: %w", err)
	}

	c.stateMu.Lock()
	c.conn = conn
	c.ch = ch
	c.stateMu.Unlock()

	if !c.opts.NoAutoDeclare {
		err = c.Queue(c.queue)
		if err != nil {
			return err
		}
	}

	prefetch := c.opts.PrefetchCount
	if prefetch <= 0 {
		prefetch = DefaultPrefetchCount
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return ErrConsumer.Detailf("set qos: %w", err)
	}

	// start consuming; c.id is used as the consumer tag for cancel/identify
	deliveryCh, err := ch.Consume(
		c.queue.Name,
		c.id,
		c.opts.AutoAck,
		c.opts.Exclusive,
		false, // noLocal, unused by RabbitMQ
		c.opts.NoWait,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		return ErrConsumer.Detailf("start consuming: %w", err)
	}

	// channels are buffered to avoid deadlocks;
	// the library sends the notification once then closes the channel

	var closeCh = ch.NotifyClose(make(chan *transport.Error, 1))
	var cancelCh = ch.NotifyCancel(make(chan string, 1))

	c.stateMu.Lock()
	c.closeCh = closeCh
	c.deliveryCh = deliveryCh
	c.cancelCh = cancelCh
	c.stateMu.Unlock()

	c.cancelled.Store(false)

	go c.handleDeliveries(ctx)
	go c.handleCancel(ctx)

	return nil
}

// disconnect closes the channel and clears consumer-specific state.
func (c *consumer) disconnect(_ context.Context) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	c.cancelCh = nil
	c.deliveryCh = nil
	c.closeCh = nil

	if c.ch != nil {
		err := c.ch.Close()
		c.ch = nil
		return ErrConsumer.Detailf("close channel: %w", err)
	}

	return nil
}

// monitor watches for channel closures.
func (c *consumer) monitor(ctx context.Context) error {
	for {
		c.stateMu.RLock()
		closeCh := c.closeCh
		c.stateMu.RUnlock()

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
			amqpErr := ErrConsumer.Detailf("channel closed: %w", err)
			if amqpErr != nil && c.opts.OnError != nil {
				go c.opts.OnError(amqpErr)
			}
			return amqpErr
		}
	}
}

// handleCancel processes consumer cancellation notifications from the server.
func (c *consumer) handleCancel(ctx context.Context) {
	for {
		c.stateMu.RLock()
		cancelCh := c.cancelCh
		c.stateMu.RUnlock()

		if cancelCh == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case tag, ok := <-cancelCh:
			if !ok {
				return
			}
			c.cancelled.Store(true)
			if c.opts.OnCancel != nil {
				go c.opts.OnCancel(tag)
			}
		}
	}
}

// handleDeliveries processes incoming message deliveries.
func (c *consumer) handleDeliveries(ctx context.Context) {
	var workers = c.opts.MaxConcurrentHandlers
	if workers == 0 {
		prefetch := c.opts.PrefetchCount
		if prefetch <= 0 {
			prefetch = DefaultPrefetchCount
		}
		workers = prefetch
	}

	var processor func(*message.Message)

	switch {
	case workers < 0:
		// spawn a goroutine per message
		processor = func(msg *message.Message) {
			c.deliveries.Add(1)
			go func() {
				defer c.deliveries.Add(-1)
				c.processMessage(ctx, msg)
			}()
		}
	case workers == 1:
		// sequential processing
		processor = func(msg *message.Message) {
			c.deliveries.Add(1)
			defer c.deliveries.Add(-1)
			c.processMessage(ctx, msg)
		}
	default:
		// semaphore-bounded worker pool
		semaphore := make(chan struct{}, workers)
		processor = func(msg *message.Message) {
			c.deliveries.Add(1)
			go func() {
				defer c.deliveries.Add(-1)
				select {
				case <-ctx.Done():
					return
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				}
				c.processMessage(ctx, msg)
			}()
		}
	}

	for {
		c.stateMu.RLock()
		deliveryCh := c.deliveryCh
		c.stateMu.RUnlock()

		if deliveryCh == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveryCh:
			if !ok {
				return
			}
			msg := deliveryToMessage(&d)
			processor(&msg)
		}
	}
}

// processMessage invokes the handler and applies the appropriate acknowledgment action.
func (c *consumer) processMessage(ctx context.Context, msg *message.Message) {
	action, err := c.handler(ctx, msg)

	if err != nil && c.opts.OnError != nil {
		err := ErrConsumer.Detailf("handler failed for message %q: %w", msg.MessageID, err)
		go c.opts.OnError(err)
	}

	if c.opts.AutoAck {
		return
	}

	var ackErr error
	switch action {
	case handler.ActionAck:
		ackErr = msg.Ack()
	case handler.ActionNackRequeue:
		ackErr = msg.Nack(true)
	case handler.ActionNackDiscard:
		ackErr = msg.Nack(false)
	case handler.ActionNoAction:
		// handler already called Ack/Nack/Reject manually
	}

	if ackErr != nil && c.opts.OnError != nil {
		err := ErrConsumer.Detailf("ack (action=%s) failed for message %q: %w", action.String(), msg.MessageID, ackErr)
		go c.opts.OnError(err)
	}
}
