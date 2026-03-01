package broker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer defines a high-level AMQP queue consumer with automatic connection management,
// background message delivery, and graceful shutdown.
//
// Ownership & Lifecycle:
//   - See [Endpoint] for connection and channel ownership details.
//   - Runs in the background upon creation, delivering messages to the handler as they arrive (push-based model).
//   - Use [Consumer.Consume] to block until cancellation, and [Consumer.Wait] to wait for all in-flight handlers to complete.
//   - Graceful shutdown ensures all in-flight message handlers complete before returning.
//
// Concurrency:
//   - Message handlers may be invoked concurrently, depending on [ConsumerOptions.MaxConcurrentHandlers].
//   - [Consumer.Wait] blocks until all active handlers finish; does not stop the consumer.
//
// Usage Patterns:
//   - Call [Consumer.Consume] to start processing messages and block until ctx is cancelled.
//   - Use [Consumer.Wait] to block until all in-flight handlers complete (without stopping consumption).
//   - Use [Consumer.Get] for synchronous polling of a single message (returns nil if queue is empty).
//   - Use [Consumer.Cancel] to stop consuming without closing the channel (can be restarted).
//
// See [ConsumerOptions] for configuration details.
type Consumer interface {
	Endpoint
	// Consume blocks and processes messages until the context is cancelled or an error occurs.
	// It automatically handles reconnection and continues consuming until explicitly stopped.
	// Returns an error if the consumer fails to start or encounters a fatal error.
	// Use Wait() to block only until all in-flight message handlers complete.
	Consume(ctx context.Context) error
	// Wait blocks until all in-flight message handlers complete.
	// Returns immediately if no messages are being processed.
	// This does not stop the consumer; messages will continue to be accepted in the background.
	Wait()
	// Get synchronously fetches one message from the queue (polling).
	// Returns nil if queue is empty. The message can be acknowledged manually
	// (using Ack/Nack/Reject), or AutoAck option can be set to true to auto-acknowledge.
	// For continuous delivery, use Consume().
	Get() (*Message, error)
	// Cancel stops consuming messages without closing the channel.
	// Call Consume() again to restart consumption.
	Cancel() error
}

// ConsumerOptions configures Consumer behavior.
type ConsumerOptions struct {
	// EndpointOptions holds reconnection and readiness configuration for the publisher.
	EndpointOptions
	// AutoAck enables automatic message acknowledgment.
	// If false, the handler must return a HandlerAction.
	// Default: false
	AutoAck bool
	// PrefetchCount sets the channel prefetch limit.
	// Default: defaultPrefetchCount (1)
	PrefetchCount int
	// Exclusive sets the consumer to be the exclusive consumer of the queue (sole consumer),
	// otherwise the server will fairly distribute the work across multiple consumers.
	// Default: false
	Exclusive bool
	// NoWait ensures that the consumer does not wait for the server to
	// confirm the consume request and starts receiving deliveries immediately.
	// Default: false
	NoWait bool
	// MaxConcurrentHandlers limits the number of messages processed concurrently.
	// <= 0: unlimited goroutines, one per message (default, [ConsumerOptions.PrefetchCount] provides backpressure)
	// 1: sequential processing, messages handled one at a time
	// N: worker pool with N concurrent handlers
	// Default: 0 (unlimited)
	MaxConcurrentHandlers int
	// OnCancel is called when the server cancels the consumer (queue deleted, failover, etc).
	// This callback is invoked asynchronously in a separate goroutine and must be safe for concurrent use.
	// It must return quickly; blocking in this callback may lead to resource leaks or degraded performance.
	// If not specified (nil), cancellations trigger automatic reconnection.
	OnCancel func(consumerTag string)
	// OnError is called when errors occur in background goroutines that cannot be returned.
	// This includes channel close errors during reconnection monitoring.
	// This callback is invoked asynchronously in a separate goroutine and must be safe for concurrent use.
	// It must return quickly; blocking in this callback may lead to resource leaks or degraded performance.
	// If not specified (nil), errors are silently ignored.
	OnError func(err error)
}

// consumer implements [Consumer] interface and
// manages the lifecycle and state of a Consumer.
type consumer struct {
	_ noCopy

	*endpoint

	opts    ConsumerOptions
	queue   Queue
	handler Handler

	// consumption cancellation state
	cancelled atomic.Bool
	// processing state for deliveries, tracks in-flight message handlers using atomic counter
	// (avoids WaitGroup race when Add() called from async handlers while Wait() may be called concurrently)
	deliveries atomic.Int64

	// consumer cancellation notification channel
	cancelCh <-chan string
	// message delivery notification channel
	deliveryCh <-chan amqp.Delivery
	// channel close notification
	closeCh <-chan *amqp.Error
}

var _ Consumer = (*consumer)(nil)

func newConsumer(id string, b *Broker, opts ConsumerOptions, q Queue, h Handler) *consumer {
	ep := newEndpoint(id, b, roleConsumer, opts.EndpointOptions)
	c := &consumer{
		endpoint: ep,
		opts:     opts,
		queue:    q,
		handler:  h,
	}
	return c
}

// Consume blocks until the provided context is cancelled, then disconnects the consumer
// and waits for all in-flight message handlers to complete.
// The consumer runs in the background (started by NewConsumer), and this method provides
// lifecycle control by blocking until the context is cancelled.
//
// When the context is cancelled:
//   - The consumer stops accepting new messages
//   - The connection is disconnected
//   - All in-flight handlers are allowed to complete
//
// Common usage patterns:
//   - Block in main: consumer.Consume(ctx) until signal
//   - Graceful shutdown: cancel context, Consume() returns after handlers finish
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go func() {
//	    <-sigCh
//	    cancel()  // Trigger shutdown
//	}()
//
//	if err := consumer.Consume(ctx); err != nil {
//	    log.Printf("consumer stopped: %v", err)
//	}
//
// After Consume() returns, the consumer is stopped and cannot be reused.
// To consume again, create a new Consumer instance.
func (c *consumer) Consume(ctx context.Context) error {
	// check if context is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// block until context cancelled
	<-ctx.Done()

	// disconnect consumer
	c.disconnect(ctx)

	// wait for all in-flight handlers to complete
	c.Wait()

	// // restart if WaitForReady is set
	// if !c.opts.NoWaitReady {
	// 	return c.start(ctx, c.connect, c.disconnect, c.monitor)
	// }

	return ctx.Err()
}

// Wait blocks until all in-flight message handlers complete.
// It returns when the WaitGroup counter reaches zero, meaning no messages are currently
// being processed. The consumer continues running and accepting new messages in the background.
//
// Common usage patterns:
//   - Graceful drain: Cancel() -> Wait() -> Close()
//   - Batch processing: send batch -> Wait() to wait for completion
//   - Synchronization: Wait() blocks while messages are actively being processed
//
// Note: Wait() may return between messages if no handlers are running.
// To stop the consumer entirely, cancel the context passed to Consume().
func (c *consumer) Wait() {
	// poll the atomic counter until all processing completes
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for c.deliveries.Load() > 0 {
		<-ticker.C
	}
}

// Get synchronously fetches one message from the queue (polling).
// Returns nil if queue is empty. For continuous delivery, use Consume().
func (c *consumer) Get() (*Message, error) {
	ch := c.Channel()
	if ch == nil {
		return nil, ErrConsumerNotConnected
	}

	delivery, ok, err := ch.Get(c.queue.Name, c.opts.AutoAck)
	if err != nil {
		return nil, fmt.Errorf("%w: get: %w", ErrConsumer, err)
	}

	if !ok {
		return nil, nil // Queue is empty
	}

	msg := amqpDeliveryToMessage(&delivery)
	msg.broker = c.broker

	return &msg, nil
}

// Cancel stops consuming messages without closing the channel.
// Call Consume() again to restart consumption.
func (c *consumer) Cancel() error {
	// swap is used here as an optimistic update, the actual cancel state
	// is set upon receiving server notification in handleCancel goroutine
	if c.cancelled.Swap(true) {
		return nil // already cancelled
	}

	ch := c.Channel()
	if ch == nil {
		return ErrConsumerNotConnected
	}

	// consumer.id is the consumer tag, see call to ch.Consume
	if err := ch.Cancel(c.id, false); err != nil {
		return fmt.Errorf("%w: cancel: %w", ErrConsumer, err)
	}

	return nil
}

// Release removes the consumer from the broker's registry and stops it.
func (c *consumer) Release() {
	if c.broker == nil {
		return
	}
	c.broker.consumersMu.Lock()
	delete(c.broker.consumers, c.id)
	c.broker.consumersMu.Unlock()
	_ = c.Close()
}

var _ endpointLifecycle = (*consumer)(nil)

// init initializes the consumer's endpoint lifecycle.
func (c *consumer) init(ctx context.Context) error {
	if err := validateConsumerOptions(c.opts); err != nil {
		return fmt.Errorf("%w: %w", ErrConsumer, err)
	}

	return c.endpoint.start(ctx, c, c.opts.OnError)
}

// connect establishes a connection, channel, and starts consuming.
func (c *consumer) connect(ctx context.Context) error {
	conn, err := c.broker.connectionMgr.assign(c.role)
	if err != nil {
		return wrapError("assign connection", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return wrapError("open channel", err)
	}

	// set channel on endpoint before declaring topology
	c.stateMu.Lock()
	c.conn = conn
	c.ch = ch
	c.stateMu.Unlock()

	if !c.opts.NoAutoDeclare {
		// declare queue (or redeclare upon reconnection)
		err = c.Queue(c.queue)
		if err != nil {
			return err
		}
	}

	// set QoS
	prefetch := c.opts.PrefetchCount
	if prefetch <= 0 {
		prefetch = defaultPrefetchCount
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return wrapError("set qos", err)
	}

	// start consuming
	// NOTE: do not use ch.ConsumeWithContext here, as it would block cancellation
	// the code here replicates what ch.ConsumeWithContext does but in a non-blocking way
	deliveryCh, err := ch.Consume(
		c.queue.Name,
		c.id,
		c.opts.AutoAck,
		c.opts.Exclusive,
		false, /* unused */
		c.opts.NoWait,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		return wrapError("start consuming", err)
	}

	// channels are buffered channels to avoid deadlocks, unless otherwise noted,
	// the library sends the notification once then closes the X channel

	// set up close notification channel
	var closeCh = ch.NotifyClose(make(chan *amqp.Error, 1))
	// set up cancel notification channel
	var cancelCh = ch.NotifyCancel(make(chan string, 1))

	c.stateMu.Lock()
	c.closeCh = closeCh
	c.deliveryCh = deliveryCh
	c.cancelCh = cancelCh
	c.stateMu.Unlock()

	c.cancelled.Store(false) // start in uncancelled state until cancel notification is received

	go c.handleDeliveries(ctx) // start delivery handler goroutine
	go c.handleCancel(ctx)     // start cancel handler goroutine

	return nil
}

// disconnect closes the channel and clears consumer-specific state.
func (c *consumer) disconnect(_ context.Context) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	// clear consumer-specific state
	c.cancelCh = nil
	c.deliveryCh = nil
	c.closeCh = nil

	if c.ch != nil {
		err := c.ch.Close()
		c.ch = nil

		return wrapError("close channel", err)
	}

	return nil
}

// monitor watches for channel closures and cancellations.
func (c *consumer) monitor(ctx context.Context) error {
	for {
		c.stateMu.RLock()
		closeCh := c.closeCh
		c.stateMu.RUnlock()

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
			// set cancelled state
			c.cancelled.Store(true)
			// notify user if callback is provided
			if c.opts.OnCancel != nil {
				go c.opts.OnCancel(tag)
			}
		}
	}
}

// handleDeliveries processes incoming message deliveries.
func (c *consumer) handleDeliveries(ctx context.Context) {
	var workers = c.opts.MaxConcurrentHandlers
	var processor func(*Message)

	switch {
	case workers <= 0:
		// spawns a goroutine for each message
		processor = func(msg *Message) {
			c.deliveries.Add(1)
			go func() {
				defer c.deliveries.Add(-1)
				c.processMessage(ctx, msg)
			}()
		}
	case workers == 1:
		// processes messages one at a time
		processor = func(msg *Message) {
			c.deliveries.Add(1)
			defer c.deliveries.Add(-1)
			c.processMessage(ctx, msg)
		}
	default:
		// uses a semaphore to limit concurrent handlers
		semaphore := make(chan struct{}, workers)
		processor = func(msg *Message) {
			c.deliveries.Add(1)
			go func() {
				defer c.deliveries.Add(-1)

				// acquire semaphore
				select {
				case <-ctx.Done():
					return
				case semaphore <- struct{}{}:
					// release semaphore
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

			msg := amqpDeliveryToMessage(&d)
			msg.broker = c.broker
			processor(&msg)
		}
	}
}

// processMessage handles a single message with error handling and acknowledgment.
func (c *consumer) processMessage(ctx context.Context, msg *Message) {
	action, err := c.handler(ctx, msg)

	// notify user of handler error if callback provided
	if err != nil && c.opts.OnError != nil {
		err := fmt.Errorf("%w: handler failed for message %q: %w", ErrConsumer, msg.MessageID, err)
		go c.opts.OnError(err)
	}

	if c.opts.AutoAck {
		return
	}

	var ackErr error
	switch action {
	case HandlerActionAck:
		ackErr = msg.metadata.Ack()
	case HandlerActionNackRequeue:
		ackErr = msg.metadata.Nack(true)
	case HandlerActionNackDiscard:
		ackErr = msg.metadata.Nack(false)
	case HandlerActionNoAction:
		// handler already handled ack/nack
	}

	// notify user of ack/nack error if callback provided
	if ackErr != nil && c.opts.OnError != nil {
		err := fmt.Errorf("%w: ack (action=%s) failed for message %q: %w", ErrConsumer, action.String(), msg.MessageID, ackErr)
		go c.opts.OnError(err)
	}
}
