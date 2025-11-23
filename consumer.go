package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer provides methods for consuming messages from AMQP queues.
// Consumers own a dedicated connection and channel, and support automatic
// reconnection and graceful shutdown.
type Consumer interface {
	Endpoint
	// Consume blocks and processes messages until the context is cancelled or an error occurs.
	// It automatically handles reconnection and continues consuming until explicitly stopped.
	// Returns an error if the consumer fails to start or encounters a fatal error.
	// Use Wait() to block until all in-flight message handlers complete.
	Consume(ctx context.Context) error
	// Wait blocks until all in-flight message handlers complete.
	// Returns immediately if no messages are being processed.
	// This does not stop the consumer; messages will continue to be accepted in the background.
	Wait()
	// Get synchronously fetches one message from the queue (polling).
	// Returns nil if queue is empty. For continuous delivery, use Consume().
	Get(autoAck bool) (*Message, error)
	// Pause pauses message deliveries from the server.
	// Call Resume() to restart deliveries.
	Pause() error
	// Resume resumes message deliveries from the server.
	Resume() error
	// Cancel stops consuming messages without closing the channel.
	// Call Consume() again to restart consumption.
	Cancel() error
}

// AckAction specifies how a message should be acknowledged.
type AckAction int

func (a AckAction) String() string {
	switch a {
	case AckActionAck:
		return "ack"
	case AckActionNackRequeue:
		return "nack.requeue"
	case AckActionNackDiscard:
		return "nack.discard"
	case AckActionNoAction:
		return ""
	default:
		return "unknown"
	}
}

const (
	// AckActionAck acknowledges the message successfully.
	AckActionAck AckAction = iota
	// AckActionNackRequeue rejects the message and requeues it.
	AckActionNackRequeue
	// AckActionNackDiscard rejects the message without requeuing.
	AckActionNackDiscard
	// AckActionNoAction indicates the handler already called Ack/Nack/Reject.
	AckActionNoAction
)

// Handler is called for each consumed message.
// It returns an AckAction to control message acknowledgment.
type Handler func(ctx context.Context, msg *Message) (AckAction, error)

// ConsumerOptions configures Consumer behavior.
type ConsumerOptions struct {
	// AutoAck enables automatic message acknowledgment.
	// If false, the handler must return an AckAction.
	// Default: false
	AutoAck bool
	// PrefetchCount sets the channel prefetch limit.
	// Default: defaultPrefetchCount
	PrefetchCount int
	// Exclusive sets the consumer to be the exclusive consumer of the queue (sole consumer),
	// otherwise the server will fairly distribute the work across multiple consumers.
	// Default: false
	Exclusive bool
	// NoWait ensures that the consumer does not wait for the server to
	// confirm the consume request and starts receiving deliveries immediately.
	// Default: false
	NoWait bool
	// NoAutoReconnect disables automatic reconnection on connection failure.
	// Default: false
	NoAutoReconnect bool
	// ReconnectDelay is the delay between reconnection attempts.
	// Default: defaultReconnectDelay
	ReconnectDelay time.Duration
	// NoWaitForReady prevents new consumer from blocking until is's connected.
	// Default: false
	NoWaitForReady bool
	// ReadyTimeout is the maximum time to wait for the consumer to be ready.
	// Only used if NoWaitForReady is false.
	// Default: defaultReadyTimeout
	ReadyTimeout time.Duration
	// MaxConcurrentHandlers limits the number of messages processed concurrently.
	// <= 0: unlimited goroutines, one per message (default, PrefetchCount provides backpressure)
	// 1: sequential processing, messages handled one at a time
	// N: worker pool with N concurrent handlers
	// Default: 0 (unlimited)
	MaxConcurrentHandlers int
	// OnCancel is called when the server cancels the consumer (queue deleted, failover, etc).
	// The string parameter is the consumer tag.
	// If nil, cancellations trigger automatic reconnection.
	OnCancel func(string)
	// OnError is called when errors occur in background goroutines that cannot be returned.
	// This includes message handler errors, ack/nack errors, and channel close errors.
	// If nil, errors are silently ignored.
	OnError func(err error)
}

// consumer manages the lifecycle and state of a Consumer.
type consumer struct {
	*endpoint
	opts    ConsumerOptions
	queue   Queue
	handler Handler
	// consumer-specific state
	cancelCh <-chan string
	// track handlers
	wg sync.WaitGroup
	// track consumption state
	consuming atomic.Bool
}

var _ Consumer = (*consumer)(nil)

func newConsumer(b *Broker, id string, opts ConsumerOptions, q Queue, h Handler) *consumer {
	c := &consumer{
		endpoint: newEndpoint(id, b, roleConsumer),
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
	// Block until context cancelled
	<-ctx.Done()

	// Disconnect consumer
	c.disconnect()
	// Wait for all in-flight handlers to complete
	c.Wait()
	// // Restart if WaitForReady is set
	// if !c.opts.NoWaitForReady {
	// 	go c.run()
	// 	// Wait for ready
	// 	readyCtx, cancel := context.WithTimeout(c.broker.ctx, c.opts.ReadyTimeout)
	// 	defer cancel()
	// 	if !c.waitReady(readyCtx) {
	// 		return ErrNotReadyTimeout
	// 	}
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
	c.wg.Wait()
}

// Get synchronously fetches one message from the queue (polling).
// Returns nil if queue is empty. For continuous delivery, use Consume().
func (c *consumer) Get(autoAck bool) (*Message, error) {
	ch := c.Channel()
	if ch == nil || ch.IsClosed() {
		return nil, ErrChannelNotAvailable
	}

	delivery, ok, err := ch.Get(c.queue.Name, autoAck)
	if err != nil {
		return nil, wrapError("get message", err)
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
	if !c.consuming.Swap(false) {
		return nil // Already cancelled
	}

	ch := c.Channel()
	if ch == nil || ch.IsClosed() {
		return ErrChannelNotAvailable
	}

	// Use consumer.id as the consumer tag
	if err := ch.Cancel(c.id, false); err != nil {
		return wrapError("cancel consumer", err)
	}

	return nil
}

// Pause pauses message deliveries from the server.
// Call Resume() to restart deliveries.
func (c *consumer) Pause() error {
	ch := c.Channel()
	if ch == nil || ch.IsClosed() {
		return ErrChannelNotAvailable
	}

	if err := ch.Flow(false); err != nil {
		return wrapError("pause flow", err)
	}

	return nil
}

// Resume resumes message deliveries from the server.
func (c *consumer) Resume() error {
	ch := c.Channel()
	if ch == nil || ch.IsClosed() {
		return ErrChannelNotAvailable
	}

	if err := ch.Flow(true); err != nil {
		return wrapError("resume flow", err)
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

// run is the main goroutine for the consumer.
func (c *consumer) run() {
	ctx, cancel := context.WithCancel(c.broker.ctx)
	defer func() {
		cancel()
		c.disconnect()
	}()

	autoReconnect := !c.opts.NoAutoReconnect
	reconnectDelay := c.opts.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = defaultReconnectDelay
	}

	_ = c.makeReady(
		ctx,
		autoReconnect,
		reconnectDelay,
		c.connect,
		c.monitor,
	)
}

// connect establishes a connection, channel, and starts consuming.
func (c *consumer) connect(ctx context.Context) error {
	conn, err := c.broker.connMgr.assign(ctx, c.role)
	if err != nil {
		return wrapError("assign connection", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return wrapError("open channel", err)
	}

	// Declare queue
	err = c.endpoint.broker.declareQueue(ch, c.queue)
	if err != nil {
		return wrapError("declare queue", err)
	}

	// Set QoS
	prefetch := c.opts.PrefetchCount
	if prefetch <= 0 {
		prefetch = defaultPrefetchCount
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return wrapError("set qos", err)
	}

	// Start consuming
	deliveries, err := ch.Consume(
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

	// Set up cancel notification
	cancelCh := ch.NotifyCancel(make(chan string, 1))

	c.stateMu.Lock()
	c.conn = conn
	c.ch = ch
	c.cancelCh = cancelCh
	c.stateMu.Unlock()

	// Mark as consuming
	c.consuming.Store(true)

	// Start delivery handler
	go c.handleDeliveries(ctx, deliveries)

	return nil
}

// disconnect closes the channel and clears consumer-specific state.
func (c *consumer) disconnect() {
	c.ready.Store(false)

	c.stateMu.Lock()
	if c.ch != nil {
		_ = c.ch.Close()
		c.ch = nil
	}
	// Clear consumer-specific state
	c.cancelCh = nil
	c.stateMu.Unlock()
}

// monitor watches for channel closures and cancellations.
func (c *consumer) monitor(ctx context.Context) error {
	ch := c.Channel()
	if ch == nil {
		return ErrChannelNotAvailable
	}

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	c.stateMu.RLock()
	cancelCh := c.cancelCh
	c.stateMu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-closeCh:
		amqpErr := newAMQPError("channel closed", err)
		if amqpErr != nil && c.opts.OnError != nil {
			c.opts.OnError(amqpErr)
		}
		c.disconnect()
		return amqpErr
	case tag := <-cancelCh:
		if c.opts.OnCancel != nil {
			c.opts.OnCancel(tag)
		}
		c.disconnect()
		return fmt.Errorf("consumer cancelled: %s", tag)
	}
}

// handleDeliveries processes incoming message deliveries.
func (c *consumer) handleDeliveries(ctx context.Context, deliveries <-chan amqp.Delivery) {
	var workers = c.opts.MaxConcurrentHandlers
	var processor func(*Message)

	switch {
	case workers <= 0:
		// spawns a goroutine for each message
		processor = func(msg *Message) {
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.processMessage(ctx, msg)
			}()
		}
	case workers == 1:
		// processes messages one at a time
		processor = func(msg *Message) {
			c.wg.Add(1)
			defer c.wg.Done()
			c.processMessage(ctx, msg)
		}
	default:
		// uses a semaphore to limit concurrent handlers
		semaphore := make(chan struct{}, workers)
		processor = func(msg *Message) {
			// acquire semaphore
			select {
			case <-ctx.Done():
				return
			case semaphore <- struct{}{}:
			}

			c.wg.Add(1)
			go func() {
				defer func() {
					<-semaphore // release semaphore
					c.wg.Done()
				}()
				c.processMessage(ctx, msg)
			}()
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
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

	// Notify user of handler error if callback provided
	if err != nil && c.opts.OnError != nil {
		c.opts.OnError(fmt.Errorf("handler failed for message %s: %w", msg.MessageID, err))
	}

	if c.opts.AutoAck {
		return
	}

	var ackErr error
	switch action {
	case AckActionAck:
		ackErr = msg.amqpMetadata.Ack()
	case AckActionNackRequeue:
		ackErr = msg.amqpMetadata.Nack(true)
	case AckActionNackDiscard:
		ackErr = msg.amqpMetadata.Nack(false)
	case AckActionNoAction:
		// handler already handled ack/nack
	}

	// Notify user of ack/nack error if callback provided
	if ackErr != nil && c.opts.OnError != nil {
		c.opts.OnError(fmt.Errorf("%s failed for message %s: %w", action.String(), msg.MessageID, ackErr))
	}
}
