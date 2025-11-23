package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer provides methods for consuming messages from AMQP queues.
// Consumers own a dedicated connection and channel, and support automatic
// reconnection and graceful shutdown.
type Consumer interface {
	Endpoint
	// Consume blocks until all in-flight message handlers complete.
	Consume()
}

// AckAction specifies how a message should be acknowledged.
type AckAction int

func (a AckAction) String() string {
	switch a {
	case AckActionAck:
		return "ack"
	case AckActionNackRequeue:
		return "nack_requeue"
	case AckActionNackDiscard:
		return "nack_discard"
	case AckActionNoAction:
		return "no_action"
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

// Consume blocks until all in-flight message handlers complete.
func (c *consumer) Consume() {
	c.wg.Wait()
}

// Unregister removes the consumer from the broker's registry and stops it.
func (c *consumer) Unregister() {
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

	autoConnect := !c.opts.NoAutoReconnect
	reconnectDelay := c.opts.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = defaultReconnectDelay
	}

	c.makeReady(
		ctx,
		autoConnect,
		reconnectDelay,
		c.connect,
		c.monitor,
	)
}

// connect establishes a connection, channel, and starts consuming.
func (c *consumer) connect(ctx context.Context) error {
	conn, err := c.broker.connMgr.assign(ctx, c.role)
	if err != nil {
		return fmt.Errorf("assign connection: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	// Declare queue
	err = c.endpoint.broker.declareQueue(ch, c.queue)
	if err != nil {
		return fmt.Errorf("ensure queue declared: %w", err)
	}

	// Set QoS
	prefetch := c.opts.PrefetchCount
	if prefetch <= 0 {
		prefetch = defaultPrefetchCount
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("qos: %w", err)
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
		return fmt.Errorf("consume start: %w", err)
	}

	// Set up cancel notification
	cancelCh := ch.NotifyCancel(make(chan string, 1))

	c.stateMu.Lock()
	c.conn = conn
	c.ch = ch
	c.cancelCh = cancelCh
	c.stateMu.Unlock()

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
		return errors.New("channel not available")
	}

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	c.stateMu.RLock()
	cancelCh := c.cancelCh
	c.stateMu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-closeCh:
		if err != nil && c.opts.OnError != nil {
			c.opts.OnError(fmt.Errorf("channel closed: %w", err))
		}
		c.disconnect()
		return err
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
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}

			msg := deliveryToMessage(&d)
			msg.broker = c.broker

			c.wg.Add(1)
			go func(msg *Message) {
				defer c.wg.Done()
				action, err := c.handler(ctx, msg)

				// Notify user of handler error if callback provided
				if err != nil && c.opts.OnError != nil {
					c.opts.OnError(fmt.Errorf("handler error for message %s: %w", msg.MessageID, err))
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
			}(&msg)
		}
	}
}
