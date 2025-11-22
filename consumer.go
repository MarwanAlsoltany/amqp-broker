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
}

// consumer manages the lifecycle and state of a Consumer.
type consumer struct {
	id      string
	broker  *Broker
	opts    ConsumerOptions
	queue   Queue
	handler Handler
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
	// track handlers
	wg sync.WaitGroup
}

func newConsumer(b *Broker, id string, opts ConsumerOptions, q Queue, h Handler) *consumer {
	return &consumer{
		id:          id,
		broker:      b,
		opts:        opts,
		queue:       q,
		handler:     h,
		reconnectCh: make(chan struct{}, 1),
		readyCh:     make(chan struct{}),
	}
}

var _ Consumer = (*consumer)(nil)

// Consume blocks until all in-flight message handlers complete.
func (c *consumer) Consume() {
	c.wg.Wait()
}

// Connection returns the current connection (may be nil).
func (c *consumer) Connection() *amqp.Connection {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.conn
}

// Channel returns the current channel (may be nil).
func (c *consumer) Channel() *amqp.Channel {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.ch
}

// Close stops the consumer and releases resources.
func (c *consumer) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.stateMu.Lock()
	if c.cancelFn != nil {
		c.cancelFn()
	}
	c.stateMu.Unlock()
	return nil
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
	c.stateMu.Lock()
	c.cancelFn = cancel
	c.stateMu.Unlock()

	defer func() {
		c.cleanup()
	}()

	noAutoReconnect := c.opts.NoAutoReconnect
	reconnectDelay := c.opts.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = defaultReconnectDelay
	}

	for {
		if c.closed.Load() {
			return
		}

		// Connect and start consuming
		if err := c.connect(ctx); err != nil {
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
		c.ready.Store(true)
		select {
		case <-c.readyCh:
		default:
			close(c.readyCh)
		}

		// Monitor connection
		conn := c.Connection()
		if conn == nil {
			if noAutoReconnect {
				return
			}
			time.Sleep(reconnectDelay)
			continue
		}

		notify := conn.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-ctx.Done():
			return
		case <-c.reconnectCh:
			c.disconnect()
			continue
		case err := <-notify:
			_ = err // suppress unused variable warning
			// log.Printf("consumer: [id=%s] connection closed: %v", c.id, err)
			c.disconnect()
			if noAutoReconnect {
				return
			}
			time.Sleep(reconnectDelay)
			continue
		}
	}
}

// connect establishes a connection, channel, and starts consuming.
func (c *consumer) connect(ctx context.Context) error {
	conn, relConn, err := c.broker.connPool.get(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		relConn(true)
		return fmt.Errorf("open channel: %w", err)
	}

	// Declare queue
	err = c.broker.ensureQueueDeclared(ch, c.queue, true)
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
		relConn(true)
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
		relConn(true)
		return fmt.Errorf("consume start: %w", err)
	}

	c.stateMu.Lock()
	c.conn = conn
	c.relConn = relConn
	c.ch = ch
	c.stateMu.Unlock()

	// Start delivery handler
	go func(ctx context.Context, deliveries <-chan amqp.Delivery) {
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
					action, _ := c.handler(ctx, msg)

					if c.opts.AutoAck {
						return
					}

					switch action {
					case AckActionAck:
						_ = msg.delivery.Ack()
					case AckActionNackRequeue:
						_ = msg.delivery.Nack(true)
					case AckActionNackDiscard:
						_ = msg.delivery.Nack(false)
					case AckActionNoAction:
						// handler already handled ack/nack
					}
				}(&msg)
			}
		}
	}(ctx, deliveries)

	return nil
}

// disconnect closes the channel and releases the connection.
func (c *consumer) disconnect() {
	c.ready.Store(false)

	c.stateMu.Lock()
	if c.ch != nil {
		_ = c.ch.Close()
		c.ch = nil
	}
	if c.relConn != nil {
		c.relConn(false)
		c.relConn = nil
	}
	c.conn = nil
	c.stateMu.Unlock()
}

// reconnect signals the consumer to reconnect.
func (c *consumer) reconnect() {
	select {
	case c.reconnectCh <- struct{}{}:
	default:
	}
}

// cleanup performs final cleanup when the consumer goroutine exits.
func (c *consumer) cleanup() {
	c.disconnect()
	c.stateMu.Lock()
	if c.cancelFn != nil {
		c.cancelFn()
		c.cancelFn = nil
	}
	c.stateMu.Unlock()
}

// waitReady blocks until the consumer is ready or context is done.
func (c *consumer) waitReady(ctx context.Context) bool {
	if c.ready.Load() {
		return true
	}
	c.stateMu.RLock()
	ch := c.readyCh
	c.stateMu.RUnlock()
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
