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

var (
	ErrBrokerClosed = errors.New("broker is closed")
)

// Broker manages AMQP connections, publishers, and consumers with automatic
// reconnection, connection management, and topology management.
type Broker struct {
	id  string
	url string

	// Base context for managing lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// Closed state
	closed atomic.Bool

	// Connection manager for all connections
	connMgr      *connectionManager
	connPoolSize int

	// Topology and declaration caching
	topology   *Topology
	topologyMu sync.RWMutex

	// Cache of declared exchanges, queues, and bindings
	declarations sync.Map // map[string]struct{}

	// Publisher registry for managed publishers
	publishers   map[string]Publisher
	publishersMu sync.Mutex

	// Consumer registry for managed consumers
	consumers   map[string]Consumer
	consumersMu sync.Mutex

	// Reconnection configuration
	reconnectMin time.Duration
	reconnectMax time.Duration
}

// BrokerOption configures a Broker instance.
type BrokerOption func(*Broker)

// WithURL sets the AMQP URL for the Broker.
// Defaults to "amqp://guest:guest@localhost:5672/".
func WithURL(url string) BrokerOption {
	return func(b *Broker) {
		b.url = url
	}
}

// WithIdentifier sets a custom identifier for the Broker.
// It is used in managed publisher and consumer IDs.
// Defaults to {hostname}-{processID}.
func WithIdentifier(id string) BrokerOption {
	return func(b *Broker) {
		b.id = id
	}
}

// WithConnectionPoolSize sets the number of managed connections.
// - Size 1: All operations share one connection
// - Size 2: Publishers/Control use one, Consumers use another (recommended default)
// - Size 3+: Dedicated connections for publishers, consumers, and control
// Defaults to defaultConnPoolSize (2).
func WithConnectionPoolSize(n int) BrokerOption {
	return func(b *Broker) {
		if n <= 0 {
			n = defaultConnPoolSize
		}
		b.connPoolSize = n
	}
}

// WithReconnectConfig sets the minimum and maximum reconnection backoff.
// Defaults to defaultReconnectMin and defaultReconnectMax.
func WithReconnectConfig(min, max time.Duration) BrokerOption {
	return func(b *Broker) {
		if min > 0 {
			b.reconnectMin = min
		}
		if max > 0 {
			b.reconnectMax = max
		}
	}
}

// WithContext sets the base context for the Broker.
// The context is used for managing the lifecycle of connections and endpoints.
func WithContext(ctx context.Context) BrokerOption {
	return func(b *Broker) {
		b.ctx, b.cancel = context.WithCancel(ctx)
	}
}

// NewBroker creates a new Broker and establishes the initial control connection.
// It starts a background goroutine to maintain the control connection.
func NewBroker(opts ...BrokerOption) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		id:           defaultBrokerID,
		url:          defaultBrokerURL,
		ctx:          ctx,
		cancel:       cancel,
		connPoolSize: defaultConnPoolSize,
		publishers:   make(map[string]Publisher),
		consumers:    make(map[string]Consumer),
		reconnectMin: defaultReconnectMin,
		reconnectMax: defaultReconnectMax,
	}

	for _, opt := range opts {
		opt(b)
	}

	if b.reconnectMin <= 0 {
		return nil, fmt.Errorf("invalid reconnect config: min must be positive")
	}
	if b.reconnectMax <= b.reconnectMin {
		return nil, fmt.Errorf("invalid reconnect config: max must be greater than min")
	}

	b.connMgr = newConnectionManager(b.url, b.connPoolSize)

	// Initialize all managed connections
	if err := b.connMgr.init(ctx); err != nil {
		return nil, err
	}

	return b, nil
}

// Connection returns the control connection for topology operations.
func (b *Broker) Connection() (*amqp.Connection, error) {
	conn, err := b.connMgr.assign(b.ctx, roleControl)
	if err != nil {
		return nil, fmt.Errorf("assign control connection: %w", err)
	}

	return conn, nil
}

// Channel returns the control channel for topology operations.
//
// NOTE: A Channel returned by this method should be closed by the caller.
func (b *Broker) Channel() (*amqp.Channel, error) {
	conn, err := b.Connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create control channel: %w", err)
	}

	return ch, nil
}

// Close shuts down the broker and all managed endpoints.
func (b *Broker) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Cancel background context
	b.cancel()

	// Close all managed publishers
	b.publishersMu.Lock()
	for _, pe := range b.publishers {
		_ = pe.Close()
	}
	b.publishersMu.Unlock()

	// Close all managed consumers
	b.consumersMu.Lock()
	for _, ce := range b.consumers {
		_ = ce.Close()
	}
	b.consumersMu.Unlock()

	// Close connection manager
	if b.connMgr != nil {
		b.connMgr.close()
	}

	return nil
}

// Declare applies the given topology to the broker (exchanges, queues, and bindings).
// The topology is cached and will be reapplied on reconnection.
func (b *Broker) Declare(t *Topology) error {
	b.topologyMu.Lock()
	b.topology = t
	b.topologyMu.Unlock()

	ch, err := b.Channel()
	if err != nil {
		return fmt.Errorf("control channel is not established: %w", err)
	}
	defer ch.Close()

	// Declare all exchanges
	for _, e := range t.Exchanges {
		if err := b.declareExchange(ch, e); err != nil {
			return err
		}
	}

	// Declare all queues
	for _, q := range t.Queues {
		if err := b.declareQueue(ch, q); err != nil {
			return err
		}
	}

	// Declare all bindings
	for _, bn := range t.Bindings {
		if err := b.declareBinding(ch, bn); err != nil {
			return err
		}
	}

	return nil
}

// declareExchange declares an exchange if not already cached.
// The channel parameter should be the control channel or an endpoint's channel.
func (b *Broker) declareExchange(ch *amqp.Channel, e Exchange) error {
	if e.Name == "" {
		return fmt.Errorf("exchange name cannot be empty")
	}

	if _, loaded := b.declarations.LoadOrStore(hash(e), e); loaded {
		return nil // already declared
	}

	kind := e.Type
	if kind == "" {
		kind = defaultExchangeType
	}

	err := ch.ExchangeDeclare(
		e.Name, kind,
		e.Durable,
		e.AutoDelete,
		e.Internal,
		e.NoWait,
		e.Args,
	)
	if err != nil {
		b.declarations.Delete(hash(e)) // Remove from cache if failed
		return fmt.Errorf("failed to declare exchange %s: %w", e.Name, err)
	}

	return nil
}

// declareQueue declares a queue if not already cached.
// The channel parameter should be the control channel or an endpoint's channel.
func (b *Broker) declareQueue(ch *amqp.Channel, q Queue) error {
	if q.Name == "" {
		return fmt.Errorf("queue name cannot be empty")
	}

	if _, loaded := b.declarations.LoadOrStore(hash(q), q); loaded {
		return nil
	}

	_, err := ch.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args,
	)
	if err != nil {
		b.declarations.Delete(hash(q)) // Remove from cache if failed
		return fmt.Errorf("failed to declare queue %s: %w", q.Name, err)
	}

	return nil
}

// declareBinding declares a binding if not already cached.
// The channel parameter should be the control channel or an endpoint's channel.
func (b *Broker) declareBinding(ch *amqp.Channel, bn Binding) error {
	if bn.Source == "" || bn.Destination == "" {
		return fmt.Errorf("binding source and destination cannot be empty")
	}

	if _, loaded := b.declarations.LoadOrStore(hash(bn), bn); loaded {
		return nil
	}

	var err error
	if bn.Type == BindingTypeExchange {
		err = ch.ExchangeBind(
			bn.Destination,
			bn.Pattern,
			bn.Source,
			false, /* no-wait */
			bn.Args,
		)
	} else {
		err = ch.QueueBind(
			bn.Destination,
			bn.Pattern,
			bn.Source,
			false, /* no-wait */
			bn.Args,
		)
	}
	if err != nil {
		b.declarations.Delete(hash(bn)) // Remove from cache if failed
		return fmt.Errorf("failed to declare %q binding %s -> %s: %w", bn.Type, bn.Source, bn.Destination, err)
	}

	return nil
}

func (b *Broker) lookupExchange(name string, strict bool) (Exchange, error) {
	// Check cached declarations
	var found Exchange
	b.declarations.Range(func(key, value interface{}) bool {
		if e, ok := value.(Exchange); ok && e.Name == name {
			found = e
			return false // stop iteration
		}
		return true
	})

	// If not found, return default exchange config
	if found.Name == "" {
		if !strict {
			return defaultExchange(name), nil
		}
		return Exchange{}, fmt.Errorf("exchange not found in topology")
	}

	return found, nil
}

func (b *Broker) lookupQueue(name string, strict bool) (Queue, error) {
	var found Queue
	b.declarations.Range(func(key, value interface{}) bool {
		if q, ok := value.(Queue); ok && q.Name == name {
			found = q
			return false
		}
		return true
	})

	if found.Name == "" {
		if !strict {
			return defaultQueue(name), nil
		}
		return Queue{}, fmt.Errorf("queue not found in topology")
	}

	return found, nil
}

// NewPublisher creates a managed Publisher that owns a dedicated connection.
// The publisher will automatically reconnect on connection failure.
// Options control confirmation mode and retry behavior.
func (b *Broker) NewPublisher(opts *PublisherOptions, e Exchange) (Publisher, error) {
	if b.closed.Load() {
		return nil, ErrBrokerClosed
	}

	po := defaultPublisherOptions()
	if opts != nil {
		po = *opts
	}

	// load last declared exchange by name from topology
	// this allows users to pass minimal exchange info
	if te := b.topology.GetExchange(e.Name); te != nil {
		e = *te
	}

	b.publishersMu.Lock()
	id := fmt.Sprintf("publisher-%d@%s", len(b.publishers)+1, b.id)
	p := newPublisher(b, id, po, e)
	b.publishers[id] = p
	b.publishersMu.Unlock()

	// Start publisher goroutine
	go p.run()

	// Wait for initial connection
	if !po.NoWaitForReady {
		timeout := po.ReadyTimeout
		if timeout <= 0 {
			timeout = defaultReadyTimeout
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if !p.waitReady(ctx) {
			p.Close()
			b.publishersMu.Lock()
			delete(b.publishers, id)
			b.publishersMu.Unlock()
			return nil, errors.New("publisher not ready within timeout")
		}
	}

	return p, nil
}

// NewConsumer creates a managed Consumer that owns a dedicated connection.
// The consumer will automatically reconnect on connection failure.
// Options control prefetch settings and ready wait behavior.
func (b *Broker) NewConsumer(opts *ConsumerOptions, q Queue, h Handler) (Consumer, error) {
	if b.closed.Load() {
		return nil, ErrBrokerClosed
	}

	co := defaultConsumerOptions()
	if opts != nil {
		co = *opts
	}

	// load last declared queue by name from topology
	// this allows users to pass minimal queue info
	if tq := b.topology.GetQueue(q.Name); tq != nil {
		q = *tq
	}

	b.consumersMu.Lock()
	id := fmt.Sprintf("consumer-%d@%s", len(b.consumers)+1, b.id)
	c := newConsumer(b, id, co, q, h)
	b.consumers[id] = c
	b.consumersMu.Unlock()

	// Start consumer goroutine
	go c.run()

	// Wait for initial ready state if requested
	if !co.NoWaitForReady {
		timeout := co.ReadyTimeout
		if timeout <= 0 {
			timeout = defaultReadyTimeout
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if !c.waitReady(ctx) {
			c.Close()
			b.consumersMu.Lock()
			delete(b.consumers, id)
			b.consumersMu.Unlock()
			return nil, errors.New("consumer not ready within timeout")
		}
	}

	return c, nil
}

// Publish performs a one-off publish using a pooled channel.
// For high-throughput scenarios with confirmations, prefer NewPublisher.
func (b *Broker) Publish(ctx context.Context, exchange, routingKey string, msg ...Message) error {
	if b.closed.Load() {
		return ErrBrokerClosed
	}

	// Lookup exchange in declarations
	e, err := b.lookupExchange(exchange, false)
	if err != nil {
		return err
	}
	rk := RoutingKey(routingKey)

	p, err := b.NewPublisher(nil, e)
	if err != nil {
		return err
	}
	defer p.Close()

	return p.Publish(ctx, rk, msg...)
}

// Consume is a convenience method that creates and starts a consumer.
func (b *Broker) Consume(ctx context.Context, queue string, handler Handler) error {
	// Lookup queue in declarations
	q, err := b.lookupQueue(queue, false)
	if err != nil {
		return err
	}

	// Create and start consumer
	c, err := b.NewConsumer(nil, q, handler)
	if err != nil {
		return err
	}

	// defer c.Close()
	// 	c.Consume()
	// 	return nil

	// Block until context cancelled
	go c.Consume()
	<-ctx.Done()

	return c.Close()
}
