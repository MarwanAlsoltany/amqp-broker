package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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
	connCfg      *Config
	connPoolSize int
	connEvents   struct {
		onOpen  ConnectionOpenHandler
		onClose ConnectionCloseHandler
		onBlock ConnectionBlockHandler
	}

	// Reconnection configuration
	reconnectMin time.Duration
	reconnectMax time.Duration

	// Topology manager for declared exchanges, queues, and bindings
	topologyMgr *topologyManager

	// Publisher registry for managed publishers
	publishers   map[string]Publisher
	publishersMu sync.Mutex
	// Cached publisher pool for one-off publishes
	publishersPool *pool[Publisher]

	// Consumer registry for managed consumers
	consumers   map[string]Consumer
	consumersMu sync.Mutex
	// unlike publisher, pooling is not needed for consumers
	// as they are long-lived and get destroyed after use

	// Cache TTL for pools
	cacheTTL time.Duration
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

// WithContext sets the base context for the Broker.
// The context is used for managing the lifecycle of connections and endpoints.
func WithContext(ctx context.Context) BrokerOption {
	return func(b *Broker) {
		b.ctx, b.cancel = context.WithCancel(ctx)
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
			n = defaultConnectionPoolSize
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

// WithAMQPConfig sets the AMQP connection configuration.
// This allows full control over connection parameters including TLS, SASL,
// heartbeat, channel limits, frame size, vhost, and properties.
func WithAMQPConfig(config Config) BrokerOption {
	return func(b *Broker) {
		b.connCfg = &config
	}
}

// WithOnConnectionOpen registers a callback for connection open events.
// The callback is invoked when a connection is successfully established or re-established.
// Parameters: idx (which connection pool index)
func WithOnConnectionOpen(handler ConnectionOpenHandler) BrokerOption {
	return func(b *Broker) {
		b.connEvents.onOpen = handler
	}
}

// WithOnConnectionClose registers a callback for connection close events.
// The callback is invoked when any connection in the pool closes unexpectedly.
// Parameters: idx (connection pool index), code (AMQP error code), reason (error description),
// server (true if initiated by server), recover (true if recoverable)
func WithOnConnectionClose(handler ConnectionCloseHandler) BrokerOption {
	return func(b *Broker) {
		b.connEvents.onClose = handler
	}
}

// WithOnConnectionBlocked registers a callback for connection flow control events.
// The callback is invoked when RabbitMQ flow control activates/deactivates.
// Parameters: idx (connection pool index), active (true=blocked, false=unblocked),
// reason (only set when active=true)
func WithOnConnectionBlocked(handler ConnectionBlockHandler) BrokerOption {
	return func(b *Broker) {
		b.connEvents.onBlock = handler
	}
}

// WithCache enables pooling of one-off endpoints (e.g. Broker.Publish calls)
// Publishers are reused across calls with the same parameters, improving performance.
// TTL (time-to-live) is how long to keep idle pooled endpoints (default: 5 minutes)
func WithCache(ttl time.Duration) BrokerOption {
	return func(b *Broker) {
		if ttl <= 0 {
			ttl = defaultCacheTTL
		}
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
		connPoolSize: defaultConnectionPoolSize,
		publishers:   make(map[string]Publisher),
		consumers:    make(map[string]Consumer),
		reconnectMin: defaultReconnectMin,
		reconnectMax: defaultReconnectMax,
		topologyMgr:  newTopologyManager(), // Initialize singleton topology manager
	}

	for _, opt := range opts {
		opt(b)
	}

	if b.reconnectMin <= 0 {
		return nil, fmt.Errorf("%w: min must be positive", ErrInvalidReconnectConfig)
	}
	if b.reconnectMax <= b.reconnectMin {
		return nil, fmt.Errorf("%w: max must be greater than min", ErrInvalidReconnectConfig)
	}

	b.connMgr = newConnectionManager(b.url, b.connCfg, b.connPoolSize)

	// Set event handlers on connection manager
	if b.connEvents.onOpen != nil {
		b.connMgr.onOpen(b.connEvents.onOpen)
	}
	if b.connEvents.onClose != nil {
		b.connMgr.onClose(b.connEvents.onClose)
	}
	if b.connEvents.onBlock != nil {
		b.connMgr.onBlock(b.connEvents.onBlock)
	}

	// Initialize all managed connections
	if err := b.connMgr.init(ctx); err != nil {
		return nil, err
	}

	if b.cacheTTL > 0 {
		b.publishersPool = newPool[Publisher](b.cacheTTL)

		if err := b.publishersPool.init(ctx); err != nil {
			return nil, err
		}
	}

	return b, nil
}

// Connection returns the control connection for topology operations.
func (b *Broker) Connection() (*Connection, error) {
	conn, err := b.connMgr.assign(b.ctx, roleControl)
	if err != nil {
		return nil, wrapError("assign control connection", err)
	}

	return conn, nil
}

// Channel returns the control channel for topology operations.
//
// NOTE: A Channel returned by this method should be closed by the caller.
func (b *Broker) Channel() (*Channel, error) {
	conn, err := b.Connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, wrapError("create control channel", err)
	}

	// Check if channel is closed immediately after creation
	if ch.IsClosed() {
		return nil, ErrChannelClosed
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

	var errs []error

	// Close all managed publishers
	b.publishersMu.Lock()
	for _, pe := range b.publishers {
		if err := pe.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	b.publishersMu.Unlock()

	// Close all managed consumers
	b.consumersMu.Lock()
	for _, ce := range b.consumers {
		if err := ce.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	b.consumersMu.Unlock()

	// Close connection manager
	if b.connMgr != nil {
		if err := b.connMgr.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close all pooled publishers
	if b.publishersPool != nil {
		if err := b.publishersPool.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %v", ErrBrokerClose, errs)
	}

	return nil
}

// Declare applies the given topology to the broker (exchanges, queues, and bindings).
// The topology is merged with existing topology and will be reapplied on reconnection.
func (b *Broker) Declare(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return wrapError("get control channel", err)
	}
	defer ch.Close()

	return b.topologyMgr.declare(ch, t)
}

// Delete removes the given topology from the broker (exchanges, queues, and bindings).
// Bindings are removed first, then queues, then exchanges (reverse order of declaration).
func (b *Broker) Delete(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return wrapError("get control channel", err)
	}
	defer ch.Close()

	return b.topologyMgr.delete(ch, t)
}

// Verify checks that the given topology exists with correct configuration.
// For exchanges and queues, uses passive declarations. For bindings, redeclares them (idempotent).
func (b *Broker) Verify(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return wrapError("get control channel", err)
	}
	defer ch.Close()

	return b.topologyMgr.verify(ch, t)
}

// Sync synchronizes the broker's topology to match the desired state exactly.
// It deletes entities that exist but are not in the desired topology,
// then declares entities from the desired topology.
// This provides declarative topology management - specify desired state and Sync makes it so.
// Note that sync is aware of the topology declared on this Broker, it does not inspect the server directly.
func (b *Broker) Sync(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return wrapError("get control channel", err)
	}
	defer ch.Close()

	return b.topologyMgr.sync(ch, t)
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
	if te := b.topologyMgr.exchange(e.Name); te != nil {
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
			return nil, ErrNotReadyTimeout
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
	if tq := b.topologyMgr.queue(q.Name); tq != nil {
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
			return nil, ErrNotReadyTimeout
		}
	}

	return c, nil
}

// Publish performs a one-off publish using a cached publisher when caching is enabled.
// For high-throughput scenarios with confirmations, prefer NewPublisher.
func (b *Broker) Publish(ctx context.Context, exchange, routingKey string, msg ...Message) error {
	if b.closed.Load() {
		return ErrBrokerClosed
	}

	// Lookup exchange from topology manager or use default
	var e Exchange
	if te := b.topologyMgr.exchange(exchange); te != nil {
		e = *te
	} else {
		e = NewExchange(exchange)
	}
	rk := RoutingKey(routingKey)

	// Use pooled publisher if enabled
	if b.publishersPool != nil {
		key := "publisher:" + hash(e)

		publisher, release, err := b.publishersPool.acquire(key, func() (Publisher, error) {
			return b.NewPublisher(nil, e)
		})
		if err != nil {
			return err
		}
		defer release()

		return publisher.Publish(ctx, rk, msg...)
	}

	// Fallback to one-off publisher
	p, err := b.NewPublisher(nil, e)
	if err != nil {
		return err
	}
	defer p.Close()

	return p.Publish(ctx, rk, msg...)
}

// Consume is a convenience method that creates and starts a consumer.
// The consumer is closed automatically when the context is cancelled.
func (b *Broker) Consume(ctx context.Context, queue string, handler Handler) error {
	// Lookup queue from topology manager or use default
	var q Queue
	if tq := b.topologyMgr.queue(queue); tq != nil {
		q = *tq
	} else {
		q = NewQueue(queue)
	}

	// Create one-off consumer
	c, err := b.NewConsumer(nil, q, handler)
	if err != nil {
		return err
	}
	defer c.Close()

	// Block until context cancelled
	return c.Consume(ctx)
}

// Transaction executes fn within an AMQP transaction.
// The transaction commits if fn returns nil, otherwise rolls back.
//
// Deprecated: Do not use, transactions are SLOW (~2x overhead) in AMQP. Use publisher confirms instead.
func (b *Broker) Transaction(ctx context.Context, fn func(*Channel) error) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Tx(); err != nil {
		return wrapError("transaction start", err)
	}

	err = fn(ch)
	if err != nil {
		if rbErr := ch.TxRollback(); rbErr != nil {
			return fmt.Errorf("transaction rollback after error (%v): %w", err, rbErr)
		}
		return err
	}

	if err := ch.TxCommit(); err != nil {
		return wrapError("transaction commit", err)
	}

	return nil
}
