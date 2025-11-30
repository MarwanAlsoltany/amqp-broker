package broker

import (
	"context"
	"errors"
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

	// base context for managing lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// closed state
	closed atomic.Bool

	// connection manager for all connections
	connectionMgr      *connectionManager
	connectionCfg      *Config
	connectionPoolSize int
	connectionEvents   struct {
		onOpen  ConnectionOpenHandler
		onClose ConnectionCloseHandler
		onBlock ConnectionBlockHandler
	}

	// reconnection configuration
	reconnectMin time.Duration
	reconnectMax time.Duration

	// topology manager for declared exchanges, queues, and bindings
	topologyMgr *topologyManager

	// publisher registry for managed publishers
	publishers   map[string]Publisher
	publishersMu sync.Mutex
	// cached publisher pool for one-off publishes
	publishersPool *pool[Publisher]

	// consumer registry for managed consumers
	consumers   map[string]Consumer
	consumersMu sync.Mutex
	// unlike publisher, pooling is not needed for consumers
	// as they are long-lived and get destroyed after use

	// cache TTL for pools
	cacheTTL time.Duration
}

// BrokerOption configures a Broker instance.
type BrokerOption func(*Broker)

// WithURL sets the AMQP URL for the Broker.
// Defaults to defaultBrokerURL ("amqp://guest:guest@localhost:5672/").
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

// WithCache enables pooling of one-off endpoints (e.g. Broker.Publish calls)
// Publishers are reused across calls with the same parameters, improving performance.
// TTL (time-to-live) is how long to keep idle pooled endpoints.
// Defaults to defaultCacheTTL (5 minute). Set to <=0 to disable caching.
func WithCache(ttl time.Duration) BrokerOption {
	return func(b *Broker) {
		if ttl < 0 {
			ttl = 0
		}
		b.cacheTTL = ttl
	}
}

// WithConnectionPoolSize sets the number of managed connections.
// - Size 1: All operations share one connection
// - Size 2: Publishers/Control use one, Consumers use another (recommended default)
// - Size 3+: Dedicated connections for publishers, consumers, and control
// Defaults to defaultConnPoolSize (1).
func WithConnectionPoolSize(size int) BrokerOption {
	return func(b *Broker) {
		if size <= 0 {
			size = defaultConnectionPoolSize
		}
		b.connectionPoolSize = size
	}
}

// WithReconnectConfig sets the minimum and maximum reconnection backoff.
// Defaults to defaultReconnectMin (500ms) and defaultReconnectMax (30s).
func WithReconnectConfig(min, max time.Duration) BrokerOption {
	return func(b *Broker) {
		b.reconnectMin = min
		b.reconnectMax = max
	}
}

// WithAMQPConfig sets the AMQP connection configuration.
// This allows full control over connection parameters including TLS, SASL,
// heartbeat, channel limits, frame size, vhost, and properties.
// Defaults to none.
func WithAMQPConfig(config Config) BrokerOption {
	return func(b *Broker) {
		b.connectionCfg = &config
	}
}

// WithConnectionOnOpen registers a callback for connection open events.
// The callback is invoked when a connection is successfully established or re-established.
// Parameters: idx (which connection pool index)
// Defaults to none.
func WithConnectionOnOpen(handler ConnectionOpenHandler) BrokerOption {
	return func(b *Broker) {
		b.connectionEvents.onOpen = handler
	}
}

// WithConnectionOnClose registers a callback for connection close events.
// The callback is invoked when any connection in the pool closes unexpectedly.
// Parameters: idx (connection pool index), code (AMQP error code), reason (error description),
// server (true if initiated by server), recover (true if recoverable).
// Defaults to none.
func WithConnectionOnClose(handler ConnectionCloseHandler) BrokerOption {
	return func(b *Broker) {
		b.connectionEvents.onClose = handler
	}
}

// WithConnectionOnBlocked registers a callback for connection flow control events.
// The callback is invoked when RabbitMQ flow control activates/deactivates.
// Parameters: idx (connection pool index), active (true=blocked, false=unblocked),
// reason (only set when active=true).
// Defaults to none.
func WithConnectionOnBlocked(handler ConnectionBlockHandler) BrokerOption {
	return func(b *Broker) {
		b.connectionEvents.onBlock = handler
	}
}

// New creates a new Broker and establishes the initial control connection.
// It starts a background goroutine to maintain the control connection.
func New(opts ...BrokerOption) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background()) // default

	b := &Broker{
		id:                 defaultBrokerID,
		url:                defaultBrokerURL,
		ctx:                ctx,
		cancel:             cancel,
		connectionPoolSize: defaultConnectionPoolSize,
		publishers:         make(map[string]Publisher),
		consumers:          make(map[string]Consumer),
		reconnectMin:       defaultReconnectMin,
		reconnectMax:       defaultReconnectMax,
		cacheTTL:           defaultCacheTTL,
	}

	for _, opt := range opts {
		opt(b)
	}

	if b.reconnectMin <= 0 {
		return nil, fmt.Errorf("%w: min must be positive", ErrBrokerConfigInvalid)
	}
	if b.reconnectMax <= b.reconnectMin {
		return nil, fmt.Errorf("%w: max must be greater than min", ErrBrokerConfigInvalid)
	}

	b.connectionMgr = newConnectionManager(b.url, b.connectionCfg, b.connectionPoolSize)

	// set event handlers on connection manager
	if b.connectionEvents.onOpen != nil {
		b.connectionMgr.onOpen(b.connectionEvents.onOpen)
	}
	if b.connectionEvents.onClose != nil {
		b.connectionMgr.onClose(b.connectionEvents.onClose)
	}
	if b.connectionEvents.onBlock != nil {
		b.connectionMgr.onBlock(b.connectionEvents.onBlock)
	}

	// initialize all managed connections
	if err := b.connectionMgr.init(b.ctx); err != nil {
		return nil, err
	}

	b.topologyMgr = newTopologyManager() // initialize singleton topology manager

	if b.cacheTTL > 0 {
		b.publishersPool = newPool[Publisher](b.cacheTTL)

		if err := b.publishersPool.init(b.ctx); err != nil {
			return nil, err
		}
	}

	return b, nil
}

// Connection returns the control connection for topology operations.
func (b *Broker) Connection() (*Connection, error) {
	conn, err := b.connectionMgr.assign(roleController)
	if err != nil {
		return nil, wrapError("create control connection", err)
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

	return ch, nil
}

// Close shuts down the broker and all managed endpoints.
func (b *Broker) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}

	// cancel background context
	if b.cancel != nil {
		b.cancel()
	}

	var errs []error

	// close all pooled publishers
	if b.publishersPool != nil {
		if err := b.publishersPool.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// close all managed publishers
	b.publishersMu.Lock()
	for _, pe := range b.publishers {
		if err := pe.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	b.publishersMu.Unlock()

	// close all managed consumers
	b.consumersMu.Lock()
	for _, ce := range b.consumers {
		if err := ce.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	b.consumersMu.Unlock()

	// close connection manager
	if b.connectionMgr != nil {
		if err := b.connectionMgr.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrBrokerClose, errors.Join(errs...))
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

	go p.run() // start publisher goroutine

	// wait for initial connection
	if !po.NoWaitForReady {
		timeout := po.ReadyTimeout
		if timeout <= 0 {
			timeout = defaultReadyTimeout
		}

		ctx, cancel := context.WithTimeout(b.ctx, timeout)
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

	go c.run() // start consumer goroutine

	// wait for initial connection
	if !co.NoWaitForReady {
		timeout := co.ReadyTimeout
		if timeout <= 0 {
			timeout = defaultReadyTimeout
		}

		ctx, cancel := context.WithTimeout(b.ctx, timeout)
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

	ctx, cancel := contextWithAnyCancel(b.ctx, ctx) // derived context
	defer cancel()

	// lookup exchange from topology manager or use default
	var e Exchange
	if te := b.topologyMgr.exchange(exchange); te != nil {
		e = *te
	} else {
		e = NewExchange(exchange)
	}
	rk := RoutingKey(routingKey)

	// use pooled publisher if enabled
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

	// fallback to one-off publisher
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
	if b.closed.Load() {
		return ErrBrokerClosed
	}

	ctx, cancel := contextWithAnyCancel(b.ctx, ctx) // derived context
	defer cancel()

	// lookup queue from topology manager or use default
	var q Queue
	if tq := b.topologyMgr.queue(queue); tq != nil {
		q = *tq
	} else {
		q = NewQueue(queue)
	}

	// create one-off consumer
	c, err := b.NewConsumer(nil, q, handler)
	if err != nil {
		return err
	}
	defer c.Close()

	// block until context cancelled
	return c.Consume(ctx)
}

// Transaction executes fn within an AMQP transaction.
// The transaction commits if fn returns nil, otherwise rolls back.
//
// Deprecated: Do not use, transactions are SLOW (~2x overhead) in AMQP. Use publisher confirms instead.
// This function exists for the sake of completeness.
func (b *Broker) Transaction(ctx context.Context, fn func(*Channel) error) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Tx(); err != nil {
		return wrapError("transaction start", err)
	}

	fnErr := doSafeChannelAction(ch, fn)
	if fnErr != nil {
		if rbErr := ch.TxRollback(); rbErr != nil {
			// return an error that lets callers inspect both errors
			return wrapError("transaction rollback", fnErr, rbErr)
		}
		return fnErr
	}

	if err := ch.TxCommit(); err != nil {
		return wrapError("transaction commit", err)
	}

	return nil
}
