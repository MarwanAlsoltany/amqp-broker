package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Broker provides a high-level manager for AMQP connections, publishers, and consumers,
// offering automatic reconnection, connection pooling, and declarative topology management.
//
// Ownership & Lifecycle:
//   - Manages a pool of long-lived AMQP connections for publishers, consumers, and control operations.
//   - Handles automatic reconnection and resource cleanup for all endpoints.
//   - Maintains registries for managed publishers and consumers, and pools for one-off publishers.
//
// Topology Management:
//   - Centralizes declaration, verification, deletion, and synchronization of exchanges, queues, and bindings.
//   - Ensures topology is reapplied on reconnection and supports declarative state management.
//
// Concurrency & Safety:
//   - All public methods are safe for concurrent use.
//   - Internal registries and pools are protected by mutexes.
//
// Usage Patterns:
//   - Use New(...) to construct a Broker with custom options (URL, connection pool size, endpoint defaults, etc ...).
//   - Use [Broker.NewPublisher]/[Broker.NewConsumer] to create managed endpoints with automatic reconnection.
//   - Use [Broker.Publish]/[Broker.Consume] for one-off publishing and consumption with automatic resource management.
//   - Use [Broker.Declare], [Broker.Delete], [Broker.Verify], and [Broker.Sync] to manage AMQP topology declaratively.
//   - Use [Broker.Close] to gracefully shut down the broker and all managed resources.
//
// See [BrokerOption] (With* functions) and [EndpointOptions] for configuration details.
type Broker struct {
	id  string
	url string

	// base context for managing lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// closed state
	closed atomic.Bool

	// connection manager for all connections
	connectionMgr     *connectionManager
	connectionMgrOpts ConnectionManagerOptions

	// topology manager for declared exchanges, queues, and bindings
	topologyMgr *topologyManager

	// endpoint options defaults
	endpointOpts EndpointOptions

	// publisher registry for managed publishers
	publishers    map[string]Publisher // map of publisher ID to Publisher
	publishersMu  sync.Mutex           // protects publishers map
	publishersSeq atomic.Uint32        // publisher ID sequence
	// cached publisher pool for one-off publishes
	publishersPool *pool[Publisher]

	// consumer registry for managed consumers
	consumers    map[string]Consumer // map of consumer ID to Consumer
	consumersMu  sync.Mutex          // protects consumers map
	consumersSeq atomic.Uint32       // consumer ID sequence
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

// WithEndpointOptions sets the default EndpointOptions for all endpoints (publishers/consumers).
func WithEndpointOptions(opts EndpointOptions) BrokerOption {
	return func(b *Broker) {
		b.endpointOpts = opts
	}
}

// WithConnectionManagerOptions sets the connection pool configuration.
// This controls pool size, dial config, event handlers, and retry behavior.
// Defaults are applied for any unspecified fields.
func WithConnectionManagerOptions(opts ConnectionManagerOptions) BrokerOption {
	return func(b *Broker) {
		b.connectionMgrOpts = opts
	}
}


// WithConnectionPoolSize sets the number of managed connections.
// - Size 1: All operations share one connection
// - Size 2: Publishers/Control use one, Consumers use another (recommended for most cases)
// - Size 3+: Dedicated connections for publishers, consumers, and control
// Defaults to defaultConnectionPoolSize (1).
//
// Deprecated: Use WithConnectionManagerOptions instead.
func WithConnectionPoolSize(size int) BrokerOption {
	return func(b *Broker) {
		b.connectionMgrOpts.Size = size
	}
}

// WithConnectionConfig sets the AMQP connection configuration.
// This allows full control over connection parameters including TLS, SASL,
// heartbeat, channel limits, frame size, vhost, and properties.
// Defaults to none.
//
// Deprecated: Use WithConnectionManagerOptions instead.
func WithConnectionConfig(config Config) BrokerOption {
	return func(b *Broker) {
		b.connectionMgrOpts.Config = &config
	}
}

// WithConnectionOnOpen registers a callback for connection open events.
// The callback is invoked when a connection is successfully established or re-established.
// Parameters: idx (which connection pool index)
// Defaults to none.
//
// Deprecated: Use WithConnectionManagerOptions instead.
func WithConnectionOnOpen(handler ConnectionOnOpenHandler) BrokerOption {
	return func(b *Broker) {
		b.connectionMgrOpts.OnOpen = handler
	}
}

// WithConnectionOnClose registers a callback for connection close events.
// The callback is invoked when any connection in the pool closes unexpectedly.
// Parameters: idx (connection pool index), code (AMQP error code), reason (error description),
// server (true if initiated by server), recover (true if recoverable).
// Defaults to none.
//
// Deprecated: Use WithConnectionManagerOptions instead.
func WithConnectionOnClose(handler ConnectionOnCloseHandler) BrokerOption {
	return func(b *Broker) {
		b.connectionMgrOpts.OnClose = handler
	}
}

// WithConnectionOnBlocked registers a callback for connection flow control events.
// The callback is invoked when RabbitMQ flow control activates/deactivates.
// Parameters: idx (connection pool index), active (true=blocked, false=unblocked),
// reason (only set when active=true).
// Defaults to none.
//
// Deprecated: Use WithConnectionManagerOptions instead.
func WithConnectionOnBlocked(handler ConnectionOnBlockHandler) BrokerOption {
	return func(b *Broker) {
		b.connectionMgrOpts.OnBlock = handler
	}
}

// WithConnectionReconnectConfig sets the connection pool reconnection configuration.
// This controls automatic reconnection behavior when connections fail.
// Defaults to auto-reconnect enabled with 500ms min and 30s max backoff.
//
// Deprecated: Use WithConnectionManagerOptions instead. For example:
//
//	WithConnectionManagerOptions(ConnectionManagerOptions{
//	    NoAutoReconnect: false,
//	    ReconnectMin:    500 * time.Millisecond,
//	    ReconnectMax:    30 * time.Second,
//	})
func WithConnectionReconnectConfig(noAutoReconnect bool, reconnectMin, reconnectMax time.Duration) BrokerOption {
	return func(b *Broker) {
		b.connectionMgrOpts.NoAutoReconnect = noAutoReconnect
		b.connectionMgrOpts.ReconnectMin = reconnectMin
		b.connectionMgrOpts.ReconnectMax = reconnectMax
	}
}


// New creates a new Broker and establishes the initial control connection.
// It starts a background goroutine to maintain the control connection.
func New(opts ...BrokerOption) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background()) // default

	b := &Broker{
		id:         defaultBrokerID,
		url:        defaultBrokerURL,
		ctx:        ctx,
		cancel:     cancel,
		publishers: make(map[string]Publisher),
		consumers:  make(map[string]Consumer),
		cacheTTL:   defaultCacheTTL,
	}

	for _, opt := range opts {
		opt(b)
	}

	// merge endpoint options with defaults
	b.endpointOpts = mergeEndpointOptions(b.endpointOpts, defaultEndpointOptions())

	// merge connection manager options with defaults
	b.connectionMgrOpts = mergeConnectionManagerOptions(b.connectionMgrOpts, defaultConnectionManagerOptions())

	b.connectionMgr = newConnectionManager(b.url, &b.connectionMgrOpts)
	// initialize all managed connections
	if err := b.connectionMgr.init(b.ctx); err != nil {
		return nil, err
	}

	// initialize singleton topology manager
	b.topologyMgr = newTopologyManager()

	if b.cacheTTL > 0 {
		b.publishersPool = newPool[Publisher](b.cacheTTL)

		if err := b.publishersPool.init(b.ctx); err != nil {
			return nil, err
		}
	}

	return b, nil
}

// Connection returns the control connection for topology operations (exchanges, queues, bindings).
//
// NOTE: The returned connection is managed by the Broker and should not be closed by the caller.
//
// NOTE: The returned connection is not guaranteed to be the same across calls.
//
// Returns an error if no connection is available or the broker is closed.
func (b *Broker) Connection() (*Connection, error) {
	conn, err := b.connectionMgr.assign(roleController)
	if err != nil {
		return nil, wrapError("create control connection", err)
	}
	// safe-guard: make sure connection is open
	if conn.IsClosed() {
		return nil, ErrConnectionClosed
	}

	return conn, nil
}

// Channel returns a new control channel for topology operations (exchanges, queues, bindings).
//
// NOTE: The caller is responsible for closing the returned channel when done.
//
// NOTE: The returned channel is not guaranteed to be the same across calls.
//
// Returns an error if the control connection is unavailable or the broker is closed.
func (b *Broker) Channel() (*Channel, error) {
	conn, err := b.Connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, wrapError("create control channel", err)
	}
	// safe-guard: make sure channel is open
	if ch.IsClosed() {
		return nil, ErrChannelClosed
	}

	return ch, nil
}

// Close gracefully shuts down the broker and all managed resources.
//
// This closes all managed publishers, consumers, pooled endpoints, and connections.
// It is safe to call multiple times; subsequent calls are no-ops.
// Returns a combined error if any resources fail to close.
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

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: close failed: %w", ErrBroker, err)
	}

	return nil
}

// Declare applies the given topology (exchanges, queues, bindings) to the broker.
//
// The topology is merged with the broker's existing topology and will be reapplied on reconnection.
// Returns an error if declaration fails.
func (b *Broker) Declare(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return b.topologyMgr.declare(ch, t)
}

// Delete removes the given topology (exchanges, queues, bindings) from the broker.
//
// Bindings are removed first, then queues, then exchanges (reverse order of declaration).
// Returns an error if deletion fails.
func (b *Broker) Delete(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return b.topologyMgr.delete(ch, t)
}

// Verify checks that the given topology exists on the broker with the correct configuration.
//
// For exchanges and queues, uses passive declarations. For bindings, redeclares them (idempotent).
// Returns an error if verification fails.
func (b *Broker) Verify(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return b.topologyMgr.verify(ch, t)
}

// Sync synchronizes the broker's topology to match the desired state exactly.
//
// Entities not present in the desired topology are deleted; missing entities are declared.
// This provides declarative topology management, specify the desired state and Sync enforces it.
// Note: Sync is aware only of topology declared on this Broker, not the server's full state.
// Returns an error if synchronization fails.
func (b *Broker) Sync(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return b.topologyMgr.sync(ch, t)
}

// NewPublisher creates a managed Publisher with a dedicated connection and lifecycle.
//
// The publisher will automatically reconnect on connection failure.
// Options control confirmation mode, retry behavior, and endpoint configuration.
// Returns the Publisher or an error if creation fails.
func (b *Broker) NewPublisher(opts *PublisherOptions, e Exchange) (Publisher, error) {
	if b.closed.Load() {
		return nil, ErrBrokerClosed
	}

	po := defaultPublisherOptions()
	if opts != nil {
		po = mergePublisherOptions(*opts, po)
	}
	po.EndpointOptions = mergeEndpointOptions(po.EndpointOptions, b.endpointOpts)

	// load last declared exchange by name from topology
	// this allows users to pass minimal exchange info
	if te := b.topologyMgr.exchange(e.Name); te != nil {
		e = *te
	}

	id := fmt.Sprintf("publisher-%d@%s", b.publishersSeq.Add(1), b.id)
	p := newPublisher(id, b, po, e)

	if err := p.init(b.ctx); err != nil {
		p.Close() // cleanup
		return nil, err
	}

	b.publishersMu.Lock()
	b.publishers[id] = p
	b.publishersMu.Unlock()

	return p, nil
}

// NewConsumer creates a managed Consumer with a dedicated connection and lifecycle.
//
// The consumer will automatically reconnect on connection failure.
// Options control prefetch settings, handler concurrency, and endpoint configuration.
// Returns the Consumer or an error if creation fails.
func (b *Broker) NewConsumer(opts *ConsumerOptions, q Queue, h Handler) (Consumer, error) {
	if b.closed.Load() {
		return nil, ErrBrokerClosed
	}

	co := defaultConsumerOptions()
	if opts != nil {
		co = mergeConsumerOptions(*opts, co)
	}
	co.EndpointOptions = mergeEndpointOptions(co.EndpointOptions, b.endpointOpts)

	// load last declared queue by name from topology
	// this allows users to pass minimal queue info
	if tq := b.topologyMgr.queue(q.Name); tq != nil {
		q = *tq
	}

	id := fmt.Sprintf("consumer-%d@%s", b.consumersSeq.Add(1), b.id)
	c := newConsumer(id, b, co, q, h)

	if err := c.init(b.ctx); err != nil {
		c.Close() // cleanup
		return nil, err
	}

	b.consumersMu.Lock()
	b.consumers[id] = c
	b.consumersMu.Unlock()

	return c, nil
}

// Publish performs a one-off publish using a cached publisher if pooling is enabled.
//
// For high-throughput or confirmation scenarios, prefer using a managed Publisher.
// Returns an error if publishing fails.
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

// Consume is a convenience method that creates and starts a one-off consumer.
//
// The consumer is closed automatically when the context is cancelled.
// Returns an error if consumption fails.
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

// Transaction executes fn within an AMQP transaction on a control channel.
//
// The transaction commits if fn returns nil, otherwise rolls back.
// Deprecated: Transactions in AMQP are SLOW (~2x overhead); use publisher confirms instead.
// Returns an error if the transaction fails or rollback is required.
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
			return wrapError("transaction rollback", rbErr, fnErr)
		}
		return wrapError("transaction function", fnErr)
	}

	if err := ch.TxCommit(); err != nil {
		return wrapError("transaction commit", err)
	}

	return nil
}
