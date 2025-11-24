package broker

import (
	"context"
	"fmt"
	"io"
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

	// General purpose cache for endpoints
	cache    sync.Map // map[string]*cacheItem[Endpoint|Publisher|Consumer]
	cacheTTL time.Duration
}

// cacheItem tracks usage metadata for cached publishers
type cacheItem[T any] struct {
	value    T
	refCount atomic.Int32
	lastUsed atomic.Int64
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

// WithCache enables caching of one-off endpoints (e.g. Broker.Publish calls)
// Endpoints are reused across calls with the same parameters, improving performance.
// ttl: how long to keep idle cached endpoints (default: 5 minutes)
func WithCache(ttl time.Duration) BrokerOption {
	return func(b *Broker) {
		if ttl <= 0 {
			ttl = defaultCacheTTL
		}
		b.cacheTTL = ttl
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
		return nil, fmt.Errorf("reconnect min must be positive: %w", ErrInvalidReconnectConfig)
	}
	if b.reconnectMax <= b.reconnectMin {
		return nil, fmt.Errorf("reconnect max must be greater than min: %w", ErrInvalidReconnectConfig)
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

	// Start cache cleanup goroutine if caching is enabled
	if b.cacheTTL > 0 {
		go func() {
			ticker := time.NewTicker(b.cacheTTL / 2)
			defer ticker.Stop()

			for {
				select {
				case <-b.ctx.Done():
					return
				case <-ticker.C:
					b.cache.Range(func(k, v any) bool {
						item := v.(*cacheItem[any])
						// Check if idle (no active references and past TTL)
						if item.refCount.Load() == 0 {
							lastUsed := time.Unix(0, item.lastUsed.Load())
							if time.Since(lastUsed) > b.cacheTTL {
								// Remove from tracking and close if it is an io.Closer
								if value, ok := item.value.(io.Closer); ok {
									value.Close()
								}
								b.cache.Delete(k)
							}
						}
						return true
					})
				}
			}
		}()
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
		return wrapError("get control channel", err)
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
func (b *Broker) declareExchange(ch *Channel, e Exchange) error {
	if e.Name == "" {
		return ErrEmptyExchangeName
	}

	if _, loaded := b.declarations.LoadOrStore(hash(e), e); loaded {
		return nil // already declared
	}

	if err := e.Declare(ch); err != nil {
		b.declarations.Delete(hash(e)) // Remove from cache if failed
		return wrapEntityError("declare exchange", e.Name, err)
	}

	return nil
}

// declareQueue declares a queue if not already cached.
// The channel parameter should be the control channel or an endpoint's channel.
func (b *Broker) declareQueue(ch *Channel, q Queue) error {
	if q.Name == "" {
		return ErrEmptyQueueName
	}

	if _, loaded := b.declarations.LoadOrStore(hash(q), q); loaded {
		return nil
	}

	if err := q.Declare(ch); err != nil {
		b.declarations.Delete(hash(q)) // Remove from cache if failed
		return wrapEntityError("declare queue", q.Name, err)
	}

	return nil
}

// declareBinding declares a binding if not already cached.
// The channel parameter should be the control channel or an endpoint's channel.
func (b *Broker) declareBinding(ch *Channel, bn Binding) error {
	if bn.Source == "" || bn.Destination == "" {
		return ErrEmptyBindingName
	}

	if _, loaded := b.declarations.LoadOrStore(hash(bn), bn); loaded {
		return nil
	}

	if err := bn.Declare(ch); err != nil {
		b.declarations.Delete(hash(bn)) // Remove from cache if failed
		entity := fmt.Sprintf("%s binding %s -> %s", bn.Type, bn.Source, bn.Destination)
		return wrapEntityError("declare", entity, err)
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
		return Exchange{}, wrapEntityError("lookup exchange", name, ErrNotFoundTopology)
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
		return Queue{}, wrapEntityError("lookup queue", name, ErrNotFoundTopology)
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
	if te := b.topology.Exchange(e.Name); te != nil {
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
	if tq := b.topology.Queue(q.Name); tq != nil {
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

	// Lookup exchange in declarations
	e, err := b.lookupExchange(exchange, false)
	if err != nil {
		return err
	}
	rk := RoutingKey(routingKey)

	// Use cached publisher if enabled
	if b.cacheTTL > 0 {
		// Generate cache key from exchange properties
		cacheKey := "publisher:" + hash(e)
		cacheExtend := func(item *cacheItem[Publisher]) {
			item.refCount.Add(-1)
			item.lastUsed.Store(time.Now().UnixNano())
		}

		// Fast path: reuse existing cached publisher
		if v, ok := b.cache.Load(cacheKey); ok {
			item := v.(*cacheItem[Publisher])
			item.refCount.Add(1)
			defer cacheExtend(item)
			return item.value.Publish(ctx, rk, msg...)
		}

		// Slow path: create new cached publisher
		p, err := b.NewPublisher(nil, e)
		if err != nil {
			return err
		}

		// Store usage item with publisher pointer
		item := &cacheItem[Publisher]{value: p}
		item.refCount.Store(1)
		item.lastUsed.Store(time.Now().UnixNano())

		// Handle race: another goroutine may have created it
		if actual, loaded := b.cache.LoadOrStore(cacheKey, item); loaded {
			// Lost race, close ours and use the winner
			p.Close()
			item := actual.(*cacheItem[Publisher])
			item.refCount.Add(1)
			defer func() {
				item.refCount.Add(-1)
				item.lastUsed.Store(time.Now().UnixNano())
			}()
			return item.value.Publish(ctx, rk, msg...)
		}

		defer cacheExtend(item)

		return p.Publish(ctx, rk, msg...)
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
	// Lookup queue in declarations
	q, err := b.lookupQueue(queue, false)
	if err != nil {
		return err
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
// Deprecated: Transactions are SLOW (~2x overhead). Use publisher confirms instead.
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
