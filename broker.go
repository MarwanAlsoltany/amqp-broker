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

// Endpoint represents a publisher or consumer with access to its connection and channel.
type Endpoint interface {
	// Connection returns the current connection (may be nil if not connected).
	Connection() *amqp.Connection
	// Channel returns the current channel (may be nil if not connected).
	Channel() *amqp.Channel
	// Close stops the endpoint and releases resources.
	Close() error
	// Unregister removes the endpoint from the broker's registry and closes it.
	Unregister()
}

// Broker manages AMQP connections, publishers, and consumers with automatic
// reconnection, connection pooling, and topology management.
type Broker struct {
	id  string
	url string

	// Control connection for topology management
	conn   *amqp.Connection
	connMu sync.RWMutex

	// Connection pool for publishers and consumers
	connPool     *connectionPool
	connPoolSize int

	// Channel pool for fast one-off operations
	chPool     *channelPool
	chPoolSize int

	// Topology and declaration caching
	topology   *Topology
	topologyMu sync.RWMutex

	declarations sync.Map // map[string]struct{}

	// Publisher registry for managed publishers
	publishers   map[string]*publisher
	publishersMu sync.Mutex

	// Consumer registry for managed consumers
	consumers   map[string]*consumer
	consumersMu sync.Mutex

	closed atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

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

// WithConnectionPoolSize sets the maximum number of pooled connections.
// Defaults to defaultConnPoolSize.
func WithConnectionPoolSize(n int) BrokerOption {
	return func(b *Broker) {
		if n <= 0 {
			n = defaultConnPoolSize
		}
		b.connPoolSize = n
	}
}

// WithChannelPoolSize sets the maximum number of pooled channels.
// Defaults to defaultChanPoolSize.
func WithChannelPoolSize(n int) BrokerOption {
	return func(b *Broker) {
		if n <= 0 {
			n = defaultChPoolSize
		}
		b.chPoolSize = n
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
		id:           generateBrokerID(),
		url:          defaultBrokerURL,
		ctx:          ctx,
		cancel:       cancel,
		connPoolSize: defaultConnPoolSize,
		chPoolSize:   defaultChPoolSize,
		publishers:   make(map[string]*publisher),
		consumers:    make(map[string]*consumer),
		reconnectMin: defaultReconnectMin,
		reconnectMax: defaultReconnectMax,
	}

	for _, opt := range opts {
		opt(b)
	}

	if err := b.validateConfig(); err != nil {
		return nil, err
	}

	b.connPool = newConnectionPool(b.url, b.connPoolSize)
	b.chPool = newChannelPool(b.connPool, b.chPoolSize)

	// Establish initial control connection
	if err := b.connect(ctx); err != nil {
		return nil, fmt.Errorf("initial connect failed: %w", err)
	}

	// maintain and monitor the control connection and reconnects on failure.
	go func() {
		for {
			conn := b.Connection()
			if conn == nil {
				if err := b.connectWithRetry(ctx); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					time.Sleep(b.reconnectMin)
					continue
				}
				conn = b.Connection()
			}

			notify := conn.NotifyClose(make(chan *amqp.Error, 1))
			select {
			case <-b.ctx.Done():
				_ = conn.Close()
				return
			case err := <-notify:
				_ = err // suppress unused variable warning
				_ = conn.Close()
				b.setConnection(nil)
				// log.Printf("control connection closed: %v", err)
				// Continue to reconnect
			}
		}
	}()

	return b, nil
}

func (b *Broker) validateConfig() error {
	if b.connPoolSize <= 0 {
		return fmt.Errorf("invalid connection pool size: must be at least 1")
	}
	if b.chPoolSize <= 0 {
		return fmt.Errorf("invalid channel pool size: must be at least 1")
	}
	if b.reconnectMin <= 0 {
		return fmt.Errorf("invalid reconnect config: min must be positive")
	}
	if b.reconnectMax <= b.reconnectMin {
		return fmt.Errorf("invalid reconnect config: max must be greater than min")
	}

	return nil
}

// connect attempts a single connection attempt and applies topology.
func (b *Broker) connect(_ context.Context) error {
	// control connection, should not be pooled
	conn, err := newConnection(b.url)
	if err != nil {
		return fmt.Errorf("dial control connection: %w", err)
	}
	b.setConnection(conn)

	// Apply topology if configured
	b.topologyMu.RLock()
	t := b.topology
	b.topologyMu.RUnlock()

	if t != nil {
		if err := b.applyTopology(conn, t); err != nil {
			_ = conn.Close()
			b.setConnection(nil)
			return fmt.Errorf("apply topology: %w", err)
		}
	}

	// Notify all managed endpoints to reconnect
	b.notifyReconnect()

	return nil
}

// connectWithRetry attempts connection with exponential backoff.
func (b *Broker) connectWithRetry(ctx context.Context) error {
	backoff := b.reconnectMin
	for {
		if b.closed.Load() {
			return ErrBrokerClosed
		}
		if err := b.connect(ctx); err == nil {
			return nil
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > b.reconnectMax {
				backoff = b.reconnectMax
			}
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// notifyReconnect signals all managed publishers and consumers to reconnect.
func (b *Broker) notifyReconnect() {
	// Notify publishers
	b.publishersMu.Lock()
	for _, pe := range b.publishers {
		pe.reconnect()
	}
	b.publishersMu.Unlock()

	// Notify consumers
	b.consumersMu.Lock()
	for _, ce := range b.consumers {
		ce.reconnect()
	}
	b.consumersMu.Unlock()
}

// Connection returns the current control connection (may be nil).
func (b *Broker) Connection() *amqp.Connection {
	b.connMu.RLock()
	defer b.connMu.RUnlock()
	return b.conn
}

// setConnection updates the control connection.
func (b *Broker) setConnection(c *amqp.Connection) {
	b.connMu.Lock()
	defer b.connMu.Unlock()
	b.conn = c
}

// NewChannel opens a new channel on the control connection.
// Returns the channel and a cleanup function.
func (b *Broker) NewChannel() (*amqp.Channel, func(), error) {
	conn := b.Connection()
	if conn == nil {
		return nil, nil, errors.New("control connection is not established")
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open control channel: %w", err)
	}
	return ch, func() { _ = ch.Close() }, nil
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

	// Close pools
	if b.chPool != nil {
		b.chPool.close()
	}
	if b.connPool != nil {
		b.connPool.close()
	}

	// Close control connection
	conn := b.Connection()
	if conn != nil {
		return conn.Close()
	}

	return nil
}

// DeclareTopology declares exchanges, queues, and bindings on the control connection.
// The topology is cached and will be reapplied on reconnection.
func (b *Broker) DeclareTopology(t Topology) error {
	b.topologyMu.Lock()
	b.topology = &t
	b.topologyMu.Unlock()

	conn := b.Connection()
	if conn == nil {
		return errors.New("control connection is not established")
	}

	return b.applyTopology(conn, &t)
}

// applyTopology applies topology declarations to the given connection.
func (b *Broker) applyTopology(conn *amqp.Connection, t *Topology) error {
	// This channel is used only as a fallback, ensure*Declared functions create their own channels.
	// The reason for that is, channels in AMQP are not thread-safe and are intended to be used
	// by a single goroutine at a time. By creating a new channel for each declaration,
	// the code avoids race conditions and potential channel state corruption.
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("topology: open channel: %w", err)
	}
	defer ch.Close()

	// Declare all exchanges (strict mode)
	for _, e := range t.Exchanges {
		if err := b.ensureExchangeDeclared(ch, e, true); err != nil {
			return err
		}
	}

	// Declare all queues (strict mode)
	for _, q := range t.Queues {
		if err := b.ensureQueueDeclared(ch, q, true); err != nil {
			return err
		}
	}

	// Declare all bindings (strict mode)
	for _, bn := range t.Bindings {
		if err := b.ensureBindingDeclared(ch, bn, true); err != nil {
			return err
		}
	}

	return nil
}

// ensureExchangeDeclared declares an exchange if not already cached.
func (b *Broker) ensureExchangeDeclared(ch *amqp.Channel, e Exchange, strict bool) error {
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

	var err error
	// Try control connection first
	if conn := b.Connection(); conn != nil {
		if ctrlCh, cleanup, ctrlErr := b.NewChannel(); ctrlErr == nil {
			err = ctrlCh.ExchangeDeclare(e.Name, kind, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Args)
			cleanup()
			if err == nil || !strict {
				return err
			}
		}
	}

	// Fallback to provided channel
	err = ch.ExchangeDeclare(e.Name, kind, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Args)
	if strict && err != nil {
		b.declarations.Delete(hash(e)) // Remove from cache if failed
		return fmt.Errorf("failed to declare exchange %s: %w", e.Name, err)
	}

	return nil
}

// ensureQueueDeclared declares a queue if not already cached.
func (b *Broker) ensureQueueDeclared(ch *amqp.Channel, q Queue, strict bool) error {
	if q.Name == "" {
		return fmt.Errorf("queue name cannot be empty")
	}

	if _, loaded := b.declarations.LoadOrStore(hash(q), q); loaded {
		return nil
	}

	var err error
	// Try control connection first
	if conn := b.Connection(); conn != nil {
		if ctrlCh, cleanup, e := b.NewChannel(); e == nil {
			_, err = ctrlCh.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Args)
			cleanup()
			if err == nil || !strict {
				return err
			}
		}
	}

	// Fallback to provided channel
	_, err = ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Args)
	if strict && err != nil {
		b.declarations.Delete(hash(q)) // Remove from cache if failed
		return fmt.Errorf("failed to declare queue %s: %w", q.Name, err)
	}

	return nil
}

// ensureBindingDeclared declares a binding if not already cached.
func (b *Broker) ensureBindingDeclared(ch *amqp.Channel, bn Binding, strict bool) error {
	if bn.Source == "" || bn.Destination == "" {
		return fmt.Errorf("binding source and destination cannot be empty")
	}

	if _, loaded := b.declarations.LoadOrStore(hash(bn), bn); loaded {
		return nil
	}

	var err error
	// Try control connection first
	if conn := b.Connection(); conn != nil {
		if ctrlCh, cleanup, e := b.NewChannel(); e == nil {
			if bn.Type == BindingTypeExchange {
				err = ctrlCh.ExchangeBind(bn.Destination, bn.Pattern, bn.Source, false, bn.Args)
			} else {
				err = ctrlCh.QueueBind(bn.Destination, bn.Pattern, bn.Source, false, bn.Args)
			}
			cleanup()
			if err == nil || !strict {
				return err
			}
		}
	}

	// Fallback to provided channel
	if bn.Type == BindingTypeExchange {
		err = ch.ExchangeBind(bn.Destination, bn.Pattern, bn.Source, false, bn.Args)
	} else {
		err = ch.QueueBind(bn.Destination, bn.Pattern, bn.Source, false, bn.Args)
	}
	if strict && err != nil {
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

	return p.Publish(ctx, rk, msg, nil)
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
