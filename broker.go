package broker

import (
	"context"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
	"github.com/MarwanAlsoltany/amqp-broker/internal/endpoint"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

const (
	// Default AMQP Broker URL
	defaultBrokerURL = "amqp://guest:guest@localhost:5672/"

	// Default cache TTL for Broker endpoints
	defaultBrokerCacheTTL = 5 * time.Minute
)

var defaultBrokerID string // read-only

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()

	defaultBrokerID = fmt.Sprintf("%s-%d", hostname, pid)
}

// Error is the base error type for all broker-related errors.
type Error = internal.Error

var (
	// ErrBroker is the root sentinel error for all broker-related errors.
	// All errors returned by the broker package wrap this error, making it possible
	// to check if an error originated from the broker using errors.Is(err, ErrBroker).
	ErrBroker = internal.ErrBroker
	// ErrBrokerClosed indicates the broker is closed.
	ErrBrokerClosed = fmt.Errorf("%w: closed", ErrBroker)
	// ErrBrokerConfigInvalid indicates the broker configuration is invalid.
	ErrBrokerConfigInvalid = fmt.Errorf("%w: config invalid", ErrBroker)
)

// Broker provides a high-level manager for AMQP connections, publishers, and consumers,
// offering automatic reconnection, connection pooling, and declarative topology management.
//
// Ownership & Lifecycle:
//   - Manages a pool of long-lived AMQP connections with purpose isolation: publishers,
//     consumers, and control/topology operations each use a dedicated connection slot.
//   - Handles automatic reconnection and resource cleanup transparently for all endpoints.
//   - Maintains registries for managed publishers and consumers, and a TTL-based pool for
//     one-off publishers created via [Broker.Publish].
//
// Topology Management:
//   - Centralizes declaration, verification, deletion, and synchronization of exchanges,
//     queues, and bindings via a local registry.
//   - Topology is reapplied automatically on reconnection.
//   - [Broker.Sync] reconciles against the local registry state, not the server's full state.
//
// Context Integration:
//   - Managed consumers automatically inject the Broker into the handler context;
//     retrieve it with [FromContext] for reply-to or chained publish patterns.
//
// Concurrency & Safety:
//   - All public methods are safe for concurrent use.
//   - Internal registries and pools are protected by mutexes and atomic operations.
//
// Usage Patterns:
//   - Use [New] to construct a Broker with custom options (URL, pool size, endpoint defaults, etc.).
//   - Use [Broker.NewPublisher]/[Broker.NewConsumer] to create managed endpoints with automatic reconnection.
//   - Use [Broker.Release] to close and deregister a specific managed endpoint without closing the whole broker.
//   - Use [Broker.Publish]/[Broker.Consume] for one-off publishing and consuming with automatic resource management.
//   - Use [Broker.Declare], [Broker.Delete], [Broker.Verify], and [Broker.Sync] to manage AMQP topology declaratively.
//   - Use [Broker.Close] to gracefully shut down the broker and all managed resources.
//
// See [Option] (With* functions) and [EndpointOptions] for configuration details.
type Broker struct {
	id  string
	url string

	// base context for managing lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// closed state
	closed atomic.Bool

	// connection manager for all connections
	connectionMgr     *transport.ConnectionManager
	connectionMgrOpts ConnectionManagerOptions

	// topology manager for declared exchanges, queues, and bindings
	topologyReg *topology.Registry

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

// New creates a new Broker and establishes the initial control connection.
// It starts background goroutines to maintain connections and clean up idle pooled endpoints.
func New(opts ...Option) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background()) // default

	b := &Broker{
		id:         defaultBrokerID,
		url:        defaultBrokerURL,
		ctx:        ctx,
		cancel:     cancel,
		publishers: make(map[string]Publisher),
		consumers:  make(map[string]Consumer),
		cacheTTL:   defaultBrokerCacheTTL,
	}

	for _, opt := range opts {
		opt(b)
	}

	// merge connection manager options with defaults
	b.connectionMgrOpts = transport.MergeConnectionManagerOptions(b.connectionMgrOpts, transport.DefaultConnectionManagerOptions())
	// validate connection manager options
	if err := transport.ValidateConnectionManagerOptions(b.connectionMgrOpts); err != nil {
		cancel()
		return nil, fmt.Errorf("%w: connection manager options: %w", ErrBrokerConfigInvalid, err)
	}

	// merge endpoint options defaults, then validate
	b.endpointOpts = endpoint.MergeEndpointOptions(b.endpointOpts, endpoint.DefaultEndpointOptions())
	// validate endpoint options
	if err := endpoint.ValidateEndpointOptions(b.endpointOpts); err != nil {
		cancel()
		return nil, fmt.Errorf("%w: endpoint options: %w", ErrBrokerConfigInvalid, err)
	}

	b.connectionMgr = transport.NewConnectionManager(b.url, &b.connectionMgrOpts)
	// initialize all managed connections
	if err := b.connectionMgr.Init(b.ctx); err != nil {
		return nil, err
	}

	b.topologyReg = topology.NewRegistry()

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
func (b *Broker) Connection() (Connection, error) {
	conn, err := b.connectionMgr.Assign(transport.ConnectionPurposeControl)
	if err != nil {
		return nil, internal.Wrap("create control connection", err)
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
func (b *Broker) Channel() (Channel, error) {
	conn, err := b.Connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, internal.Wrap("create control channel", err)
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

	return b.topologyReg.Declare(ch, t)
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

	return b.topologyReg.Delete(ch, t)
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

	return b.topologyReg.Verify(ch, t)
}

// Sync synchronizes the broker's topology to match the desired state exactly.
//
// Entities not present in the desired topology are deleted; missing entities are declared.
// This provides declarative topology management; specify the desired state and Sync enforces it.
// Note: Sync is aware only of topology declared on this Broker, not the server's full state.
// Returns an error if synchronization fails.
func (b *Broker) Sync(t *Topology) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return b.topologyReg.Sync(ch, t)
}

// Exchange returns a copy of the declared exchange with the given name.
// Returns nil if the exchange has not been declared on this Broker.
func (b *Broker) Exchange(name string) *Exchange {
	return b.topologyReg.Exchange(name)
}

// Queue returns a copy of the declared queue with the given name.
// Returns nil if the queue has not been declared on this Broker.
func (b *Broker) Queue(name string) *Queue {
	return b.topologyReg.Queue(name)
}

// Binding returns a copy of the declared binding with the given source, destination, and key.
// Returns nil if the binding has not been declared on this Broker.
func (b *Broker) Binding(source, destination, key string) *Binding {
	return b.topologyReg.Binding(source, destination, key)
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

	po := endpoint.DefaultPublisherOptions()
	if opts != nil {
		po = endpoint.MergePublisherOptions(*opts, po)
	}
	po.EndpointOptions = endpoint.MergeEndpointOptions(po.EndpointOptions, b.endpointOpts)

	// load last declared exchange by name from topology
	// this allows users to pass minimal exchange info
	if te := b.topologyReg.Exchange(e.Name); te != nil {
		e = *te
	}

	id := fmt.Sprintf("publisher-%d@%s", b.publishersSeq.Add(1), b.id)

	p, err := endpoint.NewPublisher(b.ctx, id, b.connectionMgr, b.topologyReg, po, e)
	if err != nil {
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
// The handler context will include the Broker (accessible via [FromContext]).
// Returns the Consumer or an error if creation fails.
func (b *Broker) NewConsumer(opts *ConsumerOptions, q Queue, h Handler) (Consumer, error) {
	if b.closed.Load() {
		return nil, ErrBrokerClosed
	}

	co := endpoint.DefaultConsumerOptions()
	if opts != nil {
		co = endpoint.MergeConsumerOptions(*opts, co)
	}
	co.EndpointOptions = endpoint.MergeEndpointOptions(co.EndpointOptions, b.endpointOpts)

	// load last declared queue by name from topology
	// this allows users to pass minimal queue info
	if tq := b.topologyReg.Queue(q.Name); tq != nil {
		q = *tq
	}

	id := fmt.Sprintf("consumer-%d@%s", b.consumersSeq.Add(1), b.id)

	h = contextWithBroker(b, h)

	c, err := endpoint.NewConsumer(b.ctx, id, b.connectionMgr, b.topologyReg, co, q, h)
	if err != nil {
		return nil, err
	}

	b.consumersMu.Lock()
	b.consumers[id] = c
	b.consumersMu.Unlock()

	return c, nil
}

// Release closes the given endpoint and removes it from the broker's managed registry.
//
// It is the explicit teardown counterpart to [Broker.NewPublisher] and [Broker.NewConsumer],
// allowing callers to dispose of a specific managed endpoint without closing the entire broker.
//
// The endpoint is closed first; its registry entry is removed regardless of whether Close
// returns an error, so a failing endpoint is never left registered.
// If ep is not found in the registry (e.g. already released or created outside the broker),
// no error is returned, but ep.Close() is still called.
// The returned error comes solely from ep.Close() and may be safely ignored when the
// caller only cares about deregistration and not about the result of the close itself.
func (b *Broker) Release(ep Endpoint) error {
	err := ep.Close()

	switch v := ep.(type) {
	case Publisher:
		b.publishersMu.Lock()
		for id, p := range b.publishers {
			if p == v {
				delete(b.publishers, id)
				break
			}
		}
		b.publishersMu.Unlock()
	case Consumer:
		b.consumersMu.Lock()
		for id, c := range b.consumers {
			if c == v {
				delete(b.consumers, id)
				break
			}
		}
		b.consumersMu.Unlock()
	}

	return err
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
	if te := b.topologyReg.Exchange(exchange); te != nil {
		e = *te
	} else {
		e = NewExchange(exchange)
	}
	rk := RoutingKey(routingKey)

	// use pooled publisher if enabled
	if b.publishersPool != nil {
		var hash string
		{
			h := md5.New()
			_ = gob.NewEncoder(h).Encode(e) // safe to ignore, type wouldn't fail
			hash = hex.EncodeToString(h.Sum(nil))
		}

		key := "publisher:" + hash

		publisher, release, err := b.publishersPool.acquire(key, func() (Publisher, error) {
			return b.NewPublisher(nil, e)
		})
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBroker, err)
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
// The handler context will include the Broker (accessible via [FromContext]).
// Returns an error if consumption fails.
func (b *Broker) Consume(ctx context.Context, queue string, handler Handler) error {
	if b.closed.Load() {
		return ErrBrokerClosed
	}

	ctx, cancel := contextWithAnyCancel(b.ctx, ctx) // derived context
	defer cancel()

	// lookup queue from topology manager or use default
	var q Queue
	if tq := b.topologyReg.Queue(queue); tq != nil {
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
func (b *Broker) Transaction(ctx context.Context, fn func(Channel) error) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Tx(); err != nil {
		return internal.Wrap("transaction start", err)
	}

	fnErr := transport.DoSafeChannelAction(ch, fn)
	if fnErr != nil {
		if rbErr := ch.TxRollback(); rbErr != nil {
			// return an error that lets callers inspect both errors
			return internal.Wrap("transaction rollback", rbErr, fnErr)
		}
		return internal.Wrap("transaction function", fnErr)
	}

	if err := ch.TxCommit(); err != nil {
		return internal.Wrap("transaction commit", err)
	}

	return nil
}
