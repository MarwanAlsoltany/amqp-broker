package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	url string

	conn   *amqp.Connection
	connMu sync.RWMutex

	connPool     *connectionPool
	connPoolSize int

	chPool     *channelPool
	chPoolSize int

	topology   *Topology
	topologyMu sync.RWMutex

	declaredExchanges sync.Map // declared exchange cache
	declaredQueues    sync.Map // declared queue cache
	declaredBindings  sync.Map // declared queue cache

	// publishers   map[string]*publisher
	// publishersMu sync.Mutex

	consumers   map[string]*consumerEntry
	consumersMu sync.Mutex

	closed atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	reconnectMin time.Duration
	reconnectMax time.Duration
}

type BrokerOption func(*Broker)

func WithConnectionPoolSize(n int) BrokerOption {
	return func(b *Broker) {
		if n <= 0 {
			n = 4
		}
		b.connPoolSize = n
	}
}
func WithChannelPoolSize(n int) BrokerOption {
	return func(b *Broker) {
		if n <= 0 {
			n = 4
		}
		b.chPoolSize = n
	}
}
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

func NewBroker(amqpURL string, options ...BrokerOption) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		url:          amqpURL,
		consumers:    make(map[string]*consumerEntry),
		ctx:          ctx,
		cancel:       cancel,
		reconnectMin: 500 * time.Millisecond,
		reconnectMax: 30 * time.Second,
		connPoolSize: 4,
		chPoolSize:   8,
	}
	for _, option := range options {
		option(b)
	}
	b.connPool = newConnectionPool(amqpURL, b.connPoolSize)
	b.chPool = newChannelPool(b.connPool, b.chPoolSize)

	// initial connect attempt (synchronous)
	if err := b.connectOnce(ctx); err != nil {
		return b, fmt.Errorf("initial connect failed: %w", err)
	}
	// start reconnect watcher
	go b.connect()

	return b, nil
}

func (b *Broker) connect() {
	for {
		conn := b.Connection()
		if conn == nil {
			if err := b.connectWithRetry(b.ctx); err != nil {
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
			_ = conn.Close()
			b.setConnection(nil)
			log.Printf("[broker] control connection closed: %v", err)
			// continue to reconnect
		}
	}
}

func (b *Broker) connectOnce(ctx context.Context) error {
	conn, err := amqp.Dial(b.url)
	if err != nil {
		return fmt.Errorf("dial control connection: %w", err)
	}
	b.setConnection(conn)

	// apply topology if present
	b.topologyMu.RLock()
	t := b.topology
	b.topologyMu.RUnlock()
	if t != nil {
		if err := b.applyTopology(ctx, t, conn); err != nil {
			_ = conn.Close()
			b.setConnection(nil)
			return fmt.Errorf("apply topology: %w", err)
		}
	}

	// (re)start consumers on this conn
	b.consumersMu.Lock()
	defer b.consumersMu.Unlock()
	for _, ce := range b.consumers {
		_ = ce.stop()
		go b.startConsumerWhenConnected(ce)
	}

	return nil
}

func (b *Broker) connectWithRetry(ctx context.Context) error {
	backoff := b.reconnectMin
	for {
		if b.closed.Load() {
			return errors.New("broker closed")
		}
		if err := b.connectOnce(ctx); err == nil {
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

func (b *Broker) Connection() *amqp.Connection {
	b.connMu.RLock()
	defer b.connMu.RUnlock()
	return b.conn
}

func (b *Broker) setConnection(c *amqp.Connection) {
	b.connMu.Lock()
	b.conn = c
	b.connMu.Unlock()
}

func (b *Broker) NewChannel() (*amqp.Channel, func(), error) {
	conn := b.Connection()
	if conn == nil {
		return nil, nil, errors.New("no control connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open control channel: %w", err)
	}
	return ch, func() { _ = ch.Close() }, nil
}

func (b *Broker) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	b.cancel()
	// stop/close consumers
	b.consumersMu.Lock()
	for _, ce := range b.consumers {
		_ = ce.stop()
	}
	b.consumersMu.Unlock()
	// close pools
	if b.chPool != nil {
		b.chPool.Close()
	}
	if b.connPool != nil {
		b.connPool.Close()
	}
	// close control connection
	conn := b.Connection()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (b *Broker) DeclareTopology(ctx context.Context, t Topology) error {
	b.topologyMu.Lock()
	b.topology = &t
	b.topologyMu.Unlock()
	conn := b.Connection()
	if conn == nil {
		return errors.New("no control connection")
	}
	return b.applyTopology(ctx, &t, conn)
}

func (b *Broker) applyTopology(_ context.Context, t *Topology, conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("topology: open channel: %w", err)
	}
	defer ch.Close()
	for _, ex := range t.Exchanges {
		if err := ch.ExchangeDeclare(ex.Name, defaultExchangeTypeFallback(ex.Kind), ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args); err != nil {
			return fmt.Errorf("declare exchange %s: %w", ex.Name, err)
		}
		b.declaredExchanges.Store(ex.Name, struct{}{})
	}
	for _, q := range t.Queues {
		if _, err := ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Args); err != nil {
			return fmt.Errorf("declare queue %s: %w", q.Name, err)
		}
		b.declaredQueues.Store(q.Name, struct{}{})
	}
	for _, bn := range t.Bindings {
		if err := ch.QueueBind(bn.Queue, bn.RoutingKey, bn.Exchange, false, bn.Args); err != nil {
			return fmt.Errorf("bind %s->%s: %w", bn.Exchange, bn.Queue, err)
		}
		b.declaredBindings.Store(fmt.Sprintf("%s|%s|%s", bn.Exchange, bn.Queue, bn.RoutingKey), struct{}{})
	}
	return nil
}

// NewPublisher returns a Publisher that owns a connection from the Broker's connection pool and a dedicated channel.
// The publisher must be closed to return the connection to the pool.
func (b *Broker) NewPublisher(ctx context.Context, confirm bool) (Publisher, error) {
	if b.closed.Load() {
		return nil, errors.New("broker closed")
	}
	conn, relConn, err := b.connPool.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection from pool: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		relConn(true)
		return nil, fmt.Errorf("open publisher channel: %w", err)
	}
	p := &publisher{
		conn:    conn,
		relConn: relConn,
		ch:      ch,
		confirm: confirm,
		broker:  b,
	}
	if confirm {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			relConn(true)
			return nil, fmt.Errorf("enable confirm: %w", err)
		}
		p.confCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	}
	return p, nil
}

// Publish uses channel pool for fast non-confirm publishes.
// If opts.WaitForConfirm==true Broker.Publish will create a short-lived confirm Publisher.
func (b *Broker) Publish(ctx context.Context, ex Exchange, rk RoutingKey, body []byte, opts *PublishOptions) error {
	if b.closed.Load() {
		return errors.New("broker closed")
	}
	po := PublishOptions{}
	if opts != nil {
		po = *opts
	}
	if po.WaitForConfirm {
		// create a short-lived confirm publisher
		p, err := b.NewPublisher(ctx, true)
		if err != nil {
			return err
		}
		defer p.Close()
		return p.Publish(ctx, ex, rk, body, opts)
	}

	// pooled channel path (fast)
	ch, rel, err := b.chPool.Get(ctx)
	if err != nil {
		return fmt.Errorf("publish: get pooled channel: %w", err)
	}
	defer rel(false)

	if po.DeliveryMode == 0 {
		po.DeliveryMode = 2
	}
	if po.ContentType == "" {
		po.ContentType = "application/octet-stream"
	}
	pub := amqp.Publishing{
		Headers:      po.Headers,
		ContentType:  po.ContentType,
		Body:         body,
		DeliveryMode: po.DeliveryMode,
		Timestamp:    time.Now(),
	}

	// ensure exchange declared (best-effort) - use control connection if available
	if ex.Name != "" {
		if _, loaded := b.declaredExchanges.LoadOrStore(ex.Name, struct{}{}); !loaded {
			if conn := b.Connection(); conn != nil {
				chDecl, relDecl, err := b.NewChannel()
				if err == nil {
					_ = chDecl.ExchangeDeclare(ex.Name, defaultExchangeTypeFallback(ex.Kind), ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
					relDecl()
				} else {
					_ = ch.ExchangeDeclare(ex.Name, defaultExchangeTypeFallback(ex.Kind), ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
				}
			} else {
				_ = ch.ExchangeDeclare(ex.Name, defaultExchangeTypeFallback(ex.Kind), ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
			}
		}
	}

	if err := ch.PublishWithContext(ctx, ex.Name, string(rk), po.Mandatory, po.Immediate, pub); err != nil {
		rel(true)
		// Try one retry with a new channel
		ch2, rel2, err2 := b.chPool.Get(ctx)
		if err2 != nil {
			return fmt.Errorf("publish retry: get channel: %w (orig: %v)", err2, err)
		}
		defer rel2(false)
		if err3 := ch2.PublishWithContext(ctx, ex.Name, string(rk), po.Mandatory, po.Immediate, pub); err3 != nil {
			rel2(true)
			return fmt.Errorf("publish failed after retry: %w", err3)
		}
	}
	return nil
}

// PublishOnce is a convenience one-off publish that uses either pooled channel (fast) or
// a short-lived confirm publisher if WaitForConfirm requested.
func (b *Broker) PublishOnce(ctx context.Context, ex Exchange, rk RoutingKey, body []byte, opts *PublishOptions) error {
	return b.Publish(ctx, ex, rk, body, opts)
}

// NewConsumer creates and starts a consumer and returns a Consumer handle.
// For long-lived consumers prefer NewConsumer; Broker.Consume is a convenience wrapper.
func (b *Broker) NewConsumer(qu Queue, handler Handler, opts *ConsumeOptions) (Consumer, error) {
	if b.closed.Load() {
		return nil, errors.New("broker closed")
	}
	co := ConsumeOptions{}
	if opts != nil {
		co = *opts
	}

	b.consumersMu.Lock()
	id := fmt.Sprintf("c-%d", len(b.consumers)+1)
	ce := &consumerEntry{
		id:      id,
		queue:   qu,
		opts:    co,
		handler: handler,
	}
	b.consumers[id] = ce
	b.consumersMu.Unlock()

	go b.startConsumerWhenConnected(ce)

	return &consumerHandle{entry: ce, broker: b}, nil
}

func (b *Broker) Consume(queue Queue, handler Handler, opts *ConsumeOptions) (Consumer, error) {
	return b.NewConsumer(queue, handler, opts)
}

func (b *Broker) ConsumeOnce(ctx context.Context, queue Queue, handler Handler, opts *ConsumeOptions) error {
	co := ConsumeOptions{}
	if opts != nil {
		co = *opts
	}
	ce := &consumerEntry{
		id:      "once",
		queue:   queue,
		opts:    co,
		handler: handler,
	}
	return b.startConsumerOnce(ce)
}

func (b *Broker) startConsumerOnce(ce *consumerEntry) error {
	ce.runningMu.Lock()
	defer ce.runningMu.Unlock()
	if ce.cancel != nil {
		return nil // already running
	}
	conn := b.Connection()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}

	// declare queue
	if _, err := ch.QueueDeclare(ce.queue.Name, ce.queue.Durable, ce.queue.AutoDelete, ce.queue.Exclusive, ce.queue.NoWait, ce.queue.Args); err != nil {
		_ = ch.Close()
		return fmt.Errorf("declare queue: %w", err)
	}
	// bindings
	for _, bnd := range ce.opts.Bindings {
		if err := ch.QueueBind(bnd.Queue, bnd.RoutingKey, bnd.Exchange, false, bnd.Args); err != nil {
			_ = ch.Close()
			return fmt.Errorf("bind: %w", err)
		}
	}
	prefetch := ce.opts.PrefetchCount
	if prefetch <= 0 {
		prefetch = 1
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("qos: %w", err)
	}

	deliveries, err := ch.Consume(ce.queue.Name, "", ce.opts.AutoAck, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("consume start: %w", err)
	}

	// set last-known ch/conn for user access
	ce.lastMu.Lock()
	ce.lastCh = ch
	ce.lastConn = conn
	ce.lastMu.Unlock()

	ctx, cancel := context.WithCancel(b.ctx)
	ce.cancel = cancel

	go func() {
		defer func() {
			_ = ch.Close()
			ce.runningMu.Lock()
			// clear runtime values
			ce.cancel = nil
			ce.runningMu.Unlock()

			ce.lastMu.Lock()
			ce.lastCh = nil
			ce.lastConn = nil
			ce.lastMu.Unlock()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				m := &Message{
					Body:        d.Body,
					Headers:     d.Headers,
					ContentType: d.ContentType,
					Delivery:    &d,
					Broker:      b,
				}
				m.Ack = func() error { return d.Ack(false) }
				m.Nack = func(requeue bool) error { return d.Nack(false, requeue) }
				m.Reject = func() error { return d.Reject(false) }

				ce.wg.Add(1)
				go func(dd amqp.Delivery, msg *Message) {
					defer ce.wg.Done()
					action, _ := ce.handler(ctx, msg)
					if ce.opts.AutoAck {
						return
					}
					switch action {
					case AckActionAck:
						_ = msg.Ack()
					case AckActionNackRequeue:
						_ = msg.Nack(true)
					case AckActionNackDiscard:
						_ = msg.Nack(false)
					case AckActionNoAction:
						// handler did ack/nack
					}
				}(d, m)
			}
		}
	}()
	return nil
}

func (b *Broker) startConsumerWhenConnected(ce *consumerEntry) {
	for {
		if b.closed.Load() {
			return
		}
		conn := b.Connection()
		if conn == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if err := b.startConsumerOnce(ce); err != nil {
			log.Printf("[broker] start consumer failed: %v - retrying", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		// started; return. On reconnect resubscribeAll will start new instances.
		return
	}
}
