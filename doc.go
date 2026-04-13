// Package broker provides a robust, production-grade abstraction layer over
// github.com/rabbitmq/amqp091-go for working with RabbitMQ in Go.
//
// It models the AMQP domain as first-class entities (exchanges, queues, bindings,
// publishers, consumers, messages, and handlers) so intent is declared, not protocol
// steps. The goal is to eliminate the cognitive overhead of wiring up publishers and
// consumers from scratch on every service: connect, declare a topology, hand off a
// handler, and let the library own the rest. Reliability and fire-and-forget operation
// are first-class concerns, not afterthoughts.
//
// More specifically, the package offers a high-level, opinionated API for AMQP 0.9.1
// messaging, focusing on reliability, ease of use, and safe concurrency. It manages
// connections, channels, publishers, and consumers with automatic reconnection,
// resource pooling, and declarative topology management.
//
// # Features
//
//   - Connection Management: Transparent connection pooling with configurable pool sizes,
//     automatic reconnection with exponential backoff, lifecycle hooks (OnOpen, OnClose,
//     OnBlock), and flow control handling. Both transient network blips and prolonged
//     infrastructure outages are recovered from automatically; connections and all their
//     dependent endpoints (publishers, consumers) resume without any intervention required.
//     Publishers, consumers, and control/topology operations each draw from a dedicated
//     connection slot, so a busy publisher never starves a consumer or blocks topology work.
//
//   - Declarative Topology: Centralized declaration, verification, deletion, and
//     synchronization of exchanges, queues, and bindings. Topology is automatically
//     reapplied on reconnection and supports declarative state management via Sync().
//     Query declared topology by name with Exchange(), Queue(), and Binding().
//
//   - Publisher Abstraction: Managed publishers with publisher confirms, deferred
//     confirmation callbacks, returned-message and flow-control event hooks, one-off
//     publishers with automatic pooling and caching, flow control awareness, and topology
//     auto-declaration.
//
//   - Consumer Abstraction: Managed consumers with configurable handler concurrency
//     (unlimited, sequential, or a fixed-size worker pool), prefetch settings, graceful
//     shutdown coordination, automatic reconnection and re-subscription, topology
//     auto-declaration, and built-in middleware support.
//
//   - Handler Middleware: Composable middleware system with +15 built-in middlewares:
//     Logging, Metrics, Debug, Recovery, Fallback, Retry, CircuitBreaker, Concurrency,
//     RateLimit, Deduplication, Validation, Transform, Deadline, Timeout, Batch.
//
//   - Message Building: A single message type spans both sides of the wire, outgoing
//     and incoming messages share the same structure. Two construction styles for
//     outgoing messages: a fluent builder for validated construction, and a plain
//     constructor for simple cases with direct field access.
//
//   - Context Integration: The broker is injected into every handler context via
//     context.WithValue, accessible with FromContext; enables reply-to and RPC patterns
//     without explicit plumbing. Background goroutines are governed by a root context
//     cancelled on Close; public methods accept a separate caller context for
//     operation-level cancellation and scoped control over each call.
//
//   - Structured Errors: Layered error hierarchy rooted at ErrBroker; every error is
//     matchable by subsystem (transport, topology, endpoint, message, handler) or by
//     specific sentinel. All errors carry operation context via errors.As(*Error):
//     Op names the failing operation, Err holds the root cause, Data carries optional
//     structured fields.
//
//   - Safe Concurrency: All public APIs are safe for concurrent use. Internal registries
//     and pools are protected by mutexes and atomic operations. Context-based cancellation
//     throughout.
//
//   - Extensibility: Functional options for all configuration (With* functions), custom
//     dialers for test environments, fully composable middleware chains, and a minimal
//     public surface that exposes no internal implementation details.
//
// # Quick Start
//
// The snippet below shows the complete basic path: connect, declare topology,
// publish a message, and consume it.
//
//	ctx := context.Background()
//
//	// WithURL is optional; the default is amqp://guest:guest@localhost:5672/
//	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer b.Close()
//
//	// declare topology once; it is reapplied automatically on reconnection
//	t := broker.NewTopology(
//	    []broker.Exchange{broker.NewExchange("events").WithType("topic").WithDurable(true)},
//	    []broker.Queue{broker.NewQueue("notifications").WithDurable(true)},
//	    []broker.Binding{broker.NewBinding("events", "notifications", "user.*")},
//	)
//	if err := b.Declare(&t); err != nil {
//	    log.Fatal(err)
//	}
//
//	// one-off publish using a static routing key (uses a cached publisher internally)
//	msg := broker.NewMessage([]byte("Hello, AMQP!"))
//	if err := b.Publish(ctx, "events", "user.signup", msg); err != nil {
//	    log.Fatal(err)
//	}
//
//	// one-off consume; blocks until ctx is cancelled
//	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
//	    log.Printf("received: %s", msg.Body)
//	    return broker.HandlerActionAck, nil
//	}
//	if err := b.Consume(ctx, "notifications", handler); err != nil {
//	    log.Fatal(err)
//	}
//
// # One-off vs Managed Endpoints
//
// [Broker.Publish] and [Broker.Consume] are convenience methods for simple or
// low-volume use cases. They create, use, and (where applicable) pool endpoints
// automatically. No endpoint lifecycle management is needed.
//
// [Broker.NewPublisher] and [Broker.NewConsumer] create long-lived managed endpoints
// with full lifecycle control. Use them when publisher confirms, custom
// callbacks (OnConfirm, OnReturn, OnFlow, OnError, OnCancel), middleware pipelines,
// or explicit control over reconnection behavior.
//
// [Broker.Release] closes a managed endpoint and removes it from the internal
// registry. Use it instead of Close when the broker should stop tracking
// a specific endpoint without shutting down the broker itself.
//
// # Managed Publisher
//
// A managed publisher runs in the background and is ready to publish immediately.
// Publisher confirms, returned-message handling, flow-control awareness, and error
// callbacks are all configured via [PublisherOptions].
//
//	// b and ctx set up as in the Quick Start section above
//
//	pub, err := b.NewPublisher(
//	    &broker.PublisherOptions{
//	        ConfirmMode:    true,
//	        ConfirmTimeout: 5 * time.Second,
//	        // OnConfirm is called per published message when ConfirmMode is true
//	        // providing this callback enables deferred confirmation mode: Publish
//	        // returns immediately and the callback is invoked once the broker acks
//	        OnConfirm: func(deliveryTag uint64, wait func(context.Context) bool) {
//	            confirmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
//	            defer cancel()
//	            if wait(confirmCtx) {
//	                log.Printf("message %d confirmed", deliveryTag)
//	            } else {
//	                log.Printf("message %d not confirmed", deliveryTag)
//	            }
//	        },
//	        // OnReturn is called when a mandatory message cannot be routed
//	        OnReturn: func(msg broker.Message) {
//	            log.Printf("message returned: %s", msg.Body)
//	        },
//	        // OnFlow is called when the broker activates or deactivates flow control
//	        OnFlow: func(active bool) {
//	            log.Printf("flow control active: %v", active)
//	        },
//	        // OnError is called for background errors (confirmation loss, reconnect failures)
//	        OnError: func(err error) {
//	            log.Printf("publisher error: %v", err)
//	        },
//	    },
//	    broker.NewExchange("events"),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer pub.Close()
//
//	// static routing key via type conversion
//	msg := broker.NewMessage([]byte("important event"))
//	if err := pub.Publish(ctx, broker.RoutingKey("user.created"), msg); err != nil {
//	    log.Fatal(err)
//	}
//
//	// dynamic routing key with placeholder substitution
//	rk := broker.NewRoutingKey("orders.{region}.{action}", map[string]string{
//	    "region": "us-east",
//	    "action": "created",
//	})
//	if err := pub.Publish(ctx, rk, msg); err != nil {
//	    log.Fatal(err)
//	}
//
// # Managed Consumer
//
// A managed consumer runs in the background, delivering messages to the handler.
// Concurrency, prefetch, graceful shutdown, and reconnection are all configured
// via [ConsumerOptions]. Handler behavior is composed with [WrapHandler].
//
//	// b and ctx set up as in the Quick Start section above
//
//	handler := broker.WrapHandler(
//	    func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
//	        log.Printf("processing: %s", msg.MessageID)
//	        return broker.HandlerActionAck, nil
//	    },
//	    broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{}), // outermost
//	    broker.RetryMiddleware(&broker.RetryMiddlewareConfig{MaxAttempts: 3}),
//	    broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{}),
//	)
//
//	con, err := b.NewConsumer(
//	    &broker.ConsumerOptions{
//	        PrefetchCount:         10,
//	        MaxConcurrentHandlers: 5,
//	        // OnCancel is called when the server cancels this consumer
//	        OnCancel: func(consumerTag string) {
//	            log.Printf("consumer cancelled: %s", consumerTag)
//	        },
//	        // OnError is called for background errors (reconnect failures, delivery errors)
//	        OnError: func(err error) {
//	            log.Printf("consumer error: %v", err)
//	        },
//	    },
//	    broker.NewQueue("notifications"),
//	    handler,
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer con.Close()
//
//	// Consume blocks until ctx is cancelled, then waits for in-flight handlers
//	con.Consume(ctx)
//
//	// to run non-blocking, start it in a goroutine and wait for shutdown separately
//	go con.Consume(ctx)
//	con.Wait()
//
//	// Wait blocks until all in-flight handlers finish (does not stop the consumer)
//	con.Wait()
//
//	// Cancel stops delivering new messages without closing the channel
//	// call Consume again to restart delivery on the same channel
//	con.Cancel()
//
//	// Get fetches one message synchronously (polling); returns nil, nil if the queue is empty
//	single, err := con.Get()
//
// # Configuration
//
// Configure the broker with functional options:
//
//	b, err := broker.New(
//	    broker.WithURL("amqp://username:password@host:port/vhost"),
//	    broker.WithIdentifier("my-service"),
//	    broker.WithContext(ctx),
//	    // WithCache enables pooling of one-off publishers created via Broker.Publish
//	    // TTL controls how long idle pooled publishers are kept. Set to 0 to disable
//	    broker.WithCache(10 * time.Minute),
//	    // WithConnectionPoolSize is a shorthand for setting ConnectionManagerOptions.Size
//	    //   1 = all operations share one connection
//	    //   2 = publishers/control on one, consumers on another (recommended)
//	    //   3+ = dedicated connections per role
//	    broker.WithConnectionPoolSize(3),
//	    // WithConnectionManagerOptions gives full control over the pool
//	    // this and WithConnectionPoolSize are complementary: use one for quick sizing,
//	    // the other for complete configuration including hooks and reconnect timing
//	    broker.WithConnectionManagerOptions(broker.ConnectionManagerOptions{
//	        Size: 3,
//	        Config: &broker.Config{
//	            Heartbeat: 30 * time.Second,
//	            Locale:    "en_US",
//	        },
//	        OnOpen: func(idx int) {
//	            log.Printf("connection %d opened", idx)
//	        },
//	        OnClose: func(idx, code int, reason string, server, recover bool) {
//	            log.Printf("connection %d closed: %s", idx, reason)
//	        },
//	        OnBlock: func(idx int, active bool, reason string) {
//	            log.Printf("connection %d flow control: active=%v reason=%s", idx, active, reason)
//	        },
//	        ReconnectMin: 500 * time.Millisecond,
//	        ReconnectMax: 30 * time.Second,
//	    }),
//	    broker.WithEndpointOptions(broker.EndpointOptions{
//	        ReadyTimeout: 10 * time.Second,
//	        ReconnectMin: 1 * time.Second,
//	        ReconnectMax: 30 * time.Second,
//	    }),
//	)
//
// # Publisher Options
//
// [PublisherOptions] configures a managed publisher:
//
//	opts := &broker.PublisherOptions{
//	    EndpointOptions: broker.EndpointOptions{...}, // see Endpoint Options section below
//	    ConfirmMode:    true,
//	    ConfirmTimeout: 5 * time.Second,
//	    Mandatory:      false,
//	    Immediate:      false, // deprecated for RabbitMQ 3.0+
//	    OnConfirm: func(deliveryTag uint64, wait func(context.Context) bool) {
//	        // called per published message when ConfirmMode is true;
//	        // providing this enables deferred confirmation mode
//	    },
//	    OnReturn: func(msg broker.Message) {
//	        // called when a mandatory message cannot be routed
//	    },
//	    OnFlow: func(active bool) {
//	        // called when the broker activates or deactivates flow control
//	    },
//	    OnError: func(err error) {
//	        // called for background errors (confirmation loss, reconnect failures)
//	    },
//	}
//
// # Consumer Options
//
// [ConsumerOptions] configures a managed consumer:
//
//	opts := &broker.ConsumerOptions{
//	    EndpointOptions: broker.EndpointOptions{...}, // see Endpoint Options section below
//	    AutoAck:       false,
//	    PrefetchCount: 10,
//	    Exclusive:     false,
//	    NoWait:        false,
//	    //  0: default, capped to PrefetchCount
//	    // -1: unlimited goroutines (one per message; PrefetchCount is the only backpressure)
//	    //  1: sequential processing
//	    //  N: worker pool with N goroutines
//	    MaxConcurrentHandlers: 5,
//	    OnCancel: func(consumerTag string) {
//	        // called when the server cancels the consumer
//	    },
//	    OnError: func(err error) {
//	        // called for background errors (reconnect failures, delivery errors)
//	    },
//	}
//
// # Endpoint Options
//
// [EndpointOptions] is embedded in both [PublisherOptions] and [ConsumerOptions].
// It can also be set as a broker-wide default via [WithEndpointOptions]:
//
//	opts := broker.EndpointOptions{
//	    // NoAutoDeclare skips topology (re)declaration on connect/reconnect
//	    NoAutoDeclare: false,
//	    // NoAutoReconnect treats connection loss as a terminal error
//	    // when false (default), only the initial connection is fail-fast;
//	    // subsequent losses retry with exponential backoff
//	    NoAutoReconnect: false,
//	    // NoWaitReady returns from NewPublisher/NewConsumer immediately instead
//	    // of blocking until the endpoint is connected and ready
//	    NoWaitReady:  false,
//	    ReconnectMin: 500 * time.Millisecond,
//	    ReconnectMax: 30 * time.Second,
//	    ReadyTimeout: 10 * time.Second,
//	}
//
// NOTE: Due to Go's inability to distinguish between a zero value and an absent value,
// only non-zero fields in opts are propagated when merging with endpoint defaults.
// Boolean flags and numeric fields that default to zero (false/0) cannot be reliably
// inherited from broker-wide defaults. To set boolean defaults, explicit options
// must be passed directly to [Broker.NewPublisher]/[Broker.NewConsumer].
//
// # Topology Management
//
// Declarative topology management with sync support. All four bulk operations act on
// a [Topology] value and delegate to the internal registry and AMQP channel:
//
//	// NewTopology is a constructor convenience; the struct can also be initialized directly
//	t := broker.Topology{
//	    Exchanges: []broker.Exchange{broker.NewExchange("events").WithType("topic")},
//	    Queues:    []broker.Queue{broker.NewQueue("events.orders")},
//	    Bindings:  []broker.Binding{broker.NewBinding("events", "events.orders", "orders.*")},
//	}
//
//	// Declare: merges into the registry and declares on the server;
//	// reapplied automatically on reconnection
//	b.Declare(&t)
//
//	// Verify: passive-declares all entities; returns an error if any are missing
//	b.Verify(&t)
//
//	// Delete: removes entities from the server and the registry
//	b.Delete(&t)
//
//	// Sync: enforces exact desired state (declares missing, deletes extra);
//	// aware only of topology declared on this Broker, not the server's full state
//	b.Sync(&t)
//
// Query the declared registry by name:
//
//	exchange := b.Exchange("events") // returns *Exchange or nil
//	queue    := b.Queue("events.orders") // returns *Queue or nil
//	binding  := b.Binding("events", "events.orders", "orders.*") // returns *Binding or nil
//
// [Topology] also has query methods on the value itself:
//
//	exchange := t.Exchange("events") // returns *Exchange or nil
//	queue    := t.Queue("events.orders") // returns *Queue or nil
//	binding  := t.Binding("events", "events.orders", "orders.*")
//
// And direct access to its fields:
//
//	for _, e := range t.Exchanges { ... }
//	for _, q := range t.Queues    { ... }
//	for _, b := range t.Bindings  { ... }
//
// Exchange-to-exchange bindings are supported via [BindingType]:
//
//	broker.NewBinding("source", "dest-exchange", "key").WithType(broker.BindingTypeExchange)
//
// Each topology entity ([Exchange], [Queue], [Binding]) also exposes entity-level
// lifecycle methods for direct use with a raw channel when needed: Validate(),
// Declare(ch), Verify(ch), Delete(ch, ...). Queue additionally provides Purge(ch)
// and Inspect(ch). All three types provide immutable-style With* builder methods
// (e.g. WithType, WithDurable, WithAutoDelete, WithArgument, WithArguments).
// [RoutingKey] exposes Replace(map), String(), and Validate().
//
// # Handler Middleware
//
// [WrapHandler] composes a base handler with one or more middlewares. Middlewares are
// applied left-to-right; the first is the outermost wrapper.
//
// Execution order: outermost pre-logic -> ... -> handler -> ... -> outermost post-logic.
//
//	handler := broker.WrapHandler(
//	    baseHandler,
//	    broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{}), // outermost
//	    broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{}),
//	    broker.TimeoutMiddleware(&broker.TimeoutMiddlewareConfig{Timeout: 30 * time.Second}), // innermost
//	)
//
// [ActionHandler] creates a handler that always returns a fixed action,
// useful as the base for [BatchMiddleware] or in tests:
//
//	base := broker.ActionHandler(broker.HandlerActionNoAction)
//
// Available middlewares and their types:
//
//	// LoggingMiddleware           pre+post    structured slog-based lifecycle logging
//	// MetricsMiddleware           post        records duration; invokes a Record callback
//	// DebugMiddleware             pre         logs full message payload; for development only
//	// RecoveryMiddleware          post        catches panics, logs them, returns an error
//	// FallbackMiddleware          post        invokes an alternative handler when the primary fails
//	// RetryMiddleware             post        retries on HandlerActionNackRequeue with exponential backoff
//	// CircuitBreakerMiddleware    pre+post    opens after N consecutive failures; half-open probing
//	// ConcurrencyMiddleware       pre+post    semaphore-based limit on concurrent handler goroutines
//	// RateLimitMiddleware         pre         token-bucket rate limiting (RPS or Burst/RefillRate)
//	// DeduplicationMiddleware     pre         skips duplicates via a pluggable Cache + Identify function
//	// ValidationMiddleware        pre         rejects messages failing a user-supplied validate function
//	// TransformMiddleware         pre         rewrites the message body (decompression, decryption, etc.)
//	// DeadlineMiddleware          pre         discards messages whose deadline header has already passed
//	// TimeoutMiddleware           pre+post    cancels handler context after a fixed duration
//	// BatchMiddleware             terminal    accumulates messages into batches (see below)
//
// # Batch Middleware
//
// [BatchMiddleware] is special: it is a terminal middleware. The base handler is
// never called. Always use [ActionHandler]([HandlerActionNoAction]) as the base:
//
//	handler := broker.WrapHandler(
//	    broker.ActionHandler(broker.HandlerActionNoAction),
//	    broker.BatchMiddleware(ctx, batchHandler, cfg),
//	)
//
// [BatchHandler] receives a lazy indexed iterator (iter.Seq2[int, *Message]) and returns
// a single [HandlerAction] and error. Two acknowledgment patterns:
//
//  1. Uniform action: return [HandlerActionAck], [HandlerActionNackRequeue], or
//     [HandlerActionNackDiscard]. The consumer applies that action to every message
//     in the batch. Do NOT call msg.Ack/Nack/Reject manually in this mode.
//
//  2. Per-message acking: call msg.Ack(), msg.Nack(), or msg.Reject() on each visited
//     message, then return [HandlerActionNoAction]. The consumer does not touch acks.
//     Messages skipped via an early break from the range loop are handled according
//     to [BatchConfig].ErrorAction.
//
// [BatchConfig] fields:
//
//	Size          int           // number of messages that triggers a flush (default: 10)
//	FlushTimeout  time.Duration // flush partial batch after this duration (default: 1s)
//	Async         bool          // false=sync/blocking (default), true=async/background goroutine
//	EnqueueAction Action        // action returned to consumer goroutine on enqueue in async mode:
//	                            //   ActionAck (default): pre-ack on enqueue; at-most-once
//	                            //   ActionNoAction: hold-ack; at-least-once (PrefetchCount >= BufferSize required)
//	ErrorAction   Action        // action for unvisited messages on error or early break (default: ActionNackRequeue)
//	BufferSize    int           // async channel capacity (default: 10*Size)
//	OnError       func(...)     // called when batch processing fails or the buffer is full
//
// Execution models:
//
//   - Sync (Async=false): consumer goroutines block while the batch flushes. Provides
//     natural back-pressure. Consumer's PrefetchCount MUST be >= Size to avoid deadlock.
//
//   - Async (Async=false): consumer goroutines return immediately after enqueue; a
//     background goroutine processes batches. The ctx parameter controls the background
//     goroutine's lifetime. When cancelled, it drains its buffer and flushes a final
//     batch before exiting. Pass the consumer's lifecycle context.
//
// Examples:
//
//	// sync: transactional DB writes, at-least-once
//	broker.WrapHandler(
//	    broker.ActionHandler(broker.HandlerActionNoAction),
//	    broker.BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *broker.Message]) (broker.HandlerAction, error) {
//	            batch := &pgx.Batch{}
//	            for _, msg := range msgs {
//	                batch.Queue("INSERT INTO events VALUES ($1)", msg.Body)
//	            }
//	            return broker.HandlerActionAck, pool.SendBatch(ctx, batch).Close()
//	        },
//	        // PrefetchCount on the consumer MUST be >= Size in sync mode
//	        &broker.BatchConfig{Size: 50, FlushTimeout: 200 * time.Millisecond},
//	    ),
//	)
//
//	// async: fire-and-forget analytics, at-most-once
//	broker.WrapHandler(
//	    broker.ActionHandler(broker.HandlerActionNoAction),
//	    broker.BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *broker.Message]) (broker.HandlerAction, error) {
//	            for _, msg := range msgs {
//	                analytics.Record(msg)
//	            }
//	            return broker.HandlerActionAck, nil
//	        },
//	        &broker.BatchConfig{Async: true, Size: 100, FlushTimeout: 500 * time.Millisecond},
//	    ),
//	)
//
//	// async: durable pipeline, at-least-once
//	broker.WrapHandler(
//	    broker.ActionHandler(broker.HandlerActionNoAction),
//	    broker.BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *broker.Message]) (broker.HandlerAction, error) {
//	            // process batch; individual errors handled per-message
//	            return broker.HandlerActionAck, nil
//	        },
//	        &broker.BatchConfig{
//	            Async:         true,
//	            EnqueueAction: broker.HandlerActionNoAction, // hold-ack; at-least-once
//	            ErrorAction:   broker.HandlerActionNackRequeue,
//	            Size:          50,
//	            // BufferSize defaults to 10*Size=500; PrefetchCount must be >= BufferSize
//	            FlushTimeout: 200 * time.Millisecond,
//	            OnError: func(ctx context.Context, err error, count int) {
//	                log.Printf("batch of %d failed: %v", count, err)
//	            },
//	        },
//	    ),
//	)
//
// # Message Building
//
// [NewMessage] is the lightweight constructor for simple cases. The returned message
// has sensible defaults: ContentType = "application/octet-stream", DeliveryMode = 2
// (persistent), Timestamp = now (UTC). Fields are set directly:
//
//	msg := broker.NewMessage([]byte("payload"))
//	msg.ContentType = "text/plain"
//	msg.Priority = 3
//	msg.Headers = broker.Arguments{"x-custom": "value"}
//
// [NewMessageBuilder] provides a fluent, validated construction pipeline. Available
// builder methods: Body, BodyString, BodyJSON (marshals + sets ContentType to
// "application/json"), ContentType, DeliveryMode, Priority, Timestamp, Now, Header,
// Headers, CorrelationID, ReplyTo, MessageID, Expiration (TTL as ms string),
// ExpirationDuration (time.Duration convenience), Type, UserID, AppID, Persistent
// (shorthand for DeliveryMode(2)), Transient (shorthand for DeliveryMode(1)), JSON
// (shorthand for ContentType("application/json")), Text (shorthand for
// ContentType("text/plain")), and Build (validates and returns the Message):
//
//	msg, err := broker.NewMessageBuilder().
//	    BodyJSON(payload).
//	    Persistent().
//	    Priority(5).
//	    ExpirationDuration(60 * time.Second).
//	    CorrelationID("req-123").
//	    ReplyTo("rpc.responses").
//	    Header("X-Tenant", "acme").
//	    Build()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// [Message] fields available on both outgoing and incoming messages:
//
//	Body            []byte        // message payload
//	Headers         Arguments     // AMQP message headers (map[string]any)
//	ContentType     string        // MIME type
//	ContentEncoding string        // MIME encoding
//	DeliveryMode    uint8         // 1=transient, 2=persistent
//	Priority        uint8         // 0-9
//	Timestamp       time.Time     // message timestamp
//	CorrelationID   string        // request/reply correlation
//	ReplyTo         string        // reply queue name
//	MessageID       string        // application-defined message ID
//	AppID           string        // application identifier
//	UserID          string        // validated user ID
//	Expiration      string        // TTL in milliseconds as string (e.g. "60000")
//	Type            string        // application-defined message type
//
// [Message] methods available on consumed (incoming) messages:
//
//	msg.Ack() // acknowledge; removes the message from the queue
//	msg.Nack(requeue bool) // negatively acknowledge; requeue or dead-letter
//	msg.Reject() // reject; equivalent to Nack(false)
//	msg.IsConsumed() bool // true if this is an incoming message
//	msg.IsPublished() bool // true if this is an outgoing message
//	msg.IsRedelivered() bool // true if previously delivered but not acked
//	msg.IsReturned() bool // true if returned by the broker (unroutable)
//	msg.DeliveryInfo() // returns (DeliveryInfo, error); error if not consumed
//	msg.ReturnInfo() // returns (ReturnInfo, error); error if not returned
//
// [Message] utility methods:
//
//	msg.Data() any // decode body by ContentType: JSON -> map, text -> string, other -> []byte
//	msg.Copy() Message // deep copy including headers
//
// [DeliveryInfo] fields (populated for consumed messages):
//
//	DeliveryTag  uint64 // broker-assigned sequence number
//	ConsumerTag  string // consumer tag that received this delivery
//	Exchange     string // exchange the message was published to
//	RoutingKey   string // routing key used when publishing
//	Redelivered  bool   // true if previously delivered but not acked
//	MessageCount uint32 // messages remaining in queue (Get operations only)
//
// [ReturnInfo] fields (populated for returned messages):
//
//	ReplyCode   uint16
//	ReplyText   string
//	Exchange    string
//	RoutingKey  string
//
// # Context Integration
//
// The broker is automatically injected into every handler context when using
// [Broker.NewConsumer] or [Broker.Consume]. Retrieve it with [FromContext] to
// implement reply-to or RPC patterns without explicit plumbing:
//
//	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
//	    b := broker.FromContext(ctx)
//	    if b != nil && msg.ReplyTo != "" {
//	        reply := broker.NewMessage([]byte("response"))
//	        _ = b.Publish(ctx, "", msg.ReplyTo, reply)
//	    }
//	    return broker.HandlerActionAck, nil
//	}
//
// The broker holds its own internal context (rooted at the context passed
// to [New], or [context.Background]() by default) that governs the lifetime
// of every background goroutine it owns: connection monitors, lifecycle
// management, delivery pumps, and notification pumps. This context is
// cancelled when Close is called, which tears down all managed resources in
// a single step.
//
// Context propagation follows a strict tree-structured hierarchy. The user
// context is the root; the broker derives a cancellable child from it and
// passes that down to the connection manager and to each endpoint. The
// connection manager uses it for all connection monitor goroutines (one per
// pool slot), each of which watches close and block notifications and handles
// reconnection with exponential backoff. Each endpoint derives its own
// lifecycle context, which governs a lifecycle management goroutine and the
// pump goroutines it spawns on each connect: a delivery pump and cancel pump
// for consumers; a returns pump, flow pump, and (when ConfirmMode is enabled)
// a confirm pump for publishers. Every handler invocation receives a further
// child of the lifecycle context, augmented with the *[Broker] value
// (accessible via [FromContext]). Closing the broker cancels the root, which
// unwinds all layers in one step.
//
// Public methods like [Publisher.Publish], [Consumer.Consume], ... accept a
// separate caller context for a different reason: they represent a specific
// operation or session, not the broker's lifetime. [Consumer.Consume] blocks
// until the caller context is cancelled, giving the caller explicit control
// over when delivery stops. [Publisher.Publish] uses the caller context to
// bound the in-flight operation; if the caller cancels, the publish is
// abandoned without affecting the broker. Internally, both are guarded by a
// merged context that fires if either the broker context or the caller context
// is done, so a broker shutdown also unblocks any in-flight call.
//
// # Error Handling
//
// All operations return structured errors with operation context:
//
//	if err := b.Publish(ctx, "exchange", "key", msg); err != nil {
//	    // match the root to detect any broker error
//	    if errors.Is(err, broker.ErrBroker) {
//	        log.Printf("broker error: %v", err)
//	    }
//	    // match a specific sentinel
//	    if errors.Is(err, broker.ErrBrokerClosed) {
//	        log.Println("broker is closed")
//	    }
//	    if errors.Is(err, broker.ErrPublisherNotConnected) {
//	        log.Println("publisher has no active channel")
//	    }
//	    // extract structured fields
//	    var e *broker.Error
//	    if errors.As(err, &e) {
//	        log.Printf("op=%s err=%v", e.Op, e.Err)
//	    }
//	}
//
// Error hierarchy (every error satisfies errors.Is(err, ErrBroker)):
//
//	ErrBroker
//	|-> ErrBrokerClosed
//	|-> ErrBrokerConfigInvalid
//	|-> ErrTransport
//	|   |-> ErrConnection
//	|       |-> ErrConnectionClosed
//	|       |-> ErrConnectionManager
//	|       |   |-> ErrConnectionManagerClosed
//	|       |-> ErrChannel
//	|           |-> ErrChannelClosed
//	|-> ErrTopology
//	|   |-> ErrTopologyDeclareFailed
//	|   |-> ErrTopologyDeleteFailed
//	|   |-> ErrTopologyVerifyFailed
//	|   |-> ErrTopologyValidation
//	|       |-> ErrTopologyExchangeNameEmpty
//	|       |-> ErrTopologyQueueNameEmpty
//	|       |-> ErrTopologyBindingFieldsEmpty
//	|       |-> ErrTopologyRoutingKeyEmpty
//	|-> ErrEndpoint
//	|   |-> ErrEndpointClosed
//	|   |-> ErrEndpointNotConnected
//	|   |-> ErrEndpointNotReadyTimeout
//	|   |-> ErrEndpointNoAutoReconnect
//	|   |-> ErrPublisher
//	|   |   |-> ErrPublisherClosed
//	|   |   |-> ErrPublisherNotConnected
//	|   |-> ErrConsumer
//	|       |-> ErrConsumerClosed
//	|       |-> ErrConsumerNotConnected
//	|-> ErrMessage
//	|   |-> ErrMessageBuild
//	|   |-> ErrMessageNotConsumed
//	|   |-> ErrMessageNotPublished
//	|-> ErrHandler
//	    |-> ErrMiddleware
//
// The hierarchy is built with [github.com/MarwanAlsoltany/serrors]. Every
// sentinel and every wrapped error is a *[Error] (an alias for *serrors.Error),
// so errors.As(err, &e) always succeeds for any error produced by this package:
// e.Op names the failing operation, e.Err holds the root cause, and e.Data
// carries optional structured context when present.
//
// # Low-level Access
//
// Everything in this section is an escape hatch. The broker already manages
// connections, channels, topology declarations, publishing, and consuming; reaching
// for the raw primitives below is only necessary when direct protocol control is
// unavoidable: custom QoS, raw inspection calls, or features not yet surfaced at
// the higher-level API. Prefer the higher-level API whenever possible.
//
// [Broker.Connection] returns the control [Connection].
// [Broker.Channel] returns a fresh control [Channel]; the caller owns it and must
// close it when done:
//
//	conn, err := b.Connection()
//
//	ch, err := b.Channel()
//	defer ch.Close()
//
// [Connection] wraps *amqp091.Connection: channel creation, lifecycle queries, and
// close/block notification subscriptions.
//
// [Channel] wraps *amqp091.Channel: the full AMQP channel surface (publish, consume,
// QoS, topology declarations, transactions, and notification subscriptions).
//
// Channels acquired this way are not managed: the broker does not monitor them for
// AMQP protocol errors or reconnect them. For channels used internally, the broker
// detects server-side closure mid-operation (e.g. PreconditionFailed, NotFound) and
// returns the operation error combined with the closure cause as a single error;
// callers do not need to drain the close notification channel themselves.
//
// [Broker.Transaction] runs a function inside an AMQP transaction on a fresh control
// channel, committing on nil return and rolling back on error:
//
//	err := b.Transaction(ctx, func(ch broker.Channel) error {
//	    if _, err := ch.QueuePurge("staging-queue", false); err != nil {
//	        return err
//	    }
//	    return ch.QueueBind("staging-queue", "staging.*", "events", false, nil)
//	})
//
// [DefaultDialer] is the built-in dialer (amqp091.Dial / amqp091.DialConfig).
// It is exported for composition (wrap it to add TLS, instrumentation, or retry
// logic) and can be restored after a temporary override. To inject a mock
// connection in tests, use [WithConnectionDialer]:
//
//	b, _ := broker.New(
//	    broker.WithConnectionDialer(func(url string, cfg *broker.Config) (broker.Connection, error) {
//	        return mockConnection, nil
//	    }),
//	)
//
// # Main Types
//
// Core Types:
//   - [Broker]: Central manager for connections, publishers, consumers, and topology.
//   - [Publisher]: Long-lived AMQP publisher with confirms and reconnection.
//   - [Consumer]: Long-lived AMQP consumer with concurrency control and reconnection.
//   - [Endpoint]: Common interface for publishers and consumers (Connection, Channel, Ready, Close, etc.).
//
// Message Types:
//   - [Message]: AMQP message for both publishing and consumption; carries body, headers, and metadata.
//   - [MessageBuilder]: Fluent, validated builder for constructing outgoing messages.
//   - [DeliveryInfo]: Broker-assigned metadata for a consumed delivery (tag, exchange, routing key, etc.).
//   - [ReturnInfo]: Routing details for a returned (unroutable) message (reply code, text, exchange, key).
//
// Topology Types:
//   - [Topology]: DTO grouping exchanges, queues, and bindings for bulk operations.
//   - [Exchange]: AMQP exchange with configuration (type, durable, auto-delete, internal, arguments)
//     and direct lifecycle methods (Declare, Verify, Delete, With*).
//   - [Queue]: AMQP queue with configuration and direct lifecycle methods (Declare, Verify, Delete, Purge, Inspect, With*).
//   - [Binding]: Exchange-to-queue or exchange-to-exchange binding with routing key and arguments.
//   - [BindingType]: Destination type; either [BindingTypeQueue] (default) or [BindingTypeExchange].
//   - [RoutingKey]: String type supporting {placeholder} substitution via [NewRoutingKey].
//
// Handler Types:
//   - [Handler]: func(context.Context, *Message) (HandlerAction, error); the message processing function.
//   - [HandlerAction]: What to do after processing: [HandlerActionAck], [HandlerActionNackRequeue],
//     [HandlerActionNackDiscard], or [HandlerActionNoAction].
//   - [HandlerMiddleware]: func(Handler) Handler; wraps a Handler to add cross-cutting concerns.
//   - [BatchHandler]: func(context.Context, iter.Seq2[int, *Message]) (HandlerAction, error); processes a batch.
//   - [BatchConfig]: Configuration for [BatchMiddleware] (size, flush timeout, async mode, etc.).
//
// Configuration Types:
//   - [Option]: Functional option for configuring [Broker] (With* functions).
//   - [EndpointOptions]: Shared reconnection and readiness options for publishers and consumers.
//   - [PublisherOptions]: Publisher-specific options (confirms, mandatory, returns, flow, callbacks).
//   - [ConsumerOptions]: Consumer-specific options (prefetch, concurrency, auto-ack, callbacks).
//   - [ConnectionManagerOptions]: Pool size, reconnect strategy, dialer, and lifecycle hooks.
//   - [Config]: AMQP connection configuration (heartbeat, locale, SASL, TLS, etc.).
//   - [ConnectionOnOpenHandler]: Callback invoked when a connection is (re-)established.
//   - [ConnectionOnCloseHandler]: Callback invoked when a connection closes.
//   - [ConnectionOnBlockHandler]: Callback invoked when connection flow control activates/deactivates.
//
// Connection Types:
//   - [Connection]: AMQP connection interface (channel factory, lifecycle, notifications).
//   - [Channel]: AMQP channel interface (publish, consume, topology, transactions, notifications).
//   - [Dialer]: func(url string, cfg *Config) (Connection, error); for custom connection creation.
//   - [DefaultDialer]: The built-in dialer backed by amqp091-go; exported for composition.
//   - [Arguments]: AMQP table type (map[string]any); used in exchange, queue, and binding declarations.
//   - [AMQPError]: AMQP protocol error returned by the server (code, reason, server/recover flags).
//
// Error Types:
//   - [Error]: Structured error with Op (operation name) and Err (underlying cause) fields;
//     use errors.As to unwrap operation context from any error returned by this package.
//   - Sentinel errors rooted at [ErrBroker]; see the Error Handling section above.
//
// # Package Organization
//
// The broker package re-exports types from internal/* packages for convenience:
//   - internal/transport: connection and channel abstractions that decouple the broker
//     from the underlying AMQP client; enables mock injection for testing; includes a
//     connection pool manager with automatic reconnection, exponential backoff, and
//     lifecycle hooks
//   - internal/endpoint: publisher and consumer implementations with full lifecycle
//     management: background operation, automatic reconnection, topology auto-declaration,
//     and graceful shutdown; a shared lifecycle interface ties them together
//   - internal/topology: declarative AMQP entities (exchanges, queues, bindings, routing
//     keys) with immutable builder methods and direct server operations; a container type
//     groups entities for bulk broker operations; a stateful registry backs declaration
//     tracking and synchronization
//   - internal/message: unified message representation for both publishing and consumption;
//     includes a fluent builder for validated construction and a plain constructor for
//     simple cases
//   - internal/handler: handler and middleware function types with action constants; with
//     +15 purpose-built middlewares covering common patterns; the middleware system is
//     fully composable and extensible
//   - internal: structured error types
//
// Users only need to import "github.com/MarwanAlsoltany/amqp-broker".
//
// # Thread Safety
//
// All exported types and functions are safe for concurrent use unless explicitly
// documented otherwise. The broker uses mutexes, atomic operations, and channels
// to coordinate access to shared state.
//
// # See Also
//
//   - github.com/rabbitmq/amqp091-go: The underlying AMQP client library
//   - RabbitMQ documentation: https://www.rabbitmq.com/documentation.html
//   - AMQP 0.9.1 specification: https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
package broker
