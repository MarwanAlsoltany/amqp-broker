// Package broker provides a robust, production-grade abstraction layer over
// github.com/rabbitmq/amqp091-go for working with RabbitMQ in Go.
//
// The broker package offers a high-level, opinionated API for AMQP 0.9.1 messaging,
// focusing on reliability, ease of use, and safe concurrency. It manages connections,
// channels, publishers, and consumers with automatic reconnection, resource pooling,
// and declarative topology management.
//
// # Core Features
//
// Connection Management: Transparent connection pooling with configurable pool sizes,
// automatic reconnection with exponential backoff, lifecycle hooks (OnOpen, OnClose,
// OnBlock), and flow control handling. Both transient network blips and prolonged
// infrastructure outages are recovered from automatically, connections and all their
// dependant endpoints (publishers, consumers) resume without any intervention required.
// Publishers, consumers, and control/topology operations each draw from a dedicated
// connection slot, so a busy publisher never starves a consumer or blocks topology work.
//
// Publisher Abstraction: Managed publishers with publisher confirms, one-off publishers
// with automatic pooling and caching, flow control awareness, topology auto-declaration,
// and configurable retry behavior.
//
// Consumer Abstraction: Managed consumers with configurable handler concurrency,
// prefetch settings, graceful shutdown coordination, automatic reconnection and
// re-subscription, topology auto-declaration, and built-in middleware support.
//
// Declarative Topology: Centralized declaration, verification, deletion, and
// synchronization of exchanges, queues, and bindings. Topology is automatically
// reapplied on reconnection and supports declarative state management via Sync().
// Query declared topology with Exchange(), Queue(), and Binding() methods.
//
// Safe Concurrency: All public APIs are safe for concurrent use. Internal registries
// and pools are protected by mutexes and atomic operations. Context-based cancellation
// for all operations.
//
// Handler Middleware: Composable middleware system with 14+ built-in middlewares
// including retry, circuit breaker, rate limiting, timeout, logging, metrics,
// recovery, validation, deduplication, deadline, fallback, concurrency control,
// debug, and transformation.
//
// Extensibility: Flexible configuration via functional options (With* functions),
// custom endpoint options, connection lifecycle hooks, custom dialers for testing,
// and middleware composition.
//
// # Quick Start
//
// Create a broker and start publishing/consuming:
//
//	// Create broker with defaults (amqp://guest:guest@localhost:5672/)
//	b, err := broker.New()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer b.Close()
//
//	// Declare topology
//	topology := broker.NewTopology(
//	    []broker.Exchange{broker.NewExchange("events").WithType("topic").WithDurable(true)},
//	    []broker.Queue{broker.NewQueue("notifications").WithDurable(true)},
//	    []broker.Binding{broker.NewBinding("events", "notifications", "user.*")},
//	)
//	if err := b.Declare(&topology); err != nil {
//	    log.Fatal(err)
//	}
//
//	// One-off publish (uses cached publishers)
//	msg := broker.NewMessage([]byte("Hello, AMQP!"))
//	err = b.Publish(ctx, "events", "user.signup", msg)
//
//	// One-off consume (blocks until context cancelled)
//	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
//	    log.Printf("Received: %s", msg.Body)
//	    return broker.HandlerActionAck, nil
//	}
//	err = b.Consume(ctx, "notifications", handler)
//
// # Managed Endpoints
//
// For long-lived publishers and consumers, use managed endpoints:
//
//	// Create managed publisher with confirms
//	exchange := broker.NewExchange("events")
//	pub, err := b.NewPublisher(
//	    &broker.PublisherOptions{
//	        ConfirmMode:    true,
//	        ConfirmTimeout: 5 * time.Second,
//	    },
//	    exchange,
//	)
//	defer pub.Close()
//
//	// Publish with confirmation
//	msg := broker.NewMessage([]byte("data"))
//	if err := pub.Publish(ctx, broker.RoutingKey("user.login"), msg); err != nil {
//	    log.Printf("Publish failed: %v", err)
//	}
//
//	// Create managed consumer with middleware
//	handler := broker.WrapHandler(
//	    func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
//	        // Process message
//	        return broker.HandlerActionAck, nil
//	    },
//	    broker.RetryMiddleware(&broker.RetryMiddlewareConfig{MaxAttempts: 3}),
//	    broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{}),
//	    broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{}),
//	)
//
//	queue := broker.NewQueue("notifications")
//	con, err := b.NewConsumer(
//	    &broker.ConsumerOptions{
//	        PrefetchCount:         10,
//	        MaxConcurrentHandlers: 5,
//	    },
//	    queue,
//	    handler,
//	)
//	defer con.Close()
//
//	// Block until context cancelled
//	con.Consume(ctx)
//
//	// Release a managed endpoint early (closes it and removes it from the broker registry)
//	// Use this instead of Close() when you want the broker to stop tracking the endpoint.
//	err = b.Release(pub)
//
// # Configuration
//
// Configure broker with functional options:
//
//	b, err := broker.New(
//	    broker.WithURL("amqp://user:pass@rabbitmq:5672/vhost"),
//	    broker.WithIdentifier("my-service"),
//	    broker.WithCache(10 * time.Minute), // Enable publisher pooling
//	    broker.WithConnectionManagerOptions(broker.ConnectionManagerOptions{
//	        Size: 3,
//	        Config: &broker.Config{
//	            Heartbeat: 30 * time.Second,
//	            Locale:    "en_US",
//	        },
//	        OnOpen: func(idx int) {
//	            log.Printf("Connection %d opened", idx)
//	        },
//	        OnClose: func(idx, code int, reason string, server, recover bool) {
//	            log.Printf("Connection %d closed: %s", idx, reason)
//	        },
//	        ReconnectMin: 1 * time.Second,
//	        ReconnectMax: 30 * time.Second,
//	    }),
//	    broker.WithEndpointOptions(broker.EndpointOptions{
//	        ReadyTimeout:    10 * time.Second,
//	        ReconnectMin:    1 * time.Second,
//	        ReconnectMax:    30 * time.Second,
//	        NoAutoReconnect: false,
//	    }),
//	)
//
// # Topology Management
//
// Declarative topology management with sync support:
//
//	// Declare topology (merges with existing)
//	b.Declare(&topology)
//
//	// Verify topology exists with correct configuration
//	b.Verify(&topology)
//
//	// Delete topology entities
//	b.Delete(&topology)
//
//	// Sync to desired state (adds missing, removes extra)
//	b.Sync(&topology)
//
//	// Query declared topology
//	exchange := b.Exchange("events")
//	queue := b.Queue("notifications")
//	binding := b.Binding("events", "notifications", "user.*")
//
// # Handler Middleware
//
// Compose handler behavior with middleware:
//
//	handler := broker.WrapHandler(
//	    baseHandler,
//	    // Retry failed messages up to 3 times
//	    broker.RetryMiddleware(&broker.RetryMiddlewareConfig{
//	        MaxAttempts: 3,
//	        MinBackoff:  1 * time.Second,
//	    }),
//	    // Circuit breaker to prevent cascading failures
//	    broker.CircuitBreakerMiddleware(&broker.CircuitBreakerMiddlewareConfig{
//	        Threshold: 5,
//	        Cooldown:  30 * time.Second,
//	    }),
//	    // Rate limit handler invocations
//	    broker.RateLimitMiddleware(ctx, &broker.RateLimitMiddlewareConfig{
//	        RPS: 100,
//	    }),
//	    // Timeout individual message processing
//	    broker.TimeoutMiddleware(&broker.TimeoutMiddlewareConfig{
//	        Timeout: 5 * time.Second,
//	    }),
//	    // Recover from panics
//	    broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{}),
//	    // Log all messages
//	    broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{}),
//	)
//
// Available middlewares: Batch, Retry, CircuitBreaker, RateLimit, Concurrency, Timeout,
// Deadline, Validation, Deduplication, Fallback, Transform, Recovery, Logging,
// Metrics, Debug.
//
// # Context Integration
//
// Access the broker from handler context for reply-to patterns:
//
//	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
//	    // Broker is automatically injected into context
//	    b := broker.FromContext(ctx)
//	    if b != nil && msg.ReplyTo != "" {
//	        reply := broker.NewMessage([]byte("response"))
//	        b.Publish(ctx, "", msg.ReplyTo, reply)
//	    }
//	    return broker.HandlerActionAck, nil
//	}
//
// # Message Building
//
// Use MessageBuilder for fluent message construction:
//
//	msg, err := broker.NewMessageBuilder().
//	    Body([]byte("data")).
//	    ContentType("application/json").
//	    Persistent().
//	    Priority(5).
//	    Expiration("60000").
//	    CorrelationID("req-123").
//	    ReplyTo("rpc.responses").
//	    Header("X-Custom", "value").
//	    Build()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Error Handling
//
// All operations return structured errors with operation context:
//
//	if err := b.Publish(ctx, "exchange", "key", msg); err != nil {
//	    var e *broker.Error
//	    if errors.As(err, &e) {
//	        log.Printf("Operation: %s, Error: %v", e.Op, e.Err)
//	    }
//	    // Check for specific error types
//	    if errors.Is(err, broker.ErrBrokerClosed) {
//	        // Broker is closed
//	    }
//	    if errors.Is(err, broker.ErrPublisherNotConnected) {
//	        // Publisher lost connection
//	    }
//	}
//
// Error hierarchy:
//   - [ErrBroker] (package root): every error from this package satisfies errors.Is(err, ErrBroker)
//   - [ErrConnection]: transport-level connection errors
//   - [ErrConnectionManager]: connection pool/manager errors
//   - [ErrChannel]: channel-level errors
//   - [ErrTopology]: topology operation errors (declare, delete, verify, validate)
//   - [ErrEndpoint]: endpoint lifecycle errors
//   - [ErrPublisher]: publisher-specific errors
//   - [ErrConsumer]: consumer-specific errors
//   - [ErrMessage]: message build and delivery errors
//   - [ErrMiddleware]: middleware execution errors
//
// Each domain error has additional child sentinels (e.g. [ErrConnectionClosed],
// [ErrEndpointNotReadyTimeout]) discoverable via the package documentation.
//
// # Advanced Features
//
// Transactions (deprecated, use publisher confirms instead):
//
//	err := b.Transaction(ctx, func(ch broker.Channel) error {
//	    // Declare topology in transaction
//	    return topology.Exchange("temp").Declare(ch)
//	})
//
// Low-level access:
//
//	// Get control connection
//	conn, err := b.Connection()
//
//	// Get control channel
//	ch, err := b.Channel()
//	defer ch.Close()
//
// Custom dialer for testing:
//
//	broker, _ := broker.New(
//	    broker.WithConnectionDialer(func(url string, cfg *broker.Config) (broker.Connection, error) {
//	        return mockConnection, nil
//	    }),
//	)
//
// # Main Types
//
// Core Types:
//   - [Broker]: Central manager for connections, publishers, consumers, and topology.
//   - [Publisher]: High-level abstraction for publishing messages with confirmation support.
//   - [Consumer]: High-level abstraction for consuming messages with handler concurrency.
//   - [Endpoint]: Base abstraction for publishers and consumers with reconnection logic.
//
// Message Types:
//   - [Message]: Represents an AMQP message with payload and metadata.
//   - [MessageBuilder]: Fluent builder for constructing messages.
//   - [DeliveryInfo]: Metadata for consumed messages (exchange, routing key, delivery tag, etc.).
//   - [ReturnInfo]: Information about returned (unroutable) messages (reply code, text, exchange, key).
//
// Topology Types:
//   - [Topology]: Container grouping exchanges, queues, and bindings for bulk operations.
//   - [Exchange]: Declarative representation of an AMQP exchange with configuration and lifecycle.
//   - [Queue]: Declarative representation of an AMQP queue with configuration and lifecycle.
//   - [Binding]: Declarative binding from a queue (or exchange) to an exchange with a routing key.
//   - [BindingType]: Whether the binding destination is a queue ([BindingTypeQueue]) or exchange ([BindingTypeExchange]).
//   - [RoutingKey]: Routing key with {placeholder} substitution support.
//
// Handler Types:
//   - [Handler]: Function type for processing a single consumed message.
//   - [HandlerAction]: Action to take after processing (Ack, NackRequeue, NackDiscard, NoAction).
//   - [HandlerMiddleware]: Middleware function that wraps a Handler to add cross-cutting concerns.
//   - [BatchHandler]: Function type for processing a lazy batch of messages via an indexed iterator.
//   - [BatchConfig]: Configuration for BatchMiddleware (size, flush timeout, async mode, etc.).
//
// Configuration Types:
//   - [Option]: Functional option for configuring Broker (URL, identity, cache TTL, hooks, etc.).
//   - [EndpointOptions]: Shared reconnection and readiness options for publishers and consumers.
//   - [PublisherOptions]: Publisher-specific options (confirms, mandatory, returns, flow callbacks).
//   - [ConsumerOptions]: Consumer-specific options (prefetch, concurrency, auto-ack, cancel callback).
//   - [ConnectionManagerOptions]: Connection pool size, reconnect strategy, and lifecycle hooks.
//   - [Config]: AMQP connection configuration (heartbeat, locale, SASL, TLS, etc.).
//   - [ConnectionOnOpenHandler]: Callback invoked when a connection is (re-)established.
//   - [ConnectionOnCloseHandler]: Callback invoked when a connection closes (code, reason, recover flag).
//   - [ConnectionOnBlockHandler]: Callback invoked when connection flow control activates or deactivates.
//
// Connection Types:
//   - [Connection]: AMQP connection interface (channel factory, lifecycle, notifications).
//   - [Channel]: AMQP channel interface (publish, consume, topology declarations, transactions).
//   - [Dialer]: Function type for custom connection creation; enables testing and instrumentation.
//   - [Arguments]: AMQP table type (map[string]any) used in exchange, queue, and binding declarations.
//   - [AMQPError]: AMQP protocol error returned by the server (code, reason, server flag, recover flag).
//
// Error Types:
//   - [Error]: Structured error with Op (operation/object name) and Err (underlying cause) fields;
//     use errors.As to unwrap operation context from any error returned by this package.
//   - Sentinel errors rooted at [ErrBroker], see the Error Handling section above.
//
// # Package Organization
//
// The broker package re-exports types from internal/* packages for convenience:
//   - internal/transport: Connection and channel management
//   - internal/endpoint: Publisher and consumer implementations
//   - internal/topology: Topology declaration and management
//   - internal/message: Message representation and building
//   - internal/handler: Handler types and middleware implementations
//   - internal: Structured error types
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
