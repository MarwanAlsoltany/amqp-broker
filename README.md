<h1 align="center">AMQP Broker</h1>

<p align="center">
  <img src="./assets/logo.png" alt="AMQP Broker" width="192" />
</p>

<p align="center">
  <strong>A proper Go API for RabbitMQ.</strong>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/MarwanAlsoltany/amqp-broker">
    <img src="https://pkg.go.dev/badge/github.com/MarwanAlsoltany/amqp-broker.svg" alt="Go Reference" />
  </a>
  &nbsp;
  <a href="https://goreportcard.com/report/github.com/MarwanAlsoltany/amqp-broker">
    <img src="https://goreportcard.com/badge/github.com/MarwanAlsoltany/amqp-broker" alt="Go Report Card" />
  </a>
  &nbsp;
  <a href="https://github.com/MarwanAlsoltany/amqp-broker/actions/workflows/ci.yml">
    <img src="https://github.com/MarwanAlsoltany/amqp-broker/actions/workflows/ci.yml/badge.svg?branch=master" alt="CI" />
  </a>
  &nbsp;
  <a href="https://codecov.io/gh/MarwanAlsoltany/amqp-broker">
    <img src="https://codecov.io/gh/MarwanAlsoltany/amqp-broker/branch/master/graph/badge.svg" alt="Coverage" />
  </a>
  &nbsp;
  <a href="https://github.com/MarwanAlsoltany/amqp-broker/releases/latest">
    <img src="https://img.shields.io/github/v/release/MarwanAlsoltany/amqp-broker?label=version&color=blue" alt="Latest Release" />
  </a>
  &nbsp;
  <a href="https://github.com/MarwanAlsoltany/amqp-broker/blob/master/LICENSE">
    <img src="https://img.shields.io/github/license/MarwanAlsoltany/amqp-broker" alt="License" />
  </a>
</p>

---

AMQP Broker is a library for AMQP 0.9.1. It models the broker domain as first-class entities (exchanges, queues, bindings, publishers, consumers, messages, and handlers) so you declare intent, not protocol steps. Connection pooling, reconnection on failure, topology (re)application, and acknowledgment lifecycle are all handled for you.

It is a wrapper around the official [`amqp091-go`](https://github.com/rabbitmq/amqp091-go) client, with a focus on reliability, ease of use, and extensibility. The library is opinionated by design. The goal is to eliminate the cognitive overhead of wiring up publishers and consumers from scratch on every module or service: connect, setup topology, hand off a handler, and let the library own the rest. Reliability and fire-and-forget operation are first-class concerns, not afterthoughts.

## Core Features

- **Connection Management**: Configurable connection pool with dedicated slots for publishers, consumers, and topology operations, preventing resource starvation. Automatic reconnection (for both network and infrastructure failures) with exponential backoff, flow control handling, and open/close/block lifecycle hooks.
- **Declarative Topology**: Ability to declare, verify, delete, and sync exchanges, queues, and bindings as a unit. Topology is automatically reapplied on reconnection and the declared state is queryable by name.
- **Publisher Abstraction**: Managed publishers with publisher confirms, deferred confirmation callbacks, returned-message and flow-control event hooks, one-off publishing with automatic caching and pooling, and topology auto-declaration on connect and reconnect.
- **Consumer Abstraction**: Managed consumers with configurable concurrency (unlimited, sequential, or a fixed-size worker pool), QoS prefetch control, graceful shutdown coordination, automatic reconnection with re-subscription, and topology auto-declaration on connect and reconnect.
- **Handler Middleware**: Composable middleware pipeline with +15 built-in behaviors: structured logging, metrics collection, debug tracing, panic recovery, fallback, retry with backoff, circuit breaker, concurrency control, rate limiting, deduplication, validation, transformation, deadline enforcement, timeout, and batching.
- **Message Building**: A single message type spans both sides of the wire, outgoing and incoming messages share the same structure. Two construction styles for outgoing messages: a fluent builder for validated multi-step construction, and a plain constructor for simple cases with direct field access.
- **Context Integration**: The broker is automatically injected into every handler context, enabling reply-to and RPC patterns without any explicit plumbing. All background operations are governed by a root context that is cancelled on shutdown, and all public methods accept a caller context for operation-level control.
- **Structured Errors**: Layered error hierarchy with a single root sentinel; every error from the package is matchable by subsystem (transport, topology, endpoint, message, handler) or by specific operation for fine-grained handling.
- **Safe Concurrency**: All public APIs are safe for concurrent use without any external synchronization, protected by mutexes and atomic operations with context-based cancellation throughout.
- **Extensibility**: Functional options for all configuration, custom dialer injection for test environments, great defaults for 95% of use cases with full overriding possibility when needed, fully composable middleware chains, and a clean public API surface with no internal implementation details.

## Installation

```bash
go get github.com/MarwanAlsoltany/amqp-broker
```

**Requirements:**

- Go 1.25 or later
- RabbitMQ 3.8+ (or any AMQP 0.9.1 compatible broker)

---

## Getting Started

The snippet below shows the complete basic path: connect, declare topology, publish, and consume.

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"

    broker "github.com/MarwanAlsoltany/amqp-broker"
)

func main() {
    b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
    if err != nil {
        log.Fatal(err)
    }
    defer b.Close()

    // declare topology once; reapplied automatically on reconnection
    t := broker.NewTopology(
        []broker.Exchange{broker.NewExchange("events").WithType("topic").WithDurable(true)},
        []broker.Queue{broker.NewQueue("notifications").WithDurable(true)},
        []broker.Binding{broker.NewBinding("events", "notifications", "user.*")},
    )
    if err := b.Declare(&t); err != nil {
        log.Fatal(err)
    }

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    // one-off publish (uses a cached publisher internally)
    msg := broker.NewMessage([]byte("Hello, AMQP!"))
    if err := b.Publish(ctx, "events", "user.signup", msg); err != nil {
        log.Fatal(err)
    }

    // one-off consume; blocks until ctx is cancelled
    if err := b.Consume(ctx, "notifications", func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
        log.Printf("received: %s", msg.Body)
        return broker.HandlerActionAck, nil
    }); err != nil {
        log.Fatal(err)
    }
}
```

`Publish` and `Consume` are convenience methods for simple or low-volume cases. For long-lived endpoints with confirms, middleware pipelines, or explicit lifecycle control, use `NewPublisher` and `NewConsumer` below.

### Managed Publisher

```go
ctx := context.Background()

b, err := broker.New()
if err != nil {
    log.Fatal(err)
}
defer b.Close()

pub, err := b.NewPublisher(
    &broker.PublisherOptions{
        ConfirmMode:    true,
        ConfirmTimeout: 5 * time.Second,
    },
    broker.NewExchange("events"),
)
if err != nil {
    log.Fatal(err)
}
defer pub.Close()

msg := broker.NewMessage([]byte("important event"))
if err := pub.Publish(ctx, broker.RoutingKey("user.created"), msg); err != nil {
    log.Fatal(err)
}
```

### Managed Consumer

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
defer stop()

b, err := broker.New()
if err != nil {
    log.Fatal(err)
}
defer b.Close()

// wrap handler with middlewares, or pass it directly
handler := broker.WrapHandler(
    func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
        log.Printf("processing: %s", msg.MessageID)
        return broker.HandlerActionAck, nil
    },
    broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{}),
    broker.RetryMiddleware(&broker.RetryMiddlewareConfig{MaxAttempts: 3}),
    broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{}),
)

con, err := b.NewConsumer(
    &broker.ConsumerOptions{
        PrefetchCount:         10,
        MaxConcurrentHandlers: 5,
    },
    broker.NewQueue("notifications"),
    handler,
)
if err != nil {
    log.Fatal(err)
}
defer con.Close()

con.Consume(ctx) // blocks until ctx is cancelled
// for manual control, use: go con.Consume(ctx) + con.Wait()
```

### Configuration

```go
b, err := broker.New(
    broker.WithURL("amqp://username:password@host:port/vhost"),
    broker.WithIdentifier("my-service"),
    broker.WithContext(ctx),

    // publisher pooling for one-off Publish calls; 0 disables caching
    broker.WithCache(5 * time.Minute),

    // 1 = shared, 2 = control+publisher/consumer isolated, 3+ = dedicated per role
    broker.WithConnectionPoolSize(3),

    broker.WithConnectionManagerOptions(broker.ConnectionManagerOptions{
        Size: 3, // same as WithConnectionPoolSize(3)
        Config: &broker.Config{
            Heartbeat: 30 * time.Second,
            Locale:    "en_US",
        },
        ReconnectMin: 500 * time.Millisecond,
        ReconnectMax: 30 * time.Second,
        OnOpen: func(idx int) {
            log.Printf("connection %d opened", idx)
        },
        OnClose: func(idx, code int, reason string, server, recover bool) {
            log.Printf("connection %d closed: %s (code=%d)", idx, reason, code)
        },
        OnBlock: func(idx int, active bool, reason string) {
            log.Printf("connection %d flow control active=%v: %s", idx, active, reason)
        },
    }),

    broker.WithEndpointOptions(broker.EndpointOptions{
        ReconnectMin: 1 * time.Second,
        ReconnectMax: 30 * time.Second,
        ReadyTimeout: 10 * time.Second,
    }),

    // inject a dialer in tests to return a mock connection, without a real RabbitMQ server
    broker.WithConnectionDialer(myDialer),
)
```

#### Publisher Options

```go
opts := &broker.PublisherOptions{
    EndpointOptions: broker.EndpointOptions{
        NoAutoDeclare:   false,
        NoAutoReconnect: false,
        NoWaitReady:     false,
        ReadyTimeout:    10 * time.Second,
    },
    ConfirmMode:    true,
    ConfirmTimeout: 5 * time.Second,
    Mandatory:      false,
    Immediate:      false, // deprecated for RabbitMQ 3.0+
    // OnConfirm is called per published message when ConfirmMode is true;
    // providing this callback enables deferred confirmation mode: Publish
    // returns immediately and the callback fires once the broker acks or nacks
    OnConfirm: func(deliveryTag uint64, wait func(context.Context) bool) {
        confirmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
        defer cancel()
        if wait(confirmCtx) {
            log.Printf("message %d confirmed", deliveryTag)
        } else {
            log.Printf("message %d not confirmed", deliveryTag)
        }
    },
    // OnReturn is called when a mandatory message cannot be routed
    OnReturn: func(msg broker.Message) {
        log.Printf("returned: %s", msg.Body)
    },
    // OnFlow is called when the broker activates or deactivates flow control
    OnFlow: func(active bool) {
        log.Printf("flow control: %v", active)
    },
    // OnError is called for background errors (confirmation loss, reconnect failures)
    OnError: func(err error) {
        log.Printf("publisher error: %v", err)
    },
}
```

#### Consumer Options

```go
opts := &broker.ConsumerOptions{
    EndpointOptions: broker.EndpointOptions{
        NoAutoDeclare:   false,
        NoAutoReconnect: false,
        NoWaitReady:     false,
        ReadyTimeout:    10 * time.Second,
    },
    AutoAck:       false,
    PrefetchCount: 10,
    Exclusive:     false,
    NoWait:        false,
    // 0: default (capped by PrefetchCount); -1: unlimited; 1: sequential; N: worker pool
    MaxConcurrentHandlers: 5,
    // OnCancel is called when the server cancels this consumer
    OnCancel: func(consumerTag string) {
        log.Printf("consumer cancelled: %s", consumerTag)
    },
    // OnError is called for background errors (reconnect failures, delivery errors)
    OnError: func(err error) {
        log.Printf("consumer error: %v", err)
    },
}
```

#### Endpoint Options

`EndpointOptions` is embedded in both `PublisherOptions` and `ConsumerOptions`. It can also be set as a broker-wide default via `WithEndpointOptions`.

```go
opts := broker.EndpointOptions{
    NoWaitReady:     false, // return immediately instead of waiting for readiness (i.e. lazy connect)
    NoAutoDeclare:   false, // skip topology (re)declaration on connect/reconnect
    NoAutoReconnect: false, // treat connection loss as a terminal error
    ReconnectMin:    500 * time.Millisecond,
    ReconnectMax:    30 * time.Second,
    ReadyTimeout:    10 * time.Second,
}
```

### Topology Management

```go
t := broker.Topology{
    Exchanges: []broker.Exchange{{Name: "events", Type: "topic", Durable: true}},
    Queues:    []broker.Queue{{Name: "events.orders", Durable: true}},
    Bindings:  []broker.Binding{{Source: "events", Destination: "events.orders", Key: "orders.*"}},
}

// Declare: merges into the registry and declares on the server;
// reapplied automatically on reconnection
b.Declare(&t)

// Verify: passive-declares all entities; returns an error if any are missing
b.Verify(&t)

// Delete: removes entities from the server and the registry
b.Delete(&t)

// Sync: enforces exact desired state (declares missing, deletes extra);
// aware only of topology declared on this Broker, not the server's full state
b.Sync(&t)
```

Query the declared registry by name:

```go
exchange := b.Exchange("events")
queue    := b.Queue("events.orders")
binding  := b.Binding("events", "events.orders", "orders.*")
```

Exchange-to-exchange bindings via `BindingType`:

```go
broker.Binding{
    Type: broker.BindingTypeExchange,
    Source: "source",
    Destination: "dest-exchange",
    Key: "key",
}
```

`RoutingKey` supports placeholder substitution:

```go
rk := broker.NewRoutingKey("orders.{region}.{action}", map[string]string{
    "region": "us-east",
    "action": "created",
})
// resolves to "orders.us-east.created"
```

### Handler Middleware

`WrapHandler` composes a base handler with middlewares applied left-to-right (first = outermost).

```go
handler := broker.WrapHandler(
    baseHandler,
    broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{}), // outermost
    broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{}),
    broker.TimeoutMiddleware(&broker.TimeoutMiddlewareConfig{Timeout: 30 * time.Second}),
)
```

`ActionHandler(action)` creates a handler that always returns a fixed action; required as the base for `BatchMiddleware`.

**Handler actions:**

| Constant | Behavior |
| --- | --- |
| `HandlerActionAck` | Acknowledges; removes from the queue |
| `HandlerActionNackRequeue` | Negatively acknowledges and requeues |
| `HandlerActionNackDiscard` | Negatively acknowledges without requeuing; routes to dead-letter if configured |
| `HandlerActionNoAction` | No acknowledgment; message stays unacked until manually handled or consumer closes |

**Available middlewares:**

| Middleware | Type | Description |
| --- | --- | --- |
| `LoggingMiddleware` | pre+post | Structured `slog`-based lifecycle logging |
| `MetricsMiddleware` | post | Records duration; invokes a custom `Record` callback |
| `DebugMiddleware` | pre | Logs full message payload; for development only |
| `RecoveryMiddleware` | post | Catches panics and converts them to errors |
| `FallbackMiddleware` | post | Invokes an alternative handler when the primary fails |
| `RetryMiddleware` | post | Exponential backoff retry with jitter and a configurable `ShouldRetry` predicate |
| `CircuitBreakerMiddleware` | pre+post | Opens after N consecutive failures; half-open probing with cooldown |
| `ConcurrencyMiddleware` | pre+post | Semaphore-based limit on concurrent handler goroutines |
| `RateLimitMiddleware` | pre | Token-bucket rate limiting (`RPS` or explicit `Burst`/`RefillRate`) |
| `DeduplicationMiddleware` | pre | Skips duplicates via a pluggable `Cache` and `Identify` function |
| `ValidationMiddleware` | pre | Rejects messages failing a user-supplied validate function |
| `TransformMiddleware` | pre | Rewrites the message body before the handler runs |
| `DeadlineMiddleware` | pre | Discards messages whose deadline header has already passed |
| `TimeoutMiddleware` | pre+post | Cancels handler context after a fixed duration |
| `BatchMiddleware` | terminal | Accumulates messages into batches; sync (blocking) or async (background) |

### Message Building

```go
// fluent builder: validated, all properties configurable
msg, err := broker.NewMessageBuilder().
    BodyJSON(payload).
    Persistent().
    Priority(5).
    ExpirationDuration(60 * time.Second).
    CorrelationID("req-123").
    ReplyTo("rpc.responses").
    Header("X-Tenant", "acme").
    Build()

// plain constructor: defaults applied, fields set directly
msg := broker.NewMessage([]byte("payload"))
msg.ContentType = "text/plain"
msg.Priority = 3
```

### Context Integration

The broker is automatically injected into every handler context. Retrieve it with `FromContext` for reply-to or RPC patterns.

```go
handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
    b := broker.FromContext(ctx)
    if b != nil && msg.ReplyTo != "" {
        _ = b.Publish(ctx, "", msg.ReplyTo, broker.NewMessage([]byte("response")))
    }
    return broker.HandlerActionAck, nil
}
```

The broker holds its own internal context (rooted at the context passed to `New`, or `context.Background()` by default) that governs the lifetime of every background goroutine it owns: reconnect loops, connection monitors, delivery pumps, and worker pools. This context is cancelled when `Close` is called, which tears down all managed resources in a single step.

Public methods like `Publisher.Publish` and `Consumer.Consume` accept a separate caller context for a different reason: they represent a specific operation or session, not the broker's lifetime. `Consume` blocks until the caller context is cancelled, giving the caller explicit control over when delivery stops. `Publish` uses the caller context to bound the in-flight operation, if the caller cancels, the publish is abandoned without affecting the broker. Internally, both are guarded by a merged context that fires if either the broker context or the caller context is done, so a broker shutdown also unblocks any in-flight call.

Context propagation follows a strict tree-structured hierarchy:

```txt
user context   [passed to New(), or context.Background() by default]
   │   the broker cancels this entire subtree when Close() is called
   │
   │
   ├── connection manager context   [one per broker; all connection slots share it]
   │      │
   │      └── connection monitor goroutine   [one per connection in the pool]
   │              watches close/block notifications for that connection;
   │              handles reconnection internally with exponential backoff
   │
   │
   └── endpoint lifecycle context   [one per publisher/consumer]
          │
          ├── lifecycle management goroutine
          │       drives the connect -> ready -> monitor -> reconnect loop;
          │       spawns pump goroutines on each successful connect
          │
          ├── confirm pump goroutine   [publisher only, when ConfirmMode is enabled]
          │       reads broker ack/nack notifications; resolves pending confirmations
          │
          ├── returns pump goroutine   [publisher only]
          │       receives mandatory messages returned by the server as unroutable
          │
          ├── flow pump goroutine   [publisher only]
          │       watches broker flow control notifications
          │
          ├── delivery pump goroutine   [consumer only]
          │       reads deliveries from the AMQP channel and dispatches to handlers
          │
          ├── cancel pump goroutine   [consumer only]
          │       watches for server-initiated consumer cancellation
          │
          └── handler context   [one per message delivery]
                  carries *Broker, retrievable via FromContext
                     │
                     └── OPTIONAL: merged publish context
                             created when Publish() is called from within a handler;
                             cancelled if either the broker or the caller context is done
```

---

## Architecture

The package is layered into a public top-level package that re-exports types from internal sub-packages.

- **`broker` (top-level package)**
  - Central coordinator and entry point for all broker operations; re-exports all public facing internal types as aliases so a single import suffices.
- **`internal/transport`**
  - Connection and channel abstractions that decouple the broker from the underlying AMQP client; enables mock injection for testing. Includes a connection pool manager with automatic reconnection, exponential backoff, and lifecycle hooks.
- **`internal/endpoint`**
  - Publisher and consumer implementations with full lifecycle management: background operation, automatic reconnection, topology auto-declaration, and graceful shutdown. A shared lifecycle interface ties them together.
- **`internal/topology`**
  - Declarative AMQP entities (exchanges, queues, bindings, routing keys) with immutable builder methods and direct server operations. A container type groups entities for bulk broker operations; a stateful registry backs declaration tracking and synchronization.
- **`internal/message`**
  - Unified message representation for both publishing and consumption. Includes a fluent builder for validated construction and a plain constructor for simple cases.
- **`internal/handler`**
  - Handler and middleware function types with action constants. With +15 purpose-built middlewares covering common patterns. The middleware system is fully composable and extensible.

---

## Error Handling

All errors carry operation context and are matchable via `errors.Is` and `errors.As`.

```go
if err := b.Publish(ctx, "events", "user.created", msg); err != nil {
    // match the root to detect any broker error
    if errors.Is(err, broker.ErrBroker) {
        log.Printf("broker error: %v", err)
    }

    // match a specific sentinel
    if errors.Is(err, broker.ErrBrokerClosed) {
        log.Println("broker is closed")
    }
    if errors.Is(err, broker.ErrPublisherNotConnected) {
        log.Println("publisher has no active channel")
    }

    // extract structured fields
    var e *broker.Error
    if errors.As(err, &e) {
        log.Printf("op=%s err=%v", e.Op, e.Err)
    }
}
```

### Error Hierarchy

Every error from this package satisfies `errors.Is(err, ErrBroker)`. Sentinels are organized by subsystem:

```txt
ErrBroker
   ├── ErrBrokerClosed
   ├── ErrBrokerConfigInvalid
   ├── ErrTransport
   │      └── ErrConnection
   │             ├── ErrConnectionClosed
   │             ├── ErrConnectionManager
   │             │      └── ErrConnectionManagerClosed
   │             └── ErrChannel
   │                    └── ErrChannelClosed
   ├── ErrTopology
   │    ├── ErrTopologyDeclareFailed
   │    ├── ErrTopologyDeleteFailed
   │    ├── ErrTopologyVerifyFailed
   │    └── ErrTopologyValidation
   │           ├── ErrTopologyExchangeNameEmpty
   │           ├── ErrTopologyQueueNameEmpty
   │           ├── ErrTopologyBindingFieldsEmpty
   │           └── ErrTopologyRoutingKeyEmpty
   ├── ErrEndpoint
   │    ├── ErrEndpointClosed
   │    ├── ErrEndpointNotConnected
   │    ├── ErrEndpointNotReadyTimeout
   │    ├── ErrEndpointNoAutoReconnect
   │    ├── ErrPublisher
   │    │      ├── ErrPublisherClosed
   │    │      └── ErrPublisherNotConnected
   │    └── ErrConsumer
   │           ├── ErrConsumerClosed
   │           └── ErrConsumerNotConnected
   ├── ErrMessage
   │      ├── ErrMessageBuild
   │      ├── ErrMessageNotConsumed
   │      └── ErrMessageNotPublished
   └── ErrHandler
          └── ErrMiddleware
```

The hierarchy is built with [`github.com/MarwanAlsoltany/serrors`](https://github.com/MarwanAlsoltany/serrors). Every sentinel and every wrapped error is a `*broker.Error` (an alias for `*serrors.Error`), so `errors.As(err, &e)` always succeeds for any error produced by this package: `e.Op` names the failing operation, `e.Err` holds the root cause, and `e.Data` carries optional structured context when present.

This allows for both broad and fine-grained error handling strategies, depending on the application's needs.

---

## Testing

```bash
# unit tests
make test

# integration tests (requires Docker for RabbitMQ via testcontainers)
make test-integration
```

Test files follow the file they test, each file will have a corresponding `*_test.go` file and a `*_integration_test.go` file for integration tests (if applicable).

---

## Background

This library was born out of building a distributed system with heavy, real-time messaging requirements, highly dynamic configuration, complex routing patterns, multi-tenant topology, and a message volume in the tens of millions, and expected only to grow from there.

At that scale, the same friction points appear in every service. Connection management and pooling. Reconnection logic that correctly handles both transient network blips and prolonged infrastructure failures. Topology setup, re-application after reconnect, and keeping both ends in sync. Publisher and consumer lifecycle management. Correct ack, nack, and confirmation handling. And the constant small pieces of glue code that get rewritten from scratch for each service.

None of these are *novel* problems, but they are **tedious to get right and expensive to get wrong**. `amqp-broker` is the extraction of the layer that handled all of that in production, a battle-tested foundation that any Go service communicating over RabbitMQ can build on.

## Attribution

This project was created as part of the development of synQup NextGen at [www.synqup.com](https://www.synqup.com) (up to version `v0.1.0`) and is being released as open source with the company's approval.

It is not an official synQup product. All trademarks, product names, and company references remain the property of their respective owners.

The code is provided as is, without warranty.

## License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.

### Third-Party Notices

Third-party copyright notices and license terms are listed in the [NOTICE](NOTICE) file.
