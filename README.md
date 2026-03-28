# AMQP Broker

A robust, production-grade Go library providing a high-level abstraction over AMQP 0.9.1 (RabbitMQ) for reliable message broker operations.

## Features

- **Connection Management**: Automatic connection pooling, transparent reconnection, and lifecycle hooks.
- **Publisher Support**: Managed and one-off publishers with publisher confirms, flow control, and error handling.
- **Consumer Support**: Managed and one-off consumers with configurable concurrency and graceful shutdown.
- **Declarative Topology**: Centralized management of exchanges, queues, and bindings with automatic reapplication on reconnection.
- **Safe Concurrency**: All public APIs are thread-safe with mutex-protected internal state.
- **Extensibility**: Flexible configuration via functional options and event hooks.
- **Testing**: 90%+ test coverage with comprehensive unit and integration tests.

## Installation

```bash
go get github.com/MarwanAlsoltany/amqp-broker
```

**Requirements:**

- Go 1.25 or later
- RabbitMQ 3.8+ (or any AMQP 0.9.1 compatible broker)

## Quick Start

### Basic Publishing

```go
package main

import (
    "context"
    "log"

    "github.com/MarwanAlsoltany/amqp-broker"
)

func main() {
    ctx := context.Background()

    // create broker
    b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
    if err != nil {
        log.Fatal(err)
    }
    defer b.Close()

    // publish a message
    msg := broker.NewMessage([]byte("Hello, World!"))
    err = b.Publish(ctx, "my-exchange", "routing.key", msg)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Basic Consuming

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"

    "github.com/MarwanAlsoltany/amqp-broker"
)

func main() {
    ctx := context.Background()

    b, err := broker.New(broker.WithURL("amqp://localhost"))
    if err != nil {
        log.Fatal(err)
    }
    defer b.Close()

    // define message handler
    handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
        log.Printf("received: %s", msg.Body)
        return broker.ActionAck, nil
    }

    // create consumer
    consumer, err := b.NewConsumer(nil, broker.Queue{Name: "my-queue"}, handler)
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    // start consuming in background
    go consumer.Consume(ctx)

    // wait for interrupt signal
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt)
    <-sigCh
}
```

### Declaring Topology

```go
// declare exchanges, queues, and bindings
topology := &broker.Topology{
    Exchanges: []broker.Exchange{
        {Name: "events", Type: "topic", Durable: true},
        {Name: "logs", Type: "fanout", Durable: true},
    },
    Queues: []broker.Queue{
        {Name: "event-processor", Durable: true, Arguments: broker.Arguments{
            "x-message-ttl": 86400000, // 24 hours
        }},
    },
    Bindings: []broker.Binding{
        {Source: "events", Destination: "event-processor", Key: "user.*"},
    },
}

err := b.Declare(topology)
if err != nil {
    log.Fatal(err)
}
```

### Publisher with Confirmations

```go
ctx := context.Background()

// create managed publisher with confirmation support
publisherOpts := &broker.PublisherOptions{
    ConfirmMode: true,
    OnConfirm: func(deliveryTag uint64, wait func(context.Context) bool) {
        // deferred confirmation handling
        go func() {
            confirmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            defer cancel()

            if wait(confirmCtx) {
                log.Printf("message %d confirmed", deliveryTag)
            } else {
                log.Printf("message %d not confirmed", deliveryTag)
            }
        }()
    },
    Endpoint: broker.EndpointOptions{
        NoAutoDeclare:   false,
        NoAutoReconnect: false,
    },
}

publisher, err := b.NewPublisher(publisherOpts, broker.NewExchange("events"))
if err != nil {
    log.Fatal(err)
}
defer publisher.Close()

// publish with confirmation
msg := broker.NewMessage([]byte("important event"))
err = publisher.Publish(ctx, "user.created", msg)
if err != nil {
    log.Fatal(err)
}
```

## Configuration

### Broker Options

```go
broker.New(
    broker.WithURL("amqp://user:pass@localhost:5672/vhost"),
    broker.WithIdentifier("my-service"),
    broker.WithContext(ctx),
    broker.WithConnectionPoolSize(3),
    broker.WithConnectionManagerOptions(broker.ConnectionManagerOptions{
        NoAutoReconnect: false,
        ReconnectMin:    500 * time.Millisecond,
        ReconnectMax:    30 * time.Second,
    }),
    broker.WithEndpointOptions(broker.EndpointOptions{
        NoAutoDeclare:   false,
        NoAutoReconnect: false,
        NoWaitReady:     false,
    }),
    broker.WithCacheTTL(5 * time.Minute),
)
```

### Publisher Options

```go
opts := &broker.PublisherOptions{
    ConfirmMode:    true,
    ConfirmTimeout: 5 * time.Second,
    Mandatory:      false,
    Immediate:      false,
    OnConfirm: func(deliveryTag uint64, wait func(context.Context) bool) {
        // handle confirmation
    },
    OnReturn: func(msg broker.Message) {
        // handle returned message
    },
    OnError: func(err error) {
        // handle errors
    },
}
```

### Consumer Options

```go
opts := &broker.ConsumerOptions{
    AutoAck:               false,
    PrefetchCount:         10,
    Exclusive:             false,
    MaxConcurrentHandlers: 5,
    OnCancel: func(consumerTag string) {
        // handle cancellation
    },
    OnError: func(err error) {
        // handle errors
    },
}
```

## Message Building

The `MessageBuilder` provides a fluent API for message construction:

```go
msg, err := broker.NewMessageBuilder().
    BodyJSON(map[string]interface{}{"user": "john", "action": "login"}).
    Priority(5).
    Persistent().
    Header("x-request-id", "abc-123").
    CorrelationID("cid-xyz").
    Build()
```

Or use the simpler `NewMessage` function with direct field access:

```go
msg := broker.NewMessage([]byte("payload"))
msg.ContentType = "application/json"
msg.Priority = 5
msg.Headers = broker.Arguments{"x-custom": "value"}
```

## Architecture

The package is organized around these core components:

- **`Broker`**: Central manager coordinating connections, publishers, consumers, and topology. Entry point for all operations.
- **`Publisher`**: Abstraction for publishing messages to exchanges with optional confirmations and flow control.
- **`Consumer`**: Abstraction for consuming messages from queues with configurable concurrency and handler lifecycle.
- **`Message`**: Represents AMQP messages for both publishing and consumption, with metadata and convenience methods.
- **`Exchange`, `Queue`, `Binding`**: Declarative representations of AMQP topology entities.
- **`Topology`**: Container for topology declarations with atomic apply/verify/delete operations.

Connection pooling, reconnection logic, and lifecycle management are handled internally with safe concurrency primitives.

## Error Handling

All errors are wrapped with operation context for better diagnostics:

```go
if err != nil {
    var brokerErr *broker.Error
    if errors.As(err, &brokerErr) {
        log.Printf("operation: %s, error: %v", brokerErr.Operation, brokerErr.Err)
    }
}
```

## Testing

```bash
# unit tests:
make test
# integration tests (requires RabbitMQ via testcontainers):
make test-integration
```

Review test files for comprehensive usage examples:

- [`broker_test.go`](broker_test.go): broker operations
- [`publisher_test.go`](publisher_test.go): publishing patterns
- [`consumer_test.go`](consumer_test.go): consuming patterns
- [`topology_test.go`](topology_test.go): topology management
- `*_integration_test.go`: end-to-end scenarios

## Development Status

**v0.0.1**: Initial baseline release with complete feature set and 90%+ test coverage.

Future versions will include package restructuring for improved API organization and testability. Contributions welcome after the restructuring phase.

## Attribution

This project was created as part of the development of synQup NextGen at [www.synqup.com](https://www.synqup.com) and is being released as open source with the company's approval.

It is not an official synQup product. All trademarks, product names, and company references remain the property of their respective owners.

The code is provided as is, without warranty.

## License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.
