// Package broker provides a robust, production-grade abstraction layer over
// github.com/rabbitmq/amqp091-go for working with RabbitMQ in Go.
//
// The broker package offers a high-level, opinionated API for AMQP 0.9.1 messaging,
// focusing on reliability, ease of use, and safe concurrency. It manages connections,
// channels, publishers, and consumers with automatic reconnection, resource pooling,
// and declarative topology management.
//
// Core Features:
//   - Connection Management: Transparent connection pooling, automatic reconnection,
//     and lifecycle hooks for robust operation in distributed systems.
//   - Publisher Abstraction: Managed and one-off publishers with support for publisher
//     confirms, batching, flow control, and error handling.
//   - Consumer Abstraction: Managed and one-off consumers with configurable concurrency,
//     graceful shutdown, and handler lifecycle management.
//   - Declarative Topology: Centralized declaration, verification, deletion, and
//     synchronization of exchanges, queues, and bindings. Topology is reapplied on
//     reconnection and supports declarative state management.
//   - Safe Concurrency: All public APIs are safe for concurrent use. Internal registries
//     and pools are protected by mutexes and atomic operations.
//   - Extensibility: Flexible configuration via functional options, custom endpoint
//     options, and event hooks for connection and endpoint lifecycle events.
//
// Usage Overview:
//   - Use New(...) to construct a Broker with custom options (URL, connection pool size, etc ...).
//   - Use Broker.NewPublisher and Broker.NewConsumer for long-lived, managed endpoints.
//   - Use Broker.Publish and Broker.Consume for one-off operations with automatic resource management.
//   - Use Broker.Declare, Broker.Delete, Broker.Verify, and Broker.Sync to manage AMQP topology declaratively.
//   - Use Broker.Close to gracefully shut down all managed resources.
//
// See the documentation for each type for detailed usage patterns and configuration options.
//
// Main Types:
//   - Broker: Central manager for connections, publishers, consumers, and topology.
//   - Publisher: High-level abstraction for publishing messages with confirmation support.
//   - Consumer: High-level abstraction for consuming messages with handler concurrency.
//   - Message: Represents an AMQP message with payload and metadata.
//   - Exchange, Queue, Binding: Declarative representations of AMQP topology entities.
//
// Error Handling:
//   - All operations return rich, wrapped errors for diagnostics.
//
// For more information, check the dedicated documentation for each type and function for more details.
package broker
