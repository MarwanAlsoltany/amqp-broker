// Package broker is a robust, production-grade abstraction layer over
// github.com/rabbitmq/amqp091-go for working with RabbitMQ in Go.
// It provides a consistent, high-level API for publishing and consuming messages,
// with built-in connection management, auto-reconnect, and support for publisher confirms.
//
// # Connection Architecture
//
// The broker maintains a fixed set of long-lived AMQP connections based on the configured
// connection pool size. Connections are strategically assigned to different roles:
//
//   - Pool size 1: All operations (publishers, consumers, control) share one connection
//   - Pool size 2: Publishers/control share connection[0], consumers use connection[1] (recommended)
//   - Pool size 3+: Publishers use connection[0], consumers use connection[1], control uses connection[2]
//
// Each publisher and consumer receives a dedicated channel from their assigned connection.
// Channels are not shared and are owned by their respective endpoints for their lifetime.
//
// # Concurrency
//
// - Connections are thread-safe and can be shared across goroutines
// - Channels are NOT thread-safe; each endpoint owns its channel exclusively
// - Publishers serialize publish operations with a mutex to ensure channel safety
// - Consumers handle each delivery in a separate goroutine but don't write to channels concurrently
package broker
