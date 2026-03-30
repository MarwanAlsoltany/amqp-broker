// Package transport provides connection and channel abstractions over the AMQP 0.9.1 protocol.
//
// This package is internal to the broker module and not intended for direct use.
// All types and functionality are exposed through top-level broker package re-exports.
//
// This package decouples the broker from the underlying amqp091-go library by defining
// minimal interfaces for [Connection] and [Channel] types. This abstraction serves two purposes:
//
//  1. Testability: Allows injection of mock implementations for unit testing without
//     requiring actual AMQP connections.
//
//  2. Flexibility: Isolates the broker from direct dependency on amqp091-go, making it
//     easier to swap implementations or upgrade the underlying library in the future.
//
// # Core Types
//
// The package provides:
//   - [Connection]: Interface for AMQP connections
//   - [Channel]: Interface for AMQP channels
//   - [ConnectionManager]: Pool manager for long-lived connections with automatic reconnection
//   - [Dialer]: Function type for creating connections (enables custom implementations)
//   - [Config]: AMQP connection configuration (heartbeat, locale, etc.)
//   - [DoSafeChannelAction]: Utility for safe channel operations with closure detection
//   - [DoSafeChannelActionWithReturn]: Generic variant supporting return values
//
// # Implementation Strategy
//
// The package uses a hybrid approach:
//   - Minimal hand-crafted interfaces (Connection, Channel) containing only the methods
//     actually used by the broker
//   - Adapter structs (connectionAdapter, channelAdapter) that wrap amqp091-go types
//     using struct embedding to automatically satisfy the interfaces
//   - A [Dialer] abstraction for creating connections, enabling dependency injection
//
// # Connection Management
//
// [ConnectionManager] maintains a pool of AMQP connections with:
//   - Configurable pool size for connection multiplexing
//   - Automatic reconnection with exponential backoff
//   - Round-robin assignment to endpoints based on purpose (control, publish, consume)
//   - Lifecycle hooks for monitoring (OnOpen, OnClose, OnBlock)
//   - Thread-safe operations with mutex protection
//
// # Error Handling
//
// All errors are rooted at [ErrConnection] and [ErrConnectionManager], which wrap
// the base broker error. Use errors.Is for error classification.
package transport
