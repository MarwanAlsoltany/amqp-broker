// Package topology provides AMQP topology types and management for declaring,
// verifying, and deleting exchanges, queues, and bindings.
//
// This package is internal to the broker module and not intended for direct use.
// All types and functionality are exposed through top-level broker package re-exports.
//
// This package defines the core topology data types used throughout the broker:
//   - [Exchange]: Represents an AMQP exchange with Declare, Verify, and Delete methods
//   - [Queue]: Represents an AMQP queue with Declare, Verify, Delete, Purge, and Inspect methods
//   - [Binding]: Represents a queue-to-exchange or exchange-to-exchange binding with Declare and Delete methods
//   - [RoutingKey]: Represents an AMQP routing key with placeholder substitution ({key} -> value)
//   - [Topology]: Container grouping exchanges, queues, and bindings for bulk operations
//
// # Topology Registry
//
// [Registry] provides stateful topology management with:
//   - In-memory declaration cache to track what's been declared
//   - Mutex-protected state for thread-safe concurrent access
//   - Idempotent operations (redeclaring same entity is safe)
//   - Query methods to retrieve declared entities by name
//   - Bulk operations (Declare, Delete, Verify, Sync)
//
// The manager is intended for direct injection wherever topology management is needed
// without depending on the full broker.
//
// # Channel Interface
//
// All topology operations accept [transport.Channel] (an interface), enabling mock injection
// for unit testing without a live AMQP connection. This allows comprehensive testing of
// topology logic without external dependencies.
//
// # Fluent Configuration
//
// Exchange, Queue, and Binding types support fluent configuration via With* methods:
//
//	exchange := NewExchange("events").
//	    WithType("topic").
//	    WithDurable(true).
//	    WithAutoDelete(false)
//
// These methods modify the receiver in-place and return the pointer for chaining.
//
// # Error Handling
//
// All topology errors wrap [ErrTopology], allowing callers to use errors.Is checks:
//
//	if errors.Is(err, topology.ErrTopologyDeclareFailed) { ... }
//	if errors.Is(err, topology.ErrTopologyValidation) { ... }
//
// Validation errors provide detailed messages about what's wrong with the configuration.
package topology
