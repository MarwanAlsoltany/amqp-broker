// Package endpoint provides AMQP publisher and consumer implementations with lifecycle
// management, automatic reconnection, and topology declaration support.
//
// This package is internal to the broker module and not intended for direct use.
// All types and functionality are exposed through top-level broker package re-exports.
//
// # Overview
//
// The endpoint package provides three main abstractions:
//   - [Endpoint]: Common interface for AMQP publishers and consumers with lifecycle management
//   - [Publisher]: Manages message publishing with optional publisher confirms and return handling
//   - [Consumer]: Manages message consumption with configurable concurrency and acknowledgment
//
// # Connection Management
//
// Each endpoint owns a dedicated AMQP channel and shares connections through the
// [transport.ConnectionManager]. Connection assignment is based on endpoint role:
//   - Publishers use connections assigned for publishing (ConnectionPurposePublish)
//   - Consumers use connections assigned for consuming (ConnectionPurposeConsume)
//   - Control operations use connections assigned for control (ConnectionPurposeControl)
//
// # Lifecycle
//
// The exported constructors [NewPublisher] and [NewConsumer] create endpoints and initialize
// them internally, returning a ready interface value:
//
//   - Create: Constructs the endpoint with configuration
//   - Init: Validates configuration and prepares for connection
//   - Run: Starts background goroutines for reconnection and monitoring
//   - Close: Gracefully shuts down, completing in-flight operations
//
// Automatic reconnection is enabled by default with exponential backoff.
// Endpoints transition through states: uninitialized -> ready -> connected -> closed.
//
// # Topology Management
//
// Topology operations (Exchange/Queue/Binding) delegate to [topology.Registry],
// which declares entities on the AMQP channel and tracks declarations for idempotency.
// Topology is automatically redeclared on reconnection unless NoAutoDeclare is set.
//
// # Message Handling
//
// Wire-format conversions between amqp091 types and [message.Message] happen exclusively
// in this package (message.go):
//   - Consumed messages are wrapped with [message.NewConsumedMessage] to attach delivery metadata
//   - Returned messages are wrapped with [message.NewReturnedMessage] to attach return info
//   - Outgoing messages are converted to amqp091.Publishing format for transmission
//
// # Error Handling
//
// All errors are rooted at [ErrEndpoint], [ErrPublisher], and [ErrConsumer], which wrap
// the base broker error. Use errors.Is for error classification.
package endpoint
