// Package message provides AMQP message types and a fluent builder for
// publishing and consuming messages through the broker.
//
// This package is internal to the broker module and not intended for direct use.
// All types and functionality are exposed through top-level broker package re-exports.
//
// # Core Types
//
// The [Message] type represents an AMQP message for both publishing and consumption.
// Outgoing messages are created with [New] or via [Builder].
//
// # Message Builder
//
// [Builder] provides a fluent interface for constructing messages with chained
// method calls:
//
//	msg, err := NewBuilder().
//	    BodyJSON(payload).
//	    CorrelationID("req-123").
//	    ReplyTo("reply.queue").
//	    Persistent().
//	    Build()
//
// # Conversion Helpers
//
// [NewConsumedMessage] and [NewReturnedMessage] are used by the endpoint package
// to attach broker-assigned metadata (delivery info, return info) to a message.
// These are internal concerns and should not be needed by users.
//
// # Error Handling
//
// All errors are rooted at [ErrMessage], which wraps the base broker error.
// Use errors.Is for error classification:
//
//	if errors.Is(err, message.ErrMessageBuild) { ... }
package message
