package broker

import (
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

var (
	// ErrMessage is the base error for message operations.
	// All message-related errors wrap this error.
	ErrMessage = message.ErrMessage

	// ErrMessageBuild indicates a message build operation failed.
	// This occurs when MessageBuilder.Build() fails validation or encoding.
	ErrMessageBuild = message.ErrMessageBuild

	// ErrMessageNotPublished indicates the operation requires a published (outgoing) message.
	// Some operations like setting delivery info are only valid for consumed messages.
	ErrMessageNotPublished = message.ErrMessageNotPublished

	// ErrMessageNotConsumed indicates the operation requires a consumed (incoming) message.
	// Some operations like getting delivery info are only valid for consumed messages.
	ErrMessageNotConsumed = message.ErrMessageNotConsumed
)

type (
	// Message represents an AMQP message for both publishing and consumption.
	// It contains the message body, headers, properties, and metadata.
	//
	// For outgoing messages, create with NewMessage() or NewMessageBuilder().
	// For incoming messages, the broker automatically wraps consumed deliveries.
	Message = message.Message

	// MessageBuilder provides a fluent interface for constructing messages.
	// It supports method chaining for configuring all message properties.
	//
	// Example:
	//   msg, err := NewMessageBuilder().
	//     BodyJSON(data).
	//     CorrelationID("req-123").
	//     ReplyTo("reply.queue").
	//     Persistent().
	//     Build()
	MessageBuilder = message.Builder

	// DeliveryInfo holds broker-assigned metadata for a consumed message.
	// These fields are populated by the broker on delivery and are not present
	// in outgoing messages.
	DeliveryInfo = message.DeliveryInfo

	// ReturnInfo holds information about a returned (unroutable) message.
	// It's populated when a mandatory/immediate message cannot be delivered.
	ReturnInfo = message.ReturnInfo
)

// NewMessage creates a new [Message] with the given body and default settings.
// The message is ready for publishing with sensible defaults.
//
// Default settings:
//   - ContentType: "application/octet-stream"
//   - DeliveryMode: 2 (persistent)
//   - Timestamp: time of construction (now) in UTC.
//
// Use the Message.* to customize properties, or use [MessageBuilder]
// for a fluent configuration API.
//
// Example:
//
//	msg := NewMessage([]byte("Hello, World!"))
//	msg.ContentType = "text/plain"
//	msg.Priority = 5
func NewMessage(body []byte) Message {
	return message.New(body)
}

// NewMessageBuilder creates a new [MessageBuilder] for fluent message construction.
// The builder provides a chainable API for setting all message properties.
//
// Example:
//
//	msg, err := NewMessageBuilder().
//		Body([]byte("data")).
//		ContentType("application/json").
//		Persistent().
//		Priority(9).
//		Build()
func NewMessageBuilder() *MessageBuilder {
	return message.NewBuilder()
}
