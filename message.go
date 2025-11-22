package broker

import (
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message represents an AMQP message for both publishing and consumption.
// For outgoing messages, set the desired fields directly or use MessageBuilder.
// For incoming messages, the message is populated from the delivery and includes
// consumption metadata via the Consumed field.
type Message struct {
	/* core fields used in both directions (incoming and outgoing) */
	Body         []byte
	Headers      amqp.Table
	ContentType  string // MIME type, default: "application/octet-stream"
	DeliveryMode uint8  // 1=non-persistent, 2=persistent
	Priority     uint8  // 0-9, 0=lowest
	Timestamp    time.Time
	/* correlation & routing */
	CorrelationID string // For request/reply patterns
	ReplyTo       string // Queue name for replies
	MessageID     string // Application-specific message ID
	/* metadata */
	AppID      string // Application identifier
	UserID     string // Validated user ID
	Expiration string // Message TTL (milliseconds as string, e.g., "60000")
	Type       string // Application-specific message type

	// delivery holds the consumption context,
	// only set for incoming messages (consumed), for outgoing messages it must be nil.
	*delivery

	// broker is a reference to a broker instance for usage inside handlers;
	// it should be nil for outgoing messages.
	broker *Broker
}

// Broker returns the Broker associated with this message.
// Available only for incoming messages; returns nil for outgoing messages.
func (m *Message) Broker() *Broker {
	return m.broker
}

// Incoming returns true if this is a consumed message.
func (m *Message) Incoming() bool {
	return m.delivery != nil
}

// Outgoing returns true if this is a message to be published.
func (m *Message) Outgoing() bool {
	return m.delivery == nil
}

// Copy creates a deep copy of the message.
func (m *Message) Copy() Message {
	newMsg := *m
	if m.Headers != nil {
		newMsg.Headers = cloneMap(m.Headers)
	}
	return newMsg
}

// Data returns the message body decoded based on ContentType.
// For "application/json", it returns a map[string]interface{}.
// For "text/plain", it returns a string.
// For any other ContentType, it returns the raw []byte body.
// For other content types, it returns nil.
func (m *Message) Data() any {
	if m.Body == nil {
		return nil
	}
	switch m.ContentType {
	case "application/json":
		var result map[string]interface{}
		err := json.Unmarshal(m.Body, &result)
		if err != nil {
			return nil
		}
		return result
	case "text/plain":
		return string(m.Body)
	default:
		return m.Body
	}
}

// delivery contains metadata and methods specific to consumed messages.
// It should be nil for outgoing messages.
type delivery struct {
	// Underlying AMQP delivery
	Delivery *amqp.Delivery

	// Ack acknowledges the message.
	Ack func() error
	// Nack negatively acknowledges the message.
	Nack func(requeue bool) error
	// Reject rejects the message (same as Nack with requeue=false).
	Reject func() error
}

// NewMessage creates a new message for publishing with sensible defaults.
// Additional fields can be set directly on the returned Message.
func NewMessage(body []byte) *Message {
	return &Message{
		Body:         body,
		ContentType:  defaultContentType,
		DeliveryMode: defaultDeliveryMode,
		Timestamp:    time.Now(),
	}
}

// NewReplyMessage creates a reply message preserving correlation context.
// The returned Message preserves Headers, ContentType, DeliveryMode, and CorrelationID from the original message.
// The Timestamp is set to the current time. Other fields can be set as needed.
func NewReplyMessage(m *Message, body []byte) *Message {
	return &Message{
		Body:          body,
		Headers:       cloneMap(m.Headers),
		ContentType:   m.ContentType,
		DeliveryMode:  m.DeliveryMode,
		CorrelationID: m.CorrelationID,
		Timestamp:     time.Now(),
	}
}

// NewForwardMessage creates a new message for forwarding with the same properties.
// The returned Message preserves all fields except UserID, AppID, and Expiration.
// The Timestamp is set to the current time. Other fields can be set as needed.
func NewForwardMessage(m *Message) *Message {
	return &Message{
		Body:          m.Body,
		Headers:       cloneMap(m.Headers),
		ContentType:   m.ContentType,
		DeliveryMode:  m.DeliveryMode,
		Priority:      m.Priority,
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		MessageID:     m.MessageID,
		Type:          m.Type,
		Timestamp:     time.Now(),
	}
}

// messageToPublishing converts the Message to amqp.Publishing for sending.
func messageToPublishing(msg *Message) amqp.Publishing {
	return amqp.Publishing{
		Headers:       msg.Headers,
		ContentType:   msg.ContentType,
		DeliveryMode:  msg.DeliveryMode,
		Priority:      msg.Priority,
		CorrelationId: msg.CorrelationID,
		ReplyTo:       msg.ReplyTo,
		Expiration:    msg.Expiration,
		MessageId:     msg.MessageID,
		Timestamp:     msg.Timestamp,
		Type:          msg.Type,
		UserId:        msg.UserID,
		AppId:         msg.AppID,
		Body:          msg.Body,
	}
}

// deliveryToMessage populates a Message from an amqp.Delivery (internal use).
func deliveryToMessage(d *amqp.Delivery) Message {
	return Message{
		Body:          d.Body,
		Headers:       d.Headers,
		ContentType:   d.ContentType,
		DeliveryMode:  d.DeliveryMode,
		Priority:      d.Priority,
		CorrelationID: d.CorrelationId,
		ReplyTo:       d.ReplyTo,
		Expiration:    d.Expiration,
		MessageID:     d.MessageId,
		Timestamp:     d.Timestamp,
		Type:          d.Type,
		UserID:        d.UserId,
		AppID:         d.AppId,
		delivery: &delivery{
			Delivery: d,
			Ack:      func() error { return d.Ack(false) },
			Nack:     func(requeue bool) error { return d.Nack(false, requeue) },
			Reject:   func() error { return d.Reject(false) },
		},
	}
}
