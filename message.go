package broker

import (
	"encoding/json"
	"maps"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message represents an AMQP message for both publishing and consumption.
// For outgoing messages, set the desired fields directly or use MessageBuilder.
// For incoming messages, the message is populated from the delivery and includes
// consumption metadata via the Consumed field.
type Message struct {
	/* core fields used in both directions (incoming and outgoing) */
	Body            []byte
	Headers         Arguments
	ContentType     string // MIME type, default: "application/octet-stream"
	ContentEncoding string // MIME encoding
	DeliveryMode    uint8  // 1=non-persistent, 2=persistent
	Priority        uint8  // 0-9, 0=lowest
	Timestamp       time.Time
	/* correlation & routing */
	CorrelationID string // For request/reply patterns
	ReplyTo       string // Queue name for replies
	MessageID     string // Application-specific message ID
	/* metadata */
	AppID      string // Application identifier
	UserID     string // Validated user ID
	Expiration string // Message TTL (milliseconds as string, e.g., "60000")
	Type       string // Application-specific message type

	// holds metadata specific to internal implementation;
	*amqpMetadata

	// broker is a reference to a broker instance for usage inside handlers;
	// it should be nil for outgoing messages.
	broker *Broker
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

// Broker returns the Broker associated with this message.
// Available only for incoming messages; returns nil for outgoing messages.
func (m *Message) Broker() *Broker {
	return m.broker
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

// IsOutgoing returns true if this is a message to be published.
func (m *Message) IsOutgoing() bool {
	return m._delivery == nil
}

// IsReturned returns true if this message was returned by the broker.
func (m *Message) IsReturned() bool {
	return m._return != nil
}

// IsIncoming returns true if this is a consumed message.
func (m *Message) IsIncoming() bool {
	return m._delivery != nil
}

// Copy creates a deep copy of the message.
func (m *Message) Copy() Message {
	nm := *m
	// msg.broker = nil
	if m.Headers != nil {
		nm.Headers = make(Arguments, len(m.Headers))
		maps.Copy(nm.Headers, m.Headers)
	}
	return nm
}

// amqpMetadata contains metadata and methods specific to consumed messages.
// It should be nil for outgoing messages.
// The fields are internal aliases to prevent exposing implementation details.
type amqpMetadata struct {
	_delivery *amqp.Delivery
	_return   *amqp.Return
}

// Ack acknowledges the message.
func (am *amqpMetadata) Ack() error {
	return am._delivery.Ack(false)
}

// Nack negatively acknowledges the message.
func (am *amqpMetadata) Nack(requeue bool) error {
	return am._delivery.Nack(false, requeue)
}

// Reject rejects the message (same as Nack with requeue=false).
func (am *amqpMetadata) Reject() error {
	return am._delivery.Reject(false)
}

// IsRedelivered returns true if this message was redelivered.
func (am *amqpMetadata) IsRedelivered() bool {
	if am._delivery == nil {
		return false
	}
	return am._delivery.Redelivered
}

type amqpReturnDetails struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
}

// Return provides return details if the message was returned by the broker.
func (am *amqpMetadata) ReturnDetails() *amqpReturnDetails {
	if am._return == nil {
		return nil
	}
	return &amqpReturnDetails{
		ReplyCode:  am._return.ReplyCode,
		ReplyText:  am._return.ReplyText,
		Exchange:   am._return.Exchange,
		RoutingKey: am._return.RoutingKey,
	}
}

// amqpPublishingFromMessage converts the Message to amqp.Publishing for sending (internal use).
func amqpPublishingFromMessage(msg *Message) amqp.Publishing {
	return amqp.Publishing{
		Headers:         msg.Headers,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationID,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageID,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserID,
		AppId:           msg.AppID,
		Body:            msg.Body,
	}
}

// amqpDeliveryToMessage populates a Message from an delivery (internal use).
func amqpDeliveryToMessage(d *amqp.Delivery) Message {
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
		amqpMetadata:  &amqpMetadata{_delivery: d},
	}
}

// amqpReturnToMessage populates a Message from an return_ (internal use).
func amqpReturnToMessage(r *amqp.Return) Message {
	return Message{
		Body:          r.Body,
		Headers:       r.Headers,
		ContentType:   r.ContentType,
		DeliveryMode:  r.DeliveryMode,
		Priority:      r.Priority,
		CorrelationID: r.CorrelationId,
		ReplyTo:       r.ReplyTo,
		Expiration:    r.Expiration,
		MessageID:     r.MessageId,
		Timestamp:     r.Timestamp,
		Type:          r.Type,
		UserID:        r.UserId,
		AppID:         r.AppId,
		amqpMetadata:  &amqpMetadata{_return: r},
	}
}
