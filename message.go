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
	Body            []byte    // Message payload
	Headers         Arguments // AMQP message headers
	ContentType     string    // MIME type, default: "application/octet-stream"
	ContentEncoding string    // MIME encoding
	DeliveryMode    uint8     // 1=non-persistent, 2=persistent
	Priority        uint8     // 0-9, 0=lowest
	Timestamp       time.Time // Message timestamp
	/* correlation & routing */
	CorrelationID string // For request/reply patterns
	ReplyTo       string // Queue name for replies
	MessageID     string // Application-specific message ID
	/* metadata */
	AppID      string // Application identifier
	UserID     string // Validated user ID
	Expiration string // Message TTL (milliseconds as string, e.g. "60000")
	Type       string // Application-specific message type

	// holds metadata specific to internal implementation
	*metadata

	// broker is a reference to a broker instance for usage inside handlers;
	// it should be nil for published messages (outgoing).
	broker *Broker
}

// NewMessage creates a new message for publishing with sensible defaults. These include:
// ContentType="application/octet-stream", DeliveryMode=2 (persistent), and Timestamp=now.
// Additional fields can be set directly on the returned Message.
func NewMessage(body []byte) Message {
	return Message{
		Body:         body,
		ContentType:  defaultContentType,
		DeliveryMode: defaultDeliveryMode,
		Timestamp:    time.Now(),
	}
}

// Broker returns the Broker associated with this message.
// Available only for consumed messages (incoming); returns nil for published messages (outgoing).
func (m *Message) Broker() *Broker {
	return m.broker
}

// Data returns the message body decoded based on ContentType.
// For "application/json", it returns a map[string]interface{}.
// For "text/plain", it returns a string.
// For any other ContentType, it returns the raw []byte body.
// For other content types, it returns the raw body, which may be nil.
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

// Copy creates a deep copy of the message.
// If the message has a Broker reference, it will be nil in the copy.
func (m *Message) Copy() Message {
	nm := *m
	nm.broker = nil // clear broker reference

	if m.Headers != nil {
		nm.Headers = make(Arguments, len(m.Headers))
		maps.Copy(nm.Headers, m.Headers)
	}

	return nm
}

// metadata contains metadata and methods specific to consumed messages.
// It should be nil for outgoing messages.
// The fields are internal aliases to prevent exposing implementation details.
type metadata struct {
	_delivery *amqp.Delivery
	_return   *amqp.Return
}

// Ack acknowledges the message.
//
// Returns an error if the message ack fails or if the message is not a consumed message (incoming).
//
// Either [Message.Ack], [Message.Reject] or [Message.Nack] must be called for every message
// that is not automatically acknowledged (via a consumer where AutoAck is enabled).
func (m *metadata) Ack() error {
	if !m.IsConsumed() {
		return ErrMessageNotConsumed
	}
	return m._delivery.Ack(false)
}

// Nack negatively acknowledges the message.
// If requeue is true, the message will be requeued and delivered to another consumer.
//
// When requeue is true, request the server to deliver this message to a different consumer.
// If it is not possible or requeue is false, the message will be dropped or delivered to
// a server configured dead-letter queue.
//
// NOTE: This method must not be used to select or requeue messages the client wishes not to handle,
// rather it is to inform the server that the client is incapable of handling this message at this time.
//
// Returns an error if the message nack fails or if the message is not a consumed message (incoming).
//
// Either [Message.Ack], [Message.Reject] or [Message.Nack] must be called for every message
// that is not automatically acknowledged (via a consumer where AutoAck is enabled).
func (m *metadata) Nack(requeue bool) error {
	if !m.IsConsumed() {
		return ErrMessageNotConsumed
	}
	return m._delivery.Nack(false, requeue)
}

// Reject rejects the message (same as [Message.Nack] with requeue=false, i.e. discard the message).
//
// If called and the server is unable to queue this message the message will be dropped or
// delivered to a server configured dead-letter queue.
//
// Returns an error if the message reject fails or if the message is not a consumed message (incoming).
//
// Either [Message.Ack], [Message.Reject] or [Message.Nack] must be called for every message
// that is not automatically acknowledged (via a consumer where AutoAck is enabled).
func (m *metadata) Reject() error {
	if !m.IsConsumed() {
		return ErrMessageNotConsumed
	}
	return m._delivery.Reject(false)
}

// IsPublished returns true if this is a message to be published.
func (m *metadata) IsPublished() bool {
	return m != nil && m._delivery == nil
}

// IsConsumed returns true if this is a consumed message.
func (m *metadata) IsConsumed() bool {
	return m != nil && m._delivery != nil
}

// IsRedelivered returns true if this message was redelivered by the broker
// (i.e. previously delivered but not acked).
func (m *metadata) IsRedelivered() bool {
	return m != nil && m._delivery != nil && m._delivery.Redelivered
}

// IsReturned returns true if this message was returned by the broker
// (i.e. failed to be published).
func (m *metadata) IsReturned() bool {
	return m != nil && m._return != nil
}

// Return provides return details if the message was returned by the broker.
// Returns nil if the message was not returned.
func (m *metadata) ReturnDetails() *struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
} {
	if !m.IsReturned() {
		return nil
	}
	// this struct is internal and is used only here;
	// no need to predefine it elsewhere
	return &struct {
		ReplyCode  uint16
		ReplyText  string
		Exchange   string
		RoutingKey string
	}{
		ReplyCode:  m._return.ReplyCode,
		ReplyText:  m._return.ReplyText,
		Exchange:   m._return.Exchange,
		RoutingKey: m._return.RoutingKey,
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
		metadata:      &metadata{_delivery: d},
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
		metadata:      &metadata{_return: r},
	}
}
