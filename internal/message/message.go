package message

import (
	"encoding/json"
	"maps"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
)

const (
	DefaultContentType  = "application/octet-stream"
	DefaultDeliveryMode = uint8(2)
)

var (
	// ErrMessage is the base error for message operations.
	// All message-related errors wrap this error.
	ErrMessage = internal.ErrDomain.Sentinel("message")

	// ErrMessageNotConsumed indicates the operation requires a consumed (incoming) message.
	// Some operations like getting [DeliveryInfo] are only valid for consumed messages.
	ErrMessageNotConsumed = ErrMessage.Derive("not a consumed message (incoming)")

	// ErrMessageNotPublished indicates the operation requires a published (outgoing) message.
	// Some operations like getting [ReturnInfo] are only valid for published messages.
	ErrMessageNotPublished = ErrMessage.Derive("not a published message (outgoing)")
)

// Acknowledger abstracts AMQP delivery acknowledgment.
// *amqp091.Delivery satisfies this interface automatically.
type Acknowledger interface {
	Ack(multiple bool) error
	Nack(multiple, requeue bool) error
	Reject(requeue bool) error
}

// DeliveryInfo holds broker-assigned metadata for a consumed message.
// These fields are populated by the broker on delivery and are not present
// in outgoing messages.
type DeliveryInfo struct {
	DeliveryTag  uint64 // broker-assigned sequence number
	ConsumerTag  string // consumer tag that received this delivery
	Exchange     string // exchange the message was published to
	RoutingKey   string // routing key used when publishing
	Redelivered  bool   // true if previously delivered but not acked
	MessageCount uint32 // messages remaining in queue (from Get operations only)
}

// ReturnInfo holds information about a returned (unroutable) message.
// It's populated when a mandatory/immediate message cannot be delivered.
type ReturnInfo struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
}

// Message represents an AMQP message for both publishing and consumption.
// It contains the message body, headers, properties, and metadata.
//
// For outgoing messages, create with Message{}, [New] or [NewBuilder].
// For incoming messages, the broker automatically wraps consumed deliveries.
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
}

// New creates a new [Message] with the given body and default settings.
// The message is ready for publishing with sensible defaults.
//
// Default settings:
//   - ContentType: "application/octet-stream"
//   - DeliveryMode: 2 (persistent)
//   - Timestamp: time of construction (now) in UTC.
//
// Use the Message.* to customize properties, or use [Builder]
// for a fluent configuration API.
//
// Example:
//
//	msg := New([]byte("Hello, World!"))
//	msg.ContentType = "text/plain"
//	msg.Priority = 5
func New(body []byte) Message {
	return Message{
		Body:         body,
		ContentType:  DefaultContentType,
		DeliveryMode: DefaultDeliveryMode,
		Timestamp:    time.Now().UTC(),
	}
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
func (m *Message) Copy() Message {
	nm := *m

	if m.Headers != nil {
		nm.Headers = make(Arguments, len(m.Headers))
		maps.Copy(nm.Headers, m.Headers)
	}

	return nm
}

// metadata contains metadata and methods specific to consumed and returned messages.
// It is nil for outgoing (published) messages.
type metadata struct {
	acknowledger Acknowledger  // non-nil for consumed messages
	deliveryInfo *DeliveryInfo // non-nil for consumed messages
	returnInfo   *ReturnInfo   // non-nil for returned messages
}

// NewConsumedMessage creates a Message representing a received AMQP delivery.
// ack is typically *amqp091.Delivery which satisfies Acknowledger directly.
// Called by internal/endpoint when building consumed messages.
func NewConsumedMessage(msg Message, ack Acknowledger, info DeliveryInfo) Message {
	msg.metadata = &metadata{acknowledger: ack, deliveryInfo: &info}
	return msg
}

// NewReturnedMessage creates a Message representing an unroutable AMQP return.
// Called by internal/endpoint when the broker returns an unroutable message.
func NewReturnedMessage(msg Message, info ReturnInfo) Message {
	msg.metadata = &metadata{returnInfo: &info}
	return msg
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
	return m.acknowledger.Ack(false)
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
	return m.acknowledger.Nack(false, requeue)
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
	return m.acknowledger.Reject(false)
}

// IsPublished returns true if this is an outgoing (published) message.
func (m *metadata) IsPublished() bool {
	return m == nil
}

// IsConsumed returns true if this is a consumed (incoming) message.
func (m *metadata) IsConsumed() bool {
	return m != nil && m.acknowledger != nil
}

// IsRedelivered returns true if this message was redelivered by the broker
// (i.e. previously delivered but not acked).
func (m *metadata) IsRedelivered() bool {
	return m != nil && m.deliveryInfo != nil && m.deliveryInfo.Redelivered
}

// IsReturned returns true if this message was returned by the broker
// (i.e. failed to be routed).
func (m *metadata) IsReturned() bool {
	return m != nil && m.returnInfo != nil
}

// DeliveryInfo returns delivery metadata for consumed messages.
// Returns ErrMessageNotConsumed for outgoing or returned messages.
func (m *metadata) DeliveryInfo() (DeliveryInfo, error) {
	if m == nil || m.deliveryInfo == nil {
		return DeliveryInfo{}, ErrMessageNotConsumed
	}
	return *m.deliveryInfo, nil
}

// ReturnInfo returns routing details for returned messages.
// Returns ErrMessageNotPublished if the message was not returned by the broker.
func (m *metadata) ReturnInfo() (ReturnInfo, error) {
	if m == nil || m.returnInfo == nil {
		return ReturnInfo{}, ErrMessageNotPublished
	}
	return *m.returnInfo, nil
}
