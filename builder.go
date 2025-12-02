package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MessageBuilder provides a fluent interface for constructing messages.
type MessageBuilder struct {
	msg *Message
	err error // accumulates any errors encountered during building
}

// NewMessageBuilder creates a new message builder with sensible defaults.
func NewMessageBuilder() *MessageBuilder {
	return &MessageBuilder{
		msg: &Message{
			ContentType:  defaultContentType,
			DeliveryMode: defaultDeliveryMode,
			Timestamp:    time.Now(),
		},
	}
}

func (b *MessageBuilder) appendErr(err error) {
	if err != nil {
		b.err = errors.Join(b.err, err)
	}
}

// Body sets the message body.
func (b *MessageBuilder) Body(body []byte) *MessageBuilder {
	b.msg.Body = body
	return b
}

// BodyString sets the message body from a string.
func (b *MessageBuilder) BodyString(body string) *MessageBuilder {
	b.msg.Body = []byte(body)
	return b
}

// BodyJSON marshals the value to JSON and sets it as the body.
// Also sets ContentType to "application/json".
func (b *MessageBuilder) BodyJSON(v interface{}) *MessageBuilder {
	data, err := json.Marshal(v)
	if err != nil {
		b.msg.Body = []byte{}
		b.appendErr(fmt.Errorf("%w: BodyJSON: %v", ErrBrokerMessageBuildFailed, err))
	} else {
		b.msg.Body = data
		b.msg.ContentType = "application/json"
	}
	return b
}

// ContentType sets the MIME content type.
func (b *MessageBuilder) ContentType(ct string) *MessageBuilder {
	b.msg.ContentType = ct
	return b
}

// DeliveryMode sets the delivery mode (1=transient, 2=persistent).
func (b *MessageBuilder) DeliveryMode(mode uint8) *MessageBuilder {
	b.msg.DeliveryMode = mode
	return b
}

// Priority sets the message priority (0-255, 0=lowest).
func (b *MessageBuilder) Priority(p uint8) *MessageBuilder {
	b.msg.Priority = p
	return b
}

// Timestamp sets the message timestamp.
func (b *MessageBuilder) Timestamp(t time.Time) *MessageBuilder {
	b.msg.Timestamp = t
	return b
}

// Header adds or updates a single header.
func (b *MessageBuilder) Header(key string, value interface{}) *MessageBuilder {
	if b.msg.Headers == nil {
		b.msg.Headers = make(Arguments)
	}
	b.msg.Headers[key] = value
	return b
}

// Headers replaces all headers with the provided map.
func (b *MessageBuilder) Headers(headers Arguments) *MessageBuilder {
	b.msg.Headers = headers
	return b
}

// CorrelationID sets the correlation ID for request/reply patterns.
func (b *MessageBuilder) CorrelationID(id string) *MessageBuilder {
	b.msg.CorrelationID = id
	return b
}

// ReplyTo sets the reply-to queue name.
func (b *MessageBuilder) ReplyTo(queue string) *MessageBuilder {
	b.msg.ReplyTo = queue
	return b
}

// MessageID sets an application-specific message ID.
func (b *MessageBuilder) MessageID(id string) *MessageBuilder {
	b.msg.MessageID = id
	return b
}

// Expiration sets the message TTL as a string (milliseconds).
func (b *MessageBuilder) Expiration(ms string) *MessageBuilder {
	b.msg.Expiration = ms
	return b
}

// ExpirationDuration sets the message TTL from a time.Duration.
func (b *MessageBuilder) ExpirationDuration(d time.Duration) *MessageBuilder {
	ms := int64(d / time.Millisecond)
	b.msg.Expiration = fmt.Sprintf("%d", ms)
	return b
}

// Type sets an application-specific message type.
func (b *MessageBuilder) Type(typ string) *MessageBuilder {
	b.msg.Type = typ
	return b
}

// UserID sets the user ID.
func (b *MessageBuilder) UserID(userID string) *MessageBuilder {
	b.msg.UserID = userID
	return b
}

// AppID sets the application identifier.
func (b *MessageBuilder) AppID(appID string) *MessageBuilder {
	b.msg.AppID = appID
	return b
}

// Now sets the message timestamp to the current time.
func (b *MessageBuilder) Now() *MessageBuilder {
	b.msg.Timestamp = time.Now()
	return b
}

// Persistent is a shorthand for DeliveryMode(2).
// It sets the message to persistent mode (survives broker restart).
func (b *MessageBuilder) Persistent() *MessageBuilder {
	b.msg.DeliveryMode = 2
	return b
}

// Transient is a shorthand for DeliveryMode(1).
// It sets the message to transient mode (does not survive broker restart).
func (b *MessageBuilder) Transient() *MessageBuilder {
	b.msg.DeliveryMode = 1
	return b
}

// JSON is a shorthand for ContentType("application/json").
func (b *MessageBuilder) JSON() *MessageBuilder {
	b.msg.ContentType = "application/json"
	return b
}

// Text is a shorthand for ContentType("text/plain").
func (b *MessageBuilder) Text() *MessageBuilder {
	b.msg.ContentType = "text/plain"
	return b
}

// Binary is a shorthand for ContentType("application/octet-stream").
func (b *MessageBuilder) Binary() *MessageBuilder {
	b.msg.ContentType = "application/octet-stream"
	return b
}

// Reset resets the builder to a new message with defaults.
func (b *MessageBuilder) Reset() {
	b.msg = NewMessage(nil)
}

// Build returns the constructed message.
func (b *MessageBuilder) Build() (Message, error) {
	return *b.msg, b.err
}
