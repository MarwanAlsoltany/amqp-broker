package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrMessageBuild indicates a message build operation failed.
	// This occurs when [MessageBuilder.Build] fails validation or encoding.
	ErrMessageBuild = ErrMessage.Derive("build failed")
)

// Builder provides a fluent interface for constructing messages.
// It supports method chaining for configuring all message properties.
//
// Example:
//
//	msg, err := NewBuilder().
//		BodyJSON(data).
//		CorrelationID("req-123").
//		ReplyTo("reply.queue").
//		Persistent().
//		Build()
//
// Note: The Builder is safe for reuse.
type Builder struct {
	msg *Message
	err error // accumulates any errors encountered during building
}

// NewBuilder creates a new [Builder] for fluent message construction.
// The builder provides a chainable API for setting all message properties.
//
// Example:
//
//	msg, err := NewBuilder().
//		Body([]byte("data")).
//		ContentType("application/json").
//		Persistent().
//		Priority(9).
//		Build()
func NewBuilder() *Builder {
	msg := New(nil) // start with an empty message with defaults
	return &Builder{msg: &msg}
}

func (b *Builder) appendErr(err error) {
	if err != nil {
		b.err = errors.Join(b.err, err)
	}
}

// Body sets the message body.
func (b *Builder) Body(body []byte) *Builder {
	b.msg.Body = body
	return b
}

// BodyString sets the message body from a string.
func (b *Builder) BodyString(body string) *Builder {
	b.msg.Body = []byte(body)
	return b
}

// BodyJSON marshals the value to JSON and sets it as the body.
// Also sets ContentType to "application/json".
func (b *Builder) BodyJSON(v any) *Builder {
	data, err := json.Marshal(v)
	if err != nil {
		b.msg.Body = []byte{}
		b.appendErr(fmt.Errorf("BodyJSON: %w", err))
	} else {
		b.msg.Body = data
		b.msg.ContentType = "application/json"
	}
	return b
}

// ContentType sets the MIME content type.
func (b *Builder) ContentType(ct string) *Builder {
	b.msg.ContentType = ct
	return b
}

// DeliveryMode sets the delivery mode (1=transient, 2=persistent).
func (b *Builder) DeliveryMode(mode uint8) *Builder {
	b.msg.DeliveryMode = mode
	return b
}

// Priority sets the message priority (0-255, 0=lowest).
func (b *Builder) Priority(p uint8) *Builder {
	b.msg.Priority = p
	return b
}

// Timestamp sets the message timestamp.
func (b *Builder) Timestamp(t time.Time) *Builder {
	b.msg.Timestamp = t
	return b
}

// Header adds or updates a single header.
func (b *Builder) Header(key string, value interface{}) *Builder {
	if b.msg.Headers == nil {
		b.msg.Headers = make(Arguments)
	}
	b.msg.Headers[key] = value
	return b
}

// Headers replaces all headers with the provided map.
func (b *Builder) Headers(headers Arguments) *Builder {
	b.msg.Headers = headers
	return b
}

// CorrelationID sets the correlation ID for request/reply patterns.
func (b *Builder) CorrelationID(id string) *Builder {
	b.msg.CorrelationID = id
	return b
}

// ReplyTo sets the reply-to queue name.
func (b *Builder) ReplyTo(queue string) *Builder {
	b.msg.ReplyTo = queue
	return b
}

// MessageID sets an application-specific message ID.
func (b *Builder) MessageID(id string) *Builder {
	b.msg.MessageID = id
	return b
}

// Expiration sets the message TTL as a string (milliseconds).
func (b *Builder) Expiration(ms string) *Builder {
	b.msg.Expiration = ms
	return b
}

// ExpirationDuration sets the message TTL from a time.Duration.
func (b *Builder) ExpirationDuration(d time.Duration) *Builder {
	ms := int64(d / time.Millisecond)
	b.msg.Expiration = fmt.Sprintf("%d", ms)
	return b
}

// Type sets an application-specific message type.
func (b *Builder) Type(typ string) *Builder {
	b.msg.Type = typ
	return b
}

// UserID sets the user ID.
func (b *Builder) UserID(userID string) *Builder {
	b.msg.UserID = userID
	return b
}

// AppID sets the application identifier.
func (b *Builder) AppID(appID string) *Builder {
	b.msg.AppID = appID
	return b
}

// Now sets the message timestamp to the current time.
func (b *Builder) Now() *Builder {
	b.msg.Timestamp = time.Now()
	return b
}

// Persistent is a shorthand for DeliveryMode(2).
// It sets the message to persistent mode (survives broker restart).
func (b *Builder) Persistent() *Builder {
	b.msg.DeliveryMode = 2
	return b
}

// Transient is a shorthand for DeliveryMode(1).
// It sets the message to transient mode (does not survive broker restart).
func (b *Builder) Transient() *Builder {
	b.msg.DeliveryMode = 1
	return b
}

// JSON is a shorthand for ContentType("application/json").
func (b *Builder) JSON() *Builder {
	b.msg.ContentType = "application/json"
	return b
}

// Text is a shorthand for ContentType("text/plain").
func (b *Builder) Text() *Builder {
	b.msg.ContentType = "text/plain"
	return b
}

// Binary is a shorthand for ContentType("application/octet-stream").
func (b *Builder) Binary() *Builder {
	b.msg.ContentType = "application/octet-stream"
	return b
}

// Reset resets the builder to a new message with defaults.
func (b *Builder) Reset() {
	msg := New(nil)
	b.msg = &msg
}

// Build returns the constructed message.
// If any errors were accumulated during building, they are wrapped with [ErrMessageBuild].
func (b *Builder) Build() (Message, error) {
	if b.err != nil {
		return *b.msg, ErrMessageBuild.Detailf("%w", b.err)
	}
	return *b.msg, nil
}
