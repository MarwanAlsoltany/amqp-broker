package endpoint

import (
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

// Package-level sinks prevent the compiler from eliminating benchmark calls
// whose return values are otherwise unused (dead-code elimination).
var (
	benchSinkMessage    message.Message
	benchSinkPublishing transport.Publishing
)

func BenchmarkDeliveryToMessage(b *testing.B) {
	baseDelivery := transport.Delivery{
		Body:            []byte("hello world"),
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		DeliveryMode:    2,
		Priority:        0,
		CorrelationId:   "correlation-123",
		ReplyTo:         "reply.queue",
		Expiration:      "60000",
		MessageId:       "message-789",
		Timestamp:       time.Now(),
		Type:            "order.created",
		UserId:          "user-xyz",
		AppId:           "my-app",
		DeliveryTag:     1,
		ConsumerTag:     "consumer#0",
		Exchange:        "events",
		RoutingKey:      "orders.created",
	}

	b.Run("NoHeaders", func(b *testing.B) {
		b.ReportAllocs()
		d := baseDelivery
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage = deliveryToMessage(&d)
		}
	})

	b.Run("WithHeaders", func(b *testing.B) {
		b.ReportAllocs()
		d := baseDelivery
		d.Headers = transport.Arguments{
			"x-tenant":  "acme",
			"x-region":  "us-east",
			"x-version": "1.2.3",
			"x-trace":   "abc123",
		}
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage = deliveryToMessage(&d)
		}
	})
}

func BenchmarkMessageToPublishing(b *testing.B) {
	d := transport.Delivery{
		Body:          []byte("hello world"),
		ContentType:   "application/octet-stream",
		DeliveryMode:  2,
		CorrelationId: "correlation-123",
		ReplyTo:       "reply.queue",
		Expiration:    "60000",
		MessageId:     "message-789",
		Timestamp:     time.Now(),
		Type:          "order.created",
		UserId:        "user-xyz",
		AppId:         "my-app",
	}
	baseMsg := deliveryToMessage(&d)

	b.Run("NoHeaders", func(b *testing.B) {
		b.ReportAllocs()
		msg := baseMsg
		b.ResetTimer()
		for b.Loop() {
			benchSinkPublishing = messageToPublishing(&msg)
		}
	})

	b.Run("WithHeaders", func(b *testing.B) {
		b.ReportAllocs()
		msg := baseMsg
		msg.Headers = transport.Arguments{
			"x-tenant":  "acme",
			"x-region":  "us-east",
			"x-version": "1.2.3",
			"x-trace":   "abc123",
		}
		b.ResetTimer()
		for b.Loop() {
			benchSinkPublishing = messageToPublishing(&msg)
		}
	})
}

func BenchmarkReturnToMessage(b *testing.B) {
	b.ReportAllocs()
	r := transport.Return{
		ReplyCode:    312,
		ReplyText:    "NO_ROUTE",
		Exchange:     "events",
		RoutingKey:   "orders.created",
		Body:         []byte("hello world"),
		ContentType:  "application/octet-stream",
		DeliveryMode: 2,
		MessageId:    "message-789",
		Timestamp:    time.Now(),
		AppId:        "my-app",
	}
	b.ResetTimer()
	for b.Loop() {
		benchSinkMessage = returnToMessage(&r)
	}
}
