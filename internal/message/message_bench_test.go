package message

import (
	"bytes"
	"testing"
)

// Package-level sinks prevent the compiler from eliminating benchmark calls
// whose return values are otherwise unused (dead-code elimination).
var (
	benchSinkMessage Message
	benchSinkAny     any
	benchSinkError   error
)

func BenchmarkNew(b *testing.B) {
	body := []byte("benchmark test")

	b.Run("WithBody", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage = New(body)
		}
	})

	b.Run("NilBody", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage = New(nil)
		}
	})
}

func BenchmarkMessageData(b *testing.B) {
	jsonSmall := []byte(`{"key":"value","count":13}`)
	jsonMedium := bytes.Repeat([]byte(`{"k":"v"},`), 400) // ~4 KB
	jsonLarge := bytes.Repeat([]byte(`{"k":"v"},`), 6553) // ~64 KB

	// wrap in a valid JSON array
	jsonMediumBody := append([]byte("["), append(jsonMedium, ']')...)
	jsonLargeBody := append([]byte("["), append(jsonLarge, ']')...)

	b.Run("Bytes", func(b *testing.B) {
		b.ReportAllocs()
		msg := New([]byte("raw binary"))
		// default ContentType is application/octet-stream
		b.ResetTimer()
		for b.Loop() {
			benchSinkAny = msg.Data()
		}
	})

	b.Run("Text", func(b *testing.B) {
		b.ReportAllocs()
		msg := New([]byte("test"))
		msg.ContentType = "text/plain"
		b.ResetTimer()
		for b.Loop() {
			benchSinkAny = msg.Data()
		}
	})

	b.Run("JSONSmall", func(b *testing.B) {
		b.ReportAllocs()
		msg := New(jsonSmall)
		msg.ContentType = "application/json"
		b.ResetTimer()
		for b.Loop() {
			benchSinkAny = msg.Data()
		}
	})

	b.Run("JSONMedium", func(b *testing.B) {
		b.ReportAllocs()
		msg := New(jsonMediumBody)
		msg.ContentType = "application/json"
		b.ResetTimer()
		for b.Loop() {
			benchSinkAny = msg.Data()
		}
	})

	b.Run("JSONLarge", func(b *testing.B) {
		b.ReportAllocs()
		msg := New(jsonLargeBody)
		msg.ContentType = "application/json"
		b.ResetTimer()
		for b.Loop() {
			benchSinkAny = msg.Data()
		}
	})
}

func BenchmarkMessageCopy(b *testing.B) {
	b.Run("NoHeaders", func(b *testing.B) {
		b.ReportAllocs()
		msg := New([]byte("test"))
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage = msg.Copy()
		}
	})

	b.Run("WithHeaders", func(b *testing.B) {
		b.ReportAllocs()
		msg := New([]byte("test"))
		msg.Headers = Arguments{
			"x-tenant":  "acme",
			"x-region":  "us-east",
			"x-version": "1.2.3",
			"x-trace":   "abc123",
		}
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage = msg.Copy()
		}
	})
}

func BenchmarkNewConsumedMessage(b *testing.B) {
	b.ReportAllocs()
	base := New([]byte("test"))
	ack := &mockAcknowledger{}
	info := DeliveryInfo{DeliveryTag: 1, Exchange: "ex", RoutingKey: "rk"}
	b.ResetTimer()
	for b.Loop() {
		benchSinkMessage = NewConsumedMessage(base, ack, info)
	}
}

func BenchmarkNewReturnedMessage(b *testing.B) {
	b.ReportAllocs()
	base := New([]byte("test"))
	info := ReturnInfo{ReplyCode: 312, ReplyText: "NO_ROUTE", Exchange: "ex", RoutingKey: "rk"}
	b.ResetTimer()
	for b.Loop() {
		benchSinkMessage = NewReturnedMessage(base, info)
	}
}
