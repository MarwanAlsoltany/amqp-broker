package message

import (
	"bytes"
	"testing"
	"time"
)

func BenchmarkBuilder(b *testing.B) {
	smallPayload := map[string]any{"key": "value", "count": 17}
	largePayload := make(map[string]any, 1000)
	for i := range 1000 {
		largePayload[string(rune('a'+i%26))+string(rune('0'+i/26%10))] = bytes.Repeat([]byte("v"), 50)
	}

	b.Run("BodyBytesMinimal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage, benchSinkError = NewBuilder().Body([]byte("test")).Build()
		}
	})

	b.Run("BodyJSONSmall", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage, benchSinkError = NewBuilder().BodyJSON(smallPayload).Build()
		}
	})

	b.Run("BodyJSONLarge", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage, benchSinkError = NewBuilder().BodyJSON(largePayload).Build()
		}
	})

	b.Run("Full", func(b *testing.B) {
		b.ReportAllocs()
		ts := time.Now()
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage, benchSinkError = NewBuilder().
				Body([]byte("test")).
				ContentType("application/octet-stream").
				DeliveryMode(2).
				Priority(5).
				Timestamp(ts).
				CorrelationID("correlation-123").
				ReplyTo("reply.queue").
				MessageID("message-789").
				Expiration("60000").
				Type("order.created").
				UserID("user-xyz").
				AppID("my-app").
				Header("x-tenant", "acme").
				Build()
		}
	})

	b.Run("Reuse", func(b *testing.B) {
		b.ReportAllocs()
		bld := NewBuilder().Body([]byte("test")).ContentType("application/octet-stream")
		b.ResetTimer()
		for b.Loop() {
			benchSinkMessage, benchSinkError = bld.Build()
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkMessage, benchSinkError = NewBuilder().Body([]byte("test")).Build()
			}
		})
	})
}
