package broker

import (
	"testing"
)

// Package-level sinks prevent the compiler from eliminating benchmark calls
// whose return values are otherwise unused (dead-code elimination).
var (
	benchSinkAny any
)

func BenchmarkHash(b *testing.B) {
	exchange := Exchange{Name: "events", Type: "topic", Durable: true}
	exchangeWithArgs := Exchange{Name: "events", Type: "topic", Durable: true}
	exchangeWithArgs.Arguments = map[string]any{
		"x-dead-letter-exchange": "dlx",
		"x-message-ttl":          int32(60000),
	}

	b.Run("NoArgs", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAny = hash(exchange)
		}
	})

	b.Run("WithArgs", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkAny = hash(exchangeWithArgs)
		}
	})
}
