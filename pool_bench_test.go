package broker

import (
	"testing"
	"time"
)

// Package-level sinks prevent the compiler from eliminating benchmark calls
// whose return values are otherwise unused (dead-code elimination).
var (
	benchSinkPoolValue any
)

func BenchmarkPoolAcquire(b *testing.B) {
	b.Run("Hit", func(b *testing.B) {
		b.ReportAllocs()
		p := newPool[string](time.Minute)
		_ = p.init(b.Context())
		// warm the pool with a single item
		_, release, _ := p.acquire("key", func() (string, error) { return "value", nil })
		release()
		b.ResetTimer()
		for b.Loop() {
			benchSinkPoolValue, release, _ = p.acquire("key", func() (string, error) { return "value", nil })
			release()
		}
	})

	b.Run("Miss", func(b *testing.B) {
		b.ReportAllocs()
		// new key every iteration -> always a miss
		p := newPool[string](time.Minute)
		_ = p.init(b.Context())
		b.ResetTimer()
		i := 0
		var release func()
		for b.Loop() {
			key := string(rune('a'+i%26)) + string(rune('0'+i/26%10))
			benchSinkPoolValue, release, _ = p.acquire(key, func() (string, error) { return "v", nil })
			release()
			i++
		}
	})

	b.Run("ParallelHit", func(b *testing.B) {
		b.ReportAllocs()
		p := newPool[string](time.Minute)
		_ = p.init(b.Context())
		_, release, _ := p.acquire("key", func() (string, error) { return "value", nil })
		release()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				value, release, _ := p.acquire("key", func() (string, error) { return "value", nil })
				release()
				benchSinkPoolValue = value
			}
		})
	})

	b.Run("ParallelMiss", func(b *testing.B) {
		b.ReportAllocs()
		p := newPool[string](time.Minute)
		_ = p.init(b.Context())
		i := 0
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				key := string(rune('a'+i%26)) + string(rune('0'+i/26%10))
				i++
				value, release, _ := p.acquire(key, func() (string, error) { return "v", nil })
				release()
				benchSinkPoolValue = value
			}
		})
	})
}
