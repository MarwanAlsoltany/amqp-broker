package topology

import (
	"testing"
)

// Package-level sinks prevent the compiler from eliminating benchmark calls
// whose return values are otherwise unused (dead-code elimination).
var (
	benchSinkTopology   *Topology
	benchSinkExchange   *Exchange
	benchSinkQueue      *Queue
	benchSinkBinding    *Binding
	benchSinkRoutingKey RoutingKey
	benchSinkHash       string
)

func BenchmarkRegistryExchange(b *testing.B) {
	reg := NewRegistry()
	reg.stateMu.Lock()
	for i := range 20 {
		reg.exchanges = append(reg.exchanges, Exchange{Name: string(rune('a'+i%26)) + string(rune('0'+i/26)), Type: "direct"})
	}
	reg.stateMu.Unlock()

	b.Run("Found", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkExchange = reg.Exchange("t3") // exists in the slice
		}
	})

	b.Run("NotFound", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkExchange = reg.Exchange("zzz-missing") // does not exist
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkExchange = reg.Exchange("t3")
			}
		})
	})
}

func BenchmarkRegistryQueue(b *testing.B) {
	reg := NewRegistry()
	reg.stateMu.Lock()
	for i := range 20 {
		reg.queues = append(reg.queues, Queue{Name: string(rune('q')) + string(rune('0'+i))})
	}
	reg.stateMu.Unlock()

	b.Run("Found", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkQueue = reg.Queue("q5")
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkQueue = reg.Queue("q5")
			}
		})
	})
}

func BenchmarkRegistryBinding(b *testing.B) {
	reg := NewRegistry()
	reg.stateMu.Lock()
	for i := range 20 {
		reg.bindings = append(reg.bindings, Binding{
			Source:      "exchange",
			Destination: string(rune('q')) + string(rune('0'+i)),
			Key:         "key." + string(rune('0'+i)),
		})
	}
	reg.stateMu.Unlock()

	b.Run("Found", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkBinding = reg.Binding("exchange", "q5", "key.5")
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkBinding = reg.Binding("exchange", "q5", "key.5")
			}
		})
	})
}

func BenchmarkTopologyMerge(b *testing.B) {
	makeTopology := func(n int) *Topology {
		t := &Topology{}
		for i := range n {
			t.Exchanges = append(t.Exchanges, Exchange{Name: string(rune('e' + i%20)), Type: "direct"})
			t.Queues = append(t.Queues, Queue{Name: string(rune('q' + i%20))})
			t.Bindings = append(t.Bindings, Binding{
				Source:      string(rune('e' + i%20)),
				Destination: string(rune('q' + i%20)),
				Key:         "k",
			})
		}
		return t
	}

	small1 := makeTopology(1)
	small2 := makeTopology(1)
	large1 := makeTopology(50)
	large2 := makeTopology(50)

	b.Run("Small", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkTopology = small1.Merge(small2)
		}
	})

	b.Run("Large", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkTopology = large1.Merge(large2)
		}
	})
}

func BenchmarkRegistryHash(b *testing.B) {
	ex := Exchange{Name: "events", Type: "topic", Durable: true}
	q := Queue{Name: "events.orders", Durable: true}

	b.Run("Exchange", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkHash = hash(ex)
		}
	})

	b.Run("Queue", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkHash = hash(q)
		}
	})
}
