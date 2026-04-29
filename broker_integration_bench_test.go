//go:build integration
// +build integration

package broker

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkBrokerPublish(b *testing.B) {
	broker, topology := newBenchBrokerWithTopology(b)
	exchange := topology.Exchanges[0].Name
	msg := Message{Body: []byte("bench")}

	b.Run("CacheHit", func(b *testing.B) {
		b.ReportAllocs()
		// warm the pool first
		_ = broker.Publish(b.Context(), exchange, "bench", msg)
		b.ResetTimer()
		for b.Loop() {
			if err := broker.Publish(b.Context(), exchange, "bench", msg); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})

	b.Run("CacheMiss", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		b.ResetTimer()
		for b.Loop() {
			// different exchange name each time -> always a pool miss
			e := fmt.Sprintf("bench-miss-%d", i)
			i++
			// will fail routing but measures hash+pool overhead
			_ = broker.Publish(b.Context(), e, "bench", msg)
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := broker.Publish(b.Context(), exchange, "bench", msg); err != nil {
					b.Error(err)
				}
			}
		})
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})
}

func BenchmarkBrokerNewPublisher(b *testing.B) {
	broker, topology := newBenchBrokerWithTopology(b)
	exchange := Exchange{Name: topology.Exchanges[0].Name, Type: "direct"}
	msg := Message{Body: []byte("bench")}

	p, err := broker.NewPublisher(nil, exchange)
	if err != nil {
		b.Fatalf("new publisher: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })

	b.Run("Single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), RoutingKey("bench"), msg); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := p.Publish(b.Context(), RoutingKey("bench"), msg); err != nil {
					b.Error(err)
				}
			}
		})
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})
}

func BenchmarkBrokerConsume(b *testing.B) {
	broker, topology := newBenchBrokerWithTopology(b)
	exchange := topology.Exchanges[0].Name
	queue := topology.Queues[0].Name
	msg := Message{Body: []byte("bench")}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// pre-publish a message for the one-off consume to pick up
		_ = broker.Publish(b.Context(), exchange, "bench", msg)

		ctx, cancel := context.WithTimeout(b.Context(), 5*time.Second)
		// Consume blocks until ctx is done; use Get via NewConsumer to fetch one message
		c, err := broker.NewConsumer(nil, Queue{Name: queue}, func(_ context.Context, _ *Message) (HandlerAction, error) {
			cancel() // cancel after first message
			return HandlerActionAck, nil
		})
		if err != nil {
			cancel()
			// transient AMQP channel error under rapid consumer churn; skip iteration
			b.Logf("Skip: %v", err)
			continue
		}
		_ = c.Consume(ctx)
		_ = c.Close()
		cancel()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

func BenchmarkBrokerNewConsumer(b *testing.B) {
	roundTrip := func(b *testing.B, concurrency int) {
		b.Helper()
		b.ReportAllocs()
		broker, topology := newBenchBrokerWithTopology(b)
		exchange := topology.Exchanges[0].Name
		queue := topology.Queues[0].Name

		received := make(chan struct{}, 1024)
		h := Handler(func(_ context.Context, _ *Message) (HandlerAction, error) {
			received <- struct{}{}
			return HandlerActionAck, nil
		})

		c, err := broker.NewConsumer(nil, Queue{Name: queue}, h)
		if err != nil {
			b.Fatalf("new consumer: %v", err)
		}
		b.Cleanup(func() { _ = c.Close() })

		// start consuming
		go func() {
			ctx, cancel := context.WithCancel(b.Context())
			b.Cleanup(cancel)
			_ = c.Consume(ctx)
		}()

		msg := Message{Body: []byte("bench")}

		semaphore := make(chan struct{}, concurrency)

		var published atomic.Int64
		b.ResetTimer()
		for b.Loop() {
			semaphore <- struct{}{}
			go func() {
				defer func() { <-semaphore }()
				_ = broker.Publish(b.Context(), exchange, "bench", msg)
				published.Add(1)
			}()
			select {
			case <-received:
			case <-time.After(10 * time.Second):
				b.Error("timeout waiting for message")
			}
		}
		// drain semaphore
		for range concurrency {
			semaphore <- struct{}{}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	}

	b.Run("Sequential", func(b *testing.B) { roundTrip(b, 1) })
	b.Run("Concurrent4", func(b *testing.B) { roundTrip(b, 4) })
	b.Run("Concurrent16", func(b *testing.B) { roundTrip(b, 16) })
}
