//go:build integration
// +build integration

package endpoint

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/handler"
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	iTesting "github.com/MarwanAlsoltany/amqp-broker/internal/testing"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
)

// roundTripConsumer is a handler that signals receipt of each message via a channel.
func roundTripConsumer(received chan<- struct{}) handler.Handler {
	return func(_ context.Context, _ *message.Message) (handler.Action, error) {
		received <- struct{}{}
		return handler.ActionAck, nil
	}
}

func BenchmarkConsumerWorkers(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)

	runWorkers := func(b *testing.B, workers int) {
		b.Helper()
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)

		received := make(chan struct{}, 1024)
		_ = newBenchConsumer(b, mgr, reg, ConsumerOptions{
			PrefetchCount:         256,
			MaxConcurrentHandlers: workers,
		}, topo.Queue, roundTripConsumer(received))

		p := newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		msg := message.New(make([]byte, 64))

		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
				b.Fatal(err)
			}
			select {
			case <-received:
			case <-time.After(10 * time.Second):
				b.Fatal("timeout waiting for message")
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	}

	b.Run("Sequential", func(b *testing.B) { runWorkers(b, 1) })
	b.Run("4", func(b *testing.B) { runWorkers(b, 4) })
	b.Run("8", func(b *testing.B) { runWorkers(b, 8) })
	b.Run("16", func(b *testing.B) { runWorkers(b, 16) })
	b.Run("Unlimited", func(b *testing.B) { runWorkers(b, 0) })
}

func BenchmarkConsumerPrefetch(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)

	runPrefetch := func(b *testing.B, prefetch int) {
		b.Helper()
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)

		received := make(chan struct{}, 1024)
		_ = newBenchConsumer(b, mgr, reg, ConsumerOptions{
			PrefetchCount:         prefetch,
			MaxConcurrentHandlers: 8,
		}, topo.Queue, roundTripConsumer(received))

		p := newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		msg := message.New(make([]byte, 64))

		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
				b.Fatal(err)
			}
			select {
			case <-received:
			case <-time.After(10 * time.Second):
				b.Fatal("timeout waiting for message")
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	}

	b.Run("1", func(b *testing.B) { runPrefetch(b, 1) })
	b.Run("10", func(b *testing.B) { runPrefetch(b, 10) })
	b.Run("50", func(b *testing.B) { runPrefetch(b, 50) })
	b.Run("100", func(b *testing.B) { runPrefetch(b, 100) })
}

func BenchmarkConsumerAck(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)

	b.Run("Auto", func(b *testing.B) {
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)

		received := make(chan struct{}, 1024)
		_ = newBenchConsumer(b, mgr, reg, ConsumerOptions{
			PrefetchCount: 64,
			AutoAck:       true,
		}, topo.Queue, roundTripConsumer(received))

		p := newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		msg := message.New(make([]byte, 64))

		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
				b.Fatal(err)
			}
			select {
			case <-received:
			case <-time.After(10 * time.Second):
				b.Fatal("timeout")
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})

	b.Run("ManualAck", func(b *testing.B) {
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)

		received := make(chan struct{}, 1024)
		h := handler.Handler(func(_ context.Context, _ *message.Message) (handler.Action, error) {
			received <- struct{}{}
			return handler.ActionAck, nil
		})
		_ = newBenchConsumer(b, mgr, reg, ConsumerOptions{PrefetchCount: 64}, topo.Queue, h)

		p := newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		msg := message.New(make([]byte, 64))

		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
				b.Fatal(err)
			}
			select {
			case <-received:
			case <-time.After(10 * time.Second):
				b.Fatal("timeout")
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})

	b.Run("ManualNack", func(b *testing.B) {
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)

		var count atomic.Int64
		received := make(chan struct{}, 1024)
		h := handler.Handler(func(_ context.Context, _ *message.Message) (handler.Action, error) {
			// nack-discard on first pass to avoid infinite requeue loop
			if count.Add(1)%2 == 1 {
				received <- struct{}{}
				return handler.ActionNackDiscard, nil
			}
			received <- struct{}{}
			return handler.ActionAck, nil
		})
		_ = newBenchConsumer(b, mgr, reg, ConsumerOptions{PrefetchCount: 64}, topo.Queue, h)

		p := newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		msg := message.New(make([]byte, 64))

		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
				b.Fatal(err)
			}
			select {
			case <-received:
			case <-time.After(10 * time.Second):
				b.Fatal("timeout")
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})
}

func BenchmarkConsumerMessageSize(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)

	runSize := func(b *testing.B, body []byte) {
		b.Helper()
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)

		received := make(chan struct{}, 1024)
		_ = newBenchConsumer(b, mgr, reg, ConsumerOptions{
			PrefetchCount:         50,
			MaxConcurrentHandlers: 8,
		}, topo.Queue, roundTripConsumer(received))

		p := newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		msg := message.New(body)

		b.SetBytes(int64(len(body)))
		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
				b.Fatal(err)
			}
			select {
			case <-received:
			case <-time.After(10 * time.Second):
				b.Fatal("timeout")
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	}

	b.Run("Small", func(b *testing.B) { runSize(b, make([]byte, 64)) })
	b.Run("Medium", func(b *testing.B) { runSize(b, make([]byte, 4*1024)) })
	b.Run("Large", func(b *testing.B) { runSize(b, make([]byte, 64*1024)) })
}
