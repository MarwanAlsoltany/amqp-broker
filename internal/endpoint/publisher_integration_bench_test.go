//go:build integration
// +build integration

package endpoint

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	iTesting "github.com/MarwanAlsoltany/amqp-broker/internal/testing"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
)

func BenchmarkPublisherNoConfirm(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)

	runPublish := func(b *testing.B, body []byte) {
		b.Helper()
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)
		p := newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		msg := message.New(body)
		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	}

	b.Run("SmallBody", func(b *testing.B) { runPublish(b, make([]byte, 64)) })
	b.Run("MediumBody", func(b *testing.B) { runPublish(b, make([]byte, 4*1024)) })
	b.Run("LargeBody", func(b *testing.B) { runPublish(b, make([]byte, 64*1024)) })
}

func BenchmarkPublisherConfirm(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)
	b.ReportAllocs()
	mgr := newBenchConnectionManager(b, url)
	reg := topology.NewRegistry()
	topo := newBenchTopology(b, url)
	p := newBenchPublisher(b, mgr, reg, PublisherOptions{
		ConfirmMode:    true,
		ConfirmTimeout: 10 * time.Second,
	}, topo.Exchange)
	msg := message.New(make([]byte, 64))
	b.ResetTimer()
	for b.Loop() {
		if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

func BenchmarkPublisherDeferredConfirm(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)
	b.ReportAllocs()
	mgr := newBenchConnectionManager(b, url)
	reg := topology.NewRegistry()
	topo := newBenchTopology(b, url)

	var confirmed atomic.Int64
	p := newBenchPublisher(b, mgr, reg, PublisherOptions{
		ConfirmMode: true,
		OnConfirm: func(tag uint64, wait func(context.Context) bool) {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if wait(ctx) {
					confirmed.Add(1)
				}
			}()
		},
	}, topo.Exchange)
	msg := message.New(make([]byte, 64))
	b.ResetTimer()
	for b.Loop() {
		if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

func BenchmarkPublisherBatch(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)

	runBatch := func(b *testing.B, batchSize int) {
		b.Helper()
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)
		p := newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		msgs := make([]message.Message, batchSize)
		for i := range msgs {
			msgs[i] = message.New(make([]byte, 64))
		}
		b.ResetTimer()
		for b.Loop() {
			if err := p.Publish(b.Context(), topo.Key, msgs...); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)*float64(batchSize)/b.Elapsed().Seconds(), "msgs/s")
	}

	b.Run("Size10", func(b *testing.B) { runBatch(b, 10) })
	b.Run("Size100", func(b *testing.B) { runBatch(b, 100) })
}

func BenchmarkPublisherMultiple(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)

	runMulti := func(b *testing.B, n int) {
		b.Helper()
		b.ReportAllocs()
		mgr := newBenchConnectionManager(b, url)
		reg := topology.NewRegistry()
		topo := newBenchTopology(b, url)

		publishers := make([]*publisher, n)
		for i := range n {
			publishers[i] = newBenchPublisher(b, mgr, reg, PublisherOptions{}, topo.Exchange)
		}
		msg := message.New(make([]byte, 64))
		i := 0
		b.ResetTimer()
		for b.Loop() {
			p := publishers[i%n]
			if err := p.Publish(b.Context(), topo.Key, msg); err != nil {
				b.Fatal(err)
			}
			i++
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	}

	b.Run("2", func(b *testing.B) { runMulti(b, 2) })
	b.Run("4", func(b *testing.B) { runMulti(b, 4) })
	b.Run("8", func(b *testing.B) { runMulti(b, 8) })
}
