//go:build integration
// +build integration

package broker

import (
	"testing"

	iTesting "github.com/MarwanAlsoltany/amqp-broker/internal/testing"
)

func newBenchBroker(b *testing.B) *Broker {
	b.Helper()
	broker, err := New(WithURL(iTesting.RabbitMQBenchmarkURL(b)))
	if err != nil {
		b.Fatalf("new broker: %v", err)
	}
	b.Cleanup(func() { _ = broker.Close() })
	return broker
}

func newBenchBrokerWithTopology(b *testing.B) (*Broker, *Topology) {
	b.Helper()
	broker := newBenchBroker(b)

	eName := testName("bench-exchange")
	qName := testName("bench-queue")
	t := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: "bench"}},
	}
	if err := broker.Declare(t); err != nil {
		b.Fatalf("declare topology: %v", err)
	}
	b.Cleanup(func() { _ = broker.Delete(t) })
	return broker, t
}
