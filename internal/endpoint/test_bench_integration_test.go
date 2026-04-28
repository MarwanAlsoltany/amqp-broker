//go:build integration
// +build integration

package endpoint

import (
	"fmt"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/handler"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

func benchName(prefix string) string {
	now := time.Now()
	return fmt.Sprintf("%s-%04d%02d%02d-%02d%02d%02d-%06d",
		prefix, now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1e3,
	)
}

type benchTopology struct {
	Exchange topology.Exchange
	Queue    topology.Queue
	Binding  topology.Binding
	Key      topology.RoutingKey
}

func newBenchTopology(b *testing.B, url string) benchTopology {
	b.Helper()
	eName := benchName("bench-exchange")
	qName := benchName("bench-queue")
	key := topology.RoutingKey("bench-key")

	conn, err := transport.DefaultDialer(url, nil)
	if err != nil {
		b.Fatalf("dial for topology: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("channel for topology: %v", err)
	}

	reg := topology.NewRegistry()
	topo := &topology.Topology{
		Exchanges: []topology.Exchange{topology.NewExchange(eName)},
		Queues:    []topology.Queue{topology.NewQueue(qName)},
		Bindings:  []topology.Binding{topology.NewBinding(eName, qName, string(key))},
	}
	if err := reg.Declare(ch, topo); err != nil {
		b.Fatalf("declare topology: %v", err)
	}
	_ = ch.Close()
	_ = conn.Close()

	b.Cleanup(func() {
		conn2, err := transport.DefaultDialer(url, nil)
		if err != nil {
			return
		}
		defer conn2.Close()
		ch2, err := conn2.Channel()
		if err != nil {
			return
		}
		defer ch2.Close()
		_ = reg.Delete(ch2, topo)
	})

	return benchTopology{
		Exchange: topology.NewExchange(eName),
		Queue:    topology.NewQueue(qName),
		Binding:  topology.NewBinding(eName, qName, string(key)),
		Key:      key,
	}
}

func newBenchConnectionManager(b *testing.B, url string) *transport.ConnectionManager {
	b.Helper()
	mgr := transport.NewConnectionManager(url, nil)
	if err := mgr.Init(b.Context()); err != nil {
		b.Fatalf("connection manager init: %v", err)
	}
	b.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

func newBenchPublisher(b *testing.B, mgr *transport.ConnectionManager, reg *topology.Registry, opts PublisherOptions, ex topology.Exchange) *publisher {
	b.Helper()
	opts.EndpointOptions = MergeEndpointOptions(opts.EndpointOptions, DefaultEndpointOptions())
	p := newPublisher(b.Name(), mgr, reg, opts, ex)
	if err := p.init(b.Context()); err != nil {
		b.Fatalf("publisher init: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })
	return p
}

func newBenchConsumer(b *testing.B, mgr *transport.ConnectionManager, reg *topology.Registry, opts ConsumerOptions, q topology.Queue, h handler.Handler) *consumer {
	b.Helper()
	opts.EndpointOptions = MergeEndpointOptions(opts.EndpointOptions, DefaultEndpointOptions())
	c := newConsumer(b.Name(), mgr, reg, opts, q, h)
	if err := c.init(b.Context()); err != nil {
		b.Fatalf("consumer init: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	return c
}
