//go:build integration
// +build integration

package endpoint

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/MarwanAlsoltany/amqp-broker/internal/handler"
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	testing1 "github.com/MarwanAlsoltany/amqp-broker/internal/testing"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	suite.Run(t, new(integrationTestSuite))
}

// integrationTestSuite provides a shared RabbitMQ container and helpers
// for integration tests covering both publisher and consumer endpoints.
type integrationTestSuite struct {
	testing1.RabbitMQTestSuite
}

// testUniqueName generates a unique name for integration tests to avoid conflicts.
func (s *integrationTestSuite) testUniqueName(prefix string) string {
	now := time.Now()
	return fmt.Sprintf("%s-%04d%02d%02d-%02d%02d%02d-%06d",
		prefix,
		now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(),
		now.Nanosecond()/1e3,
	)
}

// newConnectionManager creates and initializes a ConnectionManager for the test.
// It registers cleanup to close the manager after the test.
func (s *integrationTestSuite) newConnectionManager() *transport.ConnectionManager {
	t := s.T()
	t.Helper()

	cm := transport.NewConnectionManager(s.URL, nil)
	s.Require().NoError(cm.Init(s.CTX))

	t.Cleanup(func() { _ = cm.Close() })

	return cm
}

// newTopologyRegistry returns a fresh TopologyManager.
func (s *integrationTestSuite) newTopologyRegistry() *topology.Registry {
	return topology.NewRegistry()
}

// newTestPublisher creates and starts a publisher for the test.
// Registers cleanup to close the publisher after the test.
func (s *integrationTestSuite) newTestPublisher(connMgr *transport.ConnectionManager, topoReg *topology.Registry, opts PublisherOptions, exchange topology.Exchange) *publisher {
	t := s.T()
	t.Helper()

	opts.EndpointOptions = MergeEndpointOptions(opts.EndpointOptions, DefaultEndpointOptions())
	p := newPublisher(t.Name(), connMgr, topoReg, opts, exchange)
	s.Require().NoError(p.init(s.CTX))

	t.Cleanup(func() { _ = p.Close() })

	return p
}

// newTestConsumer creates and starts a consumer for the test.
// Registers cleanup to close the consumer after the test.
func (s *integrationTestSuite) newTestConsumer(connMgr *transport.ConnectionManager, topoReg *topology.Registry, opts ConsumerOptions, queue topology.Queue, h handler.Handler) *consumer {
	t := s.T()
	t.Helper()

	opts.EndpointOptions = MergeEndpointOptions(opts.EndpointOptions, DefaultEndpointOptions())
	c := newConsumer(t.Name(), connMgr, topoReg, opts, queue, h)
	s.Require().NoError(c.init(s.CTX))

	t.Cleanup(func() { _ = c.Close() })

	return c
}

// testEndpointTopology holds a declared exchange+queue+binding name set for tests.
type testEndpointTopology struct {
	Exchange topology.Exchange
	Queue    topology.Queue
	Binding  topology.Binding
	Key      topology.RoutingKey
}

// declareTestTopology creates a unique exchange, queue, and binding on RabbitMQ.
// Registers cleanup to delete them after the test.
func (s *integrationTestSuite) declareTestTopology(topoReg *topology.Registry) testEndpointTopology {
	t := s.T()
	t.Helper()

	eName := s.testUniqueName("test-exchange")
	qName := s.testUniqueName("test-queue")
	kName := topology.RoutingKey("test-key")

	conn, err := transport.DefaultDialer(s.URL, nil)
	s.Require().NoError(err)
	ch, err := conn.Channel()
	s.Require().NoError(err)

	topo := &topology.Topology{
		Exchanges: []topology.Exchange{topology.NewExchange(eName)},
		Queues:    []topology.Queue{topology.NewQueue(qName)},
		Bindings:  []topology.Binding{topology.NewBinding(eName, qName, string(kName))},
	}

	s.Require().NoError(topoReg.Declare(ch, topo))
	_ = ch.Close()
	_ = conn.Close()

	t.Cleanup(func() {
		conn2, err := transport.DefaultDialer(s.URL, nil)
		if err != nil {
			return
		}
		defer conn2.Close()
		ch2, err := conn2.Channel()
		if err != nil {
			return
		}
		defer ch2.Close()
		_ = topoReg.Delete(ch2, topo)
	})

	return testEndpointTopology{
		Exchange: topology.NewExchange(eName),
		Queue:    topology.NewQueue(qName),
		Binding:  topology.NewBinding(eName, qName, string(kName)),
		Key:      kName,
	}
}

// publishTestMessages publishes N simple messages through the publisher.
func (s *integrationTestSuite) publishTestMessages(p *publisher, rk topology.RoutingKey, n int) {
	s.T().Helper()
	for i := range n {
		msg := message.Message{Body: []byte(fmt.Sprintf("msg-%d", i))}
		s.Require().NoError(p.Publish(context.Background(), rk, msg))
	}
}
