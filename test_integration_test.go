//go:build integration
// +build integration

package broker

import (
	"testing"

	"github.com/stretchr/testify/suite"

	iTesting "github.com/MarwanAlsoltany/amqp-broker/internal/testing"
)

// TestIntegration is the test suite runner
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	suite.Run(t, new(integrationTestSuite))
}

// integrationTestSuite wraps RabbitMQTestSuite to provide broker-specific helpers.
type integrationTestSuite struct {
	iTesting.RabbitMQTestSuite
}

// newTestBroker is a helper to create a broker connected to the test RabbitMQ instance.
//
// It also registers a cleanup function to close the broker after the test.
func (s *integrationTestSuite) newTestBroker(opts ...Option) *Broker {
	t := s.T()

	t.Helper()

	opts = append([]Option{WithURL(s.URL)}, opts...)

	b, err := New(opts...)
	s.Require().NoError(err)

	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			// uncomment the following for cleanup debugging
			// t.Logf("closing test broker for %s; error: %v", t.Name(), err)
		}
	})

	return b
}

// newTestBrokerWithTopology is a helper that builds on top of
// [brokerIntegrationTestSuite.newTestBroker] and creates a topology for tests.
//
// It declares an exchange, a queue, and binds them with a routing key.
// The exchange name is in the format test-exchange-<random> and the queue name is test-queue-<random>.
// The routing/binding key is "test".
//
// It also registers a cleanup function to delete the topology after the test.
func (s *integrationTestSuite) newTestBrokerWithTopology(opts ...Option) (*Broker, *Topology) {
	t := s.T()

	t.Helper()

	b := s.newTestBroker(opts...)

	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := "test"

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: bKey}},
	}

	err := b.Declare(topology)
	s.Require().NoError(err)

	t.Cleanup(func() {
		if err := b.Delete(topology); err != nil {
			// uncomment the following for cleanup debugging
			// t.Logf("deleting test topology for %s; error: %v", t.Name(), err)
		}
	})

	return b, topology
}
