//go:build integration
// +build integration

package broker

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	tsLog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func init() {
	// replace testcontainers' default logger with no-op logger.
	tsLog.SetDefault(log.New(io.Discard, "", 0))
}

// TestIntegration is the test suite runner
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	suite.Run(t, new(brokerIntegrationTestSuite))
}

// brokerIntegrationTestSuite provides a shared RabbitMQ container for integration tests
type brokerIntegrationTestSuite struct {
	suite.Suite
	container       *rabbitmq.RabbitMQContainer
	url             string
	ctx             context.Context
	rabbitmqVersion int // major version number (3 or 4)
}

// SetupSuite runs once before all tests - starts RabbitMQ container
func (s *brokerIntegrationTestSuite) SetupSuite() {
	t := s.T()

	s.ctx = t.Context()

	// if RABBITMQ_URL is set, use it directly and skip starting a container
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		t.Logf("Using external RabbitMQ at %q", url)
		s.url = url
		s.rabbitmqVersion = 3 // Default/fallback, not used for version checks
		return
	}

	// if RABBITMQ_VERSION is set, use it to select version
	version := os.Getenv("RABBITMQ_VERSION")
	if version == "" {
		version = "3" // default to v3
	}

	var image string
	switch version {
	case "4":
		image = "rabbitmq:4.2-alpine"
		s.rabbitmqVersion = 4
	case "3":
		image = "rabbitmq:3.13-alpine"
		s.rabbitmqVersion = 3
	default:
		t.Fatalf("unsupported RABBITMQ_VERSION: %s (use 3 or 4)", version)
	}

	t.Logf("Using RabbitMQ %q image", image)

	var err error
	s.container, err = rabbitmq.Run(s.ctx,
		image,
		rabbitmq.WithAdminUsername("admin"),
		rabbitmq.WithAdminPassword("admin"),
	)
	s.Require().NoError(err)
	s.url, err = s.container.AmqpURL(s.ctx)
	s.Require().NoError(err)

	t.Logf("Using RabbitMQ v%s", func() string {
		major, minor, patch := s.getRabbitMQVersion()
		return fmt.Sprintf("%s.%s.%s", major, minor, patch)
	}())

	time.Sleep(3 * time.Second) // wait for RabbitMQ to be ready
}

// TearDownSuite runs once after all tests - stops RabbitMQ container
func (s *brokerIntegrationTestSuite) TearDownSuite() {
	if s.container != nil {
		s.Require().NoError(s.container.Terminate(s.ctx))
	}
}

// getRabbitMQVersion returns the RabbitMQ server version running in the container
func (s *brokerIntegrationTestSuite) getRabbitMQVersion() (major, minor, patch string) {
	major, minor, patch = strconv.Itoa(s.rabbitmqVersion), "x", "x"

	if s.container == nil {
		return
	}

	logs, err := s.container.Logs(s.ctx)
	if err != nil {
		return
	}
	defer logs.Close()

	re := regexp.MustCompile(`RabbitMQ (\d+)\.(\d+)\.(\d+)`)
	buf := make([]byte, 4096)
	for {
		n, err := logs.Read(buf)
		if err == io.EOF || err != nil {
			break
		}

		if n > 0 {
			chunk := string(buf[:n])
			matches := re.FindStringSubmatch(chunk)
			if len(matches) == 4 {
				major = matches[1]
				minor = matches[2]
				patch = matches[3]
				break
			}
		}
	}

	return major, minor, patch
}

// newTestBroker is a helper to create a broker connected to the test RabbitMQ instance.
//
// It also registers a cleanup function to close the broker after the test.
func (s *brokerIntegrationTestSuite) newTestBroker(opts ...BrokerOption) *Broker {
	t := s.T()

	t.Helper()

	opts = append([]BrokerOption{WithURL(s.url)}, opts...)

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
func (s *brokerIntegrationTestSuite) newTestBrokerWithTopology(opts ...BrokerOption) (*Broker, *Topology) {
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
