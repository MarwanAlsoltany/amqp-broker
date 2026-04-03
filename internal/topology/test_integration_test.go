//go:build integration
// +build integration

package topology

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	iTesting "github.com/MarwanAlsoltany/amqp-broker/internal/testing"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

type integrationTestSuite struct {
	iTesting.RabbitMQTestSuite
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	suite.Run(t, new(integrationTestSuite))
}

// testName generates a unique name for integration tests to avoid conflicts.
func (s *integrationTestSuite) testName(prefix string) string {
	now := time.Now()
	return fmt.Sprintf("%s-%04d%02d%02d-%02d%02d%02d-%06d",
		prefix,
		now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(),
		now.Nanosecond()/1e3,
	)
}

// newTestChannel creates a new connection and returns an open channel.
func (s *integrationTestSuite) newTestChannel() transport.Channel {
	conn, err := transport.DefaultDialer(s.URL, nil)
	s.Require().NoError(err)
	ch, err := conn.Channel()
	s.Require().NoError(err)
	return ch
}
