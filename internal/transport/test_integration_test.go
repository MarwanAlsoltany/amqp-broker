//go:build integration
// +build integration

package transport

import (
	"testing"

	"github.com/stretchr/testify/suite"

	iTesting "github.com/MarwanAlsoltany/amqp-broker/internal/testing"
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

func (s *integrationTestSuite) newTestConnection() Connection {
	conn, err := DefaultDialer(s.URL, nil)
	s.Require().NoError(err)
	return conn
}
