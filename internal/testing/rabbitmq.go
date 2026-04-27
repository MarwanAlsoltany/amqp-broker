//go:build integration
// +build integration

package testing

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	tcLog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func init() {
	// replace testcontainers' default logger with no-op logger
	tcLog.SetDefault(log.New(io.Discard, "", 0))
}

// rabbitmqImage returns the RabbitMQ Docker image tag for the given version string.
// Defaults to v3 if version is empty or unrecognised.
func rabbitmqImage(version string) string {
	switch version {
	case "4":
		return "rabbitmq:4.2-alpine"
	default:
		return "rabbitmq:3.13-alpine"
	}
}

// RabbitMQTestSuite provides a shared RabbitMQ container for integration tests.
// Embed this in custom test suites to get access to a running RabbitMQ instance.
type RabbitMQTestSuite struct {
	suite.Suite
	container       *rabbitmq.RabbitMQContainer
	URL             string
	CTX             context.Context
	rabbitmqVersion int // major version number (3 or 4)
}

// SetupSuite runs once before all tests - starts RabbitMQ container
func (s *RabbitMQTestSuite) SetupSuite() {
	t := s.T()

	s.CTX = t.Context()

	// if RABBITMQ_URL is set, use it directly and skip starting a container
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		t.Logf("Using external RabbitMQ at %q", url)
		s.URL = url
		s.rabbitmqVersion = 3 // Default/fallback, not used for version checks
		return
	}

	// if RABBITMQ_VERSION is set, use it to select version
	version := os.Getenv("RABBITMQ_VERSION")
	if version == "" {
		version = "3" // default to v3
	}

	switch version {
	case "4":
		s.rabbitmqVersion = 4
	case "3":
		s.rabbitmqVersion = 3
	default:
		t.Fatalf("unsupported RABBITMQ_VERSION: %s (use 3 or 4)", version)
	}

	image := rabbitmqImage(version)

	t.Logf("Using RabbitMQ %q image", image)

	var err error
	s.container, err = rabbitmq.Run(
		s.CTX,
		image,
		rabbitmq.WithAdminUsername("admin"),
		rabbitmq.WithAdminPassword("admin"),
	)
	s.Require().NoError(err)
	s.URL, err = s.container.AmqpURL(s.CTX)
	s.Require().NoError(err)

	t.Logf("Using RabbitMQ v%s", func() string {
		major, minor, patch := s.GetRabbitMQVersion()
		return fmt.Sprintf("%s.%s.%s", major, minor, patch)
	}())

	time.Sleep(3 * time.Second) // wait for RabbitMQ to be ready
}

// TearDownSuite runs once after all tests - stops RabbitMQ container
func (s *RabbitMQTestSuite) TearDownSuite() {
	if s.container != nil {
		s.Require().NoError(s.container.Terminate(s.CTX))
	}
}

// GetRabbitMQVersion returns the RabbitMQ server version running in the container
func (s *RabbitMQTestSuite) GetRabbitMQVersion() (major, minor, patch string) {
	major, minor, patch = strconv.Itoa(s.rabbitmqVersion), "x", "x"

	if s.container == nil {
		return
	}

	logs, err := s.container.Logs(s.CTX)
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

// rabbitmqBenchURL resolves the AMQP URL for benchmarks exactly once per
// test binary run. Returns an empty string if no instance is available.
var rabbitmqBenchURL = sync.OnceValue(func() string {
	ctx := context.Background()

	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		return url
	}

	c, err := rabbitmq.Run(ctx,
		rabbitmqImage(os.Getenv("RABBITMQ_VERSION")),
		rabbitmq.WithAdminUsername("admin"),
		rabbitmq.WithAdminPassword("admin"),
	)
	if err != nil {
		return ""
	}

	url, err := c.AmqpURL(ctx)
	if err != nil {
		return ""
	}

	time.Sleep(3 * time.Second) // wait for RabbitMQ to be ready

	return url
})

// RabbitMQBenchmarkURL returns the AMQP URL of a shared RabbitMQ instance
// for use in integration benchmarks. The instance is started at most once
// per test binary run; subsequent calls reuse the same URL.
// It respects the RABBITMQ_URL and RABBITMQ_VERSION environment variables
// with the same semantics as RabbitMQTestSuite.
// b is skipped if no RabbitMQ instance can be obtained.
func RabbitMQBenchmarkURL(b *testing.B) string {
	b.Helper()
	if url := rabbitmqBenchURL(); url != "" {
		return url
	}
	b.Skip("no RabbitMQ available: set RABBITMQ_URL or ensure Docker is running")
	return ""
}
