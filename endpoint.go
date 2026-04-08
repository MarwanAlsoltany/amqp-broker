package broker

import (
	"github.com/MarwanAlsoltany/amqp-broker/internal/endpoint"
)

var (
	// ErrEndpoint is the base error for endpoint operations.
	// All endpoint-related errors wrap this error.
	ErrEndpoint = endpoint.ErrEndpoint

	// ErrEndpointClosed indicates the endpoint is closed.
	// This error is returned when operations are attempted on a closed endpoint.
	ErrEndpointClosed = endpoint.ErrEndpointClosed

	// ErrEndpointNotConnected indicates the endpoint is not connected.
	// This error is returned when the endpoint has no active AMQP channel.
	ErrEndpointNotConnected = endpoint.ErrEndpointNotConnected

	// ErrEndpointNotReadyTimeout indicates the endpoint did not become ready within the timeout.
	// This occurs when the endpoint fails to establish a connection in time.
	ErrEndpointNotReadyTimeout = endpoint.ErrEndpointNotReadyTimeout

	// ErrEndpointNoAutoReconnect indicates the endpoint lost its connection and auto-reconnect is disabled.
	// When auto-reconnect is disabled, connection loss is a terminal error.
	ErrEndpointNoAutoReconnect = endpoint.ErrEndpointNoAutoReconnect

	// ErrPublisher is the base error for publisher operations.
	// All publisher-specific errors wrap this error.
	ErrPublisher = endpoint.ErrPublisher

	// ErrPublisherClosed indicates the publisher is closed.
	// This error is returned when operations are attempted on a closed publisher.
	ErrPublisherClosed = endpoint.ErrPublisherClosed

	// ErrPublisherNotConnected indicates the publisher is not connected.
	// This error is returned when the publisher has no active AMQP channel.
	ErrPublisherNotConnected = endpoint.ErrPublisherNotConnected

	// ErrConsumer is the base error for consumer operations.
	// All consumer-specific errors wrap this error.
	ErrConsumer = endpoint.ErrConsumer

	// ErrConsumerClosed indicates the consumer is closed.
	// This error is returned when operations are attempted on a closed consumer.
	ErrConsumerClosed = endpoint.ErrConsumerClosed

	// ErrConsumerNotConnected indicates the consumer is not connected.
	// This error is returned when the consumer has no active AMQP channel.
	ErrConsumerNotConnected = endpoint.ErrConsumerNotConnected
)

type (
	// Endpoint defines the common interface for AMQP publishers and consumers, providing access
	// to the underlying connection, channel, and lifecycle management operations.
	//
	// Ownership & Lifecycle:
	//   - Each Endpoint owns a dedicated AMQP channel and assigns a connection from the pool.
	//   - Supports automatic reconnection and resource management.
	//   - Provides methods to declare exchanges, queues, and bindings on the channel.
	//   - Use [Endpoint.Close] to stop the endpoint and release resources.
	//
	// Concurrency:
	//   - All methods are safe for concurrent use unless otherwise specified.
	//
	// Endpoints automatically reconnect on connection loss (unless disabled)
	// and redeclare topology after reconnection.
	//
	// See [EndpointOptions] for configuration details shared by publishers and consumers.
	Endpoint = endpoint.Endpoint

	// EndpointOptions holds common reconnection and readiness configuration for endpoints.
	//
	// Key options:
	//   - NoAutoDeclare: Skip automatic topology declaration
	//   - NoAutoReconnect: Disable automatic reconnection on connection loss
	//   - ReconnectMin/Max: Control reconnection backoff timing
	//
	// These options apply to both publishers and consumers.
	EndpointOptions = endpoint.EndpointOptions

	// Publisher defines a high-level AMQP publisher with automatic connection management,
	// background confirmation handling, and graceful resource cleanup. It supports:
	//   - Publisher confirms for guaranteed delivery
	//   - Returned message handling for mandatory/immediate flags
	//   - Automatic reconnection with topology redeclaration
	//
	// Ownership & Lifecycle:
	//   - See [Endpoint] for connection and channel ownership details.
	//   - Runs in the background upon creation, ready to publish messages (push-based model).
	//   - Use [Publisher.Publish] to send messages.
	//   - Graceful shutdown ensures all in-flight publish operations and confirmations complete.
	//
	// Concurrency:
	//   - [Publisher.Publish] is safe for concurrent use by multiple goroutines.
	//
	// See [PublisherOptions] for configuration details.
	Publisher = endpoint.Publisher

	// PublisherOptions configures publisher-specific behavior.
	//
	// Key options:
	//   - ConfirmMode: Enable publisher confirms for guaranteed delivery
	//   - ConfirmTimeout: Maximum wait time for confirmations
	//   - OnReturn: Handle returned (unroutable) messages
	PublisherOptions = endpoint.PublisherOptions

	// Consumer manages message consumption with configurable concurrency.
	// It provides methods for consuming messages and controlling acknowledgment.
	//
	// Consumers support:
	//   - Automatic and manual acknowledgment
	//   - Configurable concurrency limits
	//   - QoS prefetch control
	//   - Graceful shutdown with in-flight message handling
	Consumer = endpoint.Consumer

	// ConsumerOptions configures consumer-specific behavior.
	//
	// Key options:
	//   - PrefetchCount: QoS prefetch limit (default: 1)
	//   - MaxConcurrentHandlers: Concurrency limit (default: 0, capped by PrefetchCount)
	//   - AutoAck: Enable automatic acknowledgment (default: false)
	//   - Exclusive: Create exclusive consumer (default: false)
	ConsumerOptions = endpoint.ConsumerOptions
)
