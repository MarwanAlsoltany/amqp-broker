package broker

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Sentinel errors for common failure conditions.
var (
	/* broker lifecycle errors */
	ErrBrokerClosed = errors.New("broker is closed")
	ErrBrokerClose  = errors.New("broker close error")
	/* endpoint lifecycle errors */
	ErrEndpointClosed  = errors.New("endpoint is closed")
	ErrEndpointClose   = errors.New("endpoint close error")
	ErrNotReadyTimeout = errors.New("not ready within timeout")
	ErrNoAutoReconnect = errors.New("auto-reconnect is disabled")
	/* connection errors */
	ErrConnectionNotAvailable   = errors.New("connection not available")
	ErrConnectionNotInitialized = errors.New("connection not initialized")
	ErrConnectionClosed         = errors.New("connection is closed")
	ErrConnectionClose          = errors.New("connection manager close error")
	ErrConnectionReplace        = errors.New("connection manager replace error")
	ErrConnectionManagerClosed  = errors.New("connection manager is closed")
	ErrConnectionIndexRange     = errors.New("connection index out of range")
	/* channel errors */
	ErrChannelNotAvailable = errors.New("channel not available")
	ErrChannelClosed       = errors.New("channel is closed")
	/* pool errors */
	ErrPoolClosed = errors.New("pool is closed")
	ErrPoolClose  = errors.New("pool close error")
	/* publisher-specific errors */
	ErrPublisherClosed              = errors.New("publisher is closed")
	ErrPublisherNotConnected        = errors.New("publisher not connected")
	ErrPublisherFlowPaused          = errors.New("publisher flow paused by server")
	ErrPublisherPublish             = errors.New("publish message error")
	ErrPublisherConfirmNotAvailable = errors.New("confirmation channel not available")
	ErrPublisherMessageConfirm      = errors.New("message not confirmed by server")
	ErrPublisherConfirmTimeout      = errors.New("confirmation timeout")
	/* consumer-specific errors */
	ErrConsumerClosed       = errors.New("consumer is closed")
	ErrConsumerNotConnected = errors.New("consumer not connected")
	ErrConsumerCancelled    = errors.New("consumer cancelled")
	ErrConsumerAck          = errors.New("consumer acknowledgment error")
	ErrConsumerHandler      = errors.New("consumer handler error")
	ErrConsumerMiddleware   = errors.New("consumer middleware error")
	/* configuration errors */
	ErrInvalidReconnectConfig = errors.New("reconnect config invalid")
	ErrMessageBuild           = errors.New("message build error")
	/* topology errors */
	ErrTopology           = errors.New("topology error")
	ErrTopologyDeclare    = errors.New("topology declare error")
	ErrTopologyDelete     = errors.New("topology delete error")
	ErrTopologyVerify     = errors.New("topology verify error")
	ErrTopologyValidation = errors.New("topology validation error")
	ErrEmptyExchangeName  = errors.New("exchange name cannot be empty")
	ErrEmptyQueueName     = errors.New("queue name cannot be empty")
	ErrEmptyBindingFields = errors.New("binding source and destination cannot be empty")
)

// BrokerError provides structured error information with context.
type BrokerError struct {
	Operation string // Operation that failed
	Entity    string // Any entity: connection/channel/exchange/queue/binding name (optional)
	Err       error  // Underlying error
}

// Error implements the error interface.
func (e *BrokerError) Error() string {
	if e.Entity != "" {
		return fmt.Sprintf("%s %s: %v", e.Operation, e.Entity, e.Err)
	}
	return fmt.Sprintf("%s: %v", e.Operation, e.Err)
}

// Unwrap returns the underlying error.
func (e *BrokerError) Unwrap() error {
	return e.Err
}

// BrokerAMQPError wraps an AMQP protocol error with additional context.
// This type provides a broker-agnostic interface to AMQP errors,
// allowing users to inspect error details without depending on the amqp091-go package.
type BrokerAMQPError struct {
	Operation string      // Operation that failed (e.g., "channel closed", "connection closed")
	Err       *amqp.Error // Underlying AMQP error
}

// Error implements the error interface.
func (e *BrokerAMQPError) Error() string {
	source := "client"
	if e.Err.Server {
		source = "server"
	}
	recoverable := "non-recoverable"
	if e.Err.Recover {
		recoverable = "recoverable"
	}
	return fmt.Sprintf("%s: %s (code=%d, source=%s, %s)",
		e.Operation, e.Err.Reason, e.Err.Code, source, recoverable)
}

// IsRecoverable returns true if this error can be recovered by reconnecting.
func (e *BrokerAMQPError) IsRecoverable() bool {
	return e.Err.Recover
}

// IsServerInitiated returns true if the error was initiated by the server.
func (e *BrokerAMQPError) IsServerInitiated() bool {
	return e.Err.Server
}

func newError(operation, entity string, err error) error {
	if err == nil {
		return nil
	}
	return &BrokerError{Operation: operation, Entity: entity, Err: err}
}

// newAMQPError converts an amqp.Error to an AMQPError with operation context.
// Returns nil if the input error is nil.
func newAMQPError(operation string, err *amqp.Error) *BrokerAMQPError {
	if err == nil {
		return nil
	}
	return &BrokerAMQPError{Operation: operation, Err: err}
}

func wrapError(operation string, err error) error {
	return newError(operation, "", err)
}

func wrapEntityError(operation, entity string, err error) error {
	return newError(operation, entity, err)
}
