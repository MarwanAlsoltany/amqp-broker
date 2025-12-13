package broker

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Sentinel errors for common failure conditions.
var (
	ErrBroker                     error = new(Error)
	ErrBrokerClosed                     = fmt.Errorf("%w: %s", ErrBroker, "closed")
	ErrBrokerConfigInvalid              = fmt.Errorf("%w: %s", ErrBroker, "config invalid")
	ErrConnection                       = fmt.Errorf("%w %s", ErrBroker, "connection")
	ErrConnectionClosed                 = fmt.Errorf("%w: %s", ErrConnection, "closed")
	ErrConnectionManager                = fmt.Errorf("%w %s", ErrConnection, "manager")
	ErrConnectionManagerClosed          = fmt.Errorf("%w: %s", ErrConnectionManager, "closed")
	ErrChannel                          = fmt.Errorf("%w %s", ErrConnection, "channel")
	ErrChannelClosed                    = fmt.Errorf("%w: %s", ErrChannel, "closed")
	ErrPool                             = fmt.Errorf("%w %s", ErrBroker, "pool")
	ErrPoolClosed                       = fmt.Errorf("%w: %s", ErrPool, "closed")
	ErrTopology                         = fmt.Errorf("%w %s", ErrBroker, "topology")
	ErrTopologyDeclareFailed            = fmt.Errorf("%w: %s", ErrTopology, "declare failed")
	ErrTopologyDeleteFailed             = fmt.Errorf("%w: %s", ErrTopology, "delete failed")
	ErrTopologyVerifyFailed             = fmt.Errorf("%w: %s", ErrTopology, "verify failed")
	ErrTopologyValidation               = fmt.Errorf("%w %s", ErrTopology, "validation")
	ErrTopologyExchangeNameEmpty        = fmt.Errorf("%w: %s", ErrTopologyValidation, "exchange name empty")
	ErrTopologyQueueNameEmpty           = fmt.Errorf("%w: %s", ErrTopologyValidation, "queue name empty")
	ErrTopologyBindingFieldsEmpty       = fmt.Errorf("%w: %s", ErrTopologyValidation, "binding field(s) empty")
	ErrTopologyRoutingKeyEmpty          = fmt.Errorf("%w: %s", ErrTopologyValidation, "routing key empty")
	ErrEndpoint                         = fmt.Errorf("%w %s", ErrBroker, "endpoint")
	ErrEndpointClosed                   = fmt.Errorf("%w: %s", ErrEndpoint, "closed")
	ErrEndpointNotConnected             = fmt.Errorf("%w: %s", ErrEndpoint, "not connected")
	ErrEndpointNotReadyTimeout          = fmt.Errorf("%w: %s", ErrEndpoint, "not ready within timeout")
	ErrEndpointNoAutoReconnect          = fmt.Errorf("%w: %s", ErrEndpoint, "auto-reconnect is disabled")
	ErrPublisher                        = fmt.Errorf("%w %s", ErrBroker, "publisher")
	ErrPublisherClosed                  = fmt.Errorf("%w: %s", ErrPublisher, "closed")
	ErrPublisherNotConnected            = fmt.Errorf("%w: %s", ErrPublisher, "not connected")
	ErrConsumer                         = fmt.Errorf("%w %s", ErrBroker, "consumer")
	ErrConsumerClosed                   = fmt.Errorf("%w: %s", ErrConsumer, "closed")
	ErrConsumerNotConnected             = fmt.Errorf("%w: %s", ErrConsumer, "not connected")
	ErrMiddleware                       = fmt.Errorf("%w %s", ErrBroker, "middleware")
	ErrMessage                          = fmt.Errorf("%w %s", ErrBroker, "message")
	ErrMessageBuild                     = fmt.Errorf("%w: %s", ErrMessage, "message build failed")
	ErrMessageNotPublished              = fmt.Errorf("%w: %s", ErrMessage, "not a published message (outgoing)")
	ErrMessageNotConsumed               = fmt.Errorf("%w: %s", ErrMessage, "not a consumed message (incoming)")
)

// Error provides structured error information with context.
// This type provides a broker-agnostic interface to AMQP errors,
// allowing users to inspect error details without depending directly
// on the github.com/rabbitmq/amqp091-go package.
type Error struct {
	Operation string // Object/Operation that failed
	Err       error  // Underlying error
}

// Error implements the error interface and formats joined errors compactly.
func (e *Error) Error() string {
	text := "broker"
	if e.Operation != "" {
		text = fmt.Sprintf("%s: %s", text, e.Operation)
	}
	if e.Err == nil {
		return text
	}

	// helper for formatting a wrapped errors
	var format func(err error) string
	format = func(err error) string {
		// this case is already filtered out earlier
		// it is added as a safeguard in case any custom error type
		// implements interface { Unwrap() []error } and returns nil
		if err == nil {
			return "<nil>"
		}
		// if underlying is amqp.Error, format specially
		if amqpErr, ok := err.(*amqp.Error); ok && amqpErr != nil {
			source := "client"
			if amqpErr.Server {
				source = "server"
			}
			return fmt.Sprintf("%s (source=%s, code=%d, recoverable=%v)",
				amqpErr.Reason, source, amqpErr.Code, amqpErr.Recover)
		}
		// if underlying implements interface{ Unwrap() []error }
		// (e.g. errors.Join result), format children compactly (no newlines)
		if joinedErr, ok := err.(interface{ Unwrap() []error }); ok {
			ancestors := joinedErr.Unwrap()
			parts := make([]string, 0, len(ancestors))
			for _, err := range ancestors {
				parts = append(parts, format(err))
			}
			return strings.Join(parts, "; ")
		}
		// more cases as needed ...
		return err.Error()
	}

	return fmt.Sprintf("%s: %s", text, format(e.Err))
}

// Unwrap returns the underlying error (single unwrap).
func (e *Error) Unwrap() error {
	return e.Err
}

// Is makes any *Error compare equal to the sentinel ErrBroker.
// This ensures errors.Is(err, ErrBroker) returns true for any broker Error.
func (e *Error) Is(target error) bool {
	if e == nil || target == nil {
		return false
	}

	// match broker sentinel directly
	if target == ErrBroker {
		return true
	}

	// match any *Error target type (e.g. errors.Is(err, &Error{})).
	if _, ok := target.(*Error); ok {
		return true
	}

	// fallback: delegate to the wrapped error chain (e.Err to avoid recursion)
	return errors.Is(e.Err, target)
}

// wrapError aggregates one or more underlying errors using errors.Join when appropriate.
// It returns a broker *Error whose Unwrap() returns the joined or single error.
// Broker Error provides context about the operation that failed via special handling for amqp.Error.
func wrapError(operation string, errs ...error) error {
	// filter nil errors out
	errs = slices.DeleteFunc(errs, func(err error) bool { return err == nil })
	if len(errs) == 0 {
		return nil
	}

	var err error
	if len(errs) == 1 {
		err = errs[0]
	} else {
		// errors.Join returns an error that implements Unwrap() []error
		err = errors.Join(errs...)
	}

	return &Error{
		Operation: operation,
		Err:       err,
	}
}

// wrapErrorf is a convenience wrapper around wrapError that accepts a format string.
func wrapErrorf(operation string, format string, args ...any) error {
	return wrapError(operation, fmt.Errorf(format, args...))
}
