package internal

import (
	"errors"
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ErrComponent = fmt.Errorf("%w component", ErrBroker)
var ErrComponentClosed = fmt.Errorf("%w: closed", ErrComponent)

// customJoinedError is a test helper that implements Unwrap() []error
// to test the defensive nil check in the format helper.
type customJoinedError struct {
	errs []error
}

func (e *customJoinedError) Error() string {
	return "custom joined error"
}

func (e *customJoinedError) Unwrap() []error {
	return e.errs
}

func TestError(t *testing.T) {
	t.Run("WithAMQPError", func(t *testing.T) {
		amqpErr := &amqp.Error{
			Code:    406,
			Reason:  "PRECONDITION_FAILED",
			Server:  false,
			Recover: true,
		}

		brokerErr := &Error{
			Op:  "consume queue",
			Err: amqpErr,
		}

		errStr := brokerErr.Error()
		assert.Contains(t, errStr, "broker: consume queue")
		assert.Contains(t, errStr, "PRECONDITION_FAILED")
		assert.Contains(t, errStr, "code=406")
		assert.Contains(t, errStr, "source=client")
		assert.Contains(t, errStr, "recoverable=true")
	})

	t.Run("WithGenericError", func(t *testing.T) {
		genericErr := errors.New("connection timeout")
		brokerErr := &Error{
			Op:  "connect",
			Err: genericErr,
		}

		errStr := brokerErr.Error()
		assert.Equal(t, "broker: connect: connection timeout", errStr)
	})

	t.Run("WithJoinedErrors", func(t *testing.T) {
		errA := errors.New("first error")
		errB := errors.New("second error")
		joinedErr := errors.Join(errA, errB)

		brokerErr := &Error{
			Op:  "multiple operations",
			Err: joinedErr,
		}

		errStr := brokerErr.Error()
		assert.Contains(t, errStr, "broker: multiple operations")
		assert.Contains(t, errStr, "first error; second error")
	})

	t.Run("WithNilError", func(t *testing.T) {
		brokerErr := &Error{
			Op:  "test operation",
			Err: nil,
		}

		errStr := brokerErr.Error()
		assert.Equal(t, "broker: test operation", errStr)
	})

	t.Run("UnwrapReturnsUnderlyingError", func(t *testing.T) {
		genericErr := errors.New("original error")
		brokerErr := &Error{
			Op:  "test operation",
			Err: genericErr,
		}

		unwrappedErr := brokerErr.Unwrap()
		assert.Equal(t, genericErr, unwrappedErr)
	})

	t.Run("UnwrapWithNilError", func(t *testing.T) {
		brokerErr := &Error{
			Op:  "test operation",
			Err: nil,
		}

		unwrapped := brokerErr.Unwrap()
		assert.Nil(t, unwrapped)
	})

	t.Run("IsWithNilReceiverAndNilTarget", func(t *testing.T) {
		assert.False(t, (*Error)(nil).Is(nil))
		assert.False(t, (&Error{}).Is(nil))
	})

	t.Run("IsMatchesBrokerSentinelAndTypeError", func(t *testing.T) {
		genericErr := errors.New("some error")
		wrappedErr := Wrap("test", genericErr)
		require.NotNil(t, wrappedErr)

		// should identify as ErrBroker via Is implementation
		assert.True(t, errors.Is(wrappedErr, ErrBroker))
		// should identify as *Error type when target is &Error{}
		assert.True(t, errors.Is(wrappedErr, &Error{}))
		// should NOT match unrelated sentinel
		assert.False(t, errors.Is(wrappedErr, ErrComponent))
	})

	t.Run("IsDelegatesToUnderlyingError", func(t *testing.T) {
		// Error whose underlying cause is a sentinel
		brokerErr := &Error{
			Op:  "test",
			Err: ErrComponentClosed,
		}

		assert.True(t, errors.Is(brokerErr, ErrComponentClosed), "should match underlying sentinel")
		assert.True(t, errors.Is(brokerErr, ErrComponent), "should match parent sentinel")
		assert.True(t, errors.Is(brokerErr, ErrBroker), "should match top-level sentinel")
	})

	t.Run("WithJoinedErrorsContainingNil", func(t *testing.T) {
		customErr := &customJoinedError{
			errs: []error{
				errors.New("first error"),
				nil, // should trigger defensive check
				errors.New("third error"),
			},
		}

		brokerErr := &Error{
			Op:  "test with nil",
			Err: customErr,
		}

		errStr := brokerErr.Error()
		assert.Contains(t, errStr, "broker: test with nil")
		assert.Contains(t, errStr, "first error")
		assert.Contains(t, errStr, "<nil>")
		assert.Contains(t, errStr, "third error")
	})
}

func TestWrap(t *testing.T) {
	t.Run("WithAMQPError", func(t *testing.T) {
		amqpErr := &amqp.Error{
			Code:    320,
			Reason:  "CONNECTION_FORCED",
			Server:  true,
			Recover: true,
		}
		wrapped := Wrap("close connection", amqpErr)

		require.NotNil(t, wrapped)
		brokerErr, ok := wrapped.(*Error)
		require.True(t, ok)
		assert.Equal(t, "close connection", brokerErr.Op)
		assert.Equal(t, amqpErr, brokerErr.Err)

		// verify error message formatting
		errStr := wrapped.Error()
		assert.Contains(t, errStr, "CONNECTION_FORCED")
	})

	t.Run("WithGenericError", func(t *testing.T) {
		originalErr := errors.New("test error")
		wrapped := Wrap("test operation", originalErr)

		require.NotNil(t, wrapped)
		brokerErr, ok := wrapped.(*Error)
		require.True(t, ok)
		assert.Equal(t, "test operation", brokerErr.Op)
		assert.Equal(t, originalErr, brokerErr.Err)
	})

	t.Run("WithNilError", func(t *testing.T) {
		wrapped := Wrap("test operation", nil)
		assert.Nil(t, wrapped)
	})

	t.Run("WithMultipleErrors", func(t *testing.T) {
		errA := errors.New("first error")
		errB := errors.Join(
			errors.New("second error"),
			errors.New("third error"),
			errors.New("fourth error"),
		)
		errC := &Error{
			Op: "fifth error",
			Err: &amqp.Error{
				Code:    123,
				Reason:  "ERROR",
				Server:  true,
				Recover: false,
			},
		}
		errD := (error)(nil) // nil error to be filtered out

		err := Wrap("multiple failures", errA, errB, errC, errD)

		require.NotNil(t, err)
		brokerErr, ok := err.(*Error)
		require.True(t, ok)
		assert.Equal(t, "multiple failures", brokerErr.Op)
		assert.NotNil(t, brokerErr.Err)

		// verify that Unwrap() returns a joined error
		unwrapped := brokerErr.Unwrap()
		var joined interface{ Unwrap() []error }
		require.True(t, errors.As(unwrapped, &joined))
		errs := joined.Unwrap()
		assert.Len(t, errs, 3)
		assert.ErrorIs(t, errA, errs[0])
		assert.ErrorIs(t, errB, errs[1])
		assert.ErrorIs(t, errC, errs[2])

		var errStr string
		{
			// Error.String() prefix
			errStr += "broker"
			// Error.String() concatenation
			errStr += ": "
			// Error.Operation
			errStr += "multiple failures"
			// Error.String() concatenation
			errStr += ": "
			// Wrap errA
			errStr += "first error"
			// Error.String() interface{ Unwrap() []error } separator
			errStr += "; "
			// Wrap errB + Error.String() interface{ Unwrap() []error } reformatting
			errStr += "second error; third error; fourth error"
			// Error.String() interface{ Unwrap() []error } separator
			errStr += "; "
			// Wrap errC + Error.String() prefix + concatenation + Error.Operation
			errStr += "broker: fifth error"
			// Error.String() concatenation
			errStr += ": "
			// Wrap errC.Err + Error.String() amqp.Error reformatting
			errStr += "ERROR (source=server, code=123, recoverable=false)"
		}
		assert.Equal(t, errStr, err.Error())
	})

	t.Run("WithSentinelErrorChaining", func(t *testing.T) {
		err := Wrap("close component", ErrComponentClosed)

		assert.ErrorIs(t, err, ErrComponentClosed)
		assert.ErrorIs(t, err, ErrComponent)
		assert.ErrorIs(t, err, ErrBroker)
	})
}
