package broker

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestError(t *testing.T) {
	t.Run("WithAMQPError", func(t *testing.T) {
		amqpErr := &amqp.Error{
			Code:    406,
			Reason:  "PRECONDITION_FAILED",
			Server:  false,
			Recover: true,
		}

		brokerErr := &Error{
			Operation: "consume queue",
			Err:       amqpErr,
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
			Operation: "connect",
			Err:       genericErr,
		}

		errStr := brokerErr.Error()
		assert.Equal(t, "broker: connect: connection timeout", errStr)
	})

	t.Run("WithJoinedErrors", func(t *testing.T) {
		errA := errors.New("first error")
		errB := errors.New("second error")
		joinedErr := errors.Join(errA, errB)

		brokerErr := &Error{
			Operation: "multiple operations",
			Err:       joinedErr,
		}

		errStr := brokerErr.Error()
		assert.Contains(t, errStr, "broker: multiple operations")
		assert.Contains(t, errStr, "first error; second error")
	})

	t.Run("WithNilError", func(t *testing.T) {
		brokerErr := &Error{
			Operation: "test operation",
			Err:       nil,
		}

		errStr := brokerErr.Error()
		assert.Equal(t, "broker: test operation", errStr)
	})

	t.Run("UnwrapReturnsUnderlyingError", func(t *testing.T) {
		genericErr := errors.New("original error")
		brokerErr := &Error{
			Operation: "test operation",
			Err:       genericErr,
		}

		unwrappedErr := brokerErr.Unwrap()
		assert.Equal(t, genericErr, unwrappedErr)
	})

	t.Run("UnwrapWithNilError", func(t *testing.T) {
		brokerErr := &Error{
			Operation: "test operation",
			Err:       nil,
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
		wrappedErr := wrapError("test", genericErr)
		require.NotNil(t, wrappedErr)

		// should identify as ErrBroker via Is implementation
		assert.True(t, errors.Is(wrappedErr, ErrBroker))
		// should identify as *Error type when target is &Error{}
		assert.True(t, errors.Is(wrappedErr, &Error{}))
		// should NOT match unrelated sentinel
		assert.False(t, errors.Is(wrappedErr, ErrEndpoint))
	})

	t.Run("IsDelegatesToUnderlyingError", func(t *testing.T) {
		// Error whose underlying cause is a sentinel
		brokerErr := &Error{
			Operation: "test",
			Err:       ErrEndpointClosed,
		}

		assert.True(t, errors.Is(brokerErr, ErrEndpointClosed), "should match underlying sentinel")
		assert.True(t, errors.Is(brokerErr, ErrEndpoint), "should match parent sentinel")
		assert.True(t, errors.Is(brokerErr, ErrBroker), "should match top-level sentinel")
	})
}

func TestSentinelErrors(t *testing.T) {
	// implementation
	assert.Implements(t, (*error)(nil), ErrBroker)

	t.Run("Hierarchy", func(t *testing.T) {
		// wrapping hierarchy
		assert.ErrorIs(t, ErrConnection, ErrBroker)
		assert.ErrorIs(t, ErrChannel, ErrBroker)
		assert.ErrorIs(t, ErrPool, ErrBroker)
		assert.ErrorIs(t, ErrTopology, ErrBroker)
		assert.ErrorIs(t, ErrEndpoint, ErrBroker)
		assert.ErrorIs(t, ErrPublisher, ErrBroker)
		assert.ErrorIs(t, ErrConsumer, ErrBroker)
		assert.ErrorIs(t, ErrMessage, ErrBroker)
	})

	t.Run("Broker", func(t *testing.T) {
		assert.ErrorIs(t, ErrBrokerClosed, ErrBroker)
		assert.ErrorIs(t, ErrBrokerConfigInvalid, ErrBroker)
	})

	t.Run("Connection", func(t *testing.T) {
		assert.ErrorIs(t, ErrConnection, ErrBroker)
		assert.ErrorIs(t, ErrConnectionClosed, ErrConnection)
		assert.ErrorIs(t, ErrConnectionManager, ErrConnection)
		assert.ErrorIs(t, ErrConnectionManagerClosed, ErrConnectionManager)
	})

	t.Run("Channel", func(t *testing.T) {
		assert.ErrorIs(t, ErrChannel, ErrBroker)
		assert.ErrorIs(t, ErrChannel, ErrConnection)
		assert.ErrorIs(t, ErrChannelClosed, ErrChannel)
	})

	t.Run("Pool", func(t *testing.T) {
		assert.ErrorIs(t, ErrPool, ErrBroker)
		assert.ErrorIs(t, ErrPoolClosed, ErrPool)
	})

	t.Run("Topology", func(t *testing.T) {
		assert.ErrorIs(t, ErrTopology, ErrBroker)
		assert.ErrorIs(t, ErrTopologyDeclareFailed, ErrTopology)
		assert.ErrorIs(t, ErrTopologyDeleteFailed, ErrTopology)
		assert.ErrorIs(t, ErrTopologyVerifyFailed, ErrTopology)
		assert.ErrorIs(t, ErrTopologyValidation, ErrTopology)
		assert.ErrorIs(t, ErrTopologyExchangeNameEmpty, ErrTopology)
		assert.ErrorIs(t, ErrTopologyQueueNameEmpty, ErrTopology)
		assert.ErrorIs(t, ErrTopologyBindingFieldsEmpty, ErrTopology)
	})

	t.Run("Endpoint", func(t *testing.T) {
		assert.ErrorIs(t, ErrEndpoint, ErrBroker)
		assert.ErrorIs(t, ErrEndpointClosed, ErrEndpoint)
		assert.ErrorIs(t, ErrEndpointNotConnected, ErrEndpoint)
		assert.ErrorIs(t, ErrEndpointNotReadyTimeout, ErrEndpoint)
		assert.ErrorIs(t, ErrEndpointNoAutoReconnect, ErrEndpoint)
	})

	t.Run("Publisher", func(t *testing.T) {
		assert.ErrorIs(t, ErrPublisher, ErrBroker)
		assert.ErrorIs(t, ErrPublisherClosed, ErrPublisher)
	})

	t.Run("Consumer", func(t *testing.T) {
		assert.ErrorIs(t, ErrConsumer, ErrBroker)
		assert.ErrorIs(t, ErrConsumerClosed, ErrConsumer)
	})

	t.Run("Message", func(t *testing.T) {
		assert.ErrorIs(t, ErrMessage, ErrBroker)
		assert.ErrorIs(t, ErrMessageBuild, ErrMessage)
		assert.ErrorIs(t, ErrMessageNotConsumed, ErrMessage)
		assert.ErrorIs(t, ErrMessageNotPublished, ErrMessage)
	})
}

func TestWrapError(t *testing.T) {
	t.Run("WithAMQPError", func(t *testing.T) {
		amqpErr := &amqp.Error{
			Code:    320,
			Reason:  "CONNECTION_FORCED",
			Server:  true,
			Recover: true,
		}
		wrapped := wrapError("close connection", amqpErr)

		require.NotNil(t, wrapped)
		brokerErr, ok := wrapped.(*Error)
		require.True(t, ok)
		assert.Equal(t, "close connection", brokerErr.Operation)
		assert.Equal(t, amqpErr, brokerErr.Err)

		// verify error message formatting
		errStr := wrapped.Error()
		assert.Contains(t, errStr, "CONNECTION_FORCED")
	})

	t.Run("WithGenericError", func(t *testing.T) {
		originalErr := errors.New("test error")
		wrapped := wrapError("test operation", originalErr)

		require.NotNil(t, wrapped)
		brokerErr, ok := wrapped.(*Error)
		require.True(t, ok)
		assert.Equal(t, "test operation", brokerErr.Operation)
		assert.Equal(t, originalErr, brokerErr.Err)
	})

	t.Run("WithNilError", func(t *testing.T) {
		wrapped := wrapError("test operation", nil)
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
			Operation: "fifth error",
			Err: &amqp.Error{
				Code:    123,
				Reason:  "ERROR",
				Server:  true,
				Recover: false,
			},
		}
		errD := (error)(nil) // nil error to be filtered out

		err := wrapError("multiple failures", errA, errB, errC, errD)

		require.NotNil(t, err)
		brokerErr, ok := err.(*Error)
		require.True(t, ok)
		assert.Equal(t, "multiple failures", brokerErr.Operation)
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
			// wrapError errA
			errStr += "first error"
			// Error.String() interface{ Unwrap() []error } separator
			errStr += "; "
			// wrapError errB + Error.String() interface{ Unwrap() []error } reformatting
			errStr += "second error; third error; fourth error"
			// Error.String() interface{ Unwrap() []error } separator
			errStr += "; "
			// wrapError errC + Error.String() prefix + concatenation + Error.Operation
			errStr += "broker: fifth error"
			// Error.String() concatenation
			errStr += ": "
			// wrapError errC.Err + Error.String() amqp.Error reformatting
			errStr += "ERROR (source=server, code=123, recoverable=false)"
		}
		assert.Equal(t, errStr, err.Error())
	})

	t.Run("WithSentinelErrorChaining", func(t *testing.T) {
		err := wrapError("close endpoint", ErrEndpointClosed)

		assert.ErrorIs(t, err, ErrEndpointClosed)
		assert.ErrorIs(t, err, ErrEndpoint)
		assert.ErrorIs(t, err, ErrBroker)
	})
}

func TestWrapErrorf(t *testing.T) {
	wrapped := wrapErrorf("create object", "pool index %d, size %d", 5, 10)

	require.NotNil(t, wrapped)
	brokerErr, ok := wrapped.(*Error)
	require.True(t, ok)
	assert.Equal(t, "create object", brokerErr.Operation)
	errStr := wrapped.Error()
	assert.Contains(t, errStr, "pool index 5")
	assert.Contains(t, errStr, "size 10")
}
