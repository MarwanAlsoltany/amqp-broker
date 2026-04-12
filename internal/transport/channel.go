package transport

import (
	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
)

var (
	// ErrChannel is the base error for channel operations.
	// All channel-related errors wrap this error.
	ErrChannel = ErrTransport.Derive("channel")

	// ErrChannelClosed indicates the channel is closed.
	// This error is returned when operations are attempted on a closed channel.
	ErrChannelClosed = ErrChannel.Derive("closed")
)

// DoSafeChannelAction executes a channel operation while monitoring for channel closure.
// It registers a close notification handler and executes the provided operation function.
// If the channel closes during the operation, it returns both the operation error (if any)
// and a wrapped error indicating the channel was closed.
//
// This function is useful for topology operations that might trigger channel closure
// due to AMQP protocol errors (e.g., PreconditionFailed, AccessRefused, NotFound).
//
// Usage examples:
//
//	err := amqp.DoSafeChannelAction(ch, func(ch Channel) error {
//	    return ch.ExchangeDeclare(...)
//	})
func DoSafeChannelAction(ch Channel, op func(Channel) error) error {
	_, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (struct{}, error) {
		return struct{}{}, op(ch)
	})
	return err
}

// DoSafeChannelActionWithReturn is similar to [DoSafeChannelAction] but supports operations that return a value.
// It is generic and can handle operations that return any type along with an error.
//
// Usage examples:
//
//	count, err := amqp.DoSafeChannelActionWithReturn(ch, func(ch Channel) (int, error) {
//	    return ch.QueueDelete(...)
//	})
//
//	// or for operations that don't return a value:
//	_, err := amqp.DoSafeChannelActionWithReturn(ch, func(ch Channel) (struct{}, error) {
//	    return struct{}{}, ch.ExchangeDeclare(...)
//	})
func DoSafeChannelActionWithReturn[T any](ch Channel, op func(Channel) (T, error)) (T, error) {
	var zero T

	if ch == nil {
		return zero, ErrChannel.Detail("not available")
	}

	// create a buffered channel to avoid deadlock
	// library sends notification once, then closes the channel
	closeCh := ch.NotifyClose(make(chan *Error, 1))

	// create channel to communicate results
	type result struct {
		value T
		err   error
	}
	resultCh := make(chan result, 1)

	// execute the operation in a goroutine
	go func() {
		value, err := op(ch)
		resultCh <- result{value, err}
	}()

	// wait for either operation completion or channel closure
	var opResult result
	var chCloseErr *amqp091.Error

	select {
	case opResult = <-resultCh:
		// operation completed first, check if there's a close notification
		select {
		case chCloseErr = <-closeCh:
			// channel was closed during or immediately after the operation
		default:
			// channel is still open
		}
	case chCloseErr = <-closeCh:
		// channel closed before operation completed
		// wait briefly for operation to complete
		select {
		case opResult = <-resultCh:
			// operation completed after channel closed
		default:
			// operation didn't complete yet
		}
	}

	// combine operation error and channel close error
	if opResult.err == nil && chCloseErr == nil {
		return opResult.value, nil
	}

	if chCloseErr != nil {
		// return an error that lets callers inspect both errors
		return zero, internal.ErrDomain.Wrap("channel closed during operation", opResult.err, chCloseErr)
	}

	// only operation error
	return zero, opResult.err
}
