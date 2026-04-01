package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

// mockChannel is a mock implementation of the Channel interface for testing.
// Configuration methods (withXxx) must be called before concurrent use.
type mockChannel struct {
	// control behavior (set once during initialization)
	closeDelay        time.Duration
	closeError        *amqp091.Error
	shouldClose       bool
	operationDuration time.Duration

	// runtime state (accessed concurrently)
	notifyCloseCh     chan *Error
	notifyCloseCalled atomic.Bool

	// synchronization for concurrent access to runtime state
	mu sync.Mutex
}

func newMockChannel() *mockChannel {
	return &mockChannel{}
}

// withClose configures the mock to close the channel with the given error after the specified delay.
// Must be called before concurrent use.
func (m *mockChannel) withClose(err *amqp091.Error, delay time.Duration) *mockChannel {
	m.shouldClose = true
	m.closeError = err
	m.closeDelay = delay
	return m
}

// withOperationDuration configures how long operations should take to complete.
// Must be called before concurrent use.
func (m *mockChannel) withOperationDuration(d time.Duration) *mockChannel {
	m.operationDuration = d
	return m
}

// triggerClose safely closes the notify channel, simulating a channel close event.
// Can be called concurrently from tests.
func (m *mockChannel) triggerClose() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.notifyCloseCh != nil {
		if m.closeError != nil {
			m.notifyCloseCh <- m.closeError
		}
		close(m.notifyCloseCh)
		m.notifyCloseCh = nil
	}
}

func (m *mockChannel) NotifyClose(receiver chan *Error) chan *Error {
	m.notifyCloseCalled.Store(true)
	m.mu.Lock()
	m.notifyCloseCh = receiver
	m.mu.Unlock()

	if m.shouldClose {
		go func() {
			time.Sleep(m.closeDelay)
			m.triggerClose()
		}()
	}
	return receiver
}

func (m *mockChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) error {
	return nil
}

func (m *mockChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) (*DeferredConfirmation, error) {
	return nil, nil
}

func (m *mockChannel) Confirm(noWait bool) error {
	return nil
}

func (m *mockChannel) NotifyPublish(confirm chan Confirmation) chan Confirmation {
	return confirm
}

func (m *mockChannel) NotifyReturn(returns chan Return) chan Return {
	return returns
}

func (m *mockChannel) NotifyFlow(flow chan bool) chan bool {
	return flow
}

func (m *mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args Arguments) (<-chan Delivery, error) {
	return nil, nil
}

func (m *mockChannel) Get(queue string, autoAck bool) (msg Delivery, ok bool, err error) {
	return Delivery{}, false, nil
}

func (m *mockChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return nil
}

func (m *mockChannel) Cancel(consumer string, noWait bool) error {
	return nil
}

func (m *mockChannel) NotifyCancel(cancellations chan string) chan string {
	return cancellations
}

func (m *mockChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args Arguments) error {
	time.Sleep(m.operationDuration)
	return nil
}

func (m *mockChannel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args Arguments) error {
	return nil
}

func (m *mockChannel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return nil
}

func (m *mockChannel) ExchangeBind(destination, key, source string, noWait bool, args Arguments) error {
	return nil
}

func (m *mockChannel) ExchangeUnbind(destination, key, source string, noWait bool, args Arguments) error {
	return nil
}

func (m *mockChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args Arguments) (Queue, error) {
	time.Sleep(m.operationDuration)
	return Queue{}, nil
}

func (m *mockChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args Arguments) (Queue, error) {
	return Queue{}, nil
}

func (m *mockChannel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	time.Sleep(m.operationDuration)
	return 0, nil
}

func (m *mockChannel) QueueBind(name, key, exchange string, noWait bool, args Arguments) error {
	return nil
}

func (m *mockChannel) QueueUnbind(name, key, exchange string, args Arguments) error {
	return nil
}

func (m *mockChannel) QueuePurge(name string, noWait bool) (int, error) {
	return 0, nil
}

func (m *mockChannel) Tx() error {
	return nil
}

func (m *mockChannel) TxCommit() error {
	return nil
}

func (m *mockChannel) TxRollback() error {
	return nil
}

func (m *mockChannel) Close() error {
	return nil
}

func (m *mockChannel) IsClosed() bool {
	return false
}

func TestDoSafeChannelAction(t *testing.T) {
	t.Run("WhenSuccessful", func(t *testing.T) {
		ch := newMockChannel()

		err := DoSafeChannelAction(ch, func(ch Channel) error {
			return ch.ExchangeDeclare("test", "topic", true, false, false, false, nil)
		})

		assert.NoError(t, err)
		assert.True(t, ch.notifyCloseCalled.Load(), "expected NotifyClose to be called")
	})

	t.Run("WithNilChannel", func(t *testing.T) {
		err := DoSafeChannelAction(nil, func(ch Channel) error {
			return nil
		})

		assert.Error(t, err, "expected error for nil channel")
		assert.ErrorIs(t, err, ErrChannel)
	})

	t.Run("WhenOperationFails", func(t *testing.T) {
		ch := newMockChannel()
		expectedErr := errors.New("operation failed")

		err := DoSafeChannelAction(ch, func(ch Channel) error {
			return expectedErr
		})

		assert.Error(t, err, "expected error from operation")
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("WhenChannelClosedDuringOperation", func(t *testing.T) {
		closeErr := &amqp091.Error{
			Code:   406,
			Reason: "PRECONDITION_FAILED",
			Server: true,
		}

		ch := newMockChannel().
			withClose(closeErr, 10*time.Millisecond).
			withOperationDuration(50 * time.Millisecond)

		err := DoSafeChannelAction(ch, func(ch Channel) error {
			return ch.ExchangeDeclare("test", "topic", true, false, false, false, nil)
		})

		assert.Error(t, err, "expected error when channel closes")
		assert.NotEmpty(t, err.Error(), "expected non-empty error message")
		assert.ErrorIs(t, err, closeErr)
	})

	t.Run("WhenChannelClosedBeforeCompletion", func(t *testing.T) {
		closeErr := &amqp091.Error{
			Code:   320,
			Reason: "CONNECTION_FORCED",
			Server: true,
		}

		ch := newMockChannel().
			withClose(closeErr, 1*time.Millisecond).
			withOperationDuration(100 * time.Millisecond)

		err := DoSafeChannelAction(ch, func(ch Channel) error {
			return ch.ExchangeDeclare("test", "topic", true, false, false, false, nil)
		})

		assert.Error(t, err, "expected error when channel closes")
		assert.ErrorIs(t, err, closeErr)
	})

	t.Run("WithBothOperationAndChannelErrors", func(t *testing.T) {
		closeErr := &amqp091.Error{
			Code:   404,
			Reason: "NOT_FOUND",
			Server: true,
		}

		ch := newMockChannel().withClose(closeErr, 5*time.Millisecond)

		operationErr := errors.New("operation failed")

		err := DoSafeChannelAction(ch, func(ch Channel) error {
			time.Sleep(10 * time.Millisecond)
			return operationErr
		})

		assert.Error(t, err)
		assert.ErrorIs(t, err, closeErr)
	})

	t.Run("DelegatesToWithReturn", func(t *testing.T) {
		ch := newMockChannel()

		var operationCalled bool

		err := DoSafeChannelAction(ch, func(ch Channel) error {
			operationCalled = true
			return nil
		})

		assert.NoError(t, err)
		assert.True(t, operationCalled, "expected operation to be called")
		assert.True(t, ch.notifyCloseCalled.Load(), "expected NotifyClose to be called")
	})

	t.Run("Concurrent", func(t *testing.T) {
		ch := newMockChannel()

		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func(i int) {
				err := DoSafeChannelAction(ch, func(ch Channel) error {
					time.Sleep(time.Millisecond)
					return nil
				})
				assert.NoError(t, err)
				done <- true
			}(i)
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestDoSafeChannelActionWithReturn(t *testing.T) {
	t.Run("WhenSuccessful", func(t *testing.T) {
		t.Run("Int", func(t *testing.T) {
			ch := newMockChannel()

			count, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (int, error) {
				return ch.QueueDelete("test", false, false, false)
			})

			assert.NoError(t, err)
			assert.Equal(t, 0, count)
		})

		t.Run("Queue", func(t *testing.T) {
			ch := newMockChannel()

			queue, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (Queue, error) {
				return ch.QueueDeclare("test", true, false, false, false, nil)
			})

			assert.NoError(t, err)
			assert.Empty(t, queue.Name)
		})

		t.Run("String", func(t *testing.T) {
			ch := newMockChannel()

			result, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (string, error) {
				return "success", nil
			})

			assert.NoError(t, err)
			assert.Equal(t, "success", result)
		})
	})

	t.Run("WithNilChannel", func(t *testing.T) {
		_, err := DoSafeChannelActionWithReturn(nil, func(ch Channel) (int, error) {
			return 42, nil
		})

		assert.Error(t, err, "expected error for nil channel")
		assert.ErrorIs(t, err, ErrChannel)
	})

	t.Run("WhenOperationFails", func(t *testing.T) {
		ch := newMockChannel()
		expectedErr := errors.New("queue not found")

		count, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (int, error) {
			return 0, expectedErr
		})

		assert.Error(t, err, "expected error from operation")
		assert.ErrorIs(t, err, expectedErr)
		assert.Equal(t, 0, count, "expected zero value for count")
	})

	t.Run("WhenChannelClosed", func(t *testing.T) {
		closeErr := &amqp091.Error{
			Code:   406,
			Reason: "PRECONDITION_FAILED",
			Server: true,
		}

		ch := newMockChannel().
			withClose(closeErr, 10*time.Millisecond).
			withOperationDuration(50 * time.Millisecond)

		queue, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (Queue, error) {
			return ch.QueueDeclare("test", true, false, false, false, nil)
		})

		assert.Error(t, err, "expected error when channel closes")
		assert.ErrorIs(t, err, closeErr)
		assert.Empty(t, queue.Name, "expected zero value Queue")
	})

	t.Run("WithBothOperationAndChannelErrors", func(t *testing.T) {
		closeErr := &amqp091.Error{
			Code:   403,
			Reason: "ACCESS_REFUSED",
			Server: true,
		}

		ch := newMockChannel().withClose(closeErr, 5*time.Millisecond)

		operationErr := fmt.Errorf("access denied")

		result, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (string, error) {
			time.Sleep(10 * time.Millisecond)
			return "", operationErr
		})

		assert.Error(t, err)
		assert.ErrorIs(t, err, closeErr)
		assert.Empty(t, result, "expected empty string")
	})

	t.Run("ReturnsZeroValueOnError", func(t *testing.T) {
		ch := newMockChannel()

		t.Run("Int", func(t *testing.T) {
			result, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (int, error) {
				return 42, errors.New("error")
			})

			assert.Error(t, err)
			assert.Equal(t, 0, result, "expected zero value (0)")
		})

		t.Run("String", func(t *testing.T) {
			result, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (string, error) {
				return "value", errors.New("error")
			})

			assert.Error(t, err)
			assert.Empty(t, result, "expected zero value (empty string)")
		})

		t.Run("Pointer", func(t *testing.T) {
			type testStruct struct {
				Value int
			}

			result, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (*testStruct, error) {
				return &testStruct{Value: 42}, errors.New("error")
			})

			assert.Error(t, err)
			assert.Nil(t, result, "expected zero value (nil)")
		})
	})
}
