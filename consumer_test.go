package broker

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerOptions(t *testing.T) {
	// make sure that these are changed only intentionally
	opts := defaultConsumerOptions()
	assert.False(t, opts.AutoAck)
	assert.Equal(t, defaultPrefetchCount, opts.PrefetchCount)
	assert.False(t, opts.NoWait)
	assert.False(t, opts.Exclusive)
	assert.False(t, opts.NoAutoDeclare)
	assert.False(t, opts.NoWaitReady)
	assert.Equal(t, defaultReadyTimeout, opts.ReadyTimeout)
	assert.False(t, opts.NoAutoReconnect)
	assert.Equal(t, defaultReconnectMin, opts.ReconnectMin)
	assert.Equal(t, defaultReconnectMax, opts.ReconnectMax)
	assert.Equal(t, defaultConcurrentHandlers, opts.MaxConcurrentHandlers)

	t.Run("OnCancel", func(t *testing.T) {
		var capturedTag string
		opts := ConsumerOptions{
			OnCancel: func(tag string) {
				capturedTag = tag
			},
		}

		require.NotNil(t, opts.OnCancel)
		opts.OnCancel("test-tag")
		assert.Equal(t, "test-tag", capturedTag)
	})

	t.Run("OnError", func(t *testing.T) {
		var capturedErr error
		opts := ConsumerOptions{
			OnError: func(err error) {
				capturedErr = err
			},
		}

		require.NotNil(t, opts.OnError)
		testErr := assert.AnError
		opts.OnError(testErr)
		assert.Equal(t, testErr, capturedErr)
	})
}

func TestNewConsumer(t *testing.T) {
	b := &Broker{id: "test-broker"}

	q := NewQueue("test-queue")

	opts := defaultConsumerOptions()

	c := newConsumer("test-consumer", b, opts, q, testHandler(HandlerActionAck))

	require.NotNil(t, c)
	assert.NotNil(t, c.endpoint)
	assert.Equal(t, "test-consumer", c.id)
	assert.Equal(t, b, c.broker)
	assert.Equal(t, roleConsumer, c.role)
	assert.Equal(t, opts, c.opts)
	assert.Equal(t, q, c.queue)
	assert.NotNil(t, c.handler)

	t.Run("State", func(t *testing.T) {
		b := &Broker{id: "test-broker"}

		q := NewQueue("test-queue")

		c := newConsumer("test-consumer", b, defaultConsumerOptions(), q, testHandler(HandlerActionAck))

		assert.Nil(t, c.cancelCh)
		assert.Nil(t, c.deliveryCh)
		assert.Nil(t, c.closeCh)
		assert.Zero(t, c.deliveries.Load())
		assert.False(t, c.cancelled.Load())
		assert.False(t, c.ready.Load())
		assert.False(t, c.closed.Load())

		// simulate channels being set (normally done in connect)
		c.stateMu.Lock()
		c.cancelCh = make(<-chan string)
		c.deliveryCh = make(<-chan amqp.Delivery)
		c.closeCh = make(<-chan *amqp.Error)
		c.stateMu.Unlock()

		// disconnect should clear them
		err := c.disconnect(nil)
		require.NoError(t, err)

		c.stateMu.RLock()
		assert.Nil(t, c.cancelCh)
		assert.Nil(t, c.deliveryCh)
		assert.Nil(t, c.closeCh)
		c.stateMu.RUnlock()
	})
}

func TestConsumerRelease(t *testing.T) {
	t.Run("RemovesFromBroker", func(t *testing.T) {
		b := &Broker{
			consumers: make(map[string]Consumer),
		}
		ctx, cancel := context.WithCancel(t.Context())
		b.ctx = ctx
		b.cancel = cancel
		defer cancel()

		q := NewQueue("test-queue")
		opts := defaultConsumerOptions()

		c := newConsumer("test-consumer", b, opts, q, nil)
		b.consumers["test-consumer"] = c

		assert.Len(t, b.consumers, 1)

		c.Release()

		assert.Empty(t, b.consumers)
		t.Run("WithNoBroker", func(t *testing.T) {
			// create endpoint with minimal initialization
			e := &endpoint{broker: nil}
			c := &consumer{endpoint: e}
			// should not panic
			c.Release()
		})
	})

	t.Run("ClosesConsumer", func(t *testing.T) {
		b := &Broker{
			consumers: make(map[string]Consumer),
		}
		ctx, cancel := context.WithCancel(t.Context())
		b.ctx = ctx
		b.cancel = cancel
		defer cancel()

		opts := defaultConsumerOptions()
		q := NewQueue("test-queue")

		c := newConsumer("test-consumer", b, opts, q, nil)
		b.consumers["test-consumer"] = c

		c.Release()

		assert.True(t, c.closed.Load())
	})
}

func TestConsumerWait(t *testing.T) {
	t.Run("WithNoDeliveries", func(t *testing.T) {
		c := &consumer{}

		start := time.Now()
		c.Wait()
		duration := time.Since(start)

		// should return immediately
		assert.Less(t, duration, 1*time.Millisecond)
	})

	t.Run("WithSingleCaller", func(t *testing.T) {
		c := &consumer{}
		c.deliveries.Store(3)

		doneCh := make(chan struct{})
		go func() {
			c.Wait()
			close(doneCh)
		}()

		// simulate processing completion
		for range c.deliveries.Load() {
			time.Sleep(50 * time.Millisecond)
			c.deliveries.Add(-1)
		}

		select {
		case <-doneCh:
			// Wait() returned after processing completed
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Wait() did not return after processing completed")
		}
	})

	t.Run("WithMultipleCallers", func(t *testing.T) {
		c := &consumer{}
		c.deliveries.Store(3)

		waiters := 5
		doneCh := make(chan struct{}, waiters)

		for range waiters {
			go func() {
				c.Wait()
				doneCh <- struct{}{}
			}()
		}

		// complete processing
		time.Sleep(50 * time.Millisecond)
		c.deliveries.Store(0)

		// all waiters should complete
		for i := range waiters {
			select {
			case <-doneCh:
				// expected
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("waiter %d did not complete", i)
			}
		}
	})
}

func TestConsumerCancel(t *testing.T) {
	b := &Broker{
		topologyMgr: newTopologyManager(),
	}

	t.Run("Idempotency", func(t *testing.T) {
		opts := defaultConsumerOptions()
		q := NewQueue("test-queue")

		c := newConsumer("test-consumer", b, opts, q, testHandler(HandlerActionAck))

		// first cancel
		err := c.Cancel()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConsumerNotConnected)

		c.cancelled.Store(true)

		// second cancel should be no-op
		err = c.Cancel()
		assert.NoError(t, err)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		opts := defaultConsumerOptions()
		q := NewQueue("test-queue")

		c := newConsumer("test-consumer", b, opts, q, testHandler(HandlerActionAck))

		// try to cancel without channel
		err := c.Cancel()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConsumerNotConnected)
	})
}

func TestConsumerGet(t *testing.T) {
	b := &Broker{
		topologyMgr: newTopologyManager(),
	}

	t.Run("WithNoChannel", func(t *testing.T) {
		opts := defaultConsumerOptions()
		q := NewQueue("test-queue")

		c := newConsumer("test-consumer", b, opts, q, testHandler(HandlerActionAck))
		// channel is nil initially
		msg, err := c.Get()
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.ErrorIs(t, err, ErrConsumerNotConnected)
	})
}

func TestConsumerConsume(t *testing.T) {
	b := &Broker{
		topologyMgr: newTopologyManager(),
	}

	t.Run("WithCancelledContext", func(t *testing.T) {
		opts := defaultConsumerOptions()
		q := NewQueue("test-queue")

		c := newConsumer("test-consumer", b, opts, q, nil)

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // cancel immediately

		err := c.Consume(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("WhenContextCancelled", func(t *testing.T) {
		opts := defaultConsumerOptions()
		q := NewQueue("test-queue")

		c := newConsumer("test-consumer", b, opts, q, nil)

		ctx, cancel := context.WithCancel(t.Context())

		errCh := make(chan error, 1)
		go func() {
			errCh <- c.Consume(ctx)
		}()

		// give it some time to start
		time.Sleep(50 * time.Millisecond)
		cancel() // signal done

		select {
		case err := <-errCh:
			require.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Consume() did not return after context cancellation")
		}
	})
}
