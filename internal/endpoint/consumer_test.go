package endpoint

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/handler"
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHandler returns a handler that always returns the given [handler.Action].
func testHandler(action handler.Action) handler.Handler {
	return func(_ context.Context, _ *message.Message) (handler.Action, error) {
		return action, nil
	}
}

// testErrorHandler returns a handler that always returns the given [handler.Action] and error.
func testErrorHandler(action handler.Action, err error) handler.Handler {
	return func(_ context.Context, _ *message.Message) (handler.Action, error) {
		return action, err
	}
}

// testCountingHandler returns a handler that increments the counter and returns the action.
func testCountingHandler(action handler.Action, counter *atomic.Int32) handler.Handler {
	return func(_ context.Context, _ *message.Message) (handler.Action, error) {
		counter.Add(1)
		return action, nil
	}
}

func TestConsumerOptions(t *testing.T) {
	opts := DefaultConsumerOptions()

	assert.False(t, opts.AutoAck)
	assert.Equal(t, DefaultPrefetchCount, opts.PrefetchCount)
	assert.False(t, opts.NoWait)
	assert.False(t, opts.Exclusive)
	assert.False(t, opts.NoAutoDeclare)
	assert.False(t, opts.NoWaitReady)
	assert.Equal(t, DefaultReadyTimeout, opts.ReadyTimeout)
	assert.False(t, opts.NoAutoReconnect)
	assert.Equal(t, DefaultReconnectMin, opts.ReconnectMin)
	assert.Equal(t, DefaultReconnectMax, opts.ReconnectMax)
	assert.Equal(t, DefaultConcurrentHandlers, opts.MaxConcurrentHandlers)

	t.Run("OnCancel", func(t *testing.T) {
		var capturedTag string
		opts := ConsumerOptions{
			OnCancel: func(tag string) { capturedTag = tag },
		}
		require.NotNil(t, opts.OnCancel)
		opts.OnCancel("test-tag")
		assert.Equal(t, "test-tag", capturedTag)
	})

	t.Run("OnError", func(t *testing.T) {
		var capturedErr error
		opts := ConsumerOptions{
			OnError: func(err error) { capturedErr = err },
		}
		require.NotNil(t, opts.OnError)
		opts.OnError(assert.AnError)
		assert.Equal(t, assert.AnError, capturedErr)
	})
}

func TestNewConsumer(t *testing.T) {
	topoReg := topology.NewRegistry()
	q := topology.NewQueue("test-queue")
	opts := DefaultConsumerOptions()
	h := testHandler(handler.ActionAck)

	c := newConsumer("test-consumer", nil, topoReg, opts, q, h)

	require.NotNil(t, c)
	require.NotNil(t, c.endpoint)
	assert.Equal(t, "test-consumer", c.id)
	assert.Equal(t, roleConsumer, c.role)
	assert.Equal(t, opts, c.opts)
	assert.Equal(t, q, c.queue)
	assert.NotNil(t, c.handler)

	t.Run("State", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), testHandler(handler.ActionAck))

		c.stateMu.RLock()
		assert.Nil(t, c.cancelCh)
		assert.Nil(t, c.deliveryCh)
		assert.Nil(t, c.closeCh)
		c.stateMu.RUnlock()

		assert.Zero(t, c.deliveries.Load())
		assert.False(t, c.cancelled.Load())
		assert.False(t, c.ready.Load())
		assert.False(t, c.closed.Load())
	})
}

func TestNewConsumerInterface(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("Success", func(t *testing.T) {
		cm := newTestConnectionManager(newMockConnection())
		c, err := NewConsumer(t.Context(), "test", cm, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{
				NoAutoDeclare:   true,
				NoAutoReconnect: true,
				ReconnectMin:    DefaultReconnectMin,
				ReconnectMax:    DefaultReconnectMax,
			},
		}, topology.NewQueue("queue"), testHandler(handler.ActionAck))
		require.NoError(t, err)
		require.NotNil(t, c)
		defer c.Close()
		assert.True(t, c.Ready())
		// verify the interface is returned, not the concrete type
		var _ Consumer = c
	})

	t.Run("WithInvalidOptions", func(t *testing.T) {
		c, err := NewConsumer(t.Context(), "test", nil, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: -1},
		}, topology.NewQueue("queue"), testHandler(handler.ActionAck))
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConsumer)
		assert.Nil(t, c)
	})

	t.Run("WithConnectionError", func(t *testing.T) {
		// ConnectionManager with no Init() -> pool slot is nil -> Assign returns error
		cm := transport.NewConnectionManager("amqp://invalid", &transport.ConnectionManagerOptions{Size: 1})
		c, err := NewConsumer(t.Context(), "test", cm, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{
				NoAutoDeclare: true,
				ReconnectMin:  DefaultReconnectMin,
				ReconnectMax:  DefaultReconnectMax,
			},
		}, topology.NewQueue("queue"), nil)
		assert.Error(t, err)
		assert.Nil(t, c)
	})
}

func TestConsumerConsume(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WithCancelledContext", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := c.Consume(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("WhenContextCancelled", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)

		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() { errCh <- c.Consume(ctx) }()

		time.Sleep(30 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Consume() did not return after context cancellation")
		}
	})
}

func TestConsumerWait(t *testing.T) {
	t.Run("WithNoDeliveries", func(t *testing.T) {
		c := &consumer{}

		start := time.Now()
		c.Wait()
		assert.Less(t, time.Since(start), 5*time.Millisecond)
	})

	t.Run("WaitsForDeliveries", func(t *testing.T) {
		c := &consumer{}
		c.deliveries.Store(3)

		done := make(chan struct{})
		go func() {
			c.Wait()
			close(done)
		}()

		for range 3 {
			time.Sleep(20 * time.Millisecond)
			c.deliveries.Add(-1)
		}

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Wait() did not return after deliveries completed")
		}
	})

	t.Run("MultipleConcurrentWaiters", func(t *testing.T) {
		c := &consumer{}
		c.deliveries.Store(2)

		waiters := 5
		done := make(chan struct{}, waiters)
		for range waiters {
			go func() { c.Wait(); done <- struct{}{} }()
		}

		time.Sleep(30 * time.Millisecond)
		c.deliveries.Store(0)

		for i := range waiters {
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("waiter %d did not complete", i)
			}
		}
	})
}

func TestConsumerGet(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WithNoChannel", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)

		msg, err := c.Get()
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.ErrorIs(t, err, ErrConsumerNotConnected)
	})

	t.Run("WhenQueueEmpty", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		c.stateMu.Lock()
		c.ch = newMockChannel() // default Get returns ok=false
		c.stateMu.Unlock()

		msg, err := c.Get()
		assert.NoError(t, err)
		assert.Nil(t, msg)
	})

	t.Run("WhenQueueHasMessage", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		ch := newMockChannel().withGetOK(true) // signal that a message is available
		c.stateMu.Lock()
		c.ch = ch
		c.stateMu.Unlock()

		msg, err := c.Get()
		assert.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("WithGetError", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		ch := newMockChannel().withGetError(assert.AnError)
		c.stateMu.Lock()
		c.ch = ch
		c.stateMu.Unlock()

		msg, err := c.Get()
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.ErrorIs(t, err, ErrConsumer)
		assert.ErrorIs(t, err, assert.AnError)
	})
}

func TestConsumerCancel(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("Idempotency", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		c.cancelled.Store(true) // pre-cancelled

		err := c.Cancel()
		assert.NoError(t, err) // second call is no-op
	})

	t.Run("WithNoChannel", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)

		err := c.Cancel()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConsumerNotConnected)
	})

	t.Run("WithChannel", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		c.stateMu.Lock()
		c.ch = newMockChannel()
		c.stateMu.Unlock()

		err := c.Cancel()
		assert.NoError(t, err)
		assert.True(t, c.cancelled.Load())
	})

	t.Run("WithCancelError", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		ch := newMockChannel().withCancelError(assert.AnError)
		c.stateMu.Lock()
		c.ch = ch
		c.stateMu.Unlock()

		err := c.Cancel()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConsumer)
		assert.ErrorIs(t, err, assert.AnError)
	})
}

func TestConsumerClose(t *testing.T) {
	topoReg := topology.NewRegistry()
	c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)

	assert.False(t, c.closed.Load())
	_ = c.Close()
	assert.True(t, c.closed.Load())
}

func TestConsumerInit(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WithInvalidOptions", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: -1},
		}, topology.NewQueue("queue"), testHandler(handler.ActionAck))
		err := c.init(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConsumer)
	})
}

func TestConsumerConnect(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("ConnectionError", func(t *testing.T) {
		// ConnectionManager without Init() -> pool slot is nil -> Assign returns error
		cm := transport.NewConnectionManager("amqp://invalid", &transport.ConnectionManagerOptions{Size: 1})
		c := newConsumer("test", cm, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewQueue("queue"), nil)
		err := c.connect(t.Context())
		assert.Error(t, err)
	})

	t.Run("ChannelError", func(t *testing.T) {
		conn := newMockConnection().withChannelError(assert.AnError)
		cm := newTestConnectionManager(conn)
		c := newConsumer("test", cm, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewQueue("queue"), nil)
		err := c.connect(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})

	t.Run("QueueDeclareError", func(t *testing.T) {
		ch := newMockChannel().withQueueDeclareError(assert.AnError)
		conn := newMockConnection().withChannel(ch)
		cm := newTestConnectionManager(conn)
		c := newConsumer("test", cm, topoReg, ConsumerOptions{
			// NoAutoDeclare: false -> Queue() is called -> QueueDeclare returns error
		}, topology.NewQueue("queue"), nil)
		err := c.connect(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})

	t.Run("QosError", func(t *testing.T) {
		ch := newMockChannel().withQosError(assert.AnError)
		conn := newMockConnection().withChannel(ch)
		cm := newTestConnectionManager(conn)
		c := newConsumer("test", cm, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewQueue("queue"), nil)
		err := c.connect(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})

	t.Run("ConsumeError", func(t *testing.T) {
		ch := newMockChannel().withConsumeError(assert.AnError)
		conn := newMockConnection().withChannel(ch)
		cm := newTestConnectionManager(conn)
		c := newConsumer("test", cm, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewQueue("queue"), nil)
		err := c.connect(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})

	t.Run("Success", func(t *testing.T) {
		conn := newMockConnection()
		cm := newTestConnectionManager(conn)
		c := newConsumer("test", cm, topoReg, ConsumerOptions{
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewQueue("queue"), nil)
		err := c.connect(t.Context())
		assert.NoError(t, err)
		c.stateMu.RLock()
		assert.NotNil(t, c.ch)
		assert.NotNil(t, c.closeCh)
		assert.NotNil(t, c.cancelCh)
		assert.NotNil(t, c.deliveryCh)
		c.stateMu.RUnlock()
		assert.False(t, c.cancelled.Load())
	})
}

func TestConsumerDisconnect(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("ClearsChannels", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)

		// simulate channels being set (done in connect)
		c.stateMu.Lock()
		c.cancelCh = make(chan string)
		c.deliveryCh = make(chan transport.Delivery)
		c.closeCh = make(chan *transport.Error)
		c.stateMu.Unlock()

		err := c.disconnect(nil)
		require.NoError(t, err)

		c.stateMu.RLock()
		assert.Nil(t, c.cancelCh)
		assert.Nil(t, c.deliveryCh)
		assert.Nil(t, c.closeCh)
		c.stateMu.RUnlock()
	})

	t.Run("WithCloseError", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		ch := newMockChannel().withCloseError(assert.AnError)
		c.stateMu.Lock()
		c.ch = ch
		c.stateMu.Unlock()
		err := c.disconnect(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})
}

func TestConsumerMonitor(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("NilCloseCh", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		err := c.monitor(t.Context())
		assert.NoError(t, err)
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		closeCh := make(chan *transport.Error, 1)
		c.stateMu.Lock()
		c.closeCh = closeCh
		c.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- c.monitor(ctx) }()

		cancel()

		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("monitor did not return after context cancel")
		}
	})

	t.Run("ChannelClosed", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		closeCh := make(chan *transport.Error, 1)
		c.stateMu.Lock()
		c.closeCh = closeCh
		c.stateMu.Unlock()

		done := make(chan error, 1)
		go func() { done <- c.monitor(t.Context()) }()

		close(closeCh)

		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("monitor did not return after channel close")
		}
	})

	t.Run("ChannelError", func(t *testing.T) {
		onErrCh := make(chan error, 1)
		opts := DefaultConsumerOptions()
		opts.OnError = func(err error) { onErrCh <- err }

		c := newConsumer("test", nil, topoReg, opts, topology.NewQueue("queue"), nil)
		closeCh := make(chan *transport.Error, 1)
		c.stateMu.Lock()
		c.closeCh = closeCh
		c.stateMu.Unlock()

		done := make(chan error, 1)
		go func() { done <- c.monitor(t.Context()) }()

		closeCh <- &transport.Error{Code: 500, Reason: "test error", Server: true}

		select {
		case err := <-done:
			assert.Error(t, err)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("monitor did not return after channel error")
		}

		select {
		case onErr := <-onErrCh:
			assert.Error(t, onErr)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("OnError was not called")
		}
	})
}

func TestConsumerHandleCancel(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("NilCancelCh", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		// cancelCh is nil by default
		done := make(chan struct{})
		go func() {
			c.handleCancel(t.Context())
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("handleCancel did not return with nil cancelCh")
		}
	})

	t.Run("ClosedCancelCh", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		cancelCh := make(chan string)
		c.stateMu.Lock()
		c.cancelCh = cancelCh
		c.stateMu.Unlock()

		done := make(chan struct{})
		go func() {
			c.handleCancel(t.Context())
			close(done)
		}()

		close(cancelCh)

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("handleCancel did not return after channel close")
		}
	})

	t.Run("MarksCancelled", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)

		cancelCh := make(chan string, 1)
		c.stateMu.Lock()
		c.cancelCh = cancelCh
		c.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go c.handleCancel(ctx)

		cancelCh <- "test-consumer"
		time.Sleep(30 * time.Millisecond)

		assert.True(t, c.cancelled.Load())
	})

	t.Run("CallsOnCancel", func(t *testing.T) {
		tagCh := make(chan string, 1)

		c := newConsumer("test", nil, topoReg, ConsumerOptions{
			OnCancel: func(tag string) { tagCh <- tag },
		}, topology.NewQueue("queue"), nil)

		cancelCh := make(chan string, 1)
		c.stateMu.Lock()
		c.cancelCh = cancelCh
		c.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go c.handleCancel(ctx)

		cancelCh <- "my-tag"
		select {
		case capturedTag := <-tagCh:
			assert.Equal(t, "my-tag", capturedTag)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("OnCancel was not called")
		}
	})
}

func TestConsumerHandleDeliveries(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("SequentialProcessing", func(t *testing.T) {
		var counter atomic.Int32
		opts := DefaultConsumerOptions()
		opts.MaxConcurrentHandlers = 1

		c := newConsumer("test", nil, topoReg, opts, topology.NewQueue("queue"),
			testCountingHandler(handler.ActionAck, &counter))

		ch := newMockChannel()
		deliveryCh := make(chan transport.Delivery, 4)
		c.stateMu.Lock()
		c.ch = ch
		c.deliveryCh = deliveryCh
		c.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		go c.handleDeliveries(ctx)

		for range 3 {
			deliveryCh <- transport.Delivery{Acknowledger: &mockAcknowledger{}}
		}
		time.Sleep(50 * time.Millisecond)
		cancel()
		time.Sleep(20 * time.Millisecond)

		assert.Equal(t, int32(3), counter.Load())
	})

	t.Run("DefaultConcurrency", func(t *testing.T) {
		// MaxConcurrentHandlers == 0 resolves to PrefetchCount (DefaultPrefetchCount if unset)
		var counter atomic.Int32
		opts := DefaultConsumerOptions()
		opts.PrefetchCount = 3
		opts.MaxConcurrentHandlers = 0 // default: capped by PrefetchCount

		c := newConsumer("test", nil, topoReg, opts, topology.NewQueue("queue"),
			testCountingHandler(handler.ActionAck, &counter))

		ch := newMockChannel()
		deliveryCh := make(chan transport.Delivery, 6)
		c.stateMu.Lock()
		c.ch = ch
		c.deliveryCh = deliveryCh
		c.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		go c.handleDeliveries(ctx)

		for range 6 {
			deliveryCh <- transport.Delivery{Acknowledger: &mockAcknowledger{}}
		}
		time.Sleep(80 * time.Millisecond)
		cancel()

		c.Wait()
		assert.Equal(t, int32(6), counter.Load())
	})

	t.Run("UnlimitedConcurrency", func(t *testing.T) {
		var counter atomic.Int32
		opts := DefaultConsumerOptions()
		opts.MaxConcurrentHandlers = -1 // explicit unlimited

		c := newConsumer("test", nil, topoReg, opts, topology.NewQueue("queue"),
			testCountingHandler(handler.ActionAck, &counter))

		ch := newMockChannel()
		deliveryCh := make(chan transport.Delivery, 4)
		c.stateMu.Lock()
		c.ch = ch
		c.deliveryCh = deliveryCh
		c.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		go c.handleDeliveries(ctx)

		for range 5 {
			deliveryCh <- transport.Delivery{Acknowledger: &mockAcknowledger{}}
		}
		time.Sleep(50 * time.Millisecond)
		cancel()

		c.Wait()
		assert.Equal(t, int32(5), counter.Load())
	})

	t.Run("BoundedConcurrency", func(t *testing.T) {
		var counter atomic.Int32
		opts := DefaultConsumerOptions()
		opts.MaxConcurrentHandlers = 3

		c := newConsumer("test", nil, topoReg, opts, topology.NewQueue("queue"),
			testCountingHandler(handler.ActionAck, &counter))

		ch := newMockChannel()
		deliveryCh := make(chan transport.Delivery, 8)
		c.stateMu.Lock()
		c.ch = ch
		c.deliveryCh = deliveryCh
		c.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		go c.handleDeliveries(ctx)

		for range 6 {
			deliveryCh <- transport.Delivery{Acknowledger: &mockAcknowledger{}}
		}
		time.Sleep(80 * time.Millisecond)
		cancel()

		c.Wait()
		assert.Equal(t, int32(6), counter.Load())
	})

	t.Run("NilDeliveryCh", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"), nil)
		// deliveryCh is nil by default
		done := make(chan struct{})
		go func() {
			c.handleDeliveries(t.Context())
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("handleDeliveries did not return with nil deliveryCh")
		}
	})

	t.Run("SemaphoreContextCancelled", func(t *testing.T) {
		// exercises the `case <-ctx.Done(): return` branch inside the semaphore-bounded
		// processor goroutine, requires MaxConcurrentHandlers >= 2 (default case)
		blocking := make(chan struct{}) // closed to release handlers
		opts := DefaultConsumerOptions()
		opts.MaxConcurrentHandlers = 2 // triggers the semaphore (default) branch

		// handler blocks until 'blocking' is closed, ignoring ctx so the semaphore
		// stays full long enough for the third goroutine to block and get ctx.Done()
		handler := func(_ context.Context, _ *message.Message) (handler.Action, error) {
			<-blocking
			return handler.ActionNoAction, nil
		}

		c := newConsumer("test", nil, topoReg, opts, topology.NewQueue("queue"), handler)
		deliveryCh := make(chan transport.Delivery, 5)
		c.stateMu.Lock()
		c.ch = newMockChannel()
		c.deliveryCh = deliveryCh
		c.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		go c.handleDeliveries(ctx)

		d := transport.Delivery{Acknowledger: &mockAcknowledger{}}
		// fill the semaphore (2 goroutines start and block in handler)
		deliveryCh <- d
		deliveryCh <- d
		time.Sleep(10 * time.Millisecond) // let both acquire the semaphore

		// 3rd goroutine starts, blocks in select waiting for semaphore
		deliveryCh <- d
		time.Sleep(10 * time.Millisecond) // let it reach the select

		// cancel context -> 3rd goroutine returns via case <-ctx.Done()
		cancel()
		time.Sleep(10 * time.Millisecond)

		// unblock handlers so the semaphore drains and Wait() can return
		close(blocking)
		c.Wait()
	})
	t.Run("ClosedChannelExits", func(t *testing.T) {
		c := newConsumer("test", nil, topoReg, DefaultConsumerOptions(), topology.NewQueue("queue"),
			testHandler(handler.ActionAck))

		deliveryCh := make(chan transport.Delivery)
		c.stateMu.Lock()
		c.deliveryCh = deliveryCh
		c.stateMu.Unlock()

		done := make(chan struct{})
		go func() {
			c.handleDeliveries(t.Context())
			close(done)
		}()

		close(deliveryCh)

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("handleDeliveries did not exit after channel close")
		}
	})
}

func TestConsumerProcessMessage(t *testing.T) {
	topoReg := topology.NewRegistry()

	makeConsumerWithChannel := func(opts ConsumerOptions, h handler.Handler) (*consumer, *mockChannel) {
		c := newConsumer("test", nil, topoReg, opts, topology.NewQueue("queue"), h)
		ch := newMockChannel()
		c.stateMu.Lock()
		c.ch = ch
		c.stateMu.Unlock()
		return c, ch
	}

	// build a minimal Message that has a mock Acknowledger
	// deliveryToMessage wraps a *amqp.Delivery, which embeds Acknowledger,
	makeMessageWithAcker := func() *message.Message {
		d := transport.Delivery{Acknowledger: &mockAcknowledger{}}
		msg := deliveryToMessage(&d)
		return &msg
	}

	t.Run("HandlerActionAck", func(t *testing.T) {
		c, _ := makeConsumerWithChannel(DefaultConsumerOptions(), testHandler(handler.ActionAck))
		msg := makeMessageWithAcker()
		c.processMessage(t.Context(), msg)
		// Message.Ack dispatches to the Acknowledger on the message delivery,
		// for a no-op check we just verify no error and that processMessage returns
		_ = c // nothing to assert beyond no-panic
	})

	t.Run("AutoAck", func(t *testing.T) {
		opts := DefaultConsumerOptions()
		opts.AutoAck = true

		var counter atomic.Int32
		c, _ := makeConsumerWithChannel(opts, testCountingHandler(handler.ActionNoAction, &counter))
		msg := makeMessageWithAcker()

		c.processMessage(t.Context(), msg)
		assert.Equal(t, int32(1), counter.Load())
	})

	t.Run("HandlerReturnsError", func(t *testing.T) {
		errCh := make(chan error, 1)
		opts := DefaultConsumerOptions()
		opts.OnError = func(err error) { errCh <- err }

		c, _ := makeConsumerWithChannel(opts, testErrorHandler(handler.ActionAck, assert.AnError))
		msg := makeMessageWithAcker()

		c.processMessage(t.Context(), msg)

		select {
		case capturedErr := <-errCh:
			require.NotNil(t, capturedErr)
			assert.ErrorIs(t, capturedErr, ErrConsumer)
			assert.ErrorIs(t, capturedErr, assert.AnError)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("OnError was not called")
		}
	})

	t.Run("AckError", func(t *testing.T) {
		errCh := make(chan error, 1)
		opts := DefaultConsumerOptions()
		opts.OnError = func(err error) { errCh <- err }

		c, _ := makeConsumerWithChannel(opts, testHandler(handler.ActionAck))
		acker := &mockAcknowledger{ackErr: assert.AnError}
		d := transport.Delivery{Acknowledger: acker}
		msg := deliveryToMessage(&d)

		c.processMessage(t.Context(), &msg)

		select {
		case capturedErr := <-errCh:
			require.NotNil(t, capturedErr)
			assert.ErrorIs(t, capturedErr, ErrConsumer)
			assert.ErrorIs(t, capturedErr, assert.AnError)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("OnError was not called")
		}
	})

	t.Run("NackRequeueError", func(t *testing.T) {
		errCh := make(chan error, 1)
		opts := DefaultConsumerOptions()
		opts.OnError = func(err error) { errCh <- err }

		c, _ := makeConsumerWithChannel(opts, testHandler(handler.ActionNackRequeue))
		acker := &mockAcknowledger{nackErr: assert.AnError}
		d := transport.Delivery{Acknowledger: acker}
		msg := deliveryToMessage(&d)

		c.processMessage(t.Context(), &msg)

		select {
		case capturedErr := <-errCh:
			require.NotNil(t, capturedErr)
			assert.ErrorIs(t, capturedErr, ErrConsumer)
			assert.ErrorIs(t, capturedErr, assert.AnError)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("OnError was not called")
		}
	})

	t.Run("NackDiscardError", func(t *testing.T) {
		errCh := make(chan error, 1)
		opts := DefaultConsumerOptions()
		opts.OnError = func(err error) { errCh <- err }

		c, _ := makeConsumerWithChannel(opts, testHandler(handler.ActionNackDiscard))
		acker := &mockAcknowledger{nackErr: assert.AnError}
		d := transport.Delivery{Acknowledger: acker}
		msg := deliveryToMessage(&d)

		c.processMessage(t.Context(), &msg)

		select {
		case capturedErr := <-errCh:
			require.NotNil(t, capturedErr)
			assert.ErrorIs(t, capturedErr, ErrConsumer)
			assert.ErrorIs(t, capturedErr, assert.AnError)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("OnError was not called")
		}
	})
}
