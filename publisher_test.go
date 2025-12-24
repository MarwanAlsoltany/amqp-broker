package broker

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublisherOptions(t *testing.T) {
	// make sure that these are changed only intentionally
	opts := defaultPublisherOptions()
	assert.False(t, opts.ConfirmMode)
	assert.Equal(t, defaultConfirmTimeout, opts.ConfirmTimeout)
	assert.False(t, opts.Mandatory)
	assert.False(t, opts.Immediate)
	assert.False(t, opts.NoWaitReady)
	assert.Equal(t, defaultReadyTimeout, opts.ReadyTimeout)
	assert.False(t, opts.NoAutoReconnect)
	assert.Equal(t, defaultReconnectMin, opts.ReconnectMin)
	assert.Equal(t, defaultReconnectMax, opts.ReconnectMax)

	t.Run("OnConfirm", func(t *testing.T) {
		var capturedTag uint64
		var capturedWait func(context.Context) bool

		opts := PublisherOptions{
			OnConfirm: func(deliveryTag uint64, wait func(context.Context) bool) {
				capturedTag = deliveryTag
				capturedWait = wait
			},
		}

		require.NotNil(t, opts.OnConfirm)
		mockWait := func(ctx context.Context) bool { return true }
		opts.OnConfirm(123, mockWait)

		assert.Equal(t, uint64(123), capturedTag)
		assert.NotNil(t, capturedWait)
	})

	t.Run("OnReturn", func(t *testing.T) {
		var capturedMsg Message

		opts := PublisherOptions{
			OnReturn: func(msg Message) {
				capturedMsg = msg
			},
		}

		require.NotNil(t, opts.OnReturn)
		testMsg := Message{MessageID: "test-123"}
		opts.OnReturn(testMsg)

		assert.Equal(t, "test-123", capturedMsg.MessageID)
	})

	t.Run("OnFlow", func(t *testing.T) {
		var capturedActive bool

		opts := PublisherOptions{
			OnFlow: func(active bool) {
				capturedActive = active
			},
		}

		require.NotNil(t, opts.OnFlow)
		opts.OnFlow(true)
		assert.True(t, capturedActive)

		opts.OnFlow(false)
		assert.False(t, capturedActive)
	})

	t.Run("OnError", func(t *testing.T) {
		var capturedErr error

		opts := PublisherOptions{
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

func TestNewPublisher(t *testing.T) {
	b := &Broker{id: "test-broker"}
	e := NewExchange("test-exchange")
	opts := defaultPublisherOptions()

	p := newPublisher("test-publisher", b, opts, e)

	require.NotNil(t, p)
	assert.NotNil(t, p.endpoint)
	assert.Equal(t, "test-publisher", p.id)
	assert.Equal(t, b, p.broker)
	assert.Equal(t, rolePublisher, p.role)
	assert.Equal(t, opts, p.opts)
	assert.Equal(t, e, p.exchange)

	t.Run("State", func(t *testing.T) {
		b := &Broker{id: "test-broker"}

		e := NewExchange("test-exchange")

		p := newPublisher("test-publisher", b, defaultPublisherOptions(), e)

		assert.False(t, p.flow.Load()) // flow gets activated on connect
		assert.False(t, p.ready.Load())
		assert.False(t, p.closed.Load())

		assert.Nil(t, p.flowCh)
		assert.Nil(t, p.returnCh)
		assert.Nil(t, p.confirmCh)
		assert.Nil(t, p.closeCh)

		// simulate channels being set (normally done in connect)
		p.stateMu.Lock()
		p.flowCh = make(<-chan bool)
		p.returnCh = make(<-chan amqp.Return)
		p.confirmCh = make(<-chan amqp.Confirmation)
		p.closeCh = make(<-chan *amqp.Error)
		p.stateMu.Unlock()

		// disconnect should clear them
		err := p.disconnect(nil)
		require.NoError(t, err)

		p.stateMu.RLock()
		assert.Nil(t, p.flowCh)
		assert.Nil(t, p.returnCh)
		assert.Nil(t, p.confirmCh)
		assert.Nil(t, p.closeCh)
		p.stateMu.RUnlock()
	})
}

func TestPublisherRelease(t *testing.T) {
	t.Run("RemovesFromBroker", func(t *testing.T) {
		b := &Broker{
			publishers: make(map[string]Publisher),
		}
		ctx, cancel := context.WithCancel(t.Context())
		b.ctx = ctx
		b.cancel = cancel
		defer cancel()

		p := newPublisher("test-publisher", b, defaultPublisherOptions(), NewExchange("test-exchange"))
		b.publishers["test-publisher"] = p

		assert.Len(t, b.publishers, 1)

		p.Release()

		assert.Empty(t, b.publishers)

		t.Run("WithNoBroker", func(t *testing.T) {
			e := &endpoint{broker: nil}
			p := &publisher{endpoint: e}
			// should not panic
			p.Release()
		})
	})

	t.Run("ClosesPublisher", func(t *testing.T) {
		b := &Broker{
			publishers: make(map[string]Publisher),
		}
		ctx, cancel := context.WithCancel(t.Context())
		b.ctx = ctx
		b.cancel = cancel
		defer cancel()

		p := newPublisher("test-publisher", b, defaultPublisherOptions(), NewExchange("test-exchange"))
		b.publishers["test-publisher"] = p

		p.Release()

		assert.True(t, p.closed.Load())
	})
}

func TestPublisherPublish(t *testing.T) {
	b := &Broker{
		topologyMgr: newTopologyManager(),
	}

	t.Run("WithCancelledContext", func(t *testing.T) {
		exchange := NewExchange("test-exchange")

		p := newPublisher("test-publisher", b, PublisherOptions{}, exchange)

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // cancel immediately

		err := p.Publish(ctx, "test.key", Message{Body: []byte("test")})
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		exchange := NewExchange("test-exchange")

		p := newPublisher("test-publisher", b, PublisherOptions{}, exchange)
		p.closed.Store(true)

		ctx := t.Context()
		err := p.Publish(ctx, "test.key", Message{Body: []byte("test")})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPublisherClosed)
	})

	t.Run("WithNoChannel", func(t *testing.T) {
		exchange := NewExchange("test-exchange")

		p := newPublisher("test-publisher", b, PublisherOptions{}, exchange)

		// try to publish without channel
		ctx := t.Context()
		err := p.Publish(ctx, "test.key", Message{Body: []byte("test")})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPublisherNotConnected)
	})

	t.Run("WithNoMessage", func(t *testing.T) {
		b := &Broker{
			topologyMgr: newTopologyManager(),
		}
		exchange := NewExchange("test-exchange")

		p := newPublisher("test-publisher", b, PublisherOptions{}, exchange)

		ctx := t.Context()
		err := p.Publish(ctx, "test.key" /* no message(s) */)
		assert.NoError(t, err) // empty publish is a no-op
	})

	t.Run("WhenFlowPaused", func(t *testing.T) {
		exchange := NewExchange("test-exchange")

		p := newPublisher("test-publisher", b, PublisherOptions{}, exchange)
		p.ch = &amqp.Channel{} // simulate channel set

		assert.False(t, p.flow.Load()) // flow gets activated on connect

		// try to publish when flow is paused
		ctx := t.Context()
		err := p.Publish(ctx, "test.key", Message{Body: []byte("test")})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPublisher)
		assert.ErrorContains(t, err, "flow paused")
	})

	t.Run("WhenFlowResumed", func(t *testing.T) {
		exchange := NewExchange("test-exchange")

		p := newPublisher("test-publisher", b, PublisherOptions{}, exchange)
		p.ch = &amqp.Channel{} // simulate channel set

		assert.False(t, p.flow.Load()) // flow gets activated on connect
		p.flow.Store(true)             // simulate flow resumed

		// try to publish when flow is active
		ctx := t.Context()
		// underlying library panics due to malformed instantiation of amqp.Channel
		assert.Panics(t, func() {
			_ = p.Publish(ctx, "test.key", Message{Body: []byte("test")})
		})
	})
}
