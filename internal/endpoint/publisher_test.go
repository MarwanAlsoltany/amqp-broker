package endpoint

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublisherOptions(t *testing.T) {
	opts := DefaultPublisherOptions()

	assert.False(t, opts.ConfirmMode)
	assert.Equal(t, DefaultConfirmTimeout, opts.ConfirmTimeout)
	assert.False(t, opts.Mandatory)
	assert.False(t, opts.Immediate)
	assert.False(t, opts.NoAutoDeclare)
	assert.False(t, opts.NoWaitReady)
	assert.Equal(t, DefaultReadyTimeout, opts.ReadyTimeout)
	assert.False(t, opts.NoAutoReconnect)
	assert.Equal(t, DefaultReconnectMin, opts.ReconnectMin)
	assert.Equal(t, DefaultReconnectMax, opts.ReconnectMax)

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
		mockWait := func(_ context.Context) bool { return true }
		opts.OnConfirm(123, mockWait)

		assert.Equal(t, uint64(123), capturedTag)
		assert.NotNil(t, capturedWait)
	})

	t.Run("OnReturn", func(t *testing.T) {
		var capturedMsg message.Message

		opts := PublisherOptions{
			OnReturn: func(msg message.Message) {
				capturedMsg = msg
			},
		}

		require.NotNil(t, opts.OnReturn)
		testMsg := message.Message{MessageID: "test-123"}
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
		opts.OnError(assert.AnError)

		assert.Equal(t, assert.AnError, capturedErr)
	})
}

func TestNewPublisher(t *testing.T) {
	topoReg := topology.NewRegistry()
	exchange := topology.NewExchange("test-exchange")
	opts := DefaultPublisherOptions()

	p := newPublisher("test-publisher", nil, topoReg, opts, exchange)

	require.NotNil(t, p)
	require.NotNil(t, p.endpoint)
	assert.Equal(t, "test-publisher", p.id)
	assert.Equal(t, rolePublisher, p.role)
	assert.Equal(t, opts, p.opts)
	assert.Equal(t, exchange, p.exchange)

	t.Run("State", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, DefaultPublisherOptions(), topology.NewExchange("exchange"))

		assert.False(t, p.flow.Load()) // set to true only in connect
		assert.False(t, p.ready.Load())
		assert.False(t, p.closed.Load())

		p.stateMu.RLock()
		assert.Nil(t, p.flowCh)
		assert.Nil(t, p.returnCh)
		assert.Nil(t, p.confirmCh)
		assert.Nil(t, p.closeCh)
		p.stateMu.RUnlock()
	})
}

func TestNewPublisherInterface(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("Success", func(t *testing.T) {
		cm := newTestConnectionManager(newMockConnection())
		p, err := NewPublisher(t.Context(), "test", cm, topoReg, PublisherOptions{
			EndpointOptions: EndpointOptions{
				NoAutoDeclare:   true,
				NoAutoReconnect: true,
				ReconnectMin:    DefaultReconnectMin,
				ReconnectMax:    DefaultReconnectMax,
			},
		}, topology.NewExchange("exchange"))
		require.NoError(t, err)
		require.NotNil(t, p)
		defer p.Close()
		assert.True(t, p.Ready())
		// verify the interface is returned, not the concrete type
		var _ Publisher = p
	})

	t.Run("WithInvalidOptions", func(t *testing.T) {
		p, err := NewPublisher(t.Context(), "test", nil, topoReg, PublisherOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: -1},
		}, topology.NewExchange("exchange"))
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPublisher)
		assert.Nil(t, p)
	})

	t.Run("WithConnectionError", func(t *testing.T) {
		// ConnectionManager with no Init() -> pool slot is nil -> Assign returns error
		cm := transport.NewConnectionManager("amqp://invalid", &transport.ConnectionManagerOptions{Size: 1})
		p, err := NewPublisher(t.Context(), "test", cm, topoReg, PublisherOptions{
			EndpointOptions: EndpointOptions{
				NoAutoDeclare: true,
				ReconnectMin:  DefaultReconnectMin,
				ReconnectMax:  DefaultReconnectMax,
			},
		}, topology.NewExchange("exchange"))
		assert.Error(t, err)
		assert.Nil(t, p)
	})
}

func TestPublisherClose(t *testing.T) {
	topoReg := topology.NewRegistry()
	p := newPublisher("test", nil, topoReg, DefaultPublisherOptions(), topology.NewExchange("exchange"))

	assert.False(t, p.closed.Load())
	_ = p.Close()
	assert.True(t, p.closed.Load())
}

func TestPublisherInit(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WithInvalidOptions", func(t *testing.T) {
		// ReconnectMin <= 0 fails validateEndpointOptions
		p := newPublisher("test", nil, topoReg, PublisherOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: -1},
		}, topology.NewExchange("exchange"))
		err := p.init(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPublisher)
	})
}

func TestPublisherConnect(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("ConnectionError", func(t *testing.T) {
		// ConnectionManager with no Init() -> pool slot is nil -> Assign returns error
		cm := transport.NewConnectionManager("amqp://invalid", &transport.ConnectionManagerOptions{Size: 1})
		p := newPublisher("test", cm, topoReg, PublisherOptions{
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewExchange("exchange"))
		err := p.connect(t.Context())
		assert.Error(t, err)
	})

	t.Run("ChannelError", func(t *testing.T) {
		conn := newMockConnection().withChannelError(assert.AnError)
		cm := newTestConnectionManager(conn)
		p := newPublisher("test", cm, topoReg, PublisherOptions{
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewExchange("exchange"))
		err := p.connect(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})

	t.Run("ExchangeDeclareError", func(t *testing.T) {
		ch := newMockChannel().withExchangeDeclareError(assert.AnError)
		conn := newMockConnection().withChannel(ch)
		cm := newTestConnectionManager(conn)
		p := newPublisher("test", cm, topoReg, PublisherOptions{
			// NoAutoDeclare false -> Exchange() is called -> ExchangeDeclare returns error
		}, topology.NewExchange("my-exchange"))
		err := p.connect(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})

	t.Run("EmptyExchangeNameSwallowed", func(t *testing.T) {
		// when exchange name is empty, the error should be swallowed
		// (empty name -> uses AMQP default exchange)
		conn := newMockConnection()
		cm := newTestConnectionManager(conn)
		p := newPublisher("test", cm, topoReg, PublisherOptions{
						// NoAutoDeclare: false -> Exchange() is called with empty-name exchange
		}, topology.NewExchange("")) // empty name -> ErrTopologyExchangeNameEmpty -> swallowed
		err := p.connect(t.Context())
		assert.NoError(t, err)
	})

	t.Run("ConfirmModeError", func(t *testing.T) {
		ch := newMockChannel().withConfirmError(assert.AnError)
		conn := newMockConnection().withChannel(ch)
		cm := newTestConnectionManager(conn)
		p := newPublisher("test", cm, topoReg, PublisherOptions{
			ConfirmMode:     true,
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewExchange("exchange"))
		err := p.connect(t.Context())
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})

	t.Run("ConfirmModeSuccess", func(t *testing.T) {
		conn := newMockConnection()
		cm := newTestConnectionManager(conn)
		p := newPublisher("test", cm, topoReg, PublisherOptions{
			ConfirmMode:     true,
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewExchange("exchange"))
		err := p.connect(t.Context())
		assert.NoError(t, err)
		p.stateMu.RLock()
		assert.NotNil(t, p.confirmCh)
		p.stateMu.RUnlock()
	})

	t.Run("ConfirmModeDeferredSpawnsHandleConfirmations", func(t *testing.T) {
		conn := newMockConnection()
		cm := newTestConnectionManager(conn)
		p := newPublisher("test", cm, topoReg, PublisherOptions{
			ConfirmMode:     true,
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
			OnConfirm:       func(_ uint64, _ func(context.Context) bool) {},
		}, topology.NewExchange("exchange"))
		err := p.connect(t.Context())
		assert.NoError(t, err)
	})
}

func TestPublisherDisconnect(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("ClearsChannels", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, DefaultPublisherOptions(), topology.NewExchange("exchange"))

		// simulate channels being set (done in connect)
		p.stateMu.Lock()
		p.flowCh = make(chan bool)
		p.returnCh = make(chan transport.Return)
		p.confirmCh = make(chan transport.Confirmation)
		p.closeCh = make(chan *transport.Error)
		p.stateMu.Unlock()

		err := p.disconnect(nil)
		require.NoError(t, err)

		p.stateMu.RLock()
		assert.Nil(t, p.flowCh)
		assert.Nil(t, p.returnCh)
		assert.Nil(t, p.confirmCh)
		assert.Nil(t, p.closeCh)
		p.stateMu.RUnlock()
	})

	t.Run("WithCloseError", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, DefaultPublisherOptions(), topology.NewExchange("exchange"))
		ch := newMockChannel().withCloseError(assert.AnError)
		p.stateMu.Lock()
		p.ch = ch
		p.stateMu.Unlock()

		err := p.disconnect(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, assert.AnError)
	})
}

func TestPublisherMonitor(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("NilCloseCh", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, DefaultPublisherOptions(), topology.NewExchange("exchange"))
		// closeCh is nil by default
		err := p.monitor(t.Context())
		assert.NoError(t, err)
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, DefaultPublisherOptions(), topology.NewExchange("exchange"))
		closeCh := make(chan *transport.Error, 1)
		p.stateMu.Lock()
		p.closeCh = closeCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- p.monitor(ctx) }()

		cancel()

		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("monitor did not return after context cancel")
		}
	})

	t.Run("ChannelClosed", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, DefaultPublisherOptions(), topology.NewExchange("exchange"))
		closeCh := make(chan *transport.Error, 1)
		p.stateMu.Lock()
		p.closeCh = closeCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- p.monitor(ctx) }()

		close(closeCh) // closed without error -> ok=false -> returns nil
		err := <-done
		assert.NoError(t, err)
	})

	t.Run("ChannelError", func(t *testing.T) {
		errCh := make(chan error, 1)
		p := newPublisher("test", nil, topoReg, PublisherOptions{
			OnError: func(err error) { errCh <- err },
		}, topology.NewExchange("exchange"))
		closeCh := make(chan *transport.Error, 1)
		p.stateMu.Lock()
		p.closeCh = closeCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- p.monitor(ctx) }()

		amqpErr := &transport.Error{Code: 500, Reason: "test error", Server: true}
		closeCh <- amqpErr
		monErr := <-done
		assert.Error(t, monErr)

		select {
		case capturedErr := <-errCh:
			assert.Error(t, capturedErr)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("OnError was not called")
		}
	})
}

func TestPublisherPublish(t *testing.T) {
	topoReg := topology.NewRegistry()
	newTestPublisher := func(opts PublisherOptions) *publisher {
		return newPublisher("test", nil, topoReg, opts, topology.NewExchange("test-exchange"))
	}

	t.Run("WithCancelledContext", func(t *testing.T) {
		p := newTestPublisher(PublisherOptions{})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := p.Publish(ctx, "test-key", message.Message{Body: []byte("test")})
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		p := newTestPublisher(PublisherOptions{})
		p.closed.Store(true)

		err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
		assert.ErrorIs(t, err, ErrPublisherClosed)
	})

	t.Run("WithNoMessages", func(t *testing.T) {
		p := newTestPublisher(PublisherOptions{})

		err := p.Publish(t.Context(), "test-key")
		assert.NoError(t, err) // empty publish is a no-op
	})

	t.Run("WithNoChannel", func(t *testing.T) {
		p := newTestPublisher(PublisherOptions{})

		err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
		assert.ErrorIs(t, err, ErrPublisherNotConnected)
	})

	t.Run("WhenFlowPaused", func(t *testing.T) {
		p := newTestPublisher(PublisherOptions{})
		p.stateMu.Lock()
		p.ch = newMockChannel()
		p.stateMu.Unlock()

		assert.False(t, p.flow.Load()) // flow not set until connect

		err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPublisher)
		assert.ErrorContains(t, err, "flow paused")
	})

	t.Run("WhenFlowActive", func(t *testing.T) {
		p := newTestPublisher(PublisherOptions{})
		ch := newMockChannel()
		p.stateMu.Lock()
		p.ch = ch
		p.stateMu.Unlock()
		p.flow.Store(true)

		err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("hello")})
		assert.NoError(t, err)

		ch.mu.Lock()
		published := ch.publishedMsgs
		ch.mu.Unlock()

		require.Len(t, published, 1)
		assert.Equal(t, []byte("hello"), published[0].Body)
	})

	t.Run("WithPublishError", func(t *testing.T) {
		p := newTestPublisher(PublisherOptions{})
		ch := newMockChannel().withPublishError(assert.AnError)
		p.stateMu.Lock()
		p.ch = ch
		p.stateMu.Unlock()
		p.flow.Store(true)

		err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPublisher)
		assert.ErrorIs(t, err, assert.AnError)
	})

	t.Run("MultipleMessages", func(t *testing.T) {
		p := newTestPublisher(PublisherOptions{})
		ch := newMockChannel()
		p.stateMu.Lock()
		p.ch = ch
		p.stateMu.Unlock()
		p.flow.Store(true)

		msgs := []message.Message{
			{Body: []byte("msg1")},
			{Body: []byte("msg2")},
			{Body: []byte("msg3")},
		}

		err := p.Publish(t.Context(), "test-key", msgs...)
		assert.NoError(t, err)
		assert.Equal(t, int32(3), ch.publishCalls.Load())
	})

	t.Run("ConfirmMode", func(t *testing.T) {
		t.Run("BatchConfirmSuccess", func(t *testing.T) {
			p := newTestPublisher(PublisherOptions{ConfirmMode: true})
			ch := newMockChannel()
			confirmCh := make(chan transport.Confirmation, 4)
			p.stateMu.Lock()
			p.ch = ch
			p.confirmCh = confirmCh
			p.stateMu.Unlock()
			p.flow.Store(true)

			// pre-fill ack confirmations
			for range 2 {
				confirmCh <- transport.Confirmation{Ack: true}
			}

			msgs := []message.Message{{Body: []byte("a")}, {Body: []byte("b")}}
			err := p.Publish(t.Context(), "test-key", msgs...)
			assert.NoError(t, err)
		})

		t.Run("NoConfirmChannelWhenNotConnected", func(t *testing.T) {
			p := newTestPublisher(PublisherOptions{ConfirmMode: true})
			ch := newMockChannel()
			p.stateMu.Lock()
			p.ch = ch
			p.confirmCh = nil // not set (not connected in confirm mode)
			p.stateMu.Unlock()
			p.flow.Store(true)

			err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
			assert.ErrorIs(t, err, ErrPublisherNotConnected)
		})

		t.Run("DeferredConfirmSuccess", func(t *testing.T) {
			var capturedTags []uint64
			p := newTestPublisher(PublisherOptions{
				ConfirmMode: true,
				OnConfirm: func(tag uint64, _ func(context.Context) bool) {
					capturedTags = append(capturedTags, tag)
				},
			})
			ch := newMockChannel()
			p.stateMu.Lock()
			p.ch = ch
			p.stateMu.Unlock()
			p.flow.Store(true)

			msgs := []message.Message{{Body: []byte("x")}, {Body: []byte("y")}}
			err := p.Publish(t.Context(), "test-key", msgs...)
			assert.NoError(t, err)
			assert.Len(t, capturedTags, 2)
		})

		t.Run("DeferredConfirmPublishError", func(t *testing.T) {
			p := newTestPublisher(PublisherOptions{
				ConfirmMode: true,
				OnConfirm:   func(_ uint64, _ func(context.Context) bool) {},
			})
			ch := newMockChannel().withPublishError(assert.AnError)
			p.stateMu.Lock()
			p.ch = ch
			p.stateMu.Unlock()
			p.flow.Store(true)

			err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
			assert.ErrorIs(t, err, ErrPublisher)
			assert.ErrorIs(t, err, assert.AnError)
		})

		t.Run("DeferredConfirmWaitContext", func(t *testing.T) {
			// exercises the waitFn(ctx) closure body: WaitContext path and the
			// OnError call when WaitContext returns an error (context cancelled)
			onErrCh := make(chan error, 1)
			p := newTestPublisher(PublisherOptions{
				ConfirmMode: true,
				OnError:     func(err error) { onErrCh <- err },
				OnConfirm: func(_ uint64, waitFn func(context.Context) bool) {
					// call with a cancelled context so WaitContext returns immediately
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					waitFn(ctx)
				},
			})
			ch := newMockChannel()
			p.stateMu.Lock()
			p.ch = ch
			p.stateMu.Unlock()
			p.flow.Store(true)

			err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
			assert.NoError(t, err)

			// OnError should have been called (WaitContext returned ctx.Canceled)
			select {
			case onErr := <-onErrCh:
				assert.ErrorIs(t, onErr, context.Canceled)
			case <-time.After(500 * time.Millisecond):
				t.Fatal("OnError was not called")
			}
		})

		t.Run("BatchConfirmNack", func(t *testing.T) {
			p := newTestPublisher(PublisherOptions{ConfirmMode: true})
			ch := newMockChannel()
			confirmCh := make(chan transport.Confirmation, 1)
			confirmCh <- transport.Confirmation{Ack: false}
			p.stateMu.Lock()
			p.ch = ch
			p.confirmCh = confirmCh
			p.stateMu.Unlock()
			p.flow.Store(true)

			err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
			assert.Error(t, err)
			assert.ErrorIs(t, err, ErrPublisher)
			assert.ErrorContains(t, err, "not confirmed")
		})

		t.Run("BatchConfirmTimeout", func(t *testing.T) {
			p := newTestPublisher(PublisherOptions{ConfirmMode: true, ConfirmTimeout: 20 * time.Millisecond})
			ch := newMockChannel()
			confirmCh := make(chan transport.Confirmation) // nothing sends
			p.stateMu.Lock()
			p.ch = ch
			p.confirmCh = confirmCh
			p.stateMu.Unlock()
			p.flow.Store(true)

			err := p.Publish(t.Context(), "test-key", message.Message{Body: []byte("test")})
			assert.Error(t, err)
			assert.ErrorIs(t, err, ErrPublisher)
			assert.ErrorContains(t, err, "timeout")
		})

		t.Run("BatchConfirmContextCancelled", func(t *testing.T) {
			p := newTestPublisher(PublisherOptions{ConfirmMode: true, ConfirmTimeout: 5 * time.Second})
			ch := newMockChannel()
			confirmCh := make(chan transport.Confirmation) // nothing sends
			p.stateMu.Lock()
			p.ch = ch
			p.confirmCh = confirmCh
			p.stateMu.Unlock()
			p.flow.Store(true)

			ctx, cancel := context.WithCancel(t.Context())
			go func() { time.Sleep(10 * time.Millisecond); cancel() }()

			err := p.Publish(ctx, "test-key", message.Message{Body: []byte("test")})
			assert.ErrorIs(t, err, context.Canceled)
		})
	})
}

func TestPublisherHandleFlow(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("UpdatesFlowState", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		flowCh := make(chan bool, 2)
		p.stateMu.Lock()
		p.flowCh = flowCh
		p.stateMu.Unlock()
		p.flow.Store(true)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go p.handleFlow(ctx)

		// pause
		flowCh <- false
		time.Sleep(20 * time.Millisecond)
		assert.False(t, p.flow.Load())

		// resume
		flowCh <- true
		time.Sleep(20 * time.Millisecond)
		assert.True(t, p.flow.Load())
	})

	t.Run("CallsOnFlow", func(t *testing.T) {
		var capturedActive atomic.Bool
		capturedActive.Store(true) // start true; expect to be set false by callback

		p := newPublisher("test", nil, topoReg, PublisherOptions{
			OnFlow: func(active bool) { capturedActive.Store(active) },
		}, topology.NewExchange("exchange"))

		flowCh := make(chan bool, 1)
		p.stateMu.Lock()
		p.flowCh = flowCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go p.handleFlow(ctx)

		flowCh <- false
		time.Sleep(30 * time.Millisecond)
		assert.False(t, capturedActive.Load())
	})

	t.Run("NilFlowCh", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		// flowCh is nil by default
		done := make(chan struct{})
		go func() { p.handleFlow(t.Context()); close(done) }()
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("handleFlow did not exit with nil flowCh")
		}
	})

	t.Run("ClosedFlowCh", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		flowCh := make(chan bool)
		p.stateMu.Lock()
		p.flowCh = flowCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		go func() { p.handleFlow(ctx); close(done) }()
		close(flowCh)
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("handleFlow did not exit after channel close")
		}
	})
}

func TestPublisherHandleReturns(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("CallsOnReturn", func(t *testing.T) {
		capturedBodyCh := make(chan []byte, 1)

		p := newPublisher("test", nil, topoReg, PublisherOptions{
			OnReturn: func(msg message.Message) { capturedBodyCh <- msg.Body },
		}, topology.NewExchange("exchange"))

		returnCh := make(chan transport.Return, 1)
		p.stateMu.Lock()
		p.returnCh = returnCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go p.handleReturns(ctx)

		returnCh <- transport.Return{Body: []byte("returned")}
		select {
		case body := <-capturedBodyCh:
			assert.Equal(t, []byte("returned"), body)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("OnReturn was not called")
		}
	})

	t.Run("NilOnReturnDiscards", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{
			OnReturn: nil,
		}, topology.NewExchange("exchange"))

		returnCh := make(chan transport.Return, 1)
		p.stateMu.Lock()
		p.returnCh = returnCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// should not panic
		go p.handleReturns(ctx)
		returnCh <- transport.Return{Body: []byte("discarded")}
		time.Sleep(20 * time.Millisecond)
	})

	t.Run("NilReturnCh", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		// returnCh is nil by default
		done := make(chan struct{})
		go func() { p.handleReturns(t.Context()); close(done) }()
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("handleReturns did not exit with nil returnCh")
		}
	})

	t.Run("ClosedReturnCh", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		returnCh := make(chan transport.Return)
		p.stateMu.Lock()
		p.returnCh = returnCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		go func() { p.handleReturns(ctx); close(done) }()
		close(returnCh)
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("handleReturns did not exit after channel close")
		}
	})
}

func TestPublisherHandleConfirmations(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("NilConfirmCh", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		// confirmCh is nil by default
		done := make(chan struct{})
		go func() { p.handleConfirmations(t.Context()); close(done) }()
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("handleConfirmations did not exit with nil confirmCh")
		}
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		confirmCh := make(chan transport.Confirmation, 1)
		p.stateMu.Lock()
		p.confirmCh = confirmCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // cancel immediately

		done := make(chan struct{})
		go func() { p.handleConfirmations(ctx); close(done) }()

		// must NOT exit on ctx cancellation alone
		select {
		case <-done:
			t.Fatal("handleConfirmations exited on context cancel; it must drain until channel is closed")
		case <-time.After(100 * time.Millisecond):
		}

		// must exit once the channel is closed (simulating ch.Close() in disconnect)
		close(confirmCh)
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("handleConfirmations did not exit after channel close")
		}
	})

	t.Run("ClosedConfirmCh", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		confirmCh := make(chan transport.Confirmation)
		p.stateMu.Lock()
		p.confirmCh = confirmCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		go func() { p.handleConfirmations(ctx); close(done) }()
		close(confirmCh)
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("handleConfirmations did not exit after channel close")
		}
	})

	t.Run("DrainsConfirmations", func(t *testing.T) {
		p := newPublisher("test", nil, topoReg, PublisherOptions{}, topology.NewExchange("exchange"))
		confirmCh := make(chan transport.Confirmation, 3)
		p.stateMu.Lock()
		p.confirmCh = confirmCh
		p.stateMu.Unlock()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go p.handleConfirmations(ctx)

		confirmCh <- transport.Confirmation{Ack: true}
		confirmCh <- transport.Confirmation{Ack: false}
		time.Sleep(30 * time.Millisecond)
		assert.Equal(t, 0, len(confirmCh))
	})
}
