package endpoint

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEndpoint(t *testing.T) {
	topoReg := topology.NewRegistry()

	b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())

	require.NotNil(t, b)
	assert.Equal(t, "test", b.id)
	assert.Equal(t, roleController, b.role)
	assert.Nil(t, b.conn)
	assert.Nil(t, b.ch)
	assert.False(t, b.closed.Load())
	assert.False(t, b.ready.Load())
	assert.NotNil(t, b.readyCh)

	t.Run("WithUnknownRole", func(t *testing.T) {
		b := newEndpoint("test", role(99), nil, topoReg, DefaultEndpointOptions())
		assert.Equal(t, roleController, b.role)
	})
}

func TestEndpointConnection(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WhenNotSetReturnsNil", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		assert.Nil(t, b.Connection())
	})

	t.Run("WhenSetReturnsIt", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		conn := newMockConnection()
		b.stateMu.Lock()
		b.conn = conn
		b.stateMu.Unlock()
		assert.Equal(t, conn, b.Connection())
	})

	t.Run("WhenClosedReturnsNil", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		conn := newMockConnection()
		conn.closed.Store(true) // simulate closed
		b.stateMu.Lock()
		b.conn = conn
		b.stateMu.Unlock()
		assert.Nil(t, b.Connection())
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		done := make(chan struct{})
		for range 100 {
			go func() { _ = b.Connection(); done <- struct{}{} }()
		}
		for range 100 {
			<-done
		}
	})
}

func TestEndpointChannel(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WhenNotSetReturnsNil", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		assert.Nil(t, b.Channel())
	})

	t.Run("WhenSetReturnsIt", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		ch := newMockChannel()
		b.stateMu.Lock()
		b.ch = ch
		b.stateMu.Unlock()
		assert.Equal(t, ch, b.Channel())
	})

	t.Run("WhenClosedReturnsNil", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		ch := newMockChannel()
		ch.closed.Store(true)
		b.stateMu.Lock()
		b.ch = ch
		b.stateMu.Unlock()
		assert.Nil(t, b.Channel())
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		done := make(chan struct{})
		for range 100 {
			go func() { _ = b.Channel(); done <- struct{}{} }()
		}
		for range 100 {
			<-done
		}
	})
}

func TestEndpointReady(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("FalseInitially", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		assert.False(t, b.Ready())
	})

	t.Run("TrueWhenSet", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		b.ready.Store(true)
		assert.True(t, b.Ready())
	})

	t.Run("FalseAfterReset", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		b.ready.Store(true)
		b.ready.Store(false)
		assert.False(t, b.Ready())
	})
}

func TestEndpointClose(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("Idempotency", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		b.cancel = func() {} // set a dummy cancel to avoid nil panic

		for range 5 {
			err := b.Close()
			assert.NoError(t, err)
		}
		assert.True(t, b.closed.Load())
	})

	t.Run("WithNoChannel", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		assert.NoError(t, b.Close())
		assert.True(t, b.closed.Load())
	})

	t.Run("MarksAsClosed", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		assert.False(t, b.closed.Load())
		_ = b.Close()
		assert.True(t, b.closed.Load())
	})

	t.Run("ClearsChannel", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		b.stateMu.Lock()
		b.ch = newMockChannel()
		b.stateMu.Unlock()

		require.NoError(t, b.Close())

		b.stateMu.RLock()
		assert.Nil(t, b.ch)
		b.stateMu.RUnlock()
	})

	t.Run("CallsCancelFunc", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		called := atomic.Bool{}
		b.cancel = func() { called.Store(true) }

		_ = b.Close()
		assert.True(t, called.Load())
	})

	t.Run("WithChannelCloseError", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		ch := newMockChannel().withCloseError(assert.AnError)
		b.stateMu.Lock()
		b.ch = ch
		b.stateMu.Unlock()

		err := b.Close()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrEndpoint)
		assert.ErrorIs(t, err, assert.AnError)
	})
}

func TestEndpointExchange(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WithNoChannel", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		err := b.Exchange(topology.NewExchange("test-exchange"))
		assert.ErrorIs(t, err, ErrEndpointNotConnected)
	})

	t.Run("WithChannel", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		b.stateMu.Lock()
		b.ch = newMockChannel()
		b.stateMu.Unlock()
		err := b.Exchange(topology.NewExchange("test-exchange"))
		assert.NoError(t, err)
	})
}

func TestEndpointQueue(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WithNoChannel", func(t *testing.T) {
		b := newEndpoint("test", roleConsumer, nil, topoReg, DefaultEndpointOptions())
		err := b.Queue(topology.NewQueue("test-queue"))
		assert.ErrorIs(t, err, ErrEndpointNotConnected)
	})

	t.Run("WithChannel", func(t *testing.T) {
		b := newEndpoint("test", roleConsumer, nil, topoReg, DefaultEndpointOptions())
		b.stateMu.Lock()
		b.ch = newMockChannel()
		b.stateMu.Unlock()
		err := b.Queue(topology.NewQueue("test-queue"))
		assert.NoError(t, err)
	})
}

func TestEndpointBinding(t *testing.T) {
	topoReg := topology.NewRegistry()

	t.Run("WithNoChannel", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		err := b.Binding(topology.NewBinding("s", "d", "k"))
		assert.ErrorIs(t, err, ErrEndpointNotConnected)
	})

	t.Run("WithChannel", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, DefaultEndpointOptions())
		b.stateMu.Lock()
		b.ch = newMockChannel()
		b.stateMu.Unlock()
		err := b.Binding(topology.NewBinding("s", "d", "k"))
		assert.NoError(t, err)
	})
}

func TestEndpointStart(t *testing.T) {
	topoReg := topology.NewRegistry()
	ctx := t.Context()

	// happy path
	{
		mock := &mockEndpointLifecycle{
			connectDelay: 20 * time.Millisecond,
			monitorDelay: 50 * time.Millisecond,
		}
		b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
			ReadyTimeout: 200 * time.Millisecond,
			ReconnectMin: DefaultReconnectMin,
			ReconnectMax: DefaultReconnectMax,
		})
		err := b.start(ctx, mock, nil)
		assert.NoError(t, err)
		assert.True(t, b.ready.Load())
		_ = b.Close()
	}

	connectErr := errors.New("connect failed")
	monitorErr := errors.New("monitor failed")
	disconnectErr := errors.New("disconnect failed")
	_ = disconnectErr

	t.Run("AlreadyReady", func(t *testing.T) {
		b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
			ReadyTimeout: 100 * time.Millisecond,
		})
		b.ready.Store(true)
		err := b.start(ctx, &mockEndpointLifecycle{}, nil)
		assert.NoError(t, err)
	})

	t.Run("Idempotency", func(t *testing.T) {
		mock := &mockEndpointLifecycle{
			connectDelay: 20 * time.Millisecond,
			monitorDelay: 50 * time.Millisecond,
		}
		b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
			ReadyTimeout: 200 * time.Millisecond,
			ReconnectMin: DefaultReconnectMin,
			ReconnectMax: DefaultReconnectMax,
		})
		defer b.Close()

		require.NoError(t, b.start(ctx, mock, nil))
		assert.True(t, b.ready.Load())

		// second call is a no-op
		require.NoError(t, b.start(ctx, mock, nil))
		assert.True(t, b.ready.Load())
	})

	t.Run("WhenContextCancelled", func(t *testing.T) {
		t.Run("BeforeStart", func(t *testing.T) {
			mock := &mockEndpointLifecycle{}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				ReconnectMin: DefaultReconnectMin,
				ReconnectMax: DefaultReconnectMax,
			})

			cancelledCtx, cancel := context.WithCancel(ctx)
			cancel()

			err := b.start(cancelledCtx, mock, nil)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("DuringConnect", func(t *testing.T) {
			mock := &mockEndpointLifecycle{connectDelay: 100 * time.Millisecond}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				ReadyTimeout: 200 * time.Millisecond,
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
			})
			defer b.Close()

			cancelCtx, cancel := context.WithCancel(ctx)
			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()

			err := b.start(cancelCtx, mock, nil)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("DuringMonitor", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				monitorDelay: 100 * time.Millisecond,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
				ReadyTimeout: 50 * time.Millisecond,
			})
			defer b.Close()

			cancelCtx, cancel := context.WithCancel(t.Context())

			err := b.start(cancelCtx, mock, nil)
			assert.NoError(t, err)
			assert.True(t, b.ready.Load())

			// cancel during the monitor phase
			time.Sleep(10 * time.Millisecond)
			cancel()
			time.Sleep(20 * time.Millisecond)
			assert.False(t, b.ready.Load())
		})

		t.Run("DuringConnectBackoff", func(t *testing.T) {
			connectAttempts := atomic.Int32{}
			mock := &mockEndpointLifecycle{
				connectAttempts: &connectAttempts,
				monitorDelay:    5 * time.Millisecond,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoWaitReady:  true,
				ReconnectMin: 200 * time.Millisecond, // long backoff
				ReconnectMax: 400 * time.Millisecond,
			})
			defer b.Close()

			cancelCtx, cancel := context.WithCancel(t.Context())
			defer cancel()

			err := b.start(cancelCtx, mock, nil)
			assert.NoError(t, err)

			// wait for first connect and monitor to complete
			time.Sleep(20 * time.Millisecond)

			// make subsequent connects fail to trigger backoff
			mock.mu.Lock()
			mock.connectErr = connectErr
			mock.connectFailUntil = 999
			mock.mu.Unlock()

			// wait for second connect to fail and enter backoff
			time.Sleep(20 * time.Millisecond)

			// cancel during backoff (200ms timer)
			cancel()
			time.Sleep(50 * time.Millisecond)

			assert.GreaterOrEqual(t, connectAttempts.Load(), int32(1))
			assert.LessOrEqual(t, connectAttempts.Load(), int32(3))
			assert.False(t, b.ready.Load())
		})
	})

	t.Run("ReadyTimeout", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			mock := &mockEndpointLifecycle{monitorDelay: 10 * time.Millisecond}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				ReadyTimeout: 50 * time.Millisecond,
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
			})
			defer b.Close()

			assert.NoError(t, b.start(ctx, mock, nil))
			assert.True(t, b.ready.Load())
		})

		t.Run("Failure", func(t *testing.T) {
			mock := &mockEndpointLifecycle{connectDelay: 200 * time.Millisecond}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				ReadyTimeout: 10 * time.Millisecond,
				ReconnectMin: DefaultReconnectMin,
				ReconnectMax: DefaultReconnectMax,
			})

			err := b.start(ctx, mock, nil)
			assert.Error(t, err)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("WhenZero", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 5 * time.Millisecond,
				monitorDelay: 20 * time.Millisecond,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
				ReadyTimeout: 0, // no timeout
			})
			defer b.Close()

			err := b.start(ctx, mock, nil)
			assert.NoError(t, err)
			assert.True(t, b.ready.Load())
		})
	})

	t.Run("NoWaitReady", func(t *testing.T) {
		t.Run("ReturnsImmediately", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 50 * time.Millisecond,
				monitorDelay: 200 * time.Millisecond,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoWaitReady:  true,
				ReconnectMin: DefaultReconnectMin,
				ReconnectMax: DefaultReconnectMax,
			})
			defer b.Close()

			b.readyCh = nil // ensure nil readyCh does not panic
			assert.NoError(t, b.start(ctx, mock, nil))
			assert.False(t, b.ready.Load()) // not ready yet

			time.Sleep(80 * time.Millisecond)
			assert.True(t, b.ready.Load()) // ready after connect delay
		})

		t.Run("WithOnError", func(t *testing.T) {
			var capturedErr atomic.Value
			mock := &mockEndpointLifecycle{connectErr: connectErr}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoWaitReady:     true,
				NoAutoReconnect: true,
				ReconnectMin:    DefaultReconnectMin,
				ReconnectMax:    DefaultReconnectMax,
			})
			defer b.Close()

			assert.NoError(t, b.start(ctx, mock, func(err error) { capturedErr.Store(err) }))
			time.Sleep(30 * time.Millisecond)
			require.NotNil(t, capturedErr.Load())
			assert.ErrorIs(t, capturedErr.Load().(error), connectErr)
		})

		t.Run("WithNoOnError", func(t *testing.T) {
			mock := &mockEndpointLifecycle{connectErr: connectErr}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoWaitReady:     true,
				NoAutoReconnect: true,
				ReconnectMin:    DefaultReconnectMin,
				ReconnectMax:    DefaultReconnectMax,
			})
			defer b.Close()

			// should not panic with nil onError
			assert.NoError(t, b.start(ctx, mock, nil))
			time.Sleep(20 * time.Millisecond)
		})

		t.Run("CancelChainCallsOldCancel", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 50 * time.Millisecond,
				monitorDelay: 100 * time.Millisecond,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoWaitReady:  true,
				ReconnectMin: DefaultReconnectMin,
				ReconnectMax: DefaultReconnectMax,
			})

			// first start: b.cancel was nil -> oldCancel=nil; b.cancel is now set
			require.NoError(t, b.start(ctx, mock, nil))
			// second start: b.ready is still false (connect takes 50ms), so the fast-path
			// is not hit; b.cancel is non-nil, so oldCancel is now non-nil -> so it is chain-called
			require.NoError(t, b.start(ctx, mock, nil))
			// Close() calls b.cancel() -> oldCancel() -> lifecycleCancelX()
			assert.NoError(t, b.Close())
		})
	})

	t.Run("NoAutoReconnect", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 10 * time.Millisecond,
				monitorDelay: 100 * time.Millisecond,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoWaitReady:     true,
				NoAutoReconnect: true,
			})
			defer b.Close()

			b.readyCh = nil // ensure nil readyCh does not panic
			assert.NoError(t, b.start(ctx, mock, nil))
			assert.False(t, b.ready.Load())
			time.Sleep(20 * time.Millisecond)
			assert.True(t, b.ready.Load())
		})

		t.Run("WhenConnectFails", func(t *testing.T) {
			mock := &mockEndpointLifecycle{connectErr: connectErr}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoAutoReconnect: true,
				ReconnectMin:    DefaultReconnectMin,
				ReconnectMax:    DefaultReconnectMax,
			})

			err := b.start(ctx, mock, nil)
			assert.ErrorIs(t, err, connectErr)
			assert.Contains(t, err.Error(), "auto-reconnect is disabled")
			assert.False(t, b.ready.Load())
		})

		t.Run("WhenMonitorFails", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				monitorErr:   monitorErr,
				monitorDelay: 5 * time.Millisecond,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoAutoReconnect: true,
				ReadyTimeout:    30 * time.Millisecond,
			})
			defer b.Close()

			err := b.start(ctx, mock, nil)
			assert.NoError(t, err)
			assert.True(t, b.ready.Load())

			// wait for monitor to fail
			time.Sleep(20 * time.Millisecond)
			assert.False(t, b.ready.Load())
		})

		t.Run("WhenDisconnectFails", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				monitorErr:    monitorErr,
				monitorDelay:  5 * time.Millisecond,
				disconnectErr: disconnectErr,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				NoAutoReconnect: true,
				ReadyTimeout:    30 * time.Millisecond,
			})
			defer b.Close()

			err := b.start(ctx, mock, nil)
			assert.NoError(t, err)

			// wait for monitor to fail and disconnect to be called
			time.Sleep(20 * time.Millisecond)
			assert.False(t, b.ready.Load())
		})
	})

	t.Run("AutoReconnect", func(t *testing.T) {
		t.Run("WhenConnectFails", func(t *testing.T) {
			// with autoReconnect=true and connect failing on attempt 1,
			// run() returns the raw error (fail-fast, no backoff)
			mock := &mockEndpointLifecycle{connectErr: connectErr}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				// NoAutoReconnect: false (default)
				ReadyTimeout: 30 * time.Millisecond,
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
			})

			err := b.start(ctx, mock, nil)
			assert.ErrorIs(t, err, connectErr)
			assert.False(t, errors.Is(err, ErrEndpointNoAutoReconnect))
			assert.False(t, b.ready.Load())
		})
	})

	t.Run("Consistency", func(t *testing.T) {
		if os.Getenv("CI") == "true" {
			t.Skip("Skipping consistency test in CI due to timing variability")
		}

		// this test runs many iterations to probe the race-catcher fast-paths
		// inside start()'s for loop. Due to goroutine scheduling and atomic
		// visibility semantics, triggering them deterministically is impossible;
		// a high iteration count makes it statistically likely.
		const iterations = 5000
		const threshold = 0.999

		var passed, failed int

		for range iterations {
			mock := &mockEndpointLifecycle{
				monitorDelay: 5 * time.Millisecond,
			}
			b := newEndpoint("test", rolePublisher, nil, topoReg, EndpointOptions{
				ReadyTimeout: 20 * time.Millisecond,
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
			})
			err := b.start(context.Background(), mock, nil)
			assert.NoError(t, err)
			if b.ready.Load() {
				passed++
			} else {
				failed++
			}
			_ = b.Close()
		}

		success := float64(passed) / float64(iterations)
		assert.GreaterOrEqual(t, success, threshold,
			"consistency test: %.2f%% success rate (passed=%d, failed=%d)",
			success*100, passed, failed,
		)
	})
}

func TestEndpointRun(t *testing.T) {
	topoReg := topology.NewRegistry()
	ctx := t.Context()
	noop := func(_ context.Context) error { return nil }

	t.Run("WhenClosedAtStart", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, EndpointOptions{
			ReconnectMin: 5 * time.Millisecond,
			ReconnectMax: 10 * time.Millisecond,
		})
		b.closed.Store(true)
		err := b.run(ctx, noop, noop, noop)
		assert.ErrorIs(t, err, ErrEndpointClosed)
	})

	t.Run("NoAutoReconnectAfterMonitorSuccess", func(t *testing.T) {
		b := newEndpoint("test", roleController, nil, topoReg, EndpointOptions{
			NoAutoReconnect: true,
			ReconnectMin:    5 * time.Millisecond,
			ReconnectMax:    10 * time.Millisecond,
		})
		err := b.run(ctx, noop, noop, noop)
		assert.NoError(t, err)
	})

	t.Run("NoAutoReconnectAfterMonitorError", func(t *testing.T) {
		monitorErr := errors.New("monitor failed")
		b := newEndpoint("test", roleController, nil, topoReg, EndpointOptions{
			NoAutoReconnect: true,
			ReconnectMin:    5 * time.Millisecond,
			ReconnectMax:    10 * time.Millisecond,
		})
		err := b.run(ctx, noop, noop, func(_ context.Context) error { return monitorErr })
		assert.ErrorIs(t, err, ErrEndpointNoAutoReconnect)
		assert.ErrorIs(t, err, monitorErr)
	})

	t.Run("ContextCancelledDuringReconnectBackoff", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		b := newEndpoint("test", roleController, nil, topoReg, EndpointOptions{
			ReconnectMin: 500 * time.Millisecond,
			ReconnectMax: 1 * time.Second,
		})
		// first monitor returns nil -> backoff starts (500ms); cancel well within backoff
		go func() { time.Sleep(5 * time.Millisecond); cancel() }()
		err := b.run(cancelCtx, noop, noop, noop)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("AutoReconnectLoopsOnMonitorSuccess", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		var monitorCount atomic.Int32
		monitorFn := func(ctx context.Context) error {
			if monitorCount.Add(1) >= 2 {
				cancel()
				return ctx.Err()
			}
			return nil
		}
		b := newEndpoint("test", roleController, nil, topoReg, EndpointOptions{
			ReconnectMin: 5 * time.Millisecond,
			ReconnectMax: 10 * time.Millisecond,
		})
		err := b.run(cancelCtx, noop, noop, monitorFn)
		assert.ErrorIs(t, err, context.Canceled)
		assert.GreaterOrEqual(t, monitorCount.Load(), int32(2))
	})

	t.Run("NilReadyCh", func(t *testing.T) {
		// use zero EndpointOptions so run() exercises the reconnectMin/Max default branches
		b := newEndpoint("test", roleController, nil, topoReg, EndpointOptions{
			NoAutoReconnect: true,
			// ReconnectMin/Max intentionally zero to trigger defaults inside run()
		})
		b.stateMu.Lock()
		b.readyCh = nil // force nil so run() recreates it (safe check)
		b.stateMu.Unlock()
		err := b.run(ctx, noop, noop, noop)
		assert.NoError(t, err)
	})

	t.Run("AutoReconnectConnectFailsBackoffRetry", func(t *testing.T) {
		// exercises the `time.After` branch inside the connect-fail backoff select
		//
		// sequence: connect1 succeeds -> monitor fails -> reconnect-backoff fires ->
		// connect2 fails (attempt 2, no fail-fast) -> connect-fail-backoff timer fires
		// -> continue -> ctx cancelled during next connect attempt
		var connectCount atomic.Int32
		var failConnects atomic.Bool

		connectFn := func(ctx context.Context) error {
			n := connectCount.Add(1)
			if failConnects.Load() && n > 1 {
				// immediate failure so the connect-fail backoff select is reached
				return errors.New("connect error")
			}
			return nil
		}
		monitorFn := func(_ context.Context) error {
			failConnects.Store(true)
			return errors.New("monitor done") // not ctx.Canceled -> triggers reconnect
		}

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		b := newEndpoint("test", roleController, nil, topoReg, EndpointOptions{
			ReconnectMin: 5 * time.Millisecond,
			ReconnectMax: 10 * time.Millisecond,
		})

		errs := make(chan error, 1)
		go func() { errs <- b.run(cancelCtx, connectFn, noop, monitorFn) }()

		// wait for: connect1 + monitor + reconnect-backoff + connect2 + connect-fail-backoff
		time.Sleep(35 * time.Millisecond)
		cancel()

		err := <-errs
		assert.ErrorIs(t, err, context.Canceled)
		assert.GreaterOrEqual(t, connectCount.Load(), int32(2))
	})
}

func TestEndpointRolePurpose(t *testing.T) {
	t.Run("Controller", func(t *testing.T) {
		assert.Equal(t, transport.ConnectionPurposeControl, roleController.purpose())
	})
	t.Run("Publisher", func(t *testing.T) {
		assert.Equal(t, transport.ConnectionPurposePublish, rolePublisher.purpose())
	})
	t.Run("Consumer", func(t *testing.T) {
		assert.Equal(t, transport.ConnectionPurposeConsume, roleConsumer.purpose())
	})
}
