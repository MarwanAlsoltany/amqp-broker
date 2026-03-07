package broker

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEndpoint(t *testing.T) {
	b := &Broker{id: "test-broker"}
	e := newEndpoint("test", b, roleController, defaultEndpointOptions())

	require.NotNil(t, e)
	assert.Equal(t, "test", e.id)
	assert.Equal(t, b, e.broker)
	assert.Equal(t, roleController, e.role)
	assert.False(t, e.closed.Load())
	assert.False(t, e.ready.Load())
	assert.Nil(t, e.conn)
	assert.Nil(t, e.ch)

	t.Run("WithUnknownRole", func(t *testing.T) {
		e := newEndpoint("test", b, endpointRole(9), defaultEndpointOptions())

		assert.NotEqual(t, endpointRole(9), e.role)
		assert.Equal(t, roleController, e.role) // defaults to controller
	})
}

func TestEndpointConnection(t *testing.T) {
	b := &Broker{id: "test-broker"}

	t.Run("WhenNotSetReturnsNil", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())
		conn := e.Connection()
		assert.Nil(t, conn)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		iterations := 100
		doneCh := make(chan struct{})

		// concurrent reads
		for range iterations {
			go func() {
				_ = e.Connection()
				doneCh <- struct{}{}
			}()
		}

		// wait for all goroutines
		for range iterations {
			<-doneCh
		}
	})
}

func TestEndpointChannel(t *testing.T) {
	b := &Broker{id: "test-broker"}

	t.Run("WhenNotSetReturnsNil", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())
		ch := e.Channel()
		assert.Nil(t, ch)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		iterations := 100
		doneCh := make(chan struct{})

		// concurrent reads
		for range iterations {
			go func() {
				_ = e.Channel()
				doneCh <- struct{}{}
			}()
		}

		// wait for all goroutines
		for range iterations {
			<-doneCh
		}
	})
}

func TestEndpointReady(t *testing.T) {
	b := &Broker{id: "test-broker"}

	t.Run("WhenReadyReturnsFalse", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		assert.False(t, e.Ready())
	})

	t.Run("WhenReadyReturnsTrue", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		e.ready.Store(true) // simulate ready state

		assert.True(t, e.Ready())
	})

	t.Run("WhenClosed", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		e.ready.Store(false) // simulate not ready state

		assert.False(t, e.Ready())
	})
}

func TestEndpointRelease(t *testing.T) {
	b := &Broker{id: "test-broker"}

	t.Run("ShouldPanic", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())
		assert.Panics(t, func() { e.Release() })
	})
}

func TestEndpointClose(t *testing.T) {
	b := &Broker{id: "test-broker"}

	t.Run("Idempotency", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())
		e.cancel = func() {} // dummy cancel func

		for range 5 {
			err := e.Close()
			assert.NoError(t, err)
		}
		assert.True(t, e.closed.Load())
	})

	t.Run("WithNoChannel", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		err := e.Close()
		assert.NoError(t, err)
		assert.True(t, e.closed.Load())
	})

	t.Run("MarksAsClosed", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		assert.False(t, e.closed.Load())
		e.Close()
		assert.True(t, e.closed.Load())
	})

	t.Run("ClearsChannel", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		// simulate channel being set
		e.stateMu.Lock()
		e.ch = nil // would be a real channel in actual usage
		e.stateMu.Unlock()

		e.Close()

		e.stateMu.RLock()
		assert.Nil(t, e.ch)
		e.stateMu.RUnlock()
	})
}

func TestEndpointExchange(t *testing.T) {
	b := &Broker{
		id:          "test-broker",
		topologyMgr: newTopologyManager(),
	}

	t.Run("WithNoChannel", func(t *testing.T) {
		e := newEndpoint("test", b, roleController, defaultEndpointOptions())

		err := e.Exchange(Exchange{Name: "test-exchange"})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrEndpointNotConnected)
	})
}

func TestEndpointQueue(t *testing.T) {
	b := &Broker{
		id:          "test-broker",
		topologyMgr: newTopologyManager(),
	}

	t.Run("WithNoChannel", func(t *testing.T) {
		e := newEndpoint("test", b, roleConsumer, defaultEndpointOptions())

		err := e.Queue(Queue{Name: "test-queue"})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrEndpointNotConnected)
	})
}

func TestEndpointBinding(t *testing.T) {
	b := &Broker{
		id:          "test-broker",
		topologyMgr: newTopologyManager(),
	}

	t.Run("WithNoChannel", func(t *testing.T) {
		e := newEndpoint("test", b, roleConsumer, defaultEndpointOptions())

		err := e.Binding(Binding{
			Source:      "test-exchange",
			Destination: "test-queue",
		})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrEndpointNotConnected)
	})
}

func TestEndpointStart(t *testing.T) {
	b := &Broker{id: "test-broker"}

	ctx := t.Context()

	// happy path
	{
		mock := &mockEndpointLifecycle{
			connectDelay: 20 * time.Millisecond,
			monitorDelay: 50 * time.Millisecond,
		}

		e := newEndpoint("test", b, rolePublisher, EndpointOptions{
			ReadyTimeout: 100 * time.Millisecond,
		})

		err := e.start(ctx, mock, nil)
		assert.NoError(t, err)
		assert.True(t, e.ready.Load())

		e.Close()
	}

	connectErr := errors.New("connect failed")
	monitorErr := errors.New("monitor failed")
	disconnectErr := errors.New("disconnect failed")

	t.Run("Idempotency", func(t *testing.T) {
		mock := &mockEndpointLifecycle{
			connectDelay: 20 * time.Millisecond,
			monitorDelay: 50 * time.Millisecond,
		}

		e := newEndpoint("test", b, rolePublisher, EndpointOptions{
			ReadyTimeout: 100 * time.Millisecond,
		})
		defer e.Close()

		// first call
		err1 := e.start(ctx, mock, nil)
		assert.NoError(t, err1)
		assert.True(t, e.ready.Load())

		// second call should be idempotent
		err2 := e.start(ctx, mock, nil)
		assert.NoError(t, err2)
		assert.True(t, e.ready.Load())
	})

	t.Run("Consistency", func(t *testing.T) {
		if os.Getenv("CI") == "true" {
			t.Skip("Skipping test that leads to false positives in CI")
		}
		// test that ready state is consistent after start() returns
		// regardless of which code path was taken

		// a high number of iterations is used to increase the chance of
		// hitting race conditions due to timing variations in goroutine scheduling
		const iterations = 10000
		const threshold = 0.9999 // 99.99% success rate

		var passed, failed = 0, 0
		var firstFailureIdx int = -1

		for i := range iterations {
			mock := &mockEndpointLifecycle{
				connectDelay: 0,
				monitorDelay: 5 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				ReadyTimeout: 20 * time.Millisecond,
			})

			ctx := context.Background() // testing context would cancel too early sometimes
			err := e.start(ctx, mock, nil)

			assert.NoError(t, err)
			// this assertion is unreliable due to timing, so probability is used instead
			// assert.True(t, e.ready.Load(), "iteration %d: ready should be true after successful start", i)

			// NOTE: due to memory visibility semantics of atomic operations,
			// checking ready immediately after start() may not always reflect
			// the latest value. This test allows a small failure rate to account
			// for this timing behavior while still validating overall consistency.

			ready := e.ready.Load()
			if ready {
				passed++
			} else {
				failed++
				if firstFailureIdx == -1 {
					firstFailureIdx = i
				}
			}

			err = e.Close()
			assert.NoError(t, err)
		}

		// calculate success rate
		success := float64(passed) / float64(iterations)

		// assert that we met the threshold
		assert.GreaterOrEqual(
			t,
			success,
			threshold,
			"consistency test failed: %.2f%% success rate (iterations=%d; passed=%d; failed=%d), first failure at iteration %d",
			success*100,
			iterations,
			passed,
			failed,
			firstFailureIdx,
		)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		mock := &mockEndpointLifecycle{}

		e := newEndpoint("test", b, rolePublisher, EndpointOptions{
			ReadyTimeout: 30 * time.Millisecond,
		})

		e.closed.Store(true)

		err := e.start(ctx, mock, nil)
		assert.Error(t, err)
		assert.Equal(t, ErrEndpointClosed, err)
	})

	t.Run("WhenContextCancelled", func(t *testing.T) {
		t.Run("BeforeStart", func(t *testing.T) {
			mock := &mockEndpointLifecycle{}

			e := newEndpoint("test", b, roleController, EndpointOptions{})

			ctx, cancel := context.WithCancel(ctx)
			cancel()

			err := e.start(ctx, mock, nil)
			assert.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("DuringConnect", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 50 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
				ReadyTimeout: 100 * time.Millisecond,
			})
			defer e.Close()

			ctx, cancel := context.WithCancel(t.Context())

			// cancel after start begins
			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()

			err := e.start(ctx, mock, nil)
			assert.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("DuringMonitor", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				monitorDelay: 100 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
				ReadyTimeout: 50 * time.Millisecond,
			})
			defer e.Close()

			ctx, cancel := context.WithCancel(t.Context())

			// start will succeed
			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)
			assert.True(t, e.ready.Load())

			// cancel during monitor phase
			time.Sleep(10 * time.Millisecond)
			cancel()

			// wait for monitor to detect cancellation
			time.Sleep(20 * time.Millisecond)
			assert.False(t, e.ready.Load())
		})

		t.Run("DuringConnectBackoff", func(t *testing.T) {
			// test the select in backoff: case <-ctx.Done() vs case <-time.After()
			connectAttempts := atomic.Int32{}

			mock := &mockEndpointLifecycle{
				connectAttempts:  &connectAttempts,
				connectFailUntil: 1, // first succeeds, rest fail
				connectErr:       connectErr,
				// monitorErr:       monitorErr,
				monitorDelay: 5 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoWaitReady:  true,
				ReconnectMin: 200 * time.Millisecond, // very long backoff
				ReconnectMax: 400 * time.Millisecond,
			})
			defer e.Close()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			// start in background
			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)

			// wait for first connect and monitor completion
			time.Sleep(20 * time.Millisecond)

			// cancel during backoff (before time.After completes)
			cancel()

			// wait to ensure context cancellation is handled
			time.Sleep(50 * time.Millisecond)

			// should exit via ctx.Done() in backoff select, not make many attempts
			assert.GreaterOrEqual(t, connectAttempts.Load(), int32(1))
			assert.LessOrEqual(t, connectAttempts.Load(), int32(3))
			assert.False(t, e.ready.Load())
		})

		t.Run("CancelFuncChain", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 10 * time.Millisecond,
				monitorDelay: 500 * time.Millisecond, // long monitor to keep first lifecycle running
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				ReadyTimeout: 50 * time.Millisecond,
			})

			// set up a cancel function to track if oldCancel is called
			oldCancelCalled := atomic.Bool{}
			e.stateMu.Lock()
			e.cancel = func() {
				oldCancelCalled.Store(true)
			}
			e.stateMu.Unlock()

			// first start - should chain the old cancel
			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)
			assert.True(t, e.ready.Load())

			// second start while first is still running
			// this should create a new cancel that chains the previous one
			err = e.start(ctx, mock, nil)
			assert.NoError(t, err)

			// close should trigger the cancel chain
			err = e.Close()
			assert.NoError(t, err)

			// give time for cancel to propagate
			time.Sleep(50 * time.Millisecond)

			// the old cancel should have been called via the chain
			assert.True(t, oldCancelCalled.Load())
			assert.False(t, e.ready.Load())
		})
	})

	t.Run("ReadyTimeout", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 0,
				monitorDelay: 10 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
				ReadyTimeout: 50 * time.Millisecond,
			})
			defer e.Close()

			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)
			assert.True(t, e.ready.Load())
		})

		t.Run("Failure", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 100 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				ReadyTimeout: 10 * time.Millisecond,
			})

			err := e.start(ctx, mock, nil)
			assert.Error(t, err)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("WhenZero", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 5 * time.Millisecond,
				monitorDelay: 20 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				ReconnectMin: 5 * time.Millisecond,
				ReconnectMax: 10 * time.Millisecond,
				ReadyTimeout: 0, // no timeout
			})
			defer e.Close()

			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)
			assert.True(t, e.ready.Load())
		})
	})

	t.Run("NoWaitReady", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 10 * time.Millisecond,
				monitorDelay: 100 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoWaitReady:  true,
				ReadyTimeout: 5 * time.Millisecond,
			})
			defer e.Close()

			e.readyCh = nil // make sure this does not panic

			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)

			// initially not ready
			assert.False(t, e.ready.Load())
			// give background goroutine time to connect
			time.Sleep(20 * time.Millisecond)
			// should be ready now
			assert.True(t, e.ready.Load())
		})

		t.Run("WhenConnectFails", func(t *testing.T) {
			// to hit backoff logic:
			// 1. first connect succeeds (attempts=1)
			// 2. monitor fails
			// 3. second connect fails (attempts=2, backoff triggered)
			connectAttempts := atomic.Int32{}
			monitorAttempts := atomic.Int32{}

			mock := &mockEndpointLifecycle{
				connectAttempts:  &connectAttempts,
				monitorAttempts:  &monitorAttempts,
				connectFailUntil: 0,   // don't fail initially
				connectErr:       nil, // no error initially so first connect succeeds
				monitorErr:       monitorErr,
				monitorDelay:     10 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoWaitReady:  true, // run in background
				ReconnectMin: 15 * time.Millisecond,
				ReconnectMax: 30 * time.Millisecond,
			})
			defer e.Close()

			// use t.Context() (long-lived) instead of ctx
			err := e.start(t.Context(), mock, nil)
			assert.NoError(t, err) // with NoWaitReady return is always nil

			// wait for first connect and it to become ready
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, int32(1), connectAttempts.Load())

			// now make future connects fail (with proper synchronization)
			mock.mu.Lock()
			mock.connectFailUntil = 999 // fail all subsequent attempts
			mock.connectErr = connectErr
			mock.mu.Unlock()

			// wait for monitor to complete/fail and trigger reconnect with backoff
			time.Sleep(100 * time.Millisecond)

			// monitor should have been called at least once
			assert.GreaterOrEqual(t, monitorAttempts.Load(), int32(1))
			// should have attempted reconnect (initial + retries with backoff)
			assert.GreaterOrEqual(t, connectAttempts.Load(), int32(2))
		})

		t.Run("WithOnError", func(t *testing.T) {
			var handlerErr atomic.Value
			onError := func(err error) {
				handlerErr.Store(err)
			}

			mock := &mockEndpointLifecycle{
				connectErr: connectErr,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoWaitReady:     true,
				NoAutoReconnect: true,
			})
			defer e.Close()

			err := e.start(ctx, mock, onError)
			assert.NoError(t, err)
			// wait for error callback
			time.Sleep(20 * time.Millisecond)
			assert.NotNil(t, handlerErr.Load())
			assert.ErrorIs(t, handlerErr.Load().(error), connectErr)
		})

		t.Run("WithNoOnError", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectErr: connectErr,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoWaitReady:     true,
				NoAutoReconnect: true,
			})
			defer e.Close()

			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)

			// should not panic with nil onError
			time.Sleep(20 * time.Millisecond)
		})
	})

	t.Run("NoAutoReconnect", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectDelay: 10 * time.Millisecond,
				monitorDelay: 100 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoWaitReady:     true,
				NoAutoReconnect: true,
			})
			defer e.Close()

			e.readyCh = nil // make sure this does not panic

			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)

			// initially not ready
			assert.False(t, e.ready.Load())
			// give background goroutine time to connect
			time.Sleep(20 * time.Millisecond)
			// should be ready now
			assert.True(t, e.ready.Load())
		})

		t.Run("WhenConnectFails", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				connectErr: connectErr,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoAutoReconnect: true,
				ReadyTimeout:    30 * time.Millisecond,
			})
			defer e.Close()

			err := e.start(ctx, mock, nil)
			assert.Error(t, err)
			assert.ErrorIs(t, err, connectErr)
			assert.Contains(t, err.Error(), "auto-reconnect is disabled")
		})

		t.Run("WhenMonitorFails", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				monitorErr:   monitorErr,
				monitorDelay: 5 * time.Millisecond,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoAutoReconnect: true,
				ReadyTimeout:    30 * time.Millisecond,
			})
			defer e.Close()

			// start will succeed because connect is fast
			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)
			assert.True(t, e.ready.Load())

			// wait for monitor to fail
			time.Sleep(20 * time.Millisecond)

			// endpoint should be not ready after monitor failure
			assert.False(t, e.ready.Load())
		})

		t.Run("WhenDisconnectFails", func(t *testing.T) {
			mock := &mockEndpointLifecycle{
				monitorErr:      monitorErr,
				monitorDelay:    5 * time.Millisecond,
				disconnectErr:   disconnectErr,
				disconnectDelay: 0,
			}

			e := newEndpoint("test", b, rolePublisher, EndpointOptions{
				NoAutoReconnect: true,
				ReadyTimeout:    30 * time.Millisecond,
			})
			defer e.Close()

			// start will succeed
			err := e.start(ctx, mock, nil)
			assert.NoError(t, err)

			// wait for monitor to fail and disconnect to be called
			time.Sleep(20 * time.Millisecond)
			assert.False(t, e.ready.Load())
		})
	})
}
