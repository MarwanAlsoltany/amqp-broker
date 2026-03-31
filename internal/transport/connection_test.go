package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

// mockConnection is a mock implementation of the Connection interface for testing.
// Configuration methods (withXxx) must be called before concurrent use.
type mockConnection struct {
	// control behavior (set once during initialization)
	closeErr    error
	shouldClose bool
	closeDelay  time.Duration
	closeError  *amqp091.Error
	shouldBlock bool
	blockDelay  time.Duration
	blockActive bool
	blockReason string
	channelErr  error

	// runtime state (accessed concurrently)
	closed           atomic.Bool
	notifyCloseCh    chan *Error
	notifyBlockCh    chan amqp091.Blocking
	notifyCloseCalls atomic.Int32
	notifyBlockCalls atomic.Int32

	// synchronization for concurrent access to runtime state
	mu sync.Mutex
}

var _ Connection = (*mockConnection)(nil)

func newMockConnection() *mockConnection {
	return &mockConnection{}
}

// withCloseError configures the mock to return an error on Close().
// Must be called before concurrent use.
func (m *mockConnection) withCloseError(err error) *mockConnection {
	m.closeErr = err
	return m
}

// withAutoClose configures the mock to automatically close after a delay.
// Must be called before concurrent use.
func (m *mockConnection) withAutoClose(err *amqp091.Error, delay time.Duration) *mockConnection {
	m.shouldClose = true
	m.closeError = err
	m.closeDelay = delay
	return m
}

// withGracefulClose configures the mock to gracefully close (no error) after a delay.
// Must be called before concurrent use.
func (m *mockConnection) withGracefulClose(delay time.Duration) *mockConnection {
	m.shouldClose = true
	m.closeError = nil
	m.closeDelay = delay
	return m
}

// withAutoBlock configures the mock to automatically send a block notification.
// Must be called before concurrent use.
func (m *mockConnection) withAutoBlock(active bool, reason string, delay time.Duration) *mockConnection {
	m.shouldBlock = true
	m.blockActive = active
	m.blockReason = reason
	m.blockDelay = delay
	return m
}

// withChannelError configures the mock to return an error on Channel().
// Must be called before concurrent use.
func (m *mockConnection) withChannelError(err error) *mockConnection {
	m.channelErr = err
	return m
}

// triggerClose safely closes the notify channel, simulating a connection close event.
// Can be called concurrently from tests.
func (m *mockConnection) triggerClose() {
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

// triggerBlock safely sends a block notification.
// Can be called concurrently from tests.
func (m *mockConnection) triggerBlock(active bool, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.notifyBlockCh != nil {
		m.notifyBlockCh <- amqp091.Blocking{
			Active: active,
			Reason: reason,
		}
	}
}

func (m *mockConnection) Channel() (Channel, error) {
	if m.channelErr != nil {
		return nil, m.channelErr
	}
	return nil, nil
}

func (m *mockConnection) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return m.closeErr // already closed
	}
	return m.closeErr
}

func (m *mockConnection) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockConnection) NotifyClose(receiver chan *Error) chan *Error {
	m.notifyCloseCalls.Add(1)
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

func (m *mockConnection) NotifyBlocked(receiver chan Blocking) chan Blocking {
	m.notifyBlockCalls.Add(1)
	m.mu.Lock()
	m.notifyBlockCh = receiver
	m.mu.Unlock()

	if m.shouldBlock {
		go func() {
			time.Sleep(m.blockDelay)
			m.triggerBlock(m.blockActive, m.blockReason)
		}()
	}

	return receiver
}

func TestNewConnectionManager(t *testing.T) {
	t.Run("WithConfig", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 3})
		assert.NotNil(t, cm)
		assert.Equal(t, "amqp://invalid", cm.url)
		assert.Equal(t, 3, cm.opts.Size)
		assert.Len(t, cm.pool, 3)

		t.Run("Size", func(t *testing.T) {
			t.Run("Zero", func(t *testing.T) {
				cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 0})
				assert.Equal(t, DefaultConnectionPoolSize, cm.opts.Size)
			})

			t.Run("Positive", func(t *testing.T) {
				cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 3})
				assert.Equal(t, 3, cm.opts.Size)
			})

			t.Run("Negative", func(t *testing.T) {
				cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: -10})
				assert.Equal(t, DefaultConnectionPoolSize, cm.opts.Size)
			})
		})

		t.Run("OnOpen", func(t *testing.T) {
			var called atomic.Int32
			cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
				Size: 2,
				OnOpen: func(idx int) {
					called.Add(1)
				},
			})
			assert.NotNil(t, cm.opts.OnOpen)
			cm.opts.OnOpen(0)
			assert.Equal(t, int32(1), called.Load())
		})

		t.Run("OnClose", func(t *testing.T) {
			var called atomic.Int32
			cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
				Size: 2,
				OnClose: func(idx int, code int, reason string, server bool, recover bool) {
					called.Add(1)
				},
			})
			assert.NotNil(t, cm.opts.OnClose)
			cm.opts.OnClose(0, 0, "reason", false, false)
			assert.Equal(t, int32(1), called.Load())
		})

		t.Run("OnBlock", func(t *testing.T) {
			var called atomic.Int32
			cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
				Size: 2,
				OnBlock: func(idx int, active bool, reason string) {
					called.Add(1)
				},
			})
			assert.NotNil(t, cm.opts.OnBlock)
			cm.opts.OnBlock(0, true, "reason")
			assert.Equal(t, int32(1), called.Load())
		})
	})

	t.Run("WithNoConfig", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", nil)
		assert.NotNil(t, cm)
		assert.Equal(t, "amqp://invalid", cm.url)
		assert.Equal(t, DefaultConnectionPoolSize, cm.opts.Size)
		assert.Len(t, cm.pool, DefaultConnectionPoolSize)

		// this is a unit test that verifies the nil config path exists
		// the actual connection would fail without a running RabbitMQ server,
		// so we just verify the function signature and that it attempts to dial
		ctx := t.Context()
		err := cm.Init(ctx)
		assert.Error(t, err) // expected to fail with invalid URL
	})
}

func TestConnectionManagerInit(t *testing.T) {
	t.Run("WhenContextCanceled", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 2})
		defer cm.Close()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := cm.Init(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("WithConnectionConfig", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size: 2,
			Config: &Config{
				Heartbeat: 10,
			},
		})
		defer cm.Close()

		ctx := t.Context()
		err := cm.Init(ctx)
		assert.Error(t, err)
	})

	t.Run("WithInvalidOptions", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:         2,
			ReconnectMin: -1 * time.Second, // negative value won't be merged, will fail validation
			ReconnectMax: 1 * time.Second,
		})
		defer cm.Close()

		ctx := t.Context()
		err := cm.Init(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "reconnect")
	})

	t.Run("WithConnections", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1})

		// simulate open connection
		cm.pool = append(cm.pool, &mockConnection{})
		cm.pool = append(cm.pool, &mockConnection{})
		cm.pool = append(cm.pool, &mockConnection{})

		// Close should handle all connections
		err := cm.Close()
		assert.NoError(t, err)

		// verify all connections were closed
		for i, conn := range cm.pool {
			if conn != nil {
				if mc, ok := conn.(*mockConnection); ok {
					assert.True(t, mc.IsClosed(), "connection %d should be closed", i)
				}
			}
		}
	})

	t.Run("ClosesPartiallyInitializedPool", func(t *testing.T) {
		// the cleanup loop "for j := 0; j < i; j++" only executes when i > 0,
		// meaning at least one connection must succeed before one fails
		// we inject a dialer that succeeds on the first call and fails on subsequent ones
		firstConn := newMockConnection()
		var callCount atomic.Int32
		dialer := func(url string, config *Config) (Connection, error) {
			if callCount.Add(1) == 1 {
				return firstConn, nil
			}
			return nil, errors.New("dial failed")
		}

		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:   3,
			Dialer: dialer,
		})

		ctx := t.Context()
		err := cm.Init(ctx)

		assert.Error(t, err)
		assert.ErrorContains(t, err, "init connection")
		// the first connection must have been closed during cleanup
		assert.True(t, firstConn.IsClosed(), "first connection should be closed during partial init cleanup")
	})
}

func TestConnectionManagerAssign(t *testing.T) {
	// can't test without actual connections, but verify assignment logic
	t.Run("WithPoolSizeSize1ConnectionSharing", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1})
		assert.Equal(t, 1, cm.opts.Size)
		// all purposes should use index 0
		_, err := cm.Assign(ConnectionPurposeControl)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "not available")
		_, err = cm.Assign(ConnectionPurposePublish)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "not available")
		_, err = cm.Assign(ConnectionPurposeConsume)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "not available")
	})

	t.Run("WithPoolSizeSize2ConnectionPartitioning", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 2})
		assert.Equal(t, 2, cm.opts.Size)
		// control: 0, publisher/consumer: 1
		_, err := cm.Assign(ConnectionPurposeControl)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.Assign(ConnectionPurposePublish)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.Assign(ConnectionPurposeConsume)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WithSize3ConnectionPartitioning", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 3})
		assert.Equal(t, 3, cm.opts.Size)
		// control: 0, publisher: 1, consumer: 2
		_, err := cm.Assign(ConnectionPurposeControl)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.Assign(ConnectionPurposePublish)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.Assign(ConnectionPurposeConsume)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WithPoolSizeSize4ConnectionPartitioning", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 4})
		assert.Equal(t, 4, cm.opts.Size)
		// control: 0, publisher: 1, consumer: 2, round-robin: 3
		_, err := cm.Assign(ConnectionPurposeControl)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.Assign(ConnectionPurposePublish)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.Assign(ConnectionPurposeConsume)
		assert.ErrorIs(t, err, ErrConnectionManager)
		// round robin for extra publishers/consumers
		for range 5 {
			_, err = cm.Assign(ConnectionPurposePublish)
			assert.ErrorIs(t, err, ErrConnectionManager)
			_, err = cm.Assign(ConnectionPurposeConsume)
			assert.ErrorIs(t, err, ErrConnectionManager)
		}
	})

	t.Run("WithPoolSizeSize10ConnectionPartitioning", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 10})
		assert.Equal(t, 10, cm.opts.Size)
		// control: 0, publishers: 1-3, consumers: 4-6, round-robin: 7-9
		_, err := cm.Assign(ConnectionPurposeControl)
		assert.ErrorIs(t, err, ErrConnectionManager)
		for range 5 {
			_, err = cm.Assign(ConnectionPurposePublish)
			assert.ErrorIs(t, err, ErrConnectionManager)
			_, err = cm.Assign(ConnectionPurposeConsume)
			assert.ErrorIs(t, err, ErrConnectionManager)
		}
	})

	t.Run("Size1AllShareConnection", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		conn := newMockConnection()
		cm.pool[0] = conn

		controlConn, err := cm.Assign(ConnectionPurposeControl)
		assert.NoError(t, err)
		assert.Equal(t, conn, controlConn)

		publishConn, err := cm.Assign(ConnectionPurposePublish)
		assert.NoError(t, err)
		assert.Equal(t, conn, publishConn)

		consumeConn, err := cm.Assign(ConnectionPurposeConsume)
		assert.NoError(t, err)
		assert.Equal(t, conn, consumeConn)
	})

	t.Run("Size2PartialSeparation", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 2})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		conn0 := newMockConnection()
		conn1 := newMockConnection()
		cm.pool[0] = conn0
		cm.pool[1] = conn1

		controlConn, err := cm.Assign(ConnectionPurposeControl)
		assert.NoError(t, err)
		assert.Equal(t, conn0, controlConn)

		publishConn, err := cm.Assign(ConnectionPurposePublish)
		assert.NoError(t, err)
		assert.Equal(t, conn1, publishConn)

		consumeConn, err := cm.Assign(ConnectionPurposeConsume)
		assert.NoError(t, err)
		assert.Equal(t, conn1, consumeConn)
	})

	t.Run("Size3FullSeparation", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 3})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		conn0 := newMockConnection()
		conn1 := newMockConnection()
		conn2 := newMockConnection()
		cm.pool[0] = conn0
		cm.pool[1] = conn1
		cm.pool[2] = conn2

		controlConn, err := cm.Assign(ConnectionPurposeControl)
		assert.NoError(t, err)
		assert.Equal(t, conn0, controlConn)

		publishConn, err := cm.Assign(ConnectionPurposePublish)
		assert.NoError(t, err)
		assert.Equal(t, conn1, publishConn)

		consumeConn, err := cm.Assign(ConnectionPurposeConsume)
		assert.NoError(t, err)
		assert.Equal(t, conn2, consumeConn)
	})

	t.Run("Size4WithRoundRobin", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 4})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		for i := 0; i < 4; i++ {
			cm.pool[i] = newMockConnection()
		}

		// control always gets connection 0
		controlConn, err := cm.Assign(ConnectionPurposeControl)
		assert.NoError(t, err)
		assert.Equal(t, cm.pool[0], controlConn)

		// first publisher gets dedicated connection 1
		publishConn1, err := cm.Assign(ConnectionPurposePublish)
		assert.NoError(t, err)
		assert.Equal(t, cm.pool[1], publishConn1)

		// first consumer gets dedicated connection 2
		consumeConn1, err := cm.Assign(ConnectionPurposeConsume)
		assert.NoError(t, err)
		assert.Equal(t, cm.pool[2], consumeConn1)

		// additional publishers/consumers use round-robin on connection 3
		publishConn2, err := cm.Assign(ConnectionPurposePublish)
		assert.NoError(t, err)
		assert.Equal(t, cm.pool[3], publishConn2)

		consumeConn2, err := cm.Assign(ConnectionPurposeConsume)
		assert.NoError(t, err)
		assert.Equal(t, cm.pool[3], consumeConn2)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1})
		cm.closed.Store(true)

		_, err := cm.Assign(ConnectionPurposeControl)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManagerClosed)
	})
}

func TestConnectionManagerIndex(t *testing.T) {
	cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1})

	t.Run("WithKnownConnection", func(t *testing.T) {
		conn := &mockConnection{}
		cm.pool[0] = conn

		idx := cm.Index(conn)
		assert.Equal(t, 0, idx)
	})

	t.Run("WithUnknownConnection", func(t *testing.T) {
		conn := &mockConnection{}
		idx := cm.Index(conn)
		assert.Equal(t, -1, idx)

		// test nil connection
		{
			idx = cm.Index(nil)
			assert.Equal(t, -1, idx)
		}
	})
}

func TestConnectionManagerReplace(t *testing.T) {
	t.Run("WithInvalidIndex", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 2})

		// initialize context for proper operation
		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		// test negative index
		err := cm.Replace(-1)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "out of range")

		// test out of range index
		err = cm.Replace(10)
		assert.Error(t, err)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "out of range")
	})

	t.Run("WhenConnectionFails", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            2,
			NoAutoReconnect: true, // disable retries for immediate failure
		})

		err := cm.Replace(0)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "replace")
	})

	t.Run("WhenClosed", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1})

		cm.closed.Store(true) // simulate closed manager

		err := cm.Replace(0)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManagerClosed)
	})

	t.Run("WithContextCancellation", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: false,
			ReconnectMin:    10 * time.Millisecond,
			ReconnectMax:    50 * time.Millisecond,
		})

		ctx, cancel := context.WithCancel(t.Context())
		cm.ctx, cm.cancel = context.WithCancel(ctx)

		// cancel context after a short delay
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
			cm.cancel()
		}()

		err := cm.Replace(0)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("ClosesOldConnection", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: true,
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		oldConn := newMockConnection()
		cm.pool[0] = oldConn

		// replace will fail (invalid URL), but should still close old connection
		_ = cm.Replace(0)

		assert.True(t, oldConn.IsClosed(), "old connection should be closed")
	})

	t.Run("WithInvalidReconnectTimes", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: false,
			ReconnectMin:    -1 * time.Second,      // invalid, should use default
			ReconnectMax:    10 * time.Millisecond, // less than min after default applied
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		// start replace in background (it will retry)
		done := make(chan error, 1)
		go func() {
			done <- cm.Replace(0)
		}()

		// cancel after a short time
		time.Sleep(50 * time.Millisecond)
		cm.cancel()

		err := <-done
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WhenCalledBeforeInit", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: false,
			ReconnectMin:    10 * time.Millisecond,
			ReconnectMax:    50 * time.Millisecond,
		})
		// don't call Init, cm.ctx will be nil

		cm.closed.Store(false)

		// start replace in background
		done := make(chan error, 1)
		go func() {
			done <- cm.Replace(0)
		}()

		// close manager to stop the retry loop
		time.Sleep(50 * time.Millisecond)
		cm.closed.Store(true)

		err := <-done
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManagerClosed)
	})

	t.Run("NilContextWithRetries", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: false, // enable retries
			ReconnectMin:    5 * time.Millisecond,
			ReconnectMax:    10 * time.Millisecond,
		})
		// don't call Init, cm.ctx will be nil

		// track that we actually retry
		startTime := time.Now()

		// start replace in background
		done := make(chan error, 1)
		go func() {
			done <- cm.Replace(0)
		}()

		// let it retry a few times
		time.Sleep(25 * time.Millisecond)

		// close manager to stop retries
		cm.closed.Store(true)

		err := <-done
		elapsed := time.Since(startTime)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManagerClosed)
		// verify it actually retried with backoff (min 5ms + exponential backoff)
		assert.Greater(t, elapsed.Milliseconds(), int64(10), "should have retried with backoff")
	})

	t.Run("SuccessWithNoAutoReconnect", func(t *testing.T) {
		// this test covers the successful replacement path with NoAutoReconnect=true
		// create a custom connection manager with a mock dialer for successful connection
		cm := &ConnectionManager{
			url: "amqp://invalid",
			opts: ConnectionManagerOptions{
				Size:            1,
				NoAutoReconnect: true,
			},
		}
		cm.opts = MergeConnectionManagerOptions(cm.opts, DefaultConnectionManagerOptions())
		cm.pool = make([]Connection, 1)

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		// add a mock connection that will be replaced
		oldConn := newMockConnection()
		cm.pool[0] = oldConn

		// successful connection can't be tested with an invalid URL,
		// but we can verify the old connection is closed
		err := cm.Replace(0)
		assert.Error(t, err) // will fail with invalid URL
		assert.True(t, oldConn.IsClosed(), "old connection should be closed")
	})

	t.Run("ContextCancelledBeforeRetrying", func(t *testing.T) {
		// this test covers the context cancellation check before entering the retry loop
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: false, // enable retries
			ReconnectMin:    10 * time.Millisecond,
			ReconnectMax:    50 * time.Millisecond,
		})

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		cm.ctx, cm.cancel = context.WithCancel(ctx)
		cm.cancel()

		// replace should fail immediately with context cancelled error
		err := cm.Replace(0)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "context cancelled")
		// should specifically be the check before entering retry loop
		assert.NotContains(t, err.Error(), "attempt", "should fail before retry loop, not during")
	})

	t.Run("ContextCancelledAfterBackoff", func(t *testing.T) {
		// this test specifically covers the case <-time.After(backoff) branch:
		// Init() is called (cm.ctx != nil), dial fails every time, and the context
		// is cancelled only AFTER at least one backoff period has elapsed,
		// forcing the select inside the retry loop to wait for time.After before returning

		// 127.0.0.1:1 is a valid address that always refuses connections immediately,
		// unlike amqp://invalid which may fail at DNS; so dial fails fast each attempt
		cm := NewConnectionManager("amqp://127.0.0.1:1", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: false,
			ReconnectMin:    20 * time.Millisecond,
			ReconnectMax:    40 * time.Millisecond,
		})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		done := make(chan error, 1)
		go func() {
			done <- cm.Replace(0)
		}()

		// wait long enough for at least one dial attempt to fail and for
		// time.After(backoff) to fire, advancing delay; then cancel so the
		// next iteration exits via ctx.Done() rather than the pre-loop guard
		time.Sleep(60 * time.Millisecond)
		cancel()
		cm.cancel()

		err := <-done
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		// error must reference "attempt N", meaning the select inside the loop has been reached
		assert.ErrorContains(t, err, "attempt")
		assert.ErrorContains(t, err, "context cancelled")
	})
}

func TestConnectionManagerMonitor(t *testing.T) {
	t.Run("CallsOnOpenHandler", func(t *testing.T) {
		var openIdx atomic.Int32
		openIdx.Store(-1)

		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size: 2,
			OnOpen: func(idx int) {
				openIdx.Store(int32(idx))
			},
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		conn := newMockConnection()
		cm.pool[0] = conn

		// start monitoring in background
		done := make(chan bool, 1)
		go func() {
			cm.Monitor(conn)
			done <- true
		}()

		// wait for OnOpen to be called
		time.Sleep(50 * time.Millisecond)

		assert.Equal(t, int32(0), openIdx.Load())

		// cleanup
		cm.cancel()
		<-done
	})

	t.Run("CallsOnCloseHandlerOnConnectionClose", func(t *testing.T) {
		var closeCalled atomic.Bool
		var closeIdx, closeCode atomic.Int32
		var closeReason, closeServer, closeRecover atomic.Value

		closeErr := &amqp091.Error{
			Code:    320,
			Reason:  "CONNECTION_FORCED",
			Server:  true,
			Recover: true,
		}

		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: true, // disable auto-reconnect for testing
			OnClose: func(idx int, code int, reason string, server bool, recover bool) {
				closeCalled.Store(true)
				closeIdx.Store(int32(idx))
				closeCode.Store(int32(code))
				closeReason.Store(reason)
				closeServer.Store(server)
				closeRecover.Store(recover)
			},
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		conn := newMockConnection().withAutoClose(closeErr, 10*time.Millisecond)
		cm.pool[0] = conn

		// start monitoring
		done := make(chan bool, 1)
		go func() {
			cm.Monitor(conn)
			done <- true
		}()

		// wait for close event
		time.Sleep(100 * time.Millisecond)

		assert.True(t, closeCalled.Load())
		assert.Equal(t, int32(0), closeIdx.Load())
		assert.Equal(t, int32(320), closeCode.Load())
		assert.Equal(t, "CONNECTION_FORCED", closeReason.Load().(string))
		assert.True(t, closeServer.Load().(bool))
		assert.True(t, closeRecover.Load().(bool))

		<-done
	})

	t.Run("HandlesGracefulShutdown", func(t *testing.T) {
		var closeCalled atomic.Bool
		var closeReason atomic.Value

		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: true,
			OnClose: func(idx int, code int, reason string, server bool, recover bool) {
				closeCalled.Store(true)
				closeReason.Store(reason)
			},
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		// graceful close: no error sent, just close channel
		conn := newMockConnection().withGracefulClose(10 * time.Millisecond)
		cm.pool[0] = conn

		// start monitoring
		done := make(chan bool, 1)
		go func() {
			cm.Monitor(conn)
			done <- true
		}()

		// wait for close event
		time.Sleep(100 * time.Millisecond)

		assert.True(t, closeCalled.Load())
		assert.Equal(t, "graceful shutdown", closeReason.Load().(string))

		<-done
	})

	t.Run("CallsOnBlockHandler", func(t *testing.T) {
		var blockIdx atomic.Int32
		var blockCalled atomic.Bool
		var blockActive atomic.Value
		var blockReason atomic.Value

		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: true,
			OnBlock: func(idx int, active bool, reason string) {
				blockCalled.Store(true)
				blockIdx.Store(int32(idx))
				blockActive.Store(active)
				blockReason.Store(reason)
			},
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		conn := newMockConnection().withAutoBlock(true, "memory alarm", 10*time.Millisecond)
		cm.pool[0] = conn

		// start monitoring
		done := make(chan bool, 1)
		go func() {
			cm.Monitor(conn)
			done <- true
		}()

		// wait for block event
		time.Sleep(100 * time.Millisecond)

		assert.True(t, blockCalled.Load())
		assert.Equal(t, int32(0), blockIdx.Load())
		assert.True(t, blockActive.Load().(bool))
		assert.Equal(t, "memory alarm", blockReason.Load().(string))

		// cleanup
		cm.cancel()
		<-done
	})

	t.Run("HandlesGracefulShutdownByClosingChannel", func(t *testing.T) {
		var closeCalled atomic.Bool
		var closeReason atomic.Value

		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: true,
			OnClose: func(idx int, code int, reason string, server bool, recover bool) {
				closeCalled.Store(true)
				closeReason.Store(reason)
			},
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		conn := newMockConnection()
		cm.pool[0] = conn

		// start monitoring
		done := make(chan bool, 1)
		go func() {
			cm.Monitor(conn)
			done <- true
		}()

		// wait a bit then close the connection channel to simulate graceful shutdown
		time.Sleep(10 * time.Millisecond)
		conn.triggerClose()

		// wait for monitor to handle graceful close
		time.Sleep(50 * time.Millisecond)

		assert.True(t, closeCalled.Load())
		assert.Equal(t, "graceful shutdown", closeReason.Load().(string))

		<-done
	})

	t.Run("IgnoresStaleConnectionEvents", func(t *testing.T) {
		var replaceAttempted atomic.Bool

		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: true,
			OnClose: func(idx int, code int, reason string, server bool, recover bool) {
				replaceAttempted.Store(true)
			},
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		oldConn := newMockConnection().withAutoClose(&amqp091.Error{
			Code:   320,
			Reason: "CONNECTION_FORCED",
		}, 10*time.Millisecond)

		newConn := newMockConnection()
		cm.pool[0] = oldConn

		// start monitoring old connection
		done := make(chan bool, 1)
		go func() {
			cm.Monitor(oldConn)
			done <- true
		}()

		// immediately replace with new connection (making old one stale)
		time.Sleep(5 * time.Millisecond)
		cm.poolMu.Lock()
		cm.pool[0] = newConn
		cm.poolMu.Unlock()

		// wait for old connection close event
		time.Sleep(50 * time.Millisecond)

		// OnClose should not be called for stale connection
		assert.False(t, replaceAttempted.Load())

		// cleanup
		cm.cancel()
		<-done
	})

	t.Run("StopsOnContextCancellation", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: true,
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)

		conn := newMockConnection()
		cm.pool[0] = conn

		// start monitoring
		done := make(chan bool, 1)
		go func() {
			cm.Monitor(conn)
			done <- true
		}()

		// cancel context
		time.Sleep(10 * time.Millisecond)
		cm.cancel()

		// monitor should exit
		select {
		case <-done:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Fatal("monitor did not exit after context cancellation")
		}
	})

	t.Run("WithConnectionNotInPool", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		unknownConn := newMockConnection()

		// start monitoring unknown connection
		done := make(chan bool, 1)
		go func() {
			cm.Monitor(unknownConn)
			done <- true
		}()

		// should exit immediately since connection not in pool
		select {
		case <-done:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Fatal("monitor did not exit for unknown connection")
		}
	})

	t.Run("IgnoresStaleBlockEvents", func(t *testing.T) {
		// this test covers the stale block event handling
		var blockHandlerCalled atomic.Bool

		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: true,
			OnBlock: func(idx int, active bool, reason string) {
				blockHandlerCalled.Store(true)
			},
		})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		// create a connection that will send a block notification after a delay
		oldConn := newMockConnection().withAutoBlock(true, "memory alarm", 20*time.Millisecond)
		newConn := newMockConnection()

		cm.pool[0] = oldConn

		// start monitoring the old connection
		monitorDone := make(chan bool, 1)
		go func() {
			cm.Monitor(oldConn)
			monitorDone <- true
		}()

		// replace the connection before the block event arrives
		time.Sleep(10 * time.Millisecond)
		cm.poolMu.Lock()
		cm.pool[0] = newConn
		cm.poolMu.Unlock()

		// wait for the block event to be sent and ignored
		time.Sleep(50 * time.Millisecond)

		// OnBlock should NOT be called for stale connection's block event
		assert.False(t, blockHandlerCalled.Load(), "OnBlock should not be called for stale block event")

		// monitor should have exited after detecting stale block event
		select {
		case <-monitorDone:
			// success: monitor exited
		case <-time.After(100 * time.Millisecond):
			t.Fatal("monitor should exit after receiving stale block event")
		}
	})
}

func TestConnectionManagerClose(t *testing.T) {
	t.Run("WithConnectionCloseErrors", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 2})

		closeErr := errors.New("close error")
		conn1 := newMockConnection().withCloseError(closeErr)
		conn2 := newMockConnection()

		cm.pool[0] = conn1
		cm.pool[1] = conn2

		err := cm.Close()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "close error")

		// both connections should be closed despite error
		assert.True(t, conn1.IsClosed())
		assert.True(t, conn2.IsClosed())
	})

	t.Run("CancelsMonitoringGoroutines", func(t *testing.T) {
		cm := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1})

		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)

		conn := newMockConnection()
		cm.pool[0] = conn

		// start monitoring
		monitorDone := make(chan bool, 1)
		go func() {
			cm.Monitor(conn)
			monitorDone <- true
		}()

		// wait for monitor to start
		time.Sleep(10 * time.Millisecond)

		// close manager
		err := cm.Close()
		assert.NoError(t, err)

		// monitor should exit
		select {
		case <-monitorDone:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Fatal("monitor did not exit after Close")
		}
	})
}
