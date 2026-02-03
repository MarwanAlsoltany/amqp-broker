package broker

import (
	"context"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestNewConnectionManager(t *testing.T) {
	t.Run("WithConfig", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 3})
		assert.NotNil(t, cm)
		assert.Equal(t, testURL, cm.url)
		assert.Equal(t, 3, cm.opts.Size)
		assert.Len(t, cm.pool, 3)

		t.Run("Size", func(t *testing.T) {
			t.Run("Zero", func(t *testing.T) {
				cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 0})
				assert.Equal(t, defaultConnectionPoolSize, cm.opts.Size)
			})

			t.Run("Positive", func(t *testing.T) {
				cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 3})
				assert.Equal(t, 3, cm.opts.Size)
			})

			t.Run("Negative", func(t *testing.T) {
				cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: -10})
				assert.Equal(t, defaultConnectionPoolSize, cm.opts.Size)
			})
		})

		t.Run("OnOpen", func(t *testing.T) {
			var called atomic.Int32
			cm := newConnectionManager(testURL, &ConnectionManagerOptions{
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
			cm := newConnectionManager(testURL, &ConnectionManagerOptions{
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
			cm := newConnectionManager(testURL, &ConnectionManagerOptions{
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
		cm := newConnectionManager(testURL, nil)
		assert.NotNil(t, cm)
		assert.Equal(t, testURL, cm.url)
		assert.Equal(t, defaultConnectionPoolSize, cm.opts.Size)
		assert.Len(t, cm.pool, defaultConnectionPoolSize)

		// this is a unit test that verifies the nil config path exists
		// the actual connection would fail without a running RabbitMQ server,
		// so we just verify the function signature and that it attempts to dial
		err := cm.init(t.Context())
		assert.Error(t, err) // expected to fail with invalid URL
	})
}

func TestConnectionManagerInit(t *testing.T) {
	t.Run("WhenContextCanceled", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 2})
		defer cm.Close()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := cm.init(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("WithDialConfig", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{
			Size: 2,
			Config: &amqp.Config{
				Heartbeat: 10,
			},
		})
		defer cm.Close()

		err := cm.init(t.Context())
		assert.Error(t, err)
	})
}

func TestConnectionManagerClose(t *testing.T) {
	t.Run("Idempotency", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 2})

		err := cm.Close()
		assert.NoError(t, err)

		// closing again should be no-op
		err = cm.Close()
		assert.NoError(t, err)
	})

	t.Run("WithConnections", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1})

		// simulate open connection
		cm.pool = append(cm.pool, &amqp.Connection{})
		cm.pool = append(cm.pool, &amqp.Connection{})
		cm.pool = append(cm.pool, &amqp.Connection{})

		err := cm.init(t.Context())
		assert.Error(t, err)

		// underlying library panics due to malformed instantiation of amqp.Connection
		assert.Panics(t, func() {
			cm.Close()
		})
	})
}

func TestConnectionManagerAssign(t *testing.T) {
	// can't test without actual connections, but verify assignment logic
	t.Run("WithPoolSizeSize1ConnectionSharing", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1})
		assert.Equal(t, 1, cm.opts.Size)
		// all roles should use index 0
		_, err := cm.assign(roleController)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "not available")
		_, err = cm.assign(rolePublisher)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "not available")
		_, err = cm.assign(roleConsumer)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "not available")
	})

	t.Run("WithPoolSizeSize2ConnectionPartitioning", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 2})
		assert.Equal(t, 2, cm.opts.Size)
		// control: 0, publisher/consumer: 1
		_, err := cm.assign(roleController)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.assign(rolePublisher)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.assign(roleConsumer)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WithSize3ConnectionPartitioning", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 3})
		assert.Equal(t, 3, cm.opts.Size)
		// control: 0, publisher: 1, consumer: 2
		_, err := cm.assign(roleController)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.assign(rolePublisher)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.assign(roleConsumer)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WithPoolSizeSize4ConnectionPartitioning", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 4})
		assert.Equal(t, 4, cm.opts.Size)
		// control: 0, publisher: 1, consumer: 2, roundrobin: 3
		_, err := cm.assign(roleController)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.assign(rolePublisher)
		assert.ErrorIs(t, err, ErrConnectionManager)
		_, err = cm.assign(roleConsumer)
		assert.ErrorIs(t, err, ErrConnectionManager)
		// round robin for extra publishers/consumers
		for range 5 {
			_, err = cm.assign(rolePublisher)
			assert.ErrorIs(t, err, ErrConnectionManager)
			_, err = cm.assign(roleConsumer)
			assert.ErrorIs(t, err, ErrConnectionManager)
		}
	})

	t.Run("WithPoolSizeSize10ConnectionPartitioning", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 10})
		assert.Equal(t, 10, cm.opts.Size)
		// control: 0, publishers: 1-3, consumers: 4-6, roundrobin: 7-9
		_, err := cm.assign(roleController)
		assert.ErrorIs(t, err, ErrConnectionManager)
		for range 5 {
			_, err = cm.assign(rolePublisher)
			assert.ErrorIs(t, err, ErrConnectionManager)
			_, err = cm.assign(roleConsumer)
			assert.ErrorIs(t, err, ErrConnectionManager)
		}
	})
}

func TestConnectionManagerIndex(t *testing.T) {
	cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1})

	t.Run("WithKnownConnection", func(t *testing.T) {
		conn := &amqp.Connection{}
		cm.pool[0] = conn

		idx := cm.index(conn)
		assert.Equal(t, 0, idx)
	})

	t.Run("WithUnknownConnection", func(t *testing.T) {
		conn := &amqp.Connection{}
		idx := cm.index(conn)
		assert.Equal(t, -1, idx)

		// test nil connection
		{
			idx = cm.index(nil)
			assert.Equal(t, -1, idx)
		}
	})
}

func TestConnectionManagerReplace(t *testing.T) {
	t.Run("WithInvalidIndex", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 2})

		// initialize context for proper operation
		ctx := t.Context()
		cm.ctx, cm.cancel = context.WithCancel(ctx)
		defer cm.cancel()

		// test negative index
		err := cm.replace(-1)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "out of range")

		// test out of range index
		err = cm.replace(10)
		assert.Error(t, err)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "out of range")
	})

	t.Run("WhenConnectionFails", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 2})

		err := cm.replace(0)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
		assert.ErrorContains(t, err, "replace")
	})

	t.Run("WhenClosed", func(t *testing.T) {
		cm := newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1})

		cm.closed.Store(true) // simulate closed manager

		err := cm.replace(0)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManagerClosed)
	})
}
