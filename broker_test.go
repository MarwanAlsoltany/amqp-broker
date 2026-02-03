package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBrokerOptions(t *testing.T) {
	t.Run("WithURL", func(t *testing.T) {
		b := &Broker{}

		customURL := testURL
		WithURL(customURL)(b)
		assert.Equal(t, customURL, b.url)
	})

	t.Run("WithIdentifier", func(t *testing.T) {
		b := &Broker{}

		customID := "test-broker-123"
		WithIdentifier(customID)(b)
		assert.Equal(t, customID, b.id)
	})

	t.Run("WithContext", func(t *testing.T) {
		b := &Broker{}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		WithContext(ctx)(b)

		assert.NotNil(t, b.ctx)
		assert.NotNil(t, b.cancel)

		// verify context is derived from parent
		cancel()
		select {
		case <-b.ctx.Done():
			// expected - child context cancelled when parent cancelled
		case <-time.After(100 * time.Millisecond):
			t.Fatal("child context not cancelled when parent cancelled")
		}
	})

	t.Run("WithCache", func(t *testing.T) {
		b := &Broker{}
		// when positive
		WithCache(10 * time.Minute)(b)
		assert.Equal(t, 10*time.Minute, b.cacheTTL)
		// when zero
		WithCache(0)(b)
		assert.Equal(t, time.Duration(0), b.cacheTTL)
		// when negative
		WithCache(-1 * time.Minute)(b)
		assert.Equal(t, time.Duration(0), b.cacheTTL)
	})

	t.Run("WithConnectionManagerOptions", func(t *testing.T) {
		b := &Broker{}

		t.Run("PoolSize", func(t *testing.T) {
			opts := ConnectionManagerOptions{Size: 3}
			WithConnectionManagerOptions(opts)(b)
			assert.Equal(t, 3, b.connectionMgrOpts.Size)
		})

		t.Run("ConnectionConfig", func(t *testing.T) {
			config := Config{
				Heartbeat: 30 * time.Second,
				Locale:    "en_US",
				Vhost:     "/custom",
			}
			opts := ConnectionManagerOptions{Config: &config}
			WithConnectionManagerOptions(opts)(b)
			require.NotNil(t, b.connectionMgrOpts.Config)
			assert.Equal(t, 30*time.Second, b.connectionMgrOpts.Config.Heartbeat)
			assert.Equal(t, "en_US", b.connectionMgrOpts.Config.Locale)
			assert.Equal(t, "/custom", b.connectionMgrOpts.Config.Vhost)
		})

		t.Run("OnOpen", func(t *testing.T) {
			var capturedIdx int
			handler := func(idx int) {
				capturedIdx = idx
			}
			opts := ConnectionManagerOptions{OnOpen: handler}
			WithConnectionManagerOptions(opts)(b)

			b.connectionMgrOpts.OnOpen(3)
			assert.Equal(t, 3, capturedIdx)
		})

		t.Run("OnClose", func(t *testing.T) {
			var capturedIdx, capturedCode int
			var capturedReason string
			var capturedServer, capturedRecover bool

			handler := func(idx, code int, reason string, server, recover bool) {
				capturedIdx = idx
				capturedCode = code
				capturedReason = reason
				capturedServer = server
				capturedRecover = recover
			}
			opts := ConnectionManagerOptions{OnClose: handler}
			WithConnectionManagerOptions(opts)(b)

			b.connectionMgrOpts.OnClose(2, 404, "NOT_FOUND", true, false)
			assert.Equal(t, 2, capturedIdx)
			assert.Equal(t, 404, capturedCode)
			assert.Equal(t, "NOT_FOUND", capturedReason)
			assert.True(t, capturedServer)
			assert.False(t, capturedRecover)
		})

		t.Run("OnBlocked", func(t *testing.T) {
			var capturedIdx int
			var capturedActive bool
			var capturedReason string

			handler := func(idx int, active bool, reason string) {
				capturedIdx = idx
				capturedActive = active
				capturedReason = reason
			}

			opts := ConnectionManagerOptions{OnBlock: handler}
			WithConnectionManagerOptions(opts)(b)

			b.connectionMgrOpts.OnBlock(1, true, "alarm")
			assert.Equal(t, 1, capturedIdx)
			assert.True(t, capturedActive)
			assert.Equal(t, "alarm", capturedReason)
		})

		t.Run("ReconnectConfig", func(t *testing.T) {
			opts := ConnectionManagerOptions{
				NoAutoReconnect: true,
				ReconnectMin:    1 * time.Second,
				ReconnectMax:    10 * time.Second,
			}
			WithConnectionManagerOptions(opts)(b)
			assert.True(t, b.connectionMgrOpts.NoAutoReconnect)
			assert.Equal(t, 1*time.Second, b.connectionMgrOpts.ReconnectMin)
			assert.Equal(t, 10*time.Second, b.connectionMgrOpts.ReconnectMax)
		})
	})

	t.Run("WithEndpointOptions", func(t *testing.T) {
		b := &Broker{}

		opts := EndpointOptions{
			NoWaitReady:     true,
			ReadyTimeout:    123 * time.Second,
			NoAutoReconnect: true,
			ReconnectMin:    100 * time.Millisecond,
			ReconnectMax:    10 * time.Second,
		}
		WithEndpointOptions(opts)(b)
		assert.True(t, b.endpointOpts.NoWaitReady)
		assert.Equal(t, 123*time.Second, b.endpointOpts.ReadyTimeout)
		assert.True(t, b.endpointOpts.NoAutoReconnect)
		assert.Equal(t, 100*time.Millisecond, b.endpointOpts.ReconnectMin)
		assert.Equal(t, 10*time.Second, b.endpointOpts.ReconnectMax)
	})

	t.Run("WithConnectionPoolSize", func(t *testing.T) {
		b := &Broker{}
		WithConnectionPoolSize(3)(b)
		assert.Equal(t, 3, b.connectionMgrOpts.Size)
	})

	t.Run("WithConnectionConfig", func(t *testing.T) {
		b := &Broker{}
		config := Config{
			Heartbeat: 30 * time.Second,
			Locale:    "en_US",
			Vhost:     "/custom",
		}
		WithConnectionConfig(config)(b)
		require.NotNil(t, b.connectionMgrOpts.Config)
		assert.Equal(t, 30*time.Second, b.connectionMgrOpts.Config.Heartbeat)
		assert.Equal(t, "en_US", b.connectionMgrOpts.Config.Locale)
		assert.Equal(t, "/custom", b.connectionMgrOpts.Config.Vhost)
	})

	t.Run("WithConnectionOnOpen", func(t *testing.T) {
		b := &Broker{}
		var capturedIdx int
		handler := func(idx int) {
			capturedIdx = idx
		}
		WithConnectionOnOpen(handler)(b)

		b.connectionMgrOpts.OnOpen(3)
		assert.Equal(t, 3, capturedIdx)
	})

	t.Run("WithConnectionOnClose", func(t *testing.T) {
		b := &Broker{}
		var capturedIdx, capturedCode int
		var capturedReason string
		var capturedServer, capturedRecover bool

		handler := func(idx, code int, reason string, server, recover bool) {
			capturedIdx = idx
			capturedCode = code
			capturedReason = reason
			capturedServer = server
			capturedRecover = recover
		}
		WithConnectionOnClose(handler)(b)

		b.connectionMgrOpts.OnClose(2, 404, "NOT_FOUND", true, false)
		assert.Equal(t, 2, capturedIdx)
		assert.Equal(t, 404, capturedCode)
		assert.Equal(t, "NOT_FOUND", capturedReason)
		assert.True(t, capturedServer)
		assert.False(t, capturedRecover)
	})

	t.Run("WithConnectionOnBlocked", func(t *testing.T) {
		b := &Broker{}
		var capturedIdx int
		var capturedActive bool
		var capturedReason string

		handler := func(idx int, active bool, reason string) {
			capturedIdx = idx
			capturedActive = active
			capturedReason = reason
		}
		WithConnectionOnBlocked(handler)(b)

		b.connectionMgrOpts.OnBlock(1, true, "alarm")
		assert.Equal(t, 1, capturedIdx)
		assert.True(t, capturedActive)
		assert.Equal(t, "alarm", capturedReason)
	})

	t.Run("WithConnectionReconnectConfig", func(t *testing.T) {
		b := &Broker{}
		WithConnectionReconnectConfig(true, 2*time.Second, 20*time.Second)(b)
		assert.True(t, b.connectionMgrOpts.NoAutoReconnect)
		assert.Equal(t, 2*time.Second, b.connectionMgrOpts.ReconnectMin)
		assert.Equal(t, 20*time.Second, b.connectionMgrOpts.ReconnectMax)
	})

	t.Run("Behavior", func(t *testing.T) {
		t.Run("MultipleOptions", func(t *testing.T) {
			b := &Broker{}

			customURL := testURL
			customID := "multi-test"
			poolSize := 4
			min := 1 * time.Second
			max := 60 * time.Second
			cacheTTL := 15 * time.Minute

			opts := []BrokerOption{
				WithURL(customURL),
				WithIdentifier(customID),
				WithConnectionManagerOptions(ConnectionManagerOptions{Size: poolSize}),
				WithEndpointOptions(EndpointOptions{ReconnectMin: min, ReconnectMax: max}),
				WithCache(cacheTTL),
			}

			for _, opt := range opts {
				opt(b)
			}

			assert.Equal(t, customURL, b.url)
			assert.Equal(t, customID, b.id)
			assert.Equal(t, poolSize, b.connectionMgrOpts.Size)
			assert.Equal(t, min, b.endpointOpts.ReconnectMin)
			assert.Equal(t, max, b.endpointOpts.ReconnectMax)
			assert.Equal(t, cacheTTL, b.cacheTTL)
		})

		t.Run("OptionsOrderIndependent", func(t *testing.T) {
			customID := "order-test"
			cacheTTL := 20 * time.Minute

			b1 := &Broker{}
			WithIdentifier(customID)(b1)
			WithCache(cacheTTL)(b1)

			b2 := &Broker{}
			WithCache(cacheTTL)(b2)
			WithIdentifier(customID)(b2)

			assert.Equal(t, b1.id, b2.id)
			assert.Equal(t, b1.cacheTTL, b2.cacheTTL)
		})

		t.Run("LaterOptionsOverride", func(t *testing.T) {
			b := &Broker{}

			WithIdentifier("old")(b)
			WithIdentifier("new")(b)
			assert.Equal(t, "new", b.id)

			WithConnectionManagerOptions(ConnectionManagerOptions{Size: 1})(b)
			WithConnectionManagerOptions(ConnectionManagerOptions{Size: 5})(b)
			assert.Equal(t, 5, b.connectionMgrOpts.Size)
		})
	})
}

func TestBrokerNew(t *testing.T) {
	// Note: Endpoint option validation has been moved to publisher/consumer init()
	// so New() no longer returns errors for invalid endpoint options
}

func TestBrokerClose(t *testing.T) {
	t.Run("Idempotency", func(t *testing.T) {
		b := &Broker{
			publishers: make(map[string]Publisher),
			consumers:  make(map[string]Consumer),
		}
		ctx, cancel := context.WithCancel(t.Context())
		b.ctx = ctx
		b.cancel = cancel

		// first close
		err1 := b.Close()
		assert.NoError(t, err1)
		assert.True(t, b.closed.Load())

		// second close should be no-op
		err2 := b.Close()
		assert.NoError(t, err2)
		assert.True(t, b.closed.Load())
	})

	t.Run("CancelsContext", func(t *testing.T) {
		b := &Broker{
			publishers: make(map[string]Publisher),
			consumers:  make(map[string]Consumer),
		}
		b.ctx, b.cancel = context.WithCancel(t.Context())

		b.Close()

		select {
		case <-b.ctx.Done():
			// expected
		case <-time.After(100 * time.Millisecond):
			t.Fatal("context not cancelled after close")
		}
	})

	t.Run("WithConnectionManager", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			consumers:   make(map[string]Consumer),
			topologyMgr: newTopologyManager(),
		}

		// create a real connection manager (won't actually connect)
		b.connectionMgr = newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1})
		// initialize context in connection manager
		connCtx, connCancel := context.WithCancel(ctx)
		b.connectionMgr.ctx = connCtx
		b.connectionMgr.cancel = connCancel

		_ = b.Close()
		// should succeed (connMgr.Close() is idempotent)
		assert.True(t, b.closed.Load())
	})

	t.Run("WithMockPublishers", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			consumers:   make(map[string]Consumer),
			topologyMgr: newTopologyManager(),
		}

		// add mock publisher that returns error on close
		mockPublisher := &mockPublisher{mockEndpoint{closeErr: errors.New("publisher close error")}}
		b.publishers["publisher-1"] = mockPublisher

		err := b.Close()
		// should return error from publisher close
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrBroker)
		assert.ErrorContains(t, err, "close failed")
		assert.True(t, b.closed.Load())
	})

	t.Run("WithMockConsumers", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			consumers:   make(map[string]Consumer),
			topologyMgr: newTopologyManager(),
		}

		// add mock consumer that returns error on close
		mockConsumer := &mockConsumer{mockEndpoint{closeErr: errors.New("consumer close error")}}
		b.consumers["consumer-1"] = mockConsumer

		err := b.Close()
		// should return error from consumer close
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrBroker)
		assert.ErrorContains(t, err, "close failed")
		assert.True(t, b.closed.Load())
	})

	t.Run("WithMultipleEndpointsInRegistries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			consumers:   make(map[string]Consumer),
			topologyMgr: newTopologyManager(),
		}

		// add multiple mock endpoints
		b.publishers["publisher-1"] = &mockPublisher{}
		b.publishers["publisher-2"] = &mockPublisher{}
		b.consumers["consumer-1"] = &mockConsumer{}
		b.consumers["consumer-2"] = &mockConsumer{}

		err := b.Close()
		assert.NoError(t, err)
		assert.True(t, b.closed.Load())
	})

	t.Run("WithPublishersPool", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			consumers:   make(map[string]Consumer),
			topologyMgr: newTopologyManager(),
		}

		// create pool (normally done in New)
		b.publishersPool = &pool[Publisher]{
			closed: atomic.Bool{},
		}

		err := b.Close()
		// pool close should be fine
		assert.NoError(t, err)
		assert.True(t, b.closed.Load())
	})
}

func TestBrokerConnection(t *testing.T) {
	t.Run("WithNoConnection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		ch, err := b.Channel()
		assert.Nil(t, ch)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}
		b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		conn, err := b.Connection()
		assert.Nil(t, conn)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "create control connection")
		assert.ErrorIs(t, err, ErrConnectionManagerClosed)
	})
}

func TestBrokerChannel(t *testing.T) {
	t.Run("WithConnectionError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		ch, err := b.Channel()
		assert.Nil(t, ch)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WhenConnectionClosed", func(t *testing.T) {
		// this scenario requires mocking a channel that reports as closed
		if testing.Short() {
			t.Skip("Requires integration test setup")
		}
	})

	t.Run("WhenClosed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		b.Close()

		ch, err := b.Channel()
		assert.Nil(t, ch)
		assert.ErrorContains(t, err, "create control connection")
		assert.ErrorIs(t, err, ErrConnectionManagerClosed)
	})
}

func TestBrokerDeclare(t *testing.T) {
	t.Run("WithChannelError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			topologyMgr:   newTopologyManager(),
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		topology := &Topology{
			Exchanges: []Exchange{NewExchange("test-exchange")},
		}

		err := b.Declare(topology)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})
}

func TestBrokerDelete(t *testing.T) {
	t.Run("WithChannelError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			topologyMgr:   newTopologyManager(),
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		topology := &Topology{
			Exchanges: []Exchange{NewExchange("test-exchange")},
		}

		err := b.Delete(topology)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})
}

func TestBrokerVerify(t *testing.T) {
	t.Run("WithChannelError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			topologyMgr:   newTopologyManager(),
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		topology := &Topology{
			Exchanges: []Exchange{NewExchange("test-exchange")},
		}

		err := b.Verify(topology)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})
}

func TestBrokerSync(t *testing.T) {
	t.Run("WithChannelError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			topologyMgr:   newTopologyManager(),
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		topology := &Topology{
			Exchanges: []Exchange{NewExchange("test-exchange")},
		}

		err := b.Sync(topology)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})
}

func TestBrokerNewPublisher(t *testing.T) {
	t.Run("WithConnectionError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			topologyMgr: newTopologyManager(),
			// create a connection manager that will fail to connect
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		// set a very short ready timeout
		opts := &PublisherOptions{
			EndpointOptions: EndpointOptions{
				NoWaitReady:  false,
				ReadyTimeout: 1 * time.Millisecond,
			},
		}

		p, err := b.NewPublisher(opts, NewExchange("test-exchange"))
		assert.Nil(t, p)
		assert.ErrorIs(t, err, ErrConnectionManager)

		// verify publisher was cleaned up from registry
		b.publishersMu.Lock()
		assert.Empty(t, b.publishers)
		b.publishersMu.Unlock()
	})

	t.Run("WhenClosed", func(t *testing.T) {
		b := &Broker{
			publishers: make(map[string]Publisher),
		}
		b.Close()

		p, err := b.NewPublisher(nil, NewExchange("test-exchange"))
		assert.Nil(t, p)
		assert.ErrorIs(t, err, ErrBrokerClosed)
	})
}

func TestBrokerNewConsumer(t *testing.T) {
	t.Run("WithConnectionError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			consumers:   make(map[string]Consumer),
			topologyMgr: newTopologyManager(),
			// create a connection manager that will fail to connect
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		// set a very short ready timeout
		opts := &ConsumerOptions{
			EndpointOptions: EndpointOptions{
				NoWaitReady:  false,
				ReadyTimeout: 1 * time.Millisecond,
			},
		}

		c, err := b.NewConsumer(opts, NewQueue("test-queue"), testHandler(HandlerActionNoAction))
		assert.Nil(t, c)
		assert.ErrorIs(t, err, ErrConnectionManager)

		// verify consumer was cleaned up from registry
		b.consumersMu.Lock()
		assert.Empty(t, b.consumers)
		b.consumersMu.Unlock()
	})

	t.Run("WhenClosed", func(t *testing.T) {
		b := &Broker{
			consumers: make(map[string]Consumer),
		}
		b.Close()

		c, err := b.NewConsumer(nil, NewQueue("test-queue"), testHandler(HandlerActionNoAction))
		assert.Nil(t, c)
		assert.ErrorIs(t, err, ErrBrokerClosed)
	})
}

func TestBrokerPublish(t *testing.T) {
	t.Run("WithConnectionError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			topologyMgr: newTopologyManager(),
			// create a connection manager that will fail to connect
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		err := b.Publish(t.Context(), "test", "key", Message{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WithPoolAcquireError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:        ctx,
			cancel:     cancel,
			publishers: make(map[string]Publisher),
			// create a pool that will fail
			publishersPool: newPool[Publisher](1 * time.Minute),
			topologyMgr:    newTopologyManager(),
			// create a connection manager that will fail to connect
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}

		_ = b.publishersPool.init(ctx)
		_ = b.publishersPool.Close() // close pool to force acquire error

		err := b.Publish(t.Context(), "test", "key", Message{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPoolClosed)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		b := &Broker{
			topologyMgr: newTopologyManager(),
		}
		b.Close()

		err := b.Publish(t.Context(), "test", "key", Message{})
		assert.ErrorIs(t, err, ErrBrokerClosed)
	})
}

func TestBrokerConsume(t *testing.T) {
	t.Run("WithConnectionError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			consumers:   make(map[string]Consumer),
			topologyMgr: newTopologyManager(),
			// create a connection manager that will fail to connect
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		err := b.Consume(t.Context(), "test", testHandler(HandlerActionNoAction))
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		b := &Broker{
			topologyMgr: newTopologyManager(),
		}
		b.Close()

		err := b.Consume(t.Context(), "test", testHandler(HandlerActionNoAction))
		assert.ErrorIs(t, err, ErrBrokerClosed)
	})
}

func TestBrokerTransaction(t *testing.T) {
	t.Run("WithConnectionError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			connectionMgr: newConnectionManager(testURL, &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.init(ctx)

		err := b.Transaction(t.Context(), func(ch *Channel) error {
			return nil
		})
		assert.Error(t, err)
	})
}
