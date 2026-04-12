package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

func TestNew(t *testing.T) {
	conn := &mockConnection{}

	b := newTestBrokerWithConnection(conn)
	defer b.Close()

	assert.NotNil(t, b)
	assert.NotNil(t, b.connectionMgr)
	assert.NotNil(t, b.topologyReg)
	assert.Nil(t, b.publishersPool) // cache disabled

	t.Run("WithInvalidConnectionManagerOptions", func(t *testing.T) {
		_, err := New(WithConnectionManagerOptions(ConnectionManagerOptions{
			// min == max violates max > min
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 1 * time.Second,
		}))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBrokerConfigInvalid)
		assert.ErrorContains(t, err, "invalid")
		assert.ErrorContains(t, err, "connection manager options")
	})

	t.Run("WithInvalidEndpointOptions", func(t *testing.T) {
		_, err := New(WithEndpointOptions(EndpointOptions{
			// min < 0 is invalid
			ReconnectMin: -1 * time.Second,
			ReconnectMax: 30 * time.Second,
		}))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBrokerConfigInvalid)
		assert.ErrorContains(t, err, "invalid")
		assert.ErrorContains(t, err, "endpoint options")
	})
}

func TestBrokerConnection(t *testing.T) {
	b := newTestBrokerWithConnection(&mockConnection{})
	defer b.Close()

	conn, err := b.Connection()
	require.NoError(t, err)
	assert.NotNil(t, conn)

	t.Run("WhenConnectionClosed", func(t *testing.T) {
		conn := &mockConnection{}
		conn.closed.Store(true) // pre-mark as closed

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		_, err := b.Connection()
		assert.ErrorIs(t, err, ErrConnectionClosed)
	})
}

func TestBrokerChannel(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		conn := &mockConnection{}

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		ch, err := b.Channel()
		require.NoError(t, err)
		assert.NotNil(t, ch)
		ch.Close()
	})

	t.Run("WhenChannelOpenFails", func(t *testing.T) {
		sentinel := errors.New("channel open failed")

		conn := &mockConnection{
			channelErr: sentinel,
		}

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		_, err := b.Channel()
		require.Error(t, err)
		assert.ErrorContains(t, err, "create control channel")
		assert.ErrorContains(t, err, "channel open failed")
	})

	t.Run("WhenChannelClosed", func(t *testing.T) {
		conn := &mockConnection{
			channelFn: func() (Channel, error) {
				ch := &mockChannel{}
				ch.closed.Store(true)
				return ch, nil
			},
		}

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		_, err := b.Channel()
		assert.ErrorIs(t, err, ErrChannelClosed)
	})
}

func TestBrokerTransaction(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		conn := &mockConnection{}

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		var called bool
		err := b.Transaction(t.Context(), func(Channel) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("WhenChannelFails", func(t *testing.T) {
		// no connection -> Channel() returns ErrConnectionManager
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:           ctx,
			cancel:        cancel,
			topologyReg:   topology.NewRegistry(),
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}
		_ = b.connectionMgr.Init(ctx)
		defer b.Close()

		err := b.Transaction(t.Context(), func(Channel) error { return nil })
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WhenTxFails", func(t *testing.T) {
		sentinel := errors.New("tx start failed")

		conn := &mockConnection{
			channelFn: func() (Channel, error) {
				ch := &mockChannel{
					txErr: sentinel,
				}
				return ch, nil
			},
		}

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		err := b.Transaction(t.Context(), func(Channel) error { return nil })
		require.Error(t, err)
		assert.ErrorContains(t, err, "transaction start")
		assert.ErrorContains(t, err, "tx start failed")
	})

	t.Run("WhenFnFails", func(t *testing.T) {
		sentinel := errors.New("fn error")
		conn := &mockConnection{}

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		err := b.Transaction(t.Context(), func(Channel) error { return sentinel })
		require.Error(t, err)
		assert.ErrorContains(t, err, "transaction function")
		assert.ErrorContains(t, err, "fn error")
	})

	t.Run("WhenFnAndRollbackFail", func(t *testing.T) {
		fnErr := errors.New("fn error")
		rbErr := errors.New("rollback error")

		conn := &mockConnection{
			channelFn: func() (Channel, error) {
				ch := &mockChannel{
					txRollbackErr: rbErr,
				}
				return ch, nil
			},
		}

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		err := b.Transaction(t.Context(), func(Channel) error { return fnErr })
		require.Error(t, err)
		assert.ErrorContains(t, err, "transaction rollback")
		assert.ErrorContains(t, err, "rollback error")
		assert.ErrorContains(t, err, "fn error")
	})

	t.Run("WhenTxCommitFails", func(t *testing.T) {
		sentinel := errors.New("commit failed")

		conn := &mockConnection{
			channelFn: func() (Channel, error) {
				ch := &mockChannel{
					txCommitErr: sentinel,
				}
				return ch, nil
			},
		}

		b := newTestBrokerWithConnection(conn)
		defer b.Close()

		err := b.Transaction(t.Context(), func(Channel) error { return nil })
		require.Error(t, err)
		assert.ErrorContains(t, err, "transaction commit")
		assert.ErrorContains(t, err, "commit failed")
	})
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

	t.Run("WithMockPublishers", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			consumers:   make(map[string]Consumer),
			topologyReg: topology.NewRegistry(),
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
			topologyReg: topology.NewRegistry(),
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
}

func TestBrokerDeclare(t *testing.T) {
	t.Run("WithChannelError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:           ctx,
			topologyReg:   topology.NewRegistry(),
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.Init(ctx)

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
			topologyReg:   topology.NewRegistry(),
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.Init(ctx)

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
			topologyReg:   topology.NewRegistry(),
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.Init(ctx)

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
			topologyReg:   topology.NewRegistry(),
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.Init(ctx)

		topology := &Topology{
			Exchanges: []Exchange{NewExchange("test-exchange")},
		}

		err := b.Sync(topology)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})
}

func TestBrokerExchange(t *testing.T) {
	b := &Broker{
		topologyReg: topology.NewRegistry(),
	}

	// not declared yet
	assert.Nil(t, b.Exchange("test-exchange"))

	// declare topology
	ch := &mockChannel{}
	topology := &Topology{
		Exchanges: []Exchange{NewExchange("test-exchange")},
	}
	err := b.topologyReg.Declare(ch, topology)
	assert.NoError(t, err)

	// should find it now
	ex := b.Exchange("test-exchange")
	assert.NotNil(t, ex)
	assert.Equal(t, "test-exchange", ex.Name)
}

func TestBrokerQueue(t *testing.T) {
	b := &Broker{
		topologyReg: topology.NewRegistry(),
	}

	// not declared yet
	assert.Nil(t, b.Queue("test-queue"))

	// declare topology
	ch := &mockChannel{}
	topology := &Topology{
		Queues: []Queue{NewQueue("test-queue")},
	}
	err := b.topologyReg.Declare(ch, topology)
	assert.NoError(t, err)

	// should find it now
	q := b.Queue("test-queue")
	assert.NotNil(t, q)
	assert.Equal(t, "test-queue", q.Name)
}

func TestBrokerBinding(t *testing.T) {
	b := &Broker{
		topologyReg: topology.NewRegistry(),
	}

	// not declared yet
	assert.Nil(t, b.Binding("source", "destination", "key"))

	// declare topology
	ch := &mockChannel{}
	topology := &Topology{
		Bindings: []Binding{NewBinding("source", "destination", "key")},
	}
	err := b.topologyReg.Declare(ch, topology)
	assert.NoError(t, err)

	// should find it now
	binding := b.Binding("source", "destination", "key")
	assert.NotNil(t, binding)
	assert.Equal(t, "source", binding.Source)
	assert.Equal(t, "destination", binding.Destination)
	assert.Equal(t, "key", binding.Key)
}

func TestBrokerNewPublisher(t *testing.T) {
	t.Run("WithConnectionError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		b := &Broker{
			ctx:         ctx,
			cancel:      cancel,
			publishers:  make(map[string]Publisher),
			topologyReg: topology.NewRegistry(),
			// create a connection manager that will fail to connect
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.Init(ctx)

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
			topologyReg: topology.NewRegistry(),
			// create a connection manager that will fail to connect
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.Init(ctx)

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
			topologyReg: topology.NewRegistry(),
			// create a connection manager that will fail to connect
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.Init(ctx)

		err := b.Publish(t.Context(), "test-queue", "test-key", Message{})
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
			topologyReg:    topology.NewRegistry(),
			// create a connection manager that will fail to connect
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}

		_ = b.publishersPool.init(ctx)
		_ = b.publishersPool.Close() // close pool to force acquire error

		err := b.Publish(t.Context(), "test-queue", "test-key", Message{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrBroker)
		assert.ErrorContains(t, err, "pool: closed")
	})

	t.Run("WhenClosed", func(t *testing.T) {
		b := &Broker{
			topologyReg: topology.NewRegistry(),
		}
		b.Close()

		err := b.Publish(t.Context(), "test-queue", "test-key", Message{})
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
			topologyReg: topology.NewRegistry(),
			// create a connection manager that will fail to connect
			connectionMgr: transport.NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{Size: 1}),
		}
		defer b.Close()

		// initialize will fail due to not being able to connect
		_ = b.connectionMgr.Init(ctx)

		err := b.Consume(t.Context(), "test", testHandler(HandlerActionNoAction))
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrConnectionManager)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		b := &Broker{
			topologyReg: topology.NewRegistry(),
		}
		b.Close()

		err := b.Consume(t.Context(), "test", testHandler(HandlerActionNoAction))
		assert.ErrorIs(t, err, ErrBrokerClosed)
	})
}

func TestBrokerRelease(t *testing.T) {
	t.Run("Publisher", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:        ctx,
			cancel:     cancel,
			publishers: make(map[string]Publisher),
			consumers:  make(map[string]Consumer),
		}

		p := &mockPublisher{}
		b.publishers["publisher-1"] = p

		err := b.Release(p)
		require.NoError(t, err)

		b.publishersMu.Lock()
		count := len(b.publishers)
		b.publishersMu.Unlock()
		assert.Equal(t, 0, count)
	})

	t.Run("Consumer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b := &Broker{
			ctx:        ctx,
			cancel:     cancel,
			publishers: make(map[string]Publisher),
			consumers:  make(map[string]Consumer),
		}

		c := &mockConsumer{}
		b.consumers["consumer-1"] = c

		err := b.Release(c)
		require.NoError(t, err)

		b.consumersMu.Lock()
		count := len(b.consumers)
		b.consumersMu.Unlock()
		assert.Equal(t, 0, count)
	})

	t.Run("WithCloseError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		closeErr := errors.New("close failed")
		b := &Broker{
			ctx:        ctx,
			cancel:     cancel,
			publishers: make(map[string]Publisher),
			consumers:  make(map[string]Consumer),
		}

		p := &mockPublisher{mockEndpoint{closeErr: closeErr}}
		b.publishers["publisher-1"] = p

		err := b.Release(p)
		assert.ErrorIs(t, err, closeErr)

		// still removed from registry despite close error
		b.publishersMu.Lock()
		count := len(b.publishers)
		b.publishersMu.Unlock()
		assert.Equal(t, 0, count)
	})

	t.Run("WithUnregisteredEndpoint", func(t *testing.T) {
		b := &Broker{
			publishers: make(map[string]Publisher),
			consumers:  make(map[string]Consumer),
		}

		p := &mockPublisher{} // NOT added to registry

		err := b.Release(p) // should still close without panicking
		require.NoError(t, err)

		b.publishersMu.Lock()
		count := len(b.publishers)
		b.publishersMu.Unlock()
		assert.Equal(t, 0, count)
	})
}

func TestSentinelErrors(t *testing.T) {
	// implementation
	assert.Implements(t, (*error)(nil), ErrBroker)

	t.Run("Hierarchy", func(t *testing.T) {
		// wrapping hierarchy
		assert.ErrorIs(t, ErrConnection, ErrBroker)
		assert.ErrorIs(t, ErrChannel, ErrBroker)
		assert.ErrorIs(t, ErrTopology, ErrBroker)
		assert.ErrorIs(t, ErrEndpoint, ErrBroker)
		assert.ErrorIs(t, ErrPublisher, ErrBroker)
		assert.ErrorIs(t, ErrConsumer, ErrBroker)
		assert.ErrorIs(t, ErrMessage, ErrBroker)
	})

	t.Run("Broker", func(t *testing.T) {
		assert.ErrorIs(t, ErrBrokerClosed, ErrBroker)
		assert.ErrorIs(t, ErrBrokerConfigInvalid, ErrBroker)
	})

	t.Run("Transport", func(t *testing.T) {
		assert.ErrorIs(t, ErrTransport, ErrBroker)

		assert.ErrorIs(t, ErrConnection, ErrTransport)
		assert.ErrorIs(t, ErrConnectionClosed, ErrConnection)
		assert.ErrorIs(t, ErrConnectionManager, ErrConnection)
		assert.ErrorIs(t, ErrConnectionManagerClosed, ErrConnectionManager)

		assert.ErrorIs(t, ErrChannel, ErrTransport)
		assert.ErrorIs(t, ErrChannelClosed, ErrChannel)
	})

	t.Run("Topology", func(t *testing.T) {
		assert.ErrorIs(t, ErrTopology, ErrBroker)
		assert.ErrorIs(t, ErrTopologyDeclareFailed, ErrTopology)
		assert.ErrorIs(t, ErrTopologyDeleteFailed, ErrTopology)
		assert.ErrorIs(t, ErrTopologyVerifyFailed, ErrTopology)
		assert.ErrorIs(t, ErrTopologyValidation, ErrTopology)
		assert.ErrorIs(t, ErrTopologyExchangeNameEmpty, ErrTopology)
		assert.ErrorIs(t, ErrTopologyQueueNameEmpty, ErrTopology)
		assert.ErrorIs(t, ErrTopologyBindingFieldsEmpty, ErrTopology)
	})

	t.Run("Message", func(t *testing.T) {
		assert.ErrorIs(t, ErrMessage, ErrBroker)
		assert.ErrorIs(t, ErrMessageBuild, ErrMessage)
		assert.ErrorIs(t, ErrMessageNotConsumed, ErrMessage)
		assert.ErrorIs(t, ErrMessageNotPublished, ErrMessage)
	})

	t.Run("Handler", func(t *testing.T) {
		assert.ErrorIs(t, ErrHandler, ErrBroker)
		assert.ErrorIs(t, ErrMiddleware, ErrHandler)
	})

	t.Run("Endpoint", func(t *testing.T) {
		assert.ErrorIs(t, ErrEndpoint, ErrBroker)
		assert.ErrorIs(t, ErrEndpointClosed, ErrEndpoint)
		assert.ErrorIs(t, ErrEndpointNotConnected, ErrEndpoint)
		assert.ErrorIs(t, ErrEndpointNotReadyTimeout, ErrEndpoint)
		assert.ErrorIs(t, ErrEndpointNoAutoReconnect, ErrEndpoint)

		t.Run("Publisher", func(t *testing.T) {
			assert.ErrorIs(t, ErrPublisher, ErrBroker)
			assert.ErrorIs(t, ErrPublisherClosed, ErrPublisher)
			assert.ErrorIs(t, ErrPublisherNotConnected, ErrPublisher)
		})

		t.Run("Consumer", func(t *testing.T) {
			assert.ErrorIs(t, ErrConsumer, ErrBroker)
			assert.ErrorIs(t, ErrConsumerClosed, ErrConsumer)
			assert.ErrorIs(t, ErrConsumerNotConnected, ErrConsumer)
		})
	})

}
