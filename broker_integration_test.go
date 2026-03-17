//go:build integration
// +build integration

package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

func (s *brokerIntegrationTestSuite) TestBrokerNew() {
	s.Run("WithURL", func() {
		b, err := New(WithURL(s.url))
		defer b.Close()
		s.Require().NoError(err)
		s.NotNil(b)
		s.Equal(s.url, b.url)
	})

	s.Run("WithIdentifier", func() {
		b1 := s.newTestBroker(WithIdentifier("broker-1"))
		s.Equal("broker-1", b1.id)

		b2 := s.newTestBroker(WithIdentifier("broker-2"))
		s.Equal("broker-2", b2.id)

		b3 := s.newTestBroker()
		s.Equal(defaultBrokerID, b3.id)
	})

	s.Run("WithContext", func() {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		b := s.newTestBroker(WithContext(ctx))

		// close broker and check that its context is cancelled
		err := b.Close()
		s.NoError(err)

		select {
		case <-b.ctx.Done():
			// expected
		case <-time.After(100 * time.Millisecond):
			s.Fail("broker internal context should be cancelled after Close()")
		}
	})

	s.Run("WithCache", func() {
		// when positive
		b1 := s.newTestBroker(WithCache(10 * time.Minute))
		s.Equal(10*time.Minute, b1.cacheTTL)
		// when zero
		b2 := s.newTestBroker(WithCache(0))
		s.Equal(time.Duration(0), b2.cacheTTL)
		// when negative
		b3 := s.newTestBroker(WithCache(-1 * time.Minute))
		s.Equal(time.Duration(0), b3.cacheTTL)
	})

	s.Run("WithConnectionPoolSize", func() {
		// single connection
		b1 := s.newTestBroker(WithConnectionPoolSize(1))
		s.Equal(1, b1.connectionMgr.opts.Size)
		s.Len(b1.connectionMgr.pool, 1)
		b1.Close()
		// multiple connections
		b2 := s.newTestBroker(WithConnectionPoolSize(3))
		s.Equal(3, b2.connectionMgr.opts.Size)
		s.Len(b2.connectionMgr.pool, 3)
		b2.Close()
	})

	s.Run("WithConnectionHandlers", func() {
		var openCalled atomic.Int32
		var closeCalled atomic.Int32
		var blockCalled atomic.Int32

		b := s.newTestBroker(
			WithConnectionOnOpen(func(idx int) {
				openCalled.Add(1)
			}),
			WithConnectionOnClose(func(idx int, code int, reason string, server bool, recover bool) {
				closeCalled.Add(1)
			}),
			WithConnectionOnBlocked(func(idx int, active bool, reason string) {
				blockCalled.Add(1)
			}),
		)

		time.Sleep(100 * time.Millisecond)

		// open callbacks should have been called during initialization
		s.Greater(openCalled.Load(), int32(0), "open callback should have been called")

		conn, err := b.Connection()
		s.Require().NoError(err)
		conn.Close()

		time.Sleep(100 * time.Millisecond)

		// close callbacks should have been called during conn.Close()
		s.Greater(closeCalled.Load(), int32(0), "close callback should have been called")

		// block can't be easily simulated as it depends on server behavior
	})

	s.Run("WithEndpointOptions", func() {
		b := s.newTestBroker(
			WithEndpointOptions(EndpointOptions{
				NoWaitReady:     false,
				ReadyTimeout:    5 * time.Second,
				NoAutoReconnect: true,
				ReconnectMin:    1 * time.Second,
				ReconnectMax:    10 * time.Second,
			}),
		)
		s.NotNil(b)
		s.False(b.endpointOpts.NoWaitReady)
		s.Equal(5*time.Second, b.endpointOpts.ReadyTimeout)
		s.True(b.endpointOpts.NoAutoReconnect)
		s.Equal(1*time.Second, b.endpointOpts.ReconnectMin)
		s.Equal(10*time.Second, b.endpointOpts.ReconnectMax)
	})

	s.Run("State", func() {
		b := s.newTestBroker()

		s.Equal(defaultBrokerID, b.id)
		s.Equal(defaultCacheTTL, b.cacheTTL)
		s.Equal(s.url, b.url)

		s.NotNil(b.ctx)
		s.NotNil(b.cancel)
		s.False(b.closed.Load())

		s.NotNil(b.connectionMgr)
		// connectionMgrOpts should have defaults merged
		s.Equal(defaultConnectionPoolSize, b.connectionMgrOpts.Size)
		s.Equal(defaultReconnectMin, b.connectionMgrOpts.ReconnectMin)
		s.Equal(defaultReconnectMax, b.connectionMgrOpts.ReconnectMax)
		s.False(b.connectionMgrOpts.NoAutoReconnect)
		s.Nil(b.connectionMgrOpts.Config)
		s.Nil(b.connectionMgrOpts.OnOpen)
		s.Nil(b.connectionMgrOpts.OnClose)
		s.Nil(b.connectionMgrOpts.OnBlock)

		s.NotNil(b.topologyMgr)

		s.NotNil(b.endpointOpts)

		s.Empty(b.publishers)
		s.Zero(b.publishersSeq.Load())
		s.NotNil(b.publishersPool)

		s.Empty(b.consumers)
		s.Zero(b.consumersSeq.Load())
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerClose() {
	b := s.newTestBroker()

	qName := testName("test-queue")
	eName := testName("test-exchange")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	err = b.Close()
	s.Require().NoError(err)

	// try to create publisher
	_, err = b.NewPublisher(&PublisherOptions{}, topology.Exchanges[0])
	s.Error(err)
	s.Equal(ErrBrokerClosed, err)

	// try to create consumer
	handler := testHandler(HandlerActionAck)
	_, err = b.NewConsumer(&ConsumerOptions{}, topology.Queues[0], handler)
	s.Error(err)
	s.Equal(ErrBrokerClosed, err)

	s.Run("Idempotency", func() {
		b := s.newTestBroker()
		s.False(b.closed.Load())
		err := b.Close()
		s.NoError(err)
		s.True(b.closed.Load())
		err = b.Close() // closing again should be safe
		s.NoError(err)
	})

	s.Run("WithNewPublisher", func() {
		b := s.newTestBroker()
		eName := testName("test-exchange")
		qName := testName("test-queue")
		topology := &Topology{
			Exchanges: []Exchange{{Name: eName, Type: "direct"}},
			Queues:    []Queue{{Name: qName}},
		}
		err := b.Declare(topology)
		s.Require().NoError(err)
		err = b.Close()
		s.Require().NoError(err)
		_, err = b.NewPublisher(&PublisherOptions{}, topology.Exchanges[0])
		s.Error(err)
		s.Equal(ErrBrokerClosed, err)
	})

	s.Run("WithNewConsumer", func() {
		b := s.newTestBroker()
		eName := testName("test-exchange")
		qName := testName("test-queue")
		topology := &Topology{
			Exchanges: []Exchange{{Name: eName, Type: "direct"}},
			Queues:    []Queue{{Name: qName}},
		}
		err := b.Declare(topology)
		s.Require().NoError(err)
		err = b.Close()
		s.Require().NoError(err)
		handler := testHandler(HandlerActionAck)
		_, err = b.NewConsumer(&ConsumerOptions{}, topology.Queues[0], handler)
		s.Error(err)
		s.Equal(ErrBrokerClosed, err)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerConnection() {
	b := s.newTestBroker()

	conn, err := b.Connection()
	s.NoError(err)
	s.NotNil(conn)
	defer conn.Close()

	// connection should be usable
	s.False(conn.IsClosed())

	s.Run("WhenClosed", func() {
		b := s.newTestBroker()
		err := b.Close()
		s.Require().NoError(err)
		conn, err := b.Connection()
		s.Error(err)
		s.Nil(conn)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerChannel() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.NoError(err)
	s.NotNil(ch)
	defer ch.Close()

	// channel should be usable
	s.False(ch.IsClosed())

	s.Run("WhenClosed", func() {
		b := s.newTestBroker()
		err := b.Close()
		s.Require().NoError(err)
		ch, err := b.Channel()
		s.Error(err)
		s.Nil(ch)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerDeclare() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{
			{Name: eName, Type: "direct", Durable: true},
		},
		Queues: []Queue{
			{Name: qName, Durable: true},
		},
		Bindings: []Binding{
			{Source: eName, Destination: qName, Key: "test"},
		},
	}

	err := b.Declare(topology)
	s.NoError(err)

	// verify topology is stored
	s.NotNil(b.topologyMgr)
	s.Len(b.topologyMgr.exchanges, 1)
	s.Len(b.topologyMgr.queues, 1)
	s.Len(b.topologyMgr.bindings, 1)

	// verify we can retrieve declared queue
	q := b.topologyMgr.queue(qName)
	s.NotNil(q)
	s.Equal(qName, q.Name)
	s.True(q.Durable)

	s.Run("WithInvalidTopology", func() {
		b := s.newTestBroker()

		// empty exchange name
		topology1 := &Topology{
			Exchanges: []Exchange{
				{Name: "", Type: "direct"},
			},
		}
		err := b.Declare(topology1)
		s.Error(err)
		s.ErrorIs(err, ErrTopologyExchangeNameEmpty)

		// empty queue name
		topology2 := &Topology{
			Queues: []Queue{
				{Name: ""},
			},
		}
		err = b.Declare(topology2)
		s.Error(err)
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)

		// empty binding fields
		topology3 := &Topology{
			Bindings: []Binding{
				{Source: "", Destination: "", Key: ""},
			},
		}
		err = b.Declare(topology3)
		s.Error(err)
		s.ErrorIs(err, ErrTopologyBindingFieldsEmpty)

		// invalid exchange type
		topology4 := &Topology{
			Exchanges: []Exchange{
				{Name: testName("test-exchange"), Type: "invalid-type"},
			},
		}
		err = b.Declare(topology4)
		s.Error(err)
	})

	s.Run("WhenClosed", func() {
		b, t := s.newTestBrokerWithTopology()
		err := b.Close()
		s.Require().NoError(err)
		err = b.Declare(t)
		s.Error(err)
		s.ErrorIs(err, ErrBroker)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerVerify() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName}},
		Queues:    []Queue{{Name: qName}},
	}

	err := b.Declare(topology)
	s.Require().NoError(err)

	err = b.Verify(topology)
	s.NoError(err)
	s.NotNil(b.topologyMgr.exchange(eName))
	s.NotNil(b.topologyMgr.queue(qName))

	s.Run("WithInvalidTopology", func() {
		b := s.newTestBroker()
		topology := &Topology{
			Exchanges: []Exchange{{}},
			Queues:    []Queue{{}},
			Bindings:  []Binding{{}},
		}
		err := b.Verify(topology)
		s.Error(err)
	})

	s.Run("WhenClosed", func() {
		b, t := s.newTestBrokerWithTopology()
		err := b.Close()
		s.Require().NoError(err)
		err = b.Verify(t)
		s.Error(err)
		s.ErrorIs(err, ErrBroker)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerDelete() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName}},
		Queues:    []Queue{{Name: qName}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)
	err = b.Delete(topology)
	s.NoError(err)
	s.Nil(b.topologyMgr.exchange(eName))
	s.Nil(b.topologyMgr.queue(qName))
	s.Len(b.topologyMgr.exchanges, 0)
	s.Len(b.topologyMgr.queues, 0)

	s.Run("WithInvalidTopology", func() {
		b := s.newTestBroker()

		topology := &Topology{
			Exchanges: []Exchange{{}},
			Queues:    []Queue{{}},
			Bindings:  []Binding{{}},
		}

		err = b.Delete(topology)
		s.Error(err)
	})

	s.Run("WhenClosed", func() {
		b, t := s.newTestBrokerWithTopology()
		err := b.Close()
		s.Require().NoError(err)
		err = b.Delete(t)
		s.Error(err)
		s.ErrorIs(err, ErrBroker)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerSync() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{
			{Name: eName, Type: "direct", Durable: true},
		},
		Queues: []Queue{
			{Name: qName, Durable: true},
		},
		Bindings: []Binding{
			{Source: eName, Destination: qName, Key: "test"},
		},
	}

	err := b.Sync(topology)
	s.NoError(err)

	// verify topology is stored
	s.NotNil(b.topologyMgr)
	s.Len(b.topologyMgr.exchanges, 1)
	s.Len(b.topologyMgr.queues, 1)
	s.Len(b.topologyMgr.bindings, 1)

	// verify we can retrieve declared queue
	q := b.topologyMgr.queue(qName)
	s.NotNil(q)
	s.Equal(qName, q.Name)
	s.True(q.Durable)

	s.Run("WithInvalidTopology", func() {
		b := s.newTestBroker()
		topology := &Topology{
			Exchanges: []Exchange{{}},
			Queues:    []Queue{{}},
			Bindings:  []Binding{{}},
		}
		err := b.Sync(topology)
		s.Error(err)
		s.ErrorIs(err, ErrTopologyValidation)
	})

	s.Run("WhenClosed", func() {
		b, t := s.newTestBrokerWithTopology()
		err := b.Close()
		s.Require().NoError(err)
		err = b.Sync(t)
		s.Error(err)
		s.ErrorIs(err, ErrBroker)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerNewPublisher() {
	b, t := s.newTestBrokerWithTopology()

	// publisher registry
	{
		p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
		s.Require().NoError(err)

		publisher := p.(*publisher)

		b.publishersMu.Lock()
		_, exists := b.publishers[publisher.id]
		b.publishersMu.Unlock()
		s.True(exists)

		p.Close()

		b.publishersMu.Lock()
		_, exists = b.publishers[publisher.id]
		b.publishersMu.Unlock()
		s.True(exists)

		p.Release()

		b.publishersMu.Lock()
		_, exists = b.publishers[publisher.id]
		b.publishersMu.Unlock()
		s.False(exists)
	}

	s.Run("Consistency", func() {
		publishers := make([]Publisher, 5)
		for i := range publishers {
			p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
			s.Require().NoError(err)
			publishers[i] = p
		}

		b.publishersMu.Lock()
		count := len(b.publishers)
		b.publishersMu.Unlock()
		s.Equal(5, count)

		for _, p := range publishers {
			p.Close()
		}
	})

	s.Run("WhenClosed", func() {
		b, t := s.newTestBrokerWithTopology()
		err := b.Close()
		s.Require().NoError(err)
		// try to create publisher
		_, err = b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
		s.Error(err)
		s.Equal(ErrBrokerClosed, err)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerNewConsumer() {
	b, t := s.newTestBrokerWithTopology()

	// registry
	{
		handler := testHandler(HandlerActionAck)
		c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
		s.Require().NoError(err)

		consumer := c.(*consumer)

		b.consumersMu.Lock()
		_, exists := b.consumers[consumer.id]
		b.consumersMu.Unlock()
		s.True(exists)

		c.Close()

		b.consumersMu.Lock()
		_, exists = b.consumers[consumer.id]
		b.consumersMu.Unlock()
		s.True(exists)

		c.Release()

		b.consumersMu.Lock()
		_, exists = b.consumers[consumer.id]
		b.consumersMu.Unlock()
		s.False(exists)
	}

	s.Run("Consistency", func() {
		handler := testHandler(HandlerActionAck)
		consumers := make([]Consumer, 5)
		for i := range consumers {
			c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
			s.Require().NoError(err)
			consumers[i] = c
		}

		b.consumersMu.Lock()
		count := len(b.consumers)
		b.consumersMu.Unlock()
		s.Equal(5, count)

		for _, c := range consumers {
			c.Close()
		}
	})

	s.Run("WhenClosed", func() {
		b, t := s.newTestBrokerWithTopology()
		err := b.Close()
		s.Require().NoError(err)
		// try to create consumer
		handler := testHandler(HandlerActionAck)
		_, err = b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
		s.Error(err)
		s.Equal(ErrBrokerClosed, err)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerMultipleEndpointsStress() {
	b, t := s.newTestBrokerWithTopology()

	start := time.Now()

	// create multiple publishers and consumers
	baseCount := 5
	messagesCount := (baseCount * 2) * 1000
	publishersCount := (baseCount * 2)
	consumersCount := (baseCount * 2)
	handlersCount := (baseCount * 5)

	var received atomic.Int32
	handler := testCountingHandler(HandlerActionAck, &received)

	// start 5 consumers
	consumers := make([]Consumer, consumersCount)
	for i := range consumers {
		c, err := b.NewConsumer(&ConsumerOptions{
			MaxConcurrentHandlers: handlersCount,
		}, t.Queues[0], handler)
		s.Require().NoError(err)
		consumers[i] = c
	}
	defer func() {
		for _, c := range consumers {
			c.Close()
		}
	}()

	// wait for all consumers to be fully ready and bound
	time.Sleep(1 * time.Second)

	// start publishers with proper synchronization
	wg := sync.WaitGroup{}
	for range publishersCount {
		wg.Go(func() {
			p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
			s.Require().NoError(err)
			defer p.Close()

			for j := 0; j < messagesCount/publishersCount; j++ {
				err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("data")))
				s.NoError(err)
			}
		})
	}
	wg.Wait()

	time.Sleep(2 * time.Second)

	for _, c := range consumers {
		c.Wait()
	}

	elapsed := time.Since(start)
	_ = elapsed

	// s.T().Logf("Stress test completed in %dms", elapsed/time.Millisecond)

	s.Equal(int32(messagesCount), received.Load())
}

func (s *brokerIntegrationTestSuite) TestBrokerPublisherFromTopology() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	// declare topology with exchange details
	topology := &Topology{
		Exchanges: []Exchange{
			{Name: eName, Type: "topic", Durable: true, AutoDelete: false},
		},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	// create publisher with minimal exchange info
	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName})
	s.NoError(err)
	s.NotNil(p)
	defer p.Close()

	// publisher should have loaded full exchange details from topology
	publisher := p.(*publisher)
	s.Equal("topic", publisher.exchange.Type)
	s.True(publisher.exchange.Durable)
}

func (s *brokerIntegrationTestSuite) TestBrokerPublishConsume() {
	b, t := s.newTestBrokerWithTopology()

	// create publisher
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	// create consumer
	var received atomic.Int32
	handler := testCountingHandler(HandlerActionAck, &received)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// wait for consumer to be fully ready and bound
	time.Sleep(500 * time.Millisecond)

	// publish messages
	messagesCount := 5
	for range messagesCount {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	// wait for consumption
	time.Sleep(200 * time.Millisecond)
	c.Wait()

	s.Equal(int32(messagesCount), received.Load())
}

func (s *brokerIntegrationTestSuite) TestBrokerNewPublisherConcurrentAccess() {
	b, t := s.newTestBrokerWithTopology()

	// create consumer to receive messages
	var received atomic.Int32
	handler := testCountingHandler(HandlerActionAck, &received)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// wait for consumer to be fully ready and bound
	time.Sleep(1 * time.Second)

	// create multiple publishers and publish concurrently
	messagesCount := 50
	publisherCount := 1

	wg := sync.WaitGroup{}
	for range publisherCount {
		wg.Go(func() {
			p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
			s.Require().NoError(err)
			defer p.Close()

			for j := 0; j < messagesCount/publisherCount; j++ {
				err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
				s.NoError(err)
			}
		})
	}
	wg.Wait()

	// wait for all messages
	time.Sleep(5 * time.Second)
	c.Wait()

	s.Equal(int32(messagesCount), received.Load())
}

func (s *brokerIntegrationTestSuite) TestBrokerPublish() {
	b := s.newTestBroker()

	// setup topology
	eName := testName("test-exchange")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	// publish a message, will be discarded as no queues are bound
	msg := NewMessage([]byte("test message"))
	err = b.Publish(s.ctx, eName, "test", msg)
	s.NoError(err)

	s.Run("WithUnknownExchange", func() {
		b := s.newTestBroker()
		msg := NewMessage([]byte("test message"))
		err := b.Publish(s.ctx, eName+"-unknown", "test", msg)
		s.NoError(err)
	})

	s.Run("WithNoCache", func() {
		b := s.newTestBroker(WithCache(0))
		msg := NewMessage([]byte("test message"))
		err := b.Publish(s.ctx, eName+"-no-cache", "test", msg)
		s.NoError(err)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerConsume() {
	b := s.newTestBroker()

	// setup topology
	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: "test"}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	// publish a message
	msg := NewMessage([]byte("test message"))
	err = b.Publish(s.ctx, eName, "test", msg)
	s.NoError(err)

	// consume using convenience method
	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()

	receivedCh := make(chan bool, 1)
	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		receivedCh <- true
		cancel() // cancel to stop consuming
		return HandlerActionAck, nil
	}

	err = b.Consume(ctx, qName, handler)
	s.Error(err) // should error with context.Canceled
	s.ErrorIs(err, context.Canceled)

	// verify message was received
	select {
	case <-receivedCh:
		// success
	case <-time.After(3 * time.Second):
		s.Fail("message not received")
	}

	s.Run("WithUnknownQueue", func() {
		ctx, cancel := context.WithTimeout(s.ctx, 1*time.Second)
		defer cancel()
		err := b.Consume(ctx, qName+"-unknown", handler)
		s.Error(err)
	})
}

func (s *brokerIntegrationTestSuite) TestBrokerTransaction() {
	b := s.newTestBroker()

	e := testName("test-exchange")
	q := testName("test-queue")
	k := testName("test-key")

	topology := &Topology{
		Exchanges: []Exchange{{Name: e, Durable: true, Type: "direct"}},
		Queues:    []Queue{{Name: q, Durable: true, Exclusive: false}},
		Bindings:  []Binding{{Source: e, Destination: q, Key: k}},
	}

	var err error

	// should succeed
	err = b.Transaction(b.ctx, func(ch *Channel) error {
		exchange := topology.Exchange(e)
		queue := topology.Queue(q)
		binding := topology.Binding(e, q, k)

		if err := exchange.Declare(ch); err != nil {
			return err
		}
		if err := queue.Declare(ch); err != nil {
			return err
		}
		if err := binding.Declare(ch); err != nil {
			return err
		}

		return nil
	})
	s.NoError(err)

	// verify topology
	err = b.Verify(topology)
	s.NoError(err, "verify topology after transaction")

	// should fail, declaration with the same name exists but durable=true
	err = b.Transaction(b.ctx, func(ch *Channel) error {
		exchange := *topology.Exchange(e)
		queue := *topology.Queue(q)
		binding := *topology.Binding(e, q, k)

		exchange.Durable = false // conflicting declaration
		queue.Exclusive = true   // conflicting declaration

		var err error
		if err = exchange.Declare(ch); err != nil {
			return err
		}
		if err = queue.Declare(ch); err != nil {
			return err
		}
		if err = binding.Declare(ch); err != nil {
			return err
		}

		return err
	})
	s.Error(err, "transaction with conflicting declaration should fail")

	// verify topology remains unchanged
	err = b.Verify(topology)
	s.NoError(err, "verify topology remains unchanged after failed transaction")

	// should fail, committing on closed channel
	err = b.Transaction(s.ctx, func(ch *Channel) error {
		// close the channel to cause commit to fail
		ch.Close()
		return nil
	})
	s.Error(err, "transaction commit should fail with closed channel")
}
