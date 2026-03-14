//go:build integration
// +build integration

package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

func (s *brokerIntegrationTestSuite) TestEndpointExchange() {
	b := s.newTestBroker()

	eName1 := testName("test-exchange-1")
	exchange1 := Exchange{Name: eName1, Type: "direct"}

	// create publisher endpoint
	p, err := b.NewPublisher(&PublisherOptions{}, exchange1)
	s.Require().NoError(err)
	defer p.Close()

	// declare another exchange through endpoint
	eName2 := testName("test-exchange-2")
	exchange2 := Exchange{Name: eName2, Type: "fanout"}

	err = p.Exchange(exchange2)
	s.NoError(err)

	// verify exchange was declared
	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	err = exchange2.Verify(ch)
	s.NoError(err, "exchange should exist")
}

func (s *brokerIntegrationTestSuite) TestEndpointQueue() {
	b := s.newTestBroker()

	eName1 := testName("test-exchange-1")
	qName1 := testName("test-queue-1")

	// declare initial topology
	topology := &Topology{
		Exchanges: []Exchange{{Name: eName1, Type: "direct"}},
		Queues:    []Queue{{Name: qName1}},
		Bindings:  []Binding{{Source: eName1, Destination: qName1, Key: "test"}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	queue1 := topology.Queues[0]
	// create consumer endpoint
	c, err := b.NewConsumer(&ConsumerOptions{}, queue1, nil)
	s.Require().NoError(err)
	defer c.Close()

	// declare another queue through endpoint
	qName2 := testName("test-queue-2")
	queue2 := Queue{Name: qName2, Durable: true}

	err = c.Queue(queue2)
	s.NoError(err)

	// verify queue was declared
	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	err = queue2.Verify(ch)
	s.NoError(err, "queue should exist")
}

func (s *brokerIntegrationTestSuite) TestEndpointBinding() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	// declare topology
	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	// create publisher endpoint
	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	// declare binding through endpoint
	err = p.Binding(Binding{Source: eName, Destination: qName, Key: "test"})
	s.NoError(err)

	// verify binding works
	msg := NewMessage([]byte("test binding"))
	err = p.Publish(s.ctx, RoutingKey("test"), msg)
	s.NoError(err)

	// consume to verify message was routed
	receivedCh := make(chan bool, 1)
	c, err := b.NewConsumer(&ConsumerOptions{AutoAck: true}, Queue{Name: qName}, func(ctx context.Context, msg *Message) (HandlerAction, error) {
		receivedCh <- true
		return HandlerActionAck, nil
	})
	s.Require().NoError(err)
	defer c.Close()

	go c.Consume(s.ctx)

	select {
	case <-receivedCh:
		// success
	case <-time.After(2 * time.Second):
		s.Fail("message should have been routed through binding")
	}
}

func (s *brokerIntegrationTestSuite) TestEndpointConnectionAccess() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	// access connection
	conn := p.Connection()
	s.NotNil(conn, "endpoint should have connection")
	s.False(conn.IsClosed(), "connection should be open")
}

func (s *brokerIntegrationTestSuite) TestEndpointChannelAccess() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	// access channel
	ch := p.Channel()
	s.NotNil(ch, "endpoint should have channel")
	s.False(ch.IsClosed(), "channel should be open")
}

func (s *brokerIntegrationTestSuite) TestEndpointClose() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)

	publisher := p.(*publisher)
	s.False(publisher.endpoint.closed.Load(), "endpoint should not be closed initially")

	err = p.Close()
	s.NoError(err)

	s.True(publisher.endpoint.closed.Load(), "endpoint should be closed")

	// double close should be no-op
	err = p.Close()
	s.NoError(err)
}

func (s *brokerIntegrationTestSuite) TestEndpointRelease() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)

	publisher := p.(*publisher)

	// verify publisher is registered
	b.publishersMu.Lock()
	_, exists := b.publishers[publisher.id]
	b.publishersMu.Unlock()
	s.True(exists, "publisher should be registered")

	// release
	p.Release()

	// verify unregistered and closed
	b.publishersMu.Lock()
	_, exists = b.publishers[publisher.id]
	b.publishersMu.Unlock()
	s.False(exists, "publisher should be unregistered")
	s.True(publisher.endpoint.closed.Load(), "endpoint should be closed")
}

func (s *brokerIntegrationTestSuite) TestEndpointChannelRecreation() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: "test"}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	// get initial channel
	ch1 := p.Channel()
	s.NotNil(ch1)

	// force close the channel
	err = ch1.Close()
	s.NoError(err)

	// wait for channel to be detected as closed
	time.Sleep(100 * time.Millisecond)

	// Channel() should return nil for closed channel
	ch2 := p.Channel()

	// depending on timing, might be nil (closed detected) or new channel (reconnected)
	if ch2 != nil {
		s.False(ch2.IsClosed(), "if channel exists, should be open")
	}
}

func (s *brokerIntegrationTestSuite) TestEndpointPublisherRole() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	publisher := p.(*publisher)
	s.Equal(rolePublisher, publisher.endpoint.role, "publisher should have publisher role")
}

func (s *brokerIntegrationTestSuite) TestEndpointConsumerRole() {
	b := s.newTestBroker()

	qName := testName("test-queue")

	topology := &Topology{
		Queues: []Queue{{Name: qName}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	c, err := b.NewConsumer(&ConsumerOptions{}, Queue{Name: qName}, nil)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)
	s.Equal(roleConsumer, consumer.endpoint.role, "consumer should have consumer role")
}

func (s *brokerIntegrationTestSuite) TestEndpointCloseBeforeReady() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	// create publisher with NoWaitForReady
	p, err := b.NewPublisher(&PublisherOptions{
		EndpointOptions: EndpointOptions{
			NoWaitReady: true,
		},
	}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)

	publisher := p.(*publisher)

	// close immediately
	err = p.Close()
	s.NoError(err)

	s.False(publisher.endpoint.ready.Load(), "should not be ready when closed")
	s.True(publisher.endpoint.closed.Load(), "endpoint should be closed")
}

func (s *brokerIntegrationTestSuite) TestEndpointMultipleEndpointsSameConnection() {
	// use connection pool size 1 to force sharing
	b := s.newTestBroker(
		WithURL(s.url),
		WithContext(s.ctx),
	)

	eName := testName("test-exchange")

	// create multiple publishers
	p1, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p1.Close()

	p2, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p2.Close()

	// should share same connection
	publisher1 := p1.(*publisher)
	publisher2 := p2.(*publisher)

	conn1 := publisher1.endpoint.Connection()
	conn2 := publisher2.endpoint.Connection()

	s.Equal(conn1, conn2, "publishers should share connection with pool size 1")

	// but should have different channels
	ch1 := publisher1.endpoint.Channel()
	ch2 := publisher2.endpoint.Channel()
	s.NotEqual(ch1, ch2, "publishers should have different channels")
}

func (s *brokerIntegrationTestSuite) TestEndpointChannelErrorRecovery() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	var errCount atomic.Int32
	p, err := b.NewPublisher(&PublisherOptions{
		OnError: func(err error) {
			errCount.Add(1)
		},
	}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	publisher := p.(*publisher)
	ch := publisher.endpoint.Channel()
	s.NotNil(ch)

	exchange := Exchange{Name: eName, Type: "fanout"}

	// cause channel error by declaring conflicting exchange
	err = exchange.Declare(ch)
	// this should cause channel error (precondition failed)
	// the channel will close
	s.Error(err, "should get precondition failed error")

	// wait for error to be detected and recovery
	time.Sleep(500 * time.Millisecond)

	// verify publisher recovered - should have new channel or be attempting recovery
	newCh := publisher.endpoint.Channel()
	if newCh != nil && newCh != ch {
		// successfully recovered with new channel
		s.False(newCh.IsClosed(), "new channel should be open after recovery")
		s.GreaterOrEqual(errCount.Load(), int32(1), "error callback should be called")
	} else {
		// still recovering or no auto-reconnect
		// at minimum, error callback should have been invoked
		s.GreaterOrEqual(errCount.Load(), int32(1), "error callback should be called on channel error")
	}
}

func (s *brokerIntegrationTestSuite) TestEndpointNoAutoReconnect() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: "test"}},
	}
	err := b.Declare(topology)
	s.Require().NoError(err)

	c, err := b.NewConsumer(&ConsumerOptions{
		EndpointOptions: EndpointOptions{
			NoAutoReconnect: true,
		},
	}, Queue{Name: qName}, nil)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)
	ch := consumer.endpoint.Channel()
	s.Require().NotNil(ch, "should have initial channel")
	s.True(consumer.Ready(), "should be ready initially")

	// close channel to simulate error
	err = ch.Close()
	s.NoError(err)

	// wait for disconnect to be detected
	time.Sleep(1 * time.Second)

	// with NoAutoReconnect, should NOT have reconnected
	s.False(consumer.Ready(), "should not be ready after disconnect with NoAutoReconnect")
	ch2 := consumer.Channel()
	s.Nil(ch2, "should not have reconnected with NoAutoReconnect")
}

func (s *brokerIntegrationTestSuite) TestEndpointReadyTimeout() {
	b := s.newTestBroker()

	eName := testName("test-exchange")

	// create endpoint with short ready timeout
	// since RabbitMQ is actually running, this should succeed
	p, err := b.NewPublisher(&PublisherOptions{
		EndpointOptions: EndpointOptions{
			ReadyTimeout: 100 * time.Millisecond,
		},
	}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	s.True(p.Ready(), "endpoint should be ready")
}

func (s *brokerIntegrationTestSuite) TestEndpointDeclareErrorsPropagate() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	// try to declare exchange with empty name (should fail)
	err = p.Exchange(Exchange{Name: "", Type: "direct"})
	s.Error(err, "should fail to declare exchange with empty name")
}

func (s *brokerIntegrationTestSuite) TestEndpointConcurrentAccess() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)
	defer p.Close()

	// concurrently access connection, channel
	var wg sync.WaitGroup
	concurrency := 20
	for range concurrency {
		wg.Add(2)
		go func() {
			defer wg.Done()
			conn := p.Connection()
			s.NotNil(conn)
		}()
		go func() {
			defer wg.Done()
			ch := p.Channel()
			s.NotNil(ch)
		}()
	}

	wg.Wait()
}

func (s *brokerIntegrationTestSuite) TestEndpointConcurrentAccessAndClose() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	p, err := b.NewPublisher(&PublisherOptions{}, Exchange{Name: eName, Type: "direct"})
	s.Require().NoError(err)

	var wg sync.WaitGroup

	// start goroutines accessing endpoint
	for range 10 {
		wg.Go(func() {
			for range 100 {
				_ = p.Connection()
				_ = p.Channel()
				time.Sleep(1 * time.Millisecond)
			}
		})
	}

	// close while access is happening
	time.Sleep(50 * time.Millisecond)
	err = p.Close()
	s.NoError(err)

	wg.Wait()
}
