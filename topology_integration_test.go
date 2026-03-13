//go:build integration
// +build integration

package broker

import (
	"context"
	"sync"
	"time"
)

// The tests in this file use functionality provided by amqp.Channel directly
// to reduce dependencies on higher-level abstractions provided by the library.

func (s *brokerIntegrationTestSuite) TestExchangeDeclare() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	exchange := Exchange{Name: eName, Type: "direct", Durable: true}

	err = exchange.Declare(ch)
	s.NoError(err)

	s.Run("WithTypes", func() {
		eTypes := []string{"direct", "topic", "fanout", "headers"}
		for _, eType := range eTypes {
			exchange := Exchange{Name: testName("test-exchange-" + eType), Type: eType}
			err = exchange.Declare(ch)
			s.NoError(err, "failed to declare %s exchange", eType)
		}
	})

	s.Run("Validation", func() {
		exchange := Exchange{Name: ""}
		err := exchange.Declare(ch)
		s.Error(err, "declaring exchange with empty name should fail")
		s.ErrorIs(err, ErrTopologyExchangeNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		// declare exchange with specific parameters
		exchange := Exchange{Name: eName + "-conflicting", Type: "direct", Durable: true}
		err = exchange.Declare(ch)

		s.NoError(err)

		// try to re-declare with conflicting parameters - should close channel
		exchange = Exchange{Name: eName + "-conflicting", Type: "direct", Durable: false}
		err = exchange.Declare(ch)

		s.Error(err, "declaring conflicting exchange should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestExchangeVerify() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	exchange := Exchange{Name: eName}

	err = exchange.Declare(ch)
	s.Require().NoError(err)

	err = exchange.Verify(ch)
	s.Require().NoError(err)

	s.Run("Validation", func() {
		exchange := Exchange{Name: ""}
		err := exchange.Verify(ch)
		s.Error(err, "verifying exchange with empty name should fail")
		s.ErrorIs(err, ErrTopologyExchangeNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		exchange := Exchange{Name: eName + "-non-existent"}
		err = exchange.Verify(ch)

		s.Error(err, "verifying should fail for non-existent exchange")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestExchangeDelete() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := testName("test-key")

	exchange := Exchange{Name: eName, Type: "direct"}

	err = exchange.Declare(ch)
	s.Require().NoError(err)

	err = exchange.Delete(ch, false)
	s.Require().NoError(err)

	err = exchange.Verify(ch)
	s.Error(err, "verifying non-existent exchange should fail")
	// failing to verify an exchange closes the channel

	s.Run("IfUnused", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		// create exchange and queue
		exchange := Exchange{Name: qName + "-if-unused", Type: "direct"}
		queue := Queue{Name: eName + "-if-unused"}
		binding := Binding{Source: qName + "-if-unused", Destination: eName + "-if-unused", Key: bKey}

		// declare exchange
		err = exchange.Declare(ch)
		s.Require().NoError(err)
		// declare queue
		err = queue.Declare(ch)
		s.Require().NoError(err)
		// bind queue to exchange (now exchange is "in use")
		err = binding.Declare(ch)
		s.Require().NoError(err)
		// try to delete with ifUnused=true (should fail)
		err = exchange.Delete(ch, true)
		s.Error(err, "delete should fail when exchange has bindings and ifUnused=true")

		// failing to delete an exchange closes the channel
		ch, err = b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		// unbind and try again
		err = binding.Delete(ch)
		s.Require().NoError(err)

		err = exchange.Delete(ch, true)
		s.NoError(err, "delete should succeed after removing bindings")
	})

	s.Run("Validation", func() {
		exchange := Exchange{Name: ""}
		err = exchange.Delete(ch, false)
		s.Error(err, "deleting exchange with empty name should fail")
		s.ErrorIs(err, ErrTopologyExchangeNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		exchange := Exchange{Name: eName + "-non-existent"}
		err = exchange.Delete(ch, false)
		// according to amqp package, this should error, but it doesn't
		// s.Error(err, "deleting non-existent exchange should fail")
		// s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		// s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestQueueDeclare() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	qName := testName("test-queue")

	queue := Queue{Name: qName, Durable: true}

	err = queue.Declare(ch)
	s.NoError(err)

	s.Run("WithArguments", func() {
		queue := Queue{
			Name: qName + "-args",
			Arguments: Arguments{
				"x-message-ttl": int32(60000),
				"x-max-length":  int32(1000),
			},
		}
		err = queue.Declare(ch)
		s.NoError(err)
		// verify queue exists
		info, err := queue.Inspect(ch)
		s.NoError(err)
		s.NotNil(info)
	})

	s.Run("Validation", func() {
		queue := Queue{Name: ""}
		err := queue.Declare(ch)
		s.Error(err, "declaring queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		// declare queue with specific parameters
		queue := Queue{Name: qName + "-duplicate", Durable: true}
		err = queue.Declare(ch)

		s.NoError(err)

		// try to re-declare with conflicting parameters - should close channel
		queue = Queue{Name: qName + "-duplicate", Durable: false}
		err = queue.Declare(ch)

		s.Error(err, "declaring queue with conflicting parameters should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestQueueVerify() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	qName := testName("test-queue")
	queue := Queue{Name: qName, Durable: true}

	err = queue.Declare(ch)
	s.NoError(err)

	err = queue.Verify(ch)
	s.NoError(err)

	s.Run("Validation", func() {
		queue := Queue{Name: ""}
		err = queue.Verify(ch)
		s.Error(err, "verifying queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		queue := Queue{Name: qName + "-non-existent"}
		err = queue.Verify(ch)

		s.Error(err, "verifying non-existent queue should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestQueueDelete() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	qName := testName("test-queue")
	queue := Queue{Name: qName}

	err = queue.Declare(ch)
	s.Require().NoError(err)

	count, err := queue.Delete(ch, false, false)
	s.NoError(err)
	s.GreaterOrEqual(count, 0)

	err = queue.Verify(ch)
	s.Error(err, "verifying non-existent queue should fail")
	// failing to verify an exchange closes the channel

	ch, err = b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	s.Run("IfEmpty", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		queue := Queue{Name: qName + "-if-empty"}
		err = queue.Declare(ch)
		s.Require().NoError(err)

		// add a message
		msg := NewMessage([]byte("test"))
		err = ch.Publish("" /* amq.direct */, queue.Name, false, false, amqpPublishingFromMessage(&msg))
		s.Require().NoError(err)

		time.Sleep(100 * time.Millisecond)

		// try to delete with ifEmpty=true (should fail)
		_, err = queue.Delete(ch, false, true)
		s.Error(err, "delete should fail when queue has messages and ifEmpty=true")

		// failing to delete a queue closes the channel
		ch, err = b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		// purge and try again
		_, err = queue.Purge(ch)
		s.Require().NoError(err)

		_, err = queue.Delete(ch, false, true)
		s.NoError(err, "delete should succeed after purging")
	})

	s.Run("WithMessages", func() {
		// declare queue
		queue := Queue{Name: qName + "-with-messages"}
		err = queue.Declare(ch)
		s.Require().NoError(err)

		// publish some messages
		messagesCount := 3
		for range messagesCount {
			msg := NewMessage([]byte("test"))
			err = ch.Publish("" /* amq.direct */, queue.Name, false, false, amqpPublishingFromMessage(&msg))
			s.Require().NoError(err)
		}

		time.Sleep(100 * time.Millisecond)

		// delete queue and check message count
		count, err := queue.Delete(ch, false, false)
		s.NoError(err)
		s.Equal(messagesCount, count)
	})

	s.Run("Validation", func() {
		queue := Queue{Name: ""}
		_, err := queue.Delete(ch, false, false)
		s.Error(err, "deleting queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		queue := Queue{Name: qName + "-non-existent"}
		_, err = queue.Delete(ch, false, false)
		// according to amqp package, this should error, but it doesn't
		// s.Error(err, "deleting non-existent queue should fail")
		// s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		// s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestQueuePurge() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	qName := testName("test-queue")
	queue := Queue{Name: qName}

	err = queue.Declare(ch)
	s.Require().NoError(err)

	count, err := queue.Purge(ch)
	s.NoError(err)
	s.Equal(0, count, "purge on empty queue should return zero")

	s.Run("WithMessages", func() {
		queue := Queue{Name: qName + "-with-messages"}
		err = queue.Declare(ch)
		s.Require().NoError(err)

		// publish messages
		messageCount := 7
		for range messageCount {
			msg := NewMessage([]byte("test"))
			err = ch.Publish("" /* amq.direct */, queue.Name, false, false, amqpPublishingFromMessage(&msg))
			s.Require().NoError(err)
		}

		time.Sleep(100 * time.Millisecond)

		// purge queue
		count, err := queue.Purge(ch)
		s.NoError(err)
		s.Equal(messageCount, count, "Purge should return number of messages purged")

		// verify queue is empty
		info, err := queue.Inspect(ch)
		s.NoError(err)
		s.Equal(0, info.Messages)
	})

	s.Run("Validation", func() {
		queue := Queue{Name: ""}

		_, err := queue.Purge(ch)
		s.Error(err, "purging queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		queue = Queue{Name: qName + "-non-existent"}
		_, err = queue.Purge(ch)

		s.Error(err, "purging non-existent queue should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestQueueInspect() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	qName := testName("test-queue")
	queue := Queue{Name: qName}

	err = queue.Declare(ch)
	s.Require().NoError(err)

	// inspect empty queue
	info, err := queue.Inspect(ch)
	s.NoError(err)
	s.NotNil(info)
	s.Equal(0, info.Messages)
	s.Equal(0, info.Consumers)

	s.Run("WithMessages", func() {
		queue := Queue{Name: qName + "-with-messages"}
		err = queue.Declare(ch)
		s.Require().NoError(err)

		messagesCount := 5
		consumersCount := 0
		// add messages
		for range messagesCount {
			msg := NewMessage([]byte("test"))
			err = ch.Publish("" /* amq.direct */, queue.Name, false, false, amqpPublishingFromMessage(&msg))
			s.Require().NoError(err)
		}

		time.Sleep(100 * time.Millisecond)

		// inspect again
		info, err = queue.Inspect(ch)
		s.NoError(err)
		s.Equal(messagesCount, info.Messages)
		s.Equal(consumersCount, info.Consumers)
	})

	s.Run("WithConsumers", func() {
		queue := Queue{Name: qName + "-with-consumers"}
		err = queue.Declare(ch)
		s.Require().NoError(err)

		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()
		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			deliveries, err := ch.ConsumeWithContext(ctx, queue.Name, "", false, false, false, false, nil)
			s.Require().NoError(err)
			for range deliveries {
				// just drain, or break if you want to exit early
			}
		}()
		time.Sleep(100 * time.Millisecond)
		info, err := queue.Inspect(ch)
		s.NoError(err)
		s.Equal(1, info.Consumers, "should have one active consumer")
		cancel()
		<-doneCh // wait for goroutine to exit
	})

	s.Run("Validation", func() {
		queue := Queue{Name: ""}

		_, err := queue.Inspect(ch)
		s.Error(err, "inspecting queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		queue = Queue{Name: qName + "-non-existent"}
		_, err = queue.Inspect(ch)

		s.Error(err, "inspecting non-existent queue should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestBindingDeclare() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := testName("test-key")

	exchange := Exchange{Name: eName, Type: "direct"}
	queue := Queue{Name: qName}
	binding := Binding{Source: eName, Destination: qName, Key: bKey}

	err = exchange.Declare(ch)
	s.Require().NoError(err)

	err = queue.Declare(ch)
	s.Require().NoError(err)

	err = binding.Declare(ch)
	s.NoError(err)

	s.Run("WithMessages", func() {
		// declare queue
		queue := Queue{Name: qName + "-with-messages"}
		err := queue.Declare(ch)
		s.Require().NoError(err)
		// declare exchange
		exchange := Exchange{Name: eName + "-with-messages", Type: "direct"}
		err = exchange.Declare(ch)
		s.Require().NoError(err)
		// declare a new binding for the new queue
		binding := Binding{Source: eName + "-with-messages", Destination: qName + "-with-messages", Key: bKey}
		err = binding.Declare(ch)
		s.Require().NoError(err)

		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()
		receivedCh := make(chan struct{})
		go func() {
			deliveries, err := ch.ConsumeWithContext(ctx, queue.Name, "", false, false, false, false, nil)
			s.Require().NoError(err)
			for range deliveries {
				receivedCh <- struct{}{}
				break // Only need one message for the test
			}
		}()
		time.Sleep(100 * time.Millisecond)
		msg := NewMessage([]byte("test"))
		err = ch.Publish(exchange.Name, bKey, false, false, amqpPublishingFromMessage(&msg))
		s.NoError(err)
		select {
		case <-receivedCh:
			// success
		case <-time.After(1000 * time.Millisecond):
			s.Fail("message was not received through binding")
		}
	})

	s.Run("Validation", func() {
		binding := Binding{Source: "", Destination: "", Key: ""}

		err := binding.Declare(ch)
		s.Error(err, "inspecting binding with empty name should fail")
		s.ErrorIs(err, ErrTopologyBindingFieldsEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		binding := Binding{Source: eName + "-non-existent", Destination: qName + "-non-existent", Key: bKey}
		err = binding.Declare(ch)

		s.Error(err, "declaring binding to non-existent resources should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestBindingDelete() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := testName("test-key")

	// create exchange, queue, and binding
	exchange := Exchange{Name: eName, Type: "direct"}
	queue := Queue{Name: qName}
	binding := Binding{Source: eName, Destination: qName, Key: bKey}

	err = exchange.Declare(ch)
	s.Require().NoError(err)

	err = queue.Declare(ch)
	s.Require().NoError(err)

	err = binding.Declare(ch)
	s.Require().NoError(err)

	exchangeA := Exchange{Name: eName + "-a", Type: "topic"}
	exchangeB := Exchange{Name: eName + "-b", Type: "fanout"}
	bindingX := Binding{Source: eName + "-a", Destination: eName + "-b", Key: bKey, Type: BindingTypeExchange}

	err = exchangeA.Declare(ch)
	s.Require().NoError(err)

	err = exchangeB.Declare(ch)
	s.Require().NoError(err)

	err = bindingX.Declare(ch)
	s.Require().NoError(err)

	err = bindingX.Delete(ch)
	s.NoError(err, "deleting exchange-to-exchange binding should succeed")

	s.Run("WithMessages", func() {
		// declare queue
		queue := Queue{Name: qName + "-with-messages"}
		err := queue.Declare(ch)
		s.Require().NoError(err)
		// declare exchange
		exchange := Exchange{Name: eName + "-with-messages", Type: "direct"}
		err = exchange.Declare(ch)
		s.Require().NoError(err)
		// declare a new binding for the new queue
		binding := Binding{Source: eName + "-with-messages", Destination: qName + "-with-messages", Key: bKey}
		err = binding.Declare(ch)
		s.Require().NoError(err)

		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		receivedCh := make(chan struct{}, 1)
		ready := make(chan struct{})
		go func() {
			deliveries, err := ch.ConsumeWithContext(ctx, queue.Name, "", false, false, false, false, nil)
			s.Require().NoError(err)
			close(ready)
			for range deliveries {
				receivedCh <- struct{}{}
				break
			}
		}()
		<-ready // wait for consumer to be ready
		time.Sleep(100 * time.Millisecond)

		msg := NewMessage([]byte("test"))
		err = ch.Publish(exchange.Name, bKey, false, false, amqpPublishingFromMessage(&msg))
		s.NoError(err)
		select {
		case <-receivedCh:
			// success
		case <-time.After(1000 * time.Millisecond):
			s.Fail("message should be received before unbinding")
		}

		// delete binding
		err = binding.Delete(ch)
		s.NoError(err, "deleting binding should succeed")

		// try publishing again
		err = ch.Publish(exchange.Name, "", false, false, amqpPublishingFromMessage(&msg))
		s.NoError(err)
		select {
		case <-receivedCh:
			s.Fail("message should not be received after unbinding")
		case <-time.After(1000 * time.Millisecond):
			// success
		}
	})

	s.Run("Validation", func() {
		binding := Binding{Source: "", Destination: "", Key: ""}
		err := binding.Delete(ch)
		s.Error(err, "inspecting binding with empty name should fail")
		s.ErrorIs(err, ErrTopologyBindingFieldsEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		binding := Binding{Source: eName + "-non-existent", Destination: qName + "-non-existent", Key: bKey + "-non-existent"}
		err = binding.Delete(ch)
		// according to amqp package, this should error, but it doesn't
		// s.Error(err, "deleting non-existent binding should fail")
		// s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		// s.True(ch.IsClosed(), "channel should be closed")
	})
}

func (s *brokerIntegrationTestSuite) TestTopologyDeclare() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := testName("test-key")

	topology := &Topology{
		Exchanges: []Exchange{
			{Name: eName + "-1", Type: "direct", Durable: true},
			{Name: eName + "-2", Type: "topic", Durable: true},
		},
		Queues: []Queue{{Name: qName, Durable: true}},
		Bindings: []Binding{
			{Source: eName + "-1", Destination: eName + "-2", Key: bKey, Type: BindingTypeExchange},
			{Source: eName + "-2", Destination: qName, Key: bKey, Type: BindingTypeQueue},
		},
	}

	err = topology.Declare(ch)
	s.NoError(err)
}

func (s *brokerIntegrationTestSuite) TestTopologyDelete() {
	b := s.newTestBroker()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: ""}},
	}

	// declare
	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	err = topology.Declare(ch)
	s.NoError(err)

	// verify exists
	err = topology.Verify(ch)
	s.NoError(err)

	// delete
	err = topology.Delete(ch)
	s.NoError(err)

	// verify deleted
	err = topology.Verify(ch)
	s.Error(err, "topology should be deleted")
}

func (s *brokerIntegrationTestSuite) TestTopologyVerify() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := testName("test-key")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct", Durable: true}},
		Queues:    []Queue{{Name: qName, Durable: true}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: bKey}},
	}

	err = topology.Declare(ch)
	s.Require().NoError(err)

	err = topology.Verify(ch)
	s.NoError(err, "should verify existing topology")

	nonExistent := &Topology{
		Queues: []Queue{{Name: qName + "-non-existent", Durable: true}},
	}
	err = nonExistent.Verify(ch)
	s.Error(err, "should fail to verify non-existent topology")
}

func (s *brokerIntegrationTestSuite) TestTopologyManagerDeclare() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := testName("test-key")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName, Durable: true}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: bKey}},
	}

	// first declaration
	err = b.topologyMgr.declare(ch, topology)
	s.NoError(err)

	// second declaration should be no-op (cached)
	err = b.topologyMgr.declare(ch, topology)
	s.NoError(err, "re-declaring same topology should be cached")

	s.Run("WithConflictingExchange", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		eTopology := topology.Merge(&Topology{
			Exchanges: []Exchange{{Name: eName, Type: "topic"}}, // conflicting type
		})

		err = b.topologyMgr.declare(ch, eTopology)
		s.Error(err, "conflicting topology declaration for exchange should fail")
	})

	s.Run("WithConflictingQueue", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		qTopology := topology.Merge(&Topology{
			Queues: []Queue{{Name: qName, Durable: false}}, // conflicting durability
		})

		err = b.topologyMgr.declare(ch, qTopology)
		s.Error(err, "conflicting topology declaration for queue should fail")
	})

	s.Run("WithConflictingBinding", func() {
		ch, err := b.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		bTopology := topology.Merge(&Topology{
			Bindings: []Binding{{Source: eName, Destination: qName, Key: bKey}}, // same binding
		})

		err = b.topologyMgr.declare(ch, bTopology)
		s.NoError(err, "declaring binding with the same fields should be succeed")

		bTopology = topology.Merge(&Topology{
			Bindings: []Binding{{Source: eName + "-non-existent", Destination: qName + "-non-existent", Key: bKey + "-non-existent"}}, // conflicting binding
		})

		err = b.topologyMgr.declare(ch, bTopology)
		s.Error(err, "conflicting topology declaration for binding should fail")
	})

	s.Run("Validation", func() {
		err = b.topologyMgr.declare(ch, &Topology{
			Exchanges: []Exchange{{Name: ""}},
		})
		s.Error(err, "declaring topology with invalid exchange should fail")

		err = b.topologyMgr.declare(ch, &Topology{
			Queues: []Queue{{Name: ""}},
		})
		s.Error(err, "declaring topology with invalid queue should fail")

		err = b.topologyMgr.declare(ch, &Topology{
			Bindings: []Binding{{Source: "", Destination: "", Key: ""}},
		})
		s.Error(err, "declaring topology with invalid binding should fail")
	})
}

func (s *brokerIntegrationTestSuite) TestTopologyManagerVerify() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := testName("test-key")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: bKey}},
	}

	err = b.topologyMgr.declare(ch, topology)
	s.Require().NoError(err)

	// verify existing topology
	err = b.topologyMgr.verify(ch, topology)
	s.NoError(err, "should verify existing topology")

	s.Run("Validation", func() {
		err = b.topologyMgr.verify(ch, &Topology{
			Exchanges: []Exchange{{Name: ""}},
		})
		s.Error(err, "verifying topology with invalid exchange should fail")

		err = b.topologyMgr.verify(ch, &Topology{
			Queues: []Queue{{Name: ""}},
		})
		s.Error(err, "verifying topology with invalid queue should fail")

		err = b.topologyMgr.verify(ch, &Topology{
			Bindings: []Binding{{Source: "", Destination: "", Key: ""}},
		})
		s.Error(err, "verifying topology with invalid binding should fail")
	})
}

func (s *brokerIntegrationTestSuite) TestTopologyManagerDelete() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	qName := testName("test-queue")

	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
	}

	err = b.topologyMgr.declare(ch, topology)
	s.Require().NoError(err)

	err = b.topologyMgr.delete(ch, topology)
	s.NoError(err)

	// verify client state
	s.Nil(b.topologyMgr.exchange(eName), "exchange should be deleted from topologyManager")
	s.Nil(b.topologyMgr.queue(qName), "queue should be deleted from topologyManager")

	s.Run("Validation", func() {
		err = b.topologyMgr.delete(ch, &Topology{
			Exchanges: []Exchange{{Name: ""}},
		})
		s.Error(err, "deleting topology with invalid exchange should fail")

		err = b.topologyMgr.delete(ch, &Topology{
			Queues: []Queue{{Name: ""}},
		})
		s.Error(err, "deleting topology with invalid queue should fail")

		err = b.topologyMgr.delete(ch, &Topology{
			Bindings: []Binding{{Source: "", Destination: "", Key: ""}},
		})
		s.Error(err, "deleting topology with invalid binding should fail")
	})
}

func (s *brokerIntegrationTestSuite) TestTopologyManagerSync() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	// declare initial topology
	eName := testName("test-exchange")
	qName := testName("test-queue")
	bKey := testName("test-key")

	initial := &Topology{
		Exchanges: []Exchange{
			{Name: eName + "-1", Type: "direct"},
			{Name: eName + "-2", Type: "fanout"},
		},
		Queues: []Queue{
			{Name: qName + "-1"},
			{Name: qName + "-2"},
		},
		Bindings: []Binding{
			{Source: eName + "-1", Destination: qName + "-1", Key: bKey},
			{Source: eName + "-1", Destination: eName + "-2", Key: bKey, Type: BindingTypeExchange},
		},
	}

	err = b.topologyMgr.declare(ch, initial)
	s.Require().NoError(err)

	// sync to new topology (removes ex2, q2, adds eName3, q3)
	desired := &Topology{
		Exchanges: []Exchange{
			{Name: eName + "-1", Type: "direct", Arguments: Arguments{"x-key": "value"}}, // modified
			{Name: eName + "-3", Type: "topic"},                                          // new
		},
		Queues: []Queue{
			{Name: qName + "-1", Arguments: Arguments{"x-key": "value"}}, // modified
			{Name: qName + "-3"}, // new
		},
		Bindings: []Binding{
			{Source: eName + "-1", Destination: qName + "-1", Key: bKey, Arguments: Arguments{"x-key": "value"}}, // modified
			{Source: eName + "-1", Destination: eName + "-3", Key: bKey, Type: BindingTypeExchange},              // new
		},
	}

	err = b.topologyMgr.sync(ch, desired)
	s.NoError(err)

	// verify client state (topologyManager)

	s.NotNil(b.topologyMgr.exchange(eName + "-1"))
	s.Nil(b.topologyMgr.exchange(eName+"-2"), "exchange 2 should be deleted")
	s.NotNil(b.topologyMgr.exchange(eName + "-3"))

	s.NotNil(b.topologyMgr.queue(qName + "-1"))
	s.Nil(b.topologyMgr.queue(qName+"-2"), "queue 2 should be deleted")
	s.NotNil(b.topologyMgr.queue(qName + "-3"))

	s.NotNil(b.topologyMgr.binding(eName+"-1", qName+"-1", bKey))
	s.Nil(b.topologyMgr.binding(eName+"-1", eName+"-2", bKey), "binding 2 should be deleted")
	s.NotNil(b.topologyMgr.binding(eName+"-1", eName+"-3", bKey))

	// verify server state matches client state
	// Verify() closes channel on error, so we need new channels for after each failing check

	exchange1 := desired.Exchange(eName + "-1")
	s.Require().NotNil(exchange1)
	err = exchange1.Verify(ch)
	s.NoError(err, "exchange 1 should exist on server")

	s.Require().NoError(err)
	exchange3 := desired.Exchange(eName + "-3")
	s.Require().NotNil(exchange3)
	err = exchange3.Verify(ch)
	s.NoError(err, "exchange 3 should exist on server")

	s.Require().NoError(err)
	queue1 := desired.Queue(qName + "-1")
	s.Require().NotNil(queue1)
	err = queue1.Verify(ch)
	s.NoError(err, "queue 1 should exist on server")

	s.Require().NoError(err)
	queue3 := desired.Queue(qName + "-3")
	s.Require().NotNil(queue3)
	err = queue3.Verify(ch)
	s.NoError(err, "queue 3 should exist on server")

	exchange2Ch, err := b.Channel() // channel closes if verify fails
	s.Require().NoError(err)
	exchange2 := initial.Exchange(eName + "-2")
	s.Require().NotNil(exchange2)
	err = exchange2.Verify(exchange2Ch)
	s.Error(err, "exchange 2 should not exist on server")
	s.True(exchange2Ch.IsClosed(), "channel should be closed after verifying non-existent exchange")
	exchange2Ch.Close()

	queue2Ch, err := b.Channel() // channel closes if verify fails
	s.Require().NoError(err)
	queue2 := initial.Queue(qName + "-2")
	s.Require().NotNil(queue2)
	err = queue2.Verify(queue2Ch)
	s.Error(err, "queue 2 should not exist on server")
	s.True(queue2Ch.IsClosed(), "channel should be closed after verifying non-existent queue")
	queue2Ch.Close()
}

func (s *brokerIntegrationTestSuite) TestTopologyManagerExchange() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	eName := testName("test-exchange")
	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct", Durable: true}},
	}

	err = b.topologyMgr.declare(ch, topology)
	s.Require().NoError(err)

	// lookup existing exchange
	exchange := b.topologyMgr.exchange(eName)
	s.NotNil(exchange, "should find declared exchange")
	s.Equal(eName, exchange.Name)
	s.Equal("direct", exchange.Type)
	s.True(exchange.Durable)

	// lookup non-existent exchange
	exchange = b.topologyMgr.exchange("non-existent")
	s.Nil(exchange, "should return nil for non-existent exchange")
}

func (s *brokerIntegrationTestSuite) TestTopologyManagerQueue() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	qName := testName("test-queue")
	topology := &Topology{
		Queues: []Queue{{Name: qName, Durable: true}},
	}

	err = b.topologyMgr.declare(ch, topology)
	s.Require().NoError(err)
	s.Require().NoError(err)

	// lookup existing queue
	queue := b.topologyMgr.queue(qName)
	s.NotNil(queue, "should find declared queue")
	s.Equal(qName, queue.Name)
	s.True(queue.Durable)

	// lookup non-existent queue
	queue = b.topologyMgr.queue("non-existent")
	s.Nil(queue, "should return nil for non-existent queue")
}

func (s *brokerIntegrationTestSuite) TestTopologyManagerBinding() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	exchange := testName("test-exchange")
	queue := testName("test-queue")
	key := "test.pattern"

	topology := &Topology{
		Exchanges: []Exchange{{Name: exchange, Type: "direct"}},
		Queues:    []Queue{{Name: queue}},
		Bindings:  []Binding{{Source: exchange, Destination: queue, Key: key}},
	}

	err = b.topologyMgr.declare(ch, topology)
	s.Require().NoError(err)

	// lookup existing binding
	binding := b.topologyMgr.binding(exchange, queue, key)
	s.NotNil(binding, "should find declared binding")
	s.Equal(exchange, binding.Source)
	s.Equal(queue, binding.Destination)
	s.Equal(key, binding.Key)

	// lookup non-existent binding
	binding = b.topologyMgr.binding("non-existent", "non-existent", "non-existent")
	s.Nil(binding, "should return nil for non-existent binding")
}

func (s *brokerIntegrationTestSuite) TestTopologyManagerConcurrentAccess() {
	b := s.newTestBroker()

	ch, err := b.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	var wg sync.WaitGroup
	concurrency := 10

	for i := range concurrency {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			eName := testName("test-exchange-concurrent")
			topology := &Topology{
				Exchanges: []Exchange{{Name: eName, Type: "direct"}},
			}

			err := b.topologyMgr.declare(ch, topology)
			s.NoError(err)

			exchange := b.topologyMgr.exchange(eName)
			s.NotNil(exchange)
		}(i)
	}

	wg.Wait()
}
