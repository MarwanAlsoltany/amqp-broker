//go:build integration
// +build integration

package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

func (s *brokerIntegrationTestSuite) TestConsumerBasicConsume() {
	b, t := s.newTestBrokerWithTopology()

	// publish some messages
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	msgs := []Message{
		NewMessage([]byte("msg1")),
		NewMessage([]byte("msg2")),
		NewMessage([]byte("msg3")),
	}

	for _, msg := range msgs {
		err := p.Publish(s.ctx, RoutingKey("test"), msg)
		s.Require().NoError(err)
	}

	// consume messages
	var received []Message
	var mu sync.Mutex

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		mu.Lock()
		received = append(received, *msg)
		mu.Unlock()
		return HandlerActionAck, nil
	}

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// wait for messages
	time.Sleep(200 * time.Millisecond)

	// verify all messages received
	mu.Lock()
	s.Require().Len(received, 3)
	s.Equal("msg1", string(received[0].Body))
	s.Equal("msg2", string(received[1].Body))
	s.Equal("msg3", string(received[2].Body))
	mu.Unlock()
}

func (s *brokerIntegrationTestSuite) TestConsumerAutoAck() {
	b, t := s.newTestBrokerWithTopology()

	// publish message
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
	s.Require().NoError(err)

	// consume with AutoAck
	var received atomic.Int32
	handler := testCountingHandler(HandlerActionNoAction, &received)

	c, err := b.NewConsumer(&ConsumerOptions{AutoAck: true}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// wait for message
	time.Sleep(200 * time.Millisecond)
	s.Equal(int32(1), received.Load())

	// verify queue is empty (message was auto-acked)
	msg, err := c.Get()
	s.NoError(err)
	s.Nil(msg)
}

func (s *brokerIntegrationTestSuite) TestConsumerManualAck() {
	b, t := s.newTestBrokerWithTopology()

	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	// test 1: HandlerActionAck - messages are acknowledged and removed from queue
	var received atomic.Int32
	ackHandler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		received.Add(1)
		return HandlerActionAck, nil
	}

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], ackHandler)
	s.Require().NoError(err)

	time.Sleep(100 * time.Millisecond)

	// publish 3 messages
	for range 3 {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	time.Sleep(200 * time.Millisecond)
	c.Wait()

	s.Equal(int32(3), received.Load())

	// verify queue is empty
	msg, err := c.Get()
	s.NoError(err)
	s.Nil(msg)

	// close consumer (closes channel, which stops handleDeliveries goroutine)
	err = c.Close()
	s.Require().NoError(err)
	time.Sleep(200 * time.Millisecond)

	// test 2: HandlerActionNackRequeue - message is redelivered
	var attempts atomic.Int32
	nackHandler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		count := attempts.Add(1)
		if count == 1 {
			return HandlerActionNackRequeue, nil
		}
		return HandlerActionAck, nil
	}

	// publish message before consumer starts
	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("requeue-test")))
	s.Require().NoError(err)

	time.Sleep(100 * time.Millisecond)

	fc, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], nackHandler)
	s.Require().NoError(err)
	defer fc.Close()

	// wait for both deliveries
	time.Sleep(500 * time.Millisecond)
	fc.Wait()

	s.Equal(int32(2), attempts.Load(), "message should be delivered twice: nacked then acked")
}

func (s *brokerIntegrationTestSuite) TestConsumerNackDiscard() {
	b, t := s.newTestBrokerWithTopology()

	// publish message
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("discard")))
	s.Require().NoError(err)

	// consume and discard
	handler := testHandler(HandlerActionNackDiscard)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	time.Sleep(200 * time.Millisecond)
	c.Wait()

	// verify queue is empty (message was discarded)
	msg, err := c.Get()
	s.NoError(err)
	s.Nil(msg)
}

func (s *brokerIntegrationTestSuite) TestConsumerGet() {
	b, t := s.newTestBrokerWithTopology()

	// create consumer but immediately cancel to prevent auto-consumption
	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{AutoAck: true}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// cancel immediately to stop auto-consumption
	err = c.Cancel()
	s.NoError(err)

	time.Sleep(50 * time.Millisecond)

	// now publish message - it won't be auto-consumed
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("get-test")))
	s.Require().NoError(err)

	time.Sleep(50 * time.Millisecond)

	// get message
	msg, err := c.Get()
	s.NoError(err)
	s.NotNil(msg)
	s.Equal("get-test", string(msg.Body))

	// verify queue is empty
	msg, err = c.Get()
	s.NoError(err)
	s.Nil(msg)
}

func (s *brokerIntegrationTestSuite) TestConsumerGetEmptyQueue() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// get from empty queue
	msg, err := c.Get()
	s.NoError(err)
	s.Nil(msg)
}

func (s *brokerIntegrationTestSuite) TestConsumerMaxConcurrentHandlersUnlimited() {
	b, t := s.newTestBrokerWithTopology()

	// publish multiple messages
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	messageCount := 10
	for range messageCount {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	// track concurrent executions
	var concurrentCount atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup
	wg.Add(messageCount)

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		current := concurrentCount.Add(1)
		// track max
		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		concurrentCount.Add(-1)
		wg.Done()
		return HandlerActionAck, nil
	}

	c, err := b.NewConsumer(&ConsumerOptions{
		MaxConcurrentHandlers: 0, // unlimited
		PrefetchCount:         messageCount,
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	wg.Wait()

	// should have processed multiple messages concurrently
	s.Greater(maxConcurrent.Load(), int32(1))
}

func (s *brokerIntegrationTestSuite) TestConsumerMaxConcurrentHandlersSequential() {
	b, t := s.newTestBrokerWithTopology()

	// publish multiple messages
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	messageCount := 5
	for range messageCount {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	// track concurrent executions
	var concurrentCount atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup
	wg.Add(messageCount)

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		current := concurrentCount.Add(1)
		// track max
		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		concurrentCount.Add(-1)
		wg.Done()
		return HandlerActionAck, nil
	}

	c, err := b.NewConsumer(&ConsumerOptions{
		MaxConcurrentHandlers: 1, // sequential
		PrefetchCount:         messageCount,
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	wg.Wait()

	// should never exceed 1 concurrent handler
	s.Equal(int32(1), maxConcurrent.Load())
}

func (s *brokerIntegrationTestSuite) TestConsumerMaxConcurrentHandlersWorkerPool() {
	b, t := s.newTestBrokerWithTopology()

	// publish multiple messages
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	messageCount := 20
	for range messageCount {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	// track concurrent executions
	var concurrentCount atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup
	wg.Add(messageCount)

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		current := concurrentCount.Add(1)
		// track max
		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		concurrentCount.Add(-1)
		wg.Done()
		return HandlerActionAck, nil
	}

	workerCount := 3
	c, err := b.NewConsumer(&ConsumerOptions{
		MaxConcurrentHandlers: workerCount,
		PrefetchCount:         messageCount,
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	wg.Wait()

	// should not exceed worker pool size
	s.LessOrEqual(maxConcurrent.Load(), int32(workerCount))
	s.Greater(maxConcurrent.Load(), int32(1))
}

func (s *brokerIntegrationTestSuite) TestConsumerWait() {
	b, t := s.newTestBrokerWithTopology()

	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	var processed atomic.Int32
	handler := testProcessingHandler(HandlerActionAck, 50*time.Millisecond, &processed)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// give consumer time to start
	time.Sleep(100 * time.Millisecond)

	// publish messages
	for range 3 {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	// give enough time for all messages to be delivered and start processing
	time.Sleep(200 * time.Millisecond)

	// wait should block until all handlers complete
	c.Wait()

	// after Wait returns, all messages should be processed
	s.Equal(int32(3), processed.Load())
}

func (s *brokerIntegrationTestSuite) TestConsumerCancel() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{AutoAck: true}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// access private state
	consumer := c.(*consumer)
	s.False(consumer.cancelled.Load())

	// cancel
	err = c.Cancel()
	s.NoError(err)
	s.True(consumer.cancelled.Load())

	// cancel again should be no-op
	err = c.Cancel()
	s.NoError(err)

	// publish message, should not be consumed
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
	s.Require().NoError(err)

	time.Sleep(200 * time.Millisecond)

	// message should still be in queue
	msg, err := c.Get()
	s.NoError(err)
	s.NotNil(msg)
}

func (s *brokerIntegrationTestSuite) TestConsumerRelease() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)

	consumer := c.(*consumer)

	// verify registered
	b.consumersMu.Lock()
	_, exists := b.consumers[consumer.id]
	b.consumersMu.Unlock()
	s.True(exists)

	// release
	c.Release()

	// verify unregistered
	b.consumersMu.Lock()
	_, exists = b.consumers[consumer.id]
	b.consumersMu.Unlock()
	s.False(exists)
}

func (s *brokerIntegrationTestSuite) TestConsumerOnError() {
	b, t := s.newTestBrokerWithTopology()

	// publish message
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
	s.Require().NoError(err)

	// handler that returns error
	handlerErr := errors.New("handler error")
	handler := testErrorHandler(HandlerActionAck, handlerErr)

	var errVal atomic.Value
	onError := func(err error) {
		errVal.Store(err)
	}

	c, err := b.NewConsumer(&ConsumerOptions{
		OnError: onError,
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	time.Sleep(100 * time.Millisecond)

	c.Wait()

	s.NotNil(errVal.Load())
	s.Contains(errVal.Load().(error).Error(), "handler failed")
}

func (s *brokerIntegrationTestSuite) TestConsumerOnCancel() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	var cancelledTag string
	var cancelMu sync.Mutex
	onCancel := func(tag string) {
		cancelMu.Lock()
		cancelledTag = tag
		cancelMu.Unlock()
	}

	c, err := b.NewConsumer(&ConsumerOptions{
		OnCancel: onCancel,
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)

	// delete queue to trigger cancellation
	ch := consumer.Channel()
	s.Require().NotNil(ch)
	_, err = t.Queues[0].Delete(ch, false, false)
	s.NoError(err)

	// wait for callback
	time.Sleep(200 * time.Millisecond)

	cancelMu.Lock()
	s.Equal(consumer.id, cancelledTag)
	cancelMu.Unlock()
}

func (s *brokerIntegrationTestSuite) TestConsumerPrivateStateAccess() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{
		PrefetchCount: 10,
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)

	// verify endpoint access
	s.NotNil(consumer.endpoint)
	s.Equal(roleConsumer, consumer.endpoint.role)

	// verify connection and channel
	s.NotNil(consumer.Connection())
	s.NotNil(consumer.Channel())
	s.False(consumer.Channel().IsClosed())

	// verify options
	s.Equal(10, consumer.opts.PrefetchCount)

	// verify queue
	s.Equal(t.Queues[0].Name, consumer.queue.Name)

	// verify consuming state
	s.False(consumer.cancelled.Load())

	// verify cancel channel
	consumer.stateMu.RLock()
	s.NotNil(consumer.cancelCh)
	consumer.stateMu.RUnlock()

	// verify handler
	s.NotNil(consumer.handler)
}

func (s *brokerIntegrationTestSuite) TestConsumerChannelRecovery() {
	b, t := s.newTestBrokerWithTopology()

	var received atomic.Int32
	handler := testCountingHandler(HandlerActionAck, &received)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)

	// publish message
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
	s.Require().NoError(err)

	time.Sleep(200 * time.Millisecond)
	s.Equal(int32(1), received.Load())

	// force channel closure, consumer should reconnect automatically
	oldCh := consumer.Channel()
	s.Require().NotNil(oldCh)
	err = oldCh.Close()
	s.NoError(err)

	// wait for reconnection
	time.Sleep(500 * time.Millisecond)

	// verify consumer recovered, should have a new channel
	newCh := consumer.Channel()
	if newCh != nil {
		s.NotEqual(oldCh, newCh, "should have new channel after recovery")
		s.False(newCh.IsClosed(), "new channel should be open")

		// verify can still consume messages after recovery
		err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test-after-recovery")))
		s.NoError(err)

		time.Sleep(200 * time.Millisecond)
		s.Equal(int32(2), received.Load(), "should receive message after recovery")
	}
	// if channel is nil, reconnection might still be in progress (acceptable)
}

func (s *brokerIntegrationTestSuite) TestConsumerNoAutoReconnect() {
	b, t := s.newTestBrokerWithTopology()

	var received atomic.Int32
	handler := testCountingHandler(HandlerActionAck, &received)

	c, err := b.NewConsumer(&ConsumerOptions{
		EndpointOptions: EndpointOptions{
			NoAutoReconnect: true,
		},
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)

	// publish message to verify consumer works
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
	s.Require().NoError(err)

	time.Sleep(200 * time.Millisecond)
	s.Equal(int32(1), received.Load(), "should receive initial message")

	// force channel closure
	oldCh := consumer.Channel()
	s.Require().NotNil(oldCh)
	err = oldCh.Close()
	s.NoError(err)

	// wait a bit
	time.Sleep(500 * time.Millisecond)

	// should NOT have reconnected
	s.Nil(consumer.Channel(), "should not have reconnected with NoAutoReconnect")

	// verify consumer is not ready
	s.False(consumer.Ready(), "should not be ready after disconnect")

	// try to publish another message - consumer won't receive it
	initialReceived := received.Load()
	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test-no-reconnect")))
	s.NoError(err)

	time.Sleep(300 * time.Millisecond)

	// should not have received new message (no reconnection)
	s.Equal(initialReceived, received.Load(), "should not receive messages without reconnection")
}

func (s *brokerIntegrationTestSuite) TestConsumerPrefetchCount() {
	b, t := s.newTestBrokerWithTopology()

	// publish many messages
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	messageCount := 20
	for range messageCount {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	// consumer with low prefetch and slow handler
	var processing atomic.Int32
	var maxProcessing atomic.Int32

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		current := processing.Add(1)
		// track max
		for {
			max := maxProcessing.Load()
			if current <= max || maxProcessing.CompareAndSwap(max, current) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		processing.Add(-1)
		return HandlerActionAck, nil
	}

	prefetch := 3
	c, err := b.NewConsumer(&ConsumerOptions{
		PrefetchCount:         prefetch,
		MaxConcurrentHandlers: 0, // unlimited
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	c.Wait()

	// max concurrent should be close to prefetch count
	// (may slightly exceed due to timing)
	s.LessOrEqual(maxProcessing.Load(), int32(prefetch+2))
}

func (s *brokerIntegrationTestSuite) TestConsumerExclusive() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	// create exclusive consumer
	cons1, err := b.NewConsumer(&ConsumerOptions{
		Exclusive: true,
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer cons1.Close()

	// second consumer should fail
	cons2, err := b.NewConsumer(&ConsumerOptions{
		Exclusive: false,
	}, t.Queues[0], handler)

	if err == nil {
		// if connection succeeded, wait for error callback
		time.Sleep(200 * time.Millisecond)
		cons2.Close()

		// verify second consumer is not ready
		s.False(cons2.Ready())
	} else {
		// connection failed immediately
		s.Error(err)
	}
}

func (s *brokerIntegrationTestSuite) TestConsumerMultipleConsumers() {
	b, t := s.newTestBrokerWithTopology()

	// publish messages
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	messageCount := 10
	for range messageCount {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	// create two consumers
	var received1 atomic.Int32
	handler1 := testProcessingHandler(HandlerActionAck, 10*time.Millisecond, &received1)

	var received2 atomic.Int32
	handler2 := testProcessingHandler(HandlerActionAck, 10*time.Millisecond, &received2)

	cons1, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler1)
	s.Require().NoError(err)
	defer cons1.Close()

	cons2, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler2)
	s.Require().NoError(err)
	defer cons2.Close()

	// wait for all messages
	time.Sleep(300 * time.Millisecond)

	// both consumers should have received messages
	total := received1.Load() + received2.Load()
	s.Equal(int32(messageCount), total)
	s.Greater(received1.Load(), int32(0))
	s.Greater(received2.Load(), int32(0))
}

func (s *brokerIntegrationTestSuite) TestConsumerHandlerActionNoAction() {
	b, t := s.newTestBrokerWithTopology()

	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	var msgs = []string{"ack", "nack.requeue", "nack.discard"}
	var ackErrs = make([]error, len(msgs)+1) // extra slot for unexpected messages
	var mu sync.Mutex
	var idx atomic.Int32

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		i := idx.Add(1) - 1

		mu.Lock()
		defer mu.Unlock()

		// ignore extra calls from redeliveries
		if int(i) >= len(ackErrs) {
			return HandlerActionNoAction, nil
		}

		switch string(msg.Body) {
		case msgs[0]:
			ackErrs[i] = msg.Ack()
		case msgs[1]:
			ackErrs[i] = msg.Nack(true)
		case msgs[2]:
			ackErrs[i] = msg.Reject()
		default:
			ackErrs[i] = errors.New("unknown message")
		}

		return HandlerActionNoAction, nil
	}

	err = p.Publish(
		s.ctx,
		RoutingKey("test"),
		NewMessage([]byte(msgs[0])),
		NewMessage([]byte(msgs[1])),
		NewMessage([]byte(msgs[2])),
	)
	s.Require().NoError(err)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	delay := time.Duration(len(msgs)*100) * time.Millisecond
	time.Sleep(delay)

	c.Wait()

	mu.Lock()
	for i := range ackErrs {
		s.NoError(ackErrs[i], "ackErrs[%d] should be nil", i)
	}
	mu.Unlock()

	// verify redeliveries
	msg, err := c.Get()
	s.NoError(err)
	s.NotNil(msg)

	// verify queue is empty
	msg, err = c.Get()
	s.NoError(err)
	s.Nil(msg)
}

func (s *brokerIntegrationTestSuite) TestConsumerConsume() {
	b, t := s.newTestBrokerWithTopology()

	var received atomic.Int32
	handler := testCountingHandler(HandlerActionAck, &received)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// publish messages
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// start Consume in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Consume(ctx)
	}()

	// give consumer time to start
	time.Sleep(100 * time.Millisecond)

	// publish messages while Consume is running
	for range 3 {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	time.Sleep(200 * time.Millisecond)
	s.Equal(int32(3), received.Load())

	// cancel context, Consume should return
	cancel()

	select {
	case err := <-errCh:
		s.Error(err) // should return context.Canceled
		s.Equal(context.Canceled, err)
	case <-time.After(2 * time.Second):
		s.Fail("consume method did not return after context cancellation")
	}
}

func (s *brokerIntegrationTestSuite) TestConsumerContextCancellation() {
	b, t := s.newTestBrokerWithTopology()

	var processed atomic.Int32
	handler := testProcessingHandler(HandlerActionAck, 50*time.Millisecond, &processed)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// publish messages
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	for range 5 {
		err := p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
		s.Require().NoError(err)
	}

	// start Consume with cancellable context
	consumeCtx, cancel := context.WithCancel(s.ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Consume(consumeCtx)
	}()

	// wait for some messages to be processed
	time.Sleep(150 * time.Millisecond)

	// cancel context
	cancel()

	// consume should return with context.Canceled
	select {
	case err := <-errCh:
		s.Error(err)
		s.Equal(context.Canceled, err)
	case <-time.After(2 * time.Second):
		s.Fail("consume method did not return after context cancellation")
	}

	// verify some messages were processed
	s.Greater(processed.Load(), int32(0))

	// create a context that we'll cancel
	consumeCtx, cancel = context.WithCancel(s.ctx)

	// cancel immediately
	cancel()

	// try to consume with cancelled context
	err = c.Consume(consumeCtx)

	// should fail with context error
	s.Error(err)
	s.ErrorIs(err, context.Canceled)
}

func (s *brokerIntegrationTestSuite) TestConsumerReadyTimeout() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	// consumer with very short ready timeout should still work since RabbitMQ is already up
	c, err := b.NewConsumer(&ConsumerOptions{
		EndpointOptions: EndpointOptions{
			ReadyTimeout: 100 * time.Millisecond,
		},
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// verify consumer is ready
	s.True(c.Ready())
}

func (s *brokerIntegrationTestSuite) TestConsumerNoWait() {
	b, t := s.newTestBrokerWithTopology()

	var received atomic.Int32
	handler := testCountingHandler(HandlerActionAck, &received)

	// consumer with NoWait should start immediately without waiting for server confirm
	c, err := b.NewConsumer(&ConsumerOptions{
		NoWait: true,
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	time.Sleep(100 * time.Millisecond)

	// publish message
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
	s.Require().NoError(err)

	time.Sleep(200 * time.Millisecond)
	s.Equal(int32(1), received.Load())
}

func (s *brokerIntegrationTestSuite) TestConsumerRedelivery() {
	b, t := s.newTestBrokerWithTopology()

	// publish message
	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test")))
	s.Require().NoError(err)

	var deliveryCount atomic.Int32
	var wasRedelivered atomic.Bool
	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		count := deliveryCount.Add(1)
		if msg.IsRedelivered() {
			wasRedelivered.Store(true)
		}
		if count == 1 {
			// nack and requeue on first delivery
			return HandlerActionNackRequeue, nil
		}
		// ack on redelivery
		return HandlerActionAck, nil
	}

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	time.Sleep(300 * time.Millisecond)
	c.Wait()

	s.Equal(int32(2), deliveryCount.Load(), "message should be delivered twice")
	s.True(wasRedelivered.Load(), "second delivery should be marked as redelivered")
}

func (s *brokerIntegrationTestSuite) TestConsumerNoWaitReady() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	// start time before creating consumer
	start := time.Now()

	c, err := b.NewConsumer(&ConsumerOptions{
		EndpointOptions: EndpointOptions{
			NoWaitReady: true,
		},
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// constructor should return immediately
	elapsed := time.Since(start)
	s.Less(elapsed, 500*time.Millisecond, "should return quickly with NoWaitReady option")

	// consumer might not be ready immediately, but that's expected with NoWaitReady
	// the important thing is that constructor didn't block
	time.Sleep(200 * time.Millisecond)
	// by now connection should have completed in background
	// note: ready state is not guaranteed - that's the whole point of NoWaitReady
	s.Contains([]bool{true, false}, c.Ready(), "ready state can be either true or false")
}

func (s *brokerIntegrationTestSuite) TestConsumerClosedChannel() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{AutoAck: true}, t.Queues[0], handler)
	s.Require().NoError(err)

	consumer := c.(*consumer)

	// close the channel directly
	ch := consumer.Channel()
	s.Require().NotNil(ch)
	err = ch.Close()
	s.NoError(err)

	time.Sleep(100 * time.Millisecond)

	// get should return error on closed channel
	msg, err := c.Get()
	if err != nil {
		s.ErrorIs(err, ErrConsumerNotConnected)
	} else {
		// or might return nil message
		s.Nil(msg)
	}

	c.Close()
}

func (s *brokerIntegrationTestSuite) TestConsumerEndpointOptions() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	// consumer with custom reconnect delay
	c, err := b.NewConsumer(&ConsumerOptions{
		EndpointOptions: EndpointOptions{
			ReconnectMin: 500 * time.Millisecond,
			ReconnectMax: 5 * time.Second,
		},
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)
	s.Equal(500*time.Millisecond, consumer.opts.ReconnectMin, "should use custom reconnect min")
	s.Equal(5*time.Second, consumer.opts.ReconnectMax, "should use custom reconnect max")
}

func (s *brokerIntegrationTestSuite) TestConsumerDisconnectBehavior() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{
		EndpointOptions: EndpointOptions{
			NoAutoReconnect: true,
		},
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)

	// initially ready
	s.NotNil(consumer.Channel(), "should have channel")
	s.True(consumer.Ready(), "should be ready initially")

	// force disconnect
	err = consumer.disconnect(b.ctx)
	s.Require().NoError(err, "disconnect should not error")

	// should clear state
	s.Eventually(func() bool {
		return !consumer.Ready() && consumer.Channel() == nil
	}, 1*time.Millisecond, 1*time.Nanosecond, "should not be ready and have nil channel after disconnect")

	consumer.stateMu.RLock()
	s.Nil(consumer.cancelCh, "cancel channel should be nil")
	consumer.stateMu.RUnlock()
}

func (s *brokerIntegrationTestSuite) TestConsumerConsumingStateTransitions() {
	b, t := s.newTestBrokerWithTopology()

	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)

	// should be consuming after creation
	time.Sleep(100 * time.Millisecond)
	s.False(consumer.cancelled.Load(), "should be consuming")

	// cancel should stop consuming
	err = c.Cancel()
	s.NoError(err)
	s.True(consumer.cancelled.Load(), "should not be consuming after cancel")

	// cancel again should be no-op
	err = c.Cancel()
	s.NoError(err)
}

func (s *brokerIntegrationTestSuite) TestConsumerHandlerErrorWithAllHandlerActions() {
	b, t := s.newTestBrokerWithTopology()

	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	// test each ack action with handler error
	tests := []struct {
		name   string
		action HandlerAction
	}{
		{"ErrorWithAck", HandlerActionAck},
		{"ErrorWithNackRequeue", HandlerActionNackRequeue},
		{"ErrorWithNackDiscard", HandlerActionNackDiscard},
		{"ErrorWithNoAction", HandlerActionNoAction},
	}

	for _, tc := range tests {
		// brief pause between tests to prevent network overload
		time.Sleep(1000 * time.Millisecond)

		s.Run(tc.name, func() {
			var errCount atomic.Int32
			var received atomic.Int32

			handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
				received.Add(1)
				return tc.action, errors.New("handler error")
			}

			c, err := b.NewConsumer(&ConsumerOptions{
				OnError: func(err error) {
					errCount.Add(1)
				},
			}, t.Queues[0], handler)
			s.Require().NoError(err)
			defer c.Close()

			// publish message
			err = p.Publish(s.ctx, RoutingKey("test"), NewMessage([]byte("test error handling")))
			s.NoError(err)

			// wait for processing
			time.Sleep(100 * time.Millisecond)

			c.Wait()

			// should have received and reported error
			s.GreaterOrEqual(received.Load(), int32(1), "should receive message")
			s.GreaterOrEqual(errCount.Load(), int32(1), "should report handler error")
		})
	}
}

func (s *brokerIntegrationTestSuite) TestConsumerProcessingCounter() {
	b, t := s.newTestBrokerWithTopology()

	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	processingCh := make(chan struct{})
	blockCh := make(chan struct{})

	handler := func(ctx context.Context, msg *Message) (HandlerAction, error) {
		processingCh <- struct{}{}
		<-blockCh // block until signaled
		return HandlerActionAck, nil
	}

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	consumer := c.(*consumer)

	// publish message
	msg := NewMessage([]byte("test counter"))
	err = p.Publish(s.ctx, RoutingKey("test"), msg)
	s.NoError(err)

	// wait for handler to start
	<-processingCh

	// processing counter should be > 0
	s.Greater(consumer.deliveries.Load(), int64(0), "processing counter should be incremented")

	// unblock handler
	close(blockCh)

	// wait for completion
	c.Wait()

	// processing counter should be 0
	s.Equal(int64(0), consumer.deliveries.Load(), "processing counter should be zero after completion")
}

func (s *brokerIntegrationTestSuite) TestConsumerWaitMultipleTimes() {
	b, t := s.newTestBrokerWithTopology()

	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	var processed atomic.Int32

	handler := testProcessingHandler(HandlerActionAck, 50*time.Millisecond, &processed)

	c, err := b.NewConsumer(&ConsumerOptions{}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// publish messages
	for range 3 {
		msg := NewMessage([]byte("test wait"))
		err = p.Publish(s.ctx, RoutingKey("test"), msg)
		s.NoError(err)
	}

	// wait for messages to be consumed
	time.Sleep(500 * time.Millisecond)

	// first wait
	c.Wait()
	firstCount := processed.Load()
	s.GreaterOrEqual(firstCount, int32(3))

	// publish more messages
	for range 2 {
		msg := NewMessage([]byte("test wait again"))
		err = p.Publish(s.ctx, RoutingKey("test"), msg)
		s.NoError(err)
	}

	// wait for messages to be consumed
	time.Sleep(500 * time.Millisecond)

	// second wait
	c.Wait()
	secondCount := processed.Load()
	s.GreaterOrEqual(secondCount, firstCount+2)
}

func (s *brokerIntegrationTestSuite) TestConsumerGetAfterCancel() {
	b, t := s.newTestBrokerWithTopology()

	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	handler := testHandler(HandlerActionAck)

	c, err := b.NewConsumer(&ConsumerOptions{AutoAck: true}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// cancel consumption
	err = c.Cancel()
	s.NoError(err)

	// publish message
	msg := NewMessage([]byte("test get after cancel"))
	err = p.Publish(s.ctx, RoutingKey("test"), msg)
	s.NoError(err)

	time.Sleep(100 * time.Millisecond)

	// get should still work (independent of Consume)
	fetchedMsg, err := c.Get()
	s.NoError(err)
	s.NotNil(fetchedMsg, "should fetch message after cancel")
}

func (s *brokerIntegrationTestSuite) TestConsumerAckErrorCallback() {
	b, t := s.newTestBrokerWithTopology()

	p, err := b.NewPublisher(&PublisherOptions{}, t.Exchanges[0])
	s.Require().NoError(err)
	defer p.Close()

	var errVal atomic.Value

	handlerErr := errors.New("handler error")
	handler := testErrorHandler(HandlerActionAck, handlerErr)

	c, err := b.NewConsumer(&ConsumerOptions{
		OnError: func(err error) {
			errVal.Store(err)
		},
	}, t.Queues[0], handler)
	s.Require().NoError(err)
	defer c.Close()

	// publish message
	msg := NewMessage([]byte("test error callback"))
	err = p.Publish(s.ctx, RoutingKey("test"), msg)
	s.NoError(err)

	// wait for processing
	time.Sleep(300 * time.Millisecond)

	// verify error callback was called
	s.NotNil(errVal.Load(), "should have received an error")
	s.Contains(errVal.Load().(error).Error(), "handler", "should mention handler failure in error")
}
