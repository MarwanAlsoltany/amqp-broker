//go:build integration
// +build integration

package endpoint

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/handler"
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

func (s *integrationTestSuite) TestConsumerConsume() {
	s.Run("Basic", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		// publish messages first
		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.publishTestMessages(p, topo.Key, 3)

		// start consumer
		var received [][]byte
		var mu sync.Mutex

		handler := func(_ context.Context, msg *message.Message) (handler.Action, error) {
			mu.Lock()
			received = append(received, msg.Body)
			mu.Unlock()
			return handler.ActionAck, nil
		}

		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{}, topo.Queue, handler)

		s.True(c.Ready(), "consumer should be ready")
		s.NotNil(c.ch, "consumer should have a channel")

		// wait for messages to be consumed
		time.Sleep(300 * time.Millisecond)
		c.Wait()

		mu.Lock()
		s.Require().Len(received, 3, "all 3 messages should be received")
		mu.Unlock()
	})

	s.Run("Roundtrip", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		const n = 10

		// collect received bodies
		received := make([][]byte, 0, n)
		var mu sync.Mutex
		done := make(chan struct{})

		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{PrefetchCount: n}, topo.Queue,
			func(_ context.Context, msg *message.Message) (handler.Action, error) {
				mu.Lock()
				received = append(received, msg.Body)
				if len(received) == n {
					select {
					case <-done:
					default:
						close(done)
					}
				}
				mu.Unlock()
				return handler.ActionAck, nil
			},
		)

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.publishTestMessages(p, topo.Key, n)

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			s.Fail("timeout waiting for all messages to be consumed")
		}

		c.Wait()

		mu.Lock()
		s.Len(received, n)
		mu.Unlock()
	})

	s.Run("AutoAck", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		// publish a message
		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.publishTestMessages(p, topo.Key, 1)

		var received atomic.Int32
		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{AutoAck: true}, topo.Queue,
			func(_ context.Context, _ *message.Message) (handler.Action, error) {
				received.Add(1)
				return handler.ActionNoAction, nil
			},
		)

		time.Sleep(200 * time.Millisecond)
		c.Wait()

		s.Equal(int32(1), received.Load())

		// queue should be empty (auto-acked)
		msg, err := c.Get()
		s.NoError(err)
		s.Nil(msg)
	})

	s.Run("ManualAck", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)

		var received atomic.Int32
		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{}, topo.Queue,
			func(_ context.Context, _ *message.Message) (handler.Action, error) {
				received.Add(1)
				return handler.ActionAck, nil
			},
		)

		s.publishTestMessages(p, topo.Key, 3)
		time.Sleep(200 * time.Millisecond)
		c.Wait()

		s.Equal(int32(3), received.Load())

		// queue should be empty after ack
		msg, err := c.Get()
		s.NoError(err)
		s.Nil(msg)
	})

	s.Run("NackRequeue", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.publishTestMessages(p, topo.Key, 1)

		// first delivery nack+requeue, second delivery ack
		var attempts atomic.Int32
		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{}, topo.Queue,
			func(_ context.Context, _ *message.Message) (handler.Action, error) {
				n := attempts.Add(1)
				if n == 1 {
					return handler.ActionNackRequeue, nil
				}
				return handler.ActionAck, nil
			},
		)

		time.Sleep(500 * time.Millisecond)
		c.Wait()

		s.Equal(int32(2), attempts.Load(), "message should be delivered twice: nacked+requeued then acked")
	})

	s.Run("NackDiscard", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.publishTestMessages(p, topo.Key, 1)

		var received atomic.Int32
		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{}, topo.Queue,
			func(_ context.Context, _ *message.Message) (handler.Action, error) {
				received.Add(1)
				return handler.ActionNackDiscard, nil
			},
		)

		time.Sleep(200 * time.Millisecond)
		c.Wait()

		s.Equal(int32(1), received.Load())

		// queue should be empty (message discarded)
		msg, err := c.Get()
		s.NoError(err)
		s.Nil(msg)
	})

	s.Run("WithHighPrefetch", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		// publish 5 messages before consumer starts
		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.publishTestMessages(p, topo.Key, 5)

		var received atomic.Int32
		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{PrefetchCount: 2}, topo.Queue,
			func(_ context.Context, _ *message.Message) (handler.Action, error) {
				received.Add(1)
				time.Sleep(50 * time.Millisecond) // simulate slow handler
				return handler.ActionAck, nil
			},
		)

		time.Sleep(1 * time.Second)
		c.Wait()

		s.Equal(int32(5), received.Load(), "all 5 messages should be consumed")
	})

	s.Run("WithConcurrentHandlers", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		// publish multiple messages
		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.publishTestMessages(p, topo.Key, 6)

		var received atomic.Int32
		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{
			PrefetchCount:         6,
			MaxConcurrentHandlers: 3,
		}, topo.Queue,
			func(_ context.Context, _ *message.Message) (handler.Action, error) {
				received.Add(1)
				return handler.ActionAck, nil
			},
		)

		time.Sleep(300 * time.Millisecond)
		c.Wait()

		s.Equal(int32(6), received.Load())
	})
}

func (s *integrationTestSuite) TestConsumerGet() {
	connMgr := s.newConnectionManager()
	topoReg := s.newTopologyRegistry()
	topo := s.declareTestTopology(topoReg)

	// start consumer then cancel it to prevent auto-consuming
	c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{AutoAck: true}, topo.Queue,
		func(_ context.Context, _ *message.Message) (handler.Action, error) {
			return handler.ActionNoAction, nil
		},
	)

	err := c.Cancel()
	s.NoError(err)
	time.Sleep(50 * time.Millisecond)

	// publish now, message won't be consumed by the cancelled consumer
	p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
	s.publishTestMessages(p, topo.Key, 1)
	time.Sleep(50 * time.Millisecond)

	// Get should fetch the message
	msg, err := c.Get()
	s.NoError(err)
	s.NotNil(msg)
}

func (s *integrationTestSuite) TestConsumerClose() {
	s.Run("Success", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{}, topo.Queue,
			func(_ context.Context, _ *message.Message) (handler.Action, error) {
				return handler.ActionAck, nil
			},
		)

		s.True(c.Ready())

		err := c.Close()
		s.NoError(err)
		s.True(c.closed.Load())
	})

	s.Run("Idempotency", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		c := s.newTestConsumer(connMgr, topoReg, ConsumerOptions{}, topo.Queue,
			func(_ context.Context, _ *message.Message) (handler.Action, error) {
				return handler.ActionAck, nil
			},
		)

		s.NoError(c.Close())
		s.NoError(c.Close())
	})
}
