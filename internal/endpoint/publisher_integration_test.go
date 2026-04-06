//go:build integration
// +build integration

package endpoint

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

func (s *integrationTestSuite) TestPublisherPublish() {
	s.Run("Basic", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)

		s.NotNil(p.conn, "publisher should have a connection")
		s.NotNil(p.ch, "publisher should have a channel")
		s.True(p.flow.Load(), "flow should be active by default")
		s.True(p.Ready(), "publisher should be ready")

		msg := message.New([]byte("test message"))
		err := p.Publish(context.Background(), topo.Key, msg)
		s.NoError(err)
	})

	s.Run("WithMultipleMessages", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.publishTestMessages(p, topo.Key, 3)
	})

	s.Run("WithConfirmMode", func() {
		s.Run("Batch", func() {
			connMgr := s.newConnectionManager()
			topoReg := s.newTopologyRegistry()
			topo := s.declareTestTopology(topoReg)

			p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{
				ConfirmMode:    true,
				ConfirmTimeout: 5 * time.Second,
			}, topo.Exchange)

			s.NotNil(p.confirmCh, "confirm channel should be set when ConfirmMode is enabled")
			s.publishTestMessages(p, topo.Key, 2)
		})

		s.Run("DeferredWithContext", func() {
			connMgr := s.newConnectionManager()
			topoReg := s.newTopologyRegistry()
			topo := s.declareTestTopology(topoReg)

			var confirmed atomic.Int32
			var wg sync.WaitGroup

			p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{
				ConfirmMode: true,
				OnConfirm: func(tag uint64, wait func(context.Context) bool) {
					wg.Add(1)
					go func() {
						defer wg.Done()
						ctx, cancel := context.WithTimeout(s.CTX, 10*time.Second)
						defer cancel()
						if wait(ctx) {
							confirmed.Add(1)
						}
					}()
				},
			}, topo.Exchange)

			const n = 3

			for i := range n {
				msg := message.New([]byte(fmt.Sprintf("deferred-%d", i)))
				s.Require().NoError(p.Publish(s.CTX, topo.Key, msg))
			}

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()

			select {
			case <-done:
				s.Equal(int32(n), confirmed.Load(), "all messages should be confirmed")
			case <-time.After(15 * time.Second):
				s.Fail("timeout waiting for deferred confirmations")
			}
		})

		s.Run("DeferredWithNoContext", func() {
			// exercises the conf.Wait() branch (nil ctx) inside the waitFn closure
			connMgr := s.newConnectionManager()
			topoReg := s.newTopologyRegistry()
			topo := s.declareTestTopology(topoReg)

			var wg sync.WaitGroup

			p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{
				ConfirmMode: true,
				OnConfirm: func(_ uint64, waitFn func(context.Context) bool) {
					wg.Go(func() {
						// pass nil to exercise the conf.Wait() branch
						waitFn(nil)
					})
				},
			}, topo.Exchange)

			msg := message.New([]byte("nil-ctx-confirm"))
			s.Require().NoError(p.Publish(s.CTX, topo.Key, msg))

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				s.Fail("timeout waiting for nil-ctx deferred confirmation")
			}
		})
	})

	s.Run("WithMandatoryReturn", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()

		// declare exchange only, no queue/binding so message is unroutable
		eName := s.testUniqueName("ep-exchange-mandatory")
		conn, err := transport.DefaultDialer(s.URL, nil)
		s.Require().NoError(err)
		ch, err := conn.Channel()
		s.Require().NoError(err)
		topo := &topology.Topology{
			Exchanges: []topology.Exchange{topology.NewExchange(eName)},
		}
		s.Require().NoError(topoReg.Declare(ch, topo))
		_ = ch.Close()
		_ = conn.Close()

		var returnedMsg *message.Message
		var once sync.Once
		returnedCh := make(chan struct{})

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{
			Mandatory: true,
			OnReturn: func(msg message.Message) {
				once.Do(func() {
					returnedMsg = &msg
					close(returnedCh)
				})
			},
		}, topology.NewExchange(eName))

		s.NotNil(p.returnCh, "return channel should be set")

		msg := message.New([]byte("unroutable"))
		s.NoError(p.Publish(s.CTX, "no-such-queue", msg))

		select {
		case <-returnedCh:
			s.NotNil(returnedMsg)
			s.True(returnedMsg.IsReturned(), "message should be marked as returned")
			s.Equal([]byte("unroutable"), returnedMsg.Body)
		case <-time.After(5 * time.Second):
			s.Fail("timeout waiting for returned message")
		}
	})

	s.Run("WithNoAutoDeclare", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()

		// use a non-existent exchange, with NoAutoDeclare=true, no declaration is attempted
		// so conn/channel are established without error
		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{
			EndpointOptions: EndpointOptions{NoAutoDeclare: true},
		}, topology.NewExchange("amq.direct" /* built-in, always exists */))

		s.True(p.Ready())
		s.NotNil(p.ch)
	})
}

func (s *integrationTestSuite) TestPublisherClose() {
	s.Run("Success", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)
		s.True(p.Ready())

		err := p.Close()
		s.NoError(err)
		s.True(p.closed.Load())
	})

	s.Run("Idempotency", func() {
		connMgr := s.newConnectionManager()
		topoReg := s.newTopologyRegistry()
		topo := s.declareTestTopology(topoReg)

		p := s.newTestPublisher(connMgr, topoReg, PublisherOptions{}, topo.Exchange)

		s.NoError(p.Close())
		s.NoError(p.Close())
	})
}
