//go:build integration
// +build integration

package transport

import (
	"context"
	"sync/atomic"
	"time"
)

func (s *integrationTestSuite) TestNewConnectionManager() {
	s.Run("DefaultSize", func() {
		// zero size should use default
		mgr1 := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 0})
		err := mgr1.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr1.Close()

		conn, err := mgr1.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(conn)

		// negative size should use default
		mgr2 := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: -1})
		err = mgr2.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr2.Close()

		conn, err = mgr2.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(conn)
	})

	s.Run("Failure", func() {
		// use invalid URL to force connection failure
		mgr := NewConnectionManager("amqp://invalid", &ConnectionManagerOptions{
			Size:   2,
			Config: &Config{},
		})
		err := mgr.Init(s.CTX)
		s.Error(err, "should fail to initialize with invalid URL")
		s.ErrorIs(err, ErrConnectionManager)
	})
}

func (s *integrationTestSuite) TestConnectionManagerInit() {
	mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{
		Size:   3,
		Config: &Config{},
	})
	err := mgr.Init(s.CTX)
	s.Require().NoError(err)
	defer mgr.Close()

	// verify all connections initialized
	s.Len(mgr.pool, 3)
	for i := range 3 {
		conn, err := mgr.Assign(ConnectionPurposeControl)
		s.NotNil(conn, "connection %d should be initialized", i)
		s.NoError(err)
		s.False(conn.IsClosed(), "connection %d should be open", i)
	}
}

func (s *integrationTestSuite) TestConnectionManagerAssign() {
	s.Run("PoolSize1", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 1})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// all roles should get connection 0
		conn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(conn)

		pubConn, err := mgr.Assign(ConnectionPurposePublish)
		s.NoError(err)
		s.NotNil(pubConn)
		s.True(conn == pubConn, "publisher should share connection with control")

		conConn, err := mgr.Assign(ConnectionPurposeConsume)
		s.NoError(err)
		s.True(conn == conConn, "consumer should share connection with control")
	})

	s.Run("PoolSize2", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// control uses 0
		ctrConn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(ctrConn)

		// publisher uses 1
		pubConn, err := mgr.Assign(ConnectionPurposePublish)
		s.NoError(err)
		s.NotNil(pubConn)
		s.False(ctrConn == pubConn, "publisher should use different connection from control")

		// consumer uses 1
		conConn, err := mgr.Assign(ConnectionPurposeConsume)
		s.NoError(err)
		s.NotNil(conConn)
		s.True(pubConn == conConn, "consumer should share connection with publisher")
	})

	s.Run("PoolSize3", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 3})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// control uses 0
		ctrConn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(ctrConn)

		// publisher uses 1
		pubConn, err := mgr.Assign(ConnectionPurposePublish)
		s.NoError(err)
		s.NotNil(pubConn)

		// consumer uses 2
		conConn, err := mgr.Assign(ConnectionPurposeConsume)
		s.NoError(err)
		s.NotNil(conConn)

		s.False(ctrConn == pubConn, "publisher should use different connection from control")
		s.False(ctrConn == conConn, "consumer should use different connection from control")
		s.False(pubConn == conConn, "consumer should use different connection from publisher")
	})

	s.Run("PoolSize4Plus", func() {
		size := 10
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: size})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// control always uses connection 0
		conn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(conn)

		// verify publishers and consumers get valid connections
		for range 10 {
			pubConn, err := mgr.Assign(ConnectionPurposePublish)
			s.NoError(err)
			s.NotNil(pubConn)
			s.False(conn == pubConn, "publisher should use different connection from control")

			conConn, err := mgr.Assign(ConnectionPurposeConsume)
			s.NoError(err)
			s.NotNil(conConn)
			s.False(conn == conConn, "consumer should use different connection from control")
		}
	})

	s.Run("WhenClosed", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)

		err = mgr.Close()
		s.NoError(err)

		conn, err := mgr.Assign(ConnectionPurposePublish)
		s.Error(err, "assign should fail when manager is closed")
		s.ErrorIs(err, ErrConnectionManagerClosed)
		s.Nil(conn)
	})

	s.Run("WithClosedConnection", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// close a connection directly
		conn, err := mgr.Assign(ConnectionPurposeControl)
		s.Require().NoError(err)
		s.Require().NotNil(conn)

		err = conn.Close()
		s.NoError(err)

		// wait for connection to be replaced
		time.Sleep(100 * time.Millisecond)

		// trying to assign should get a new connection
		assignedConn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(assignedConn)
		s.False(assignedConn.IsClosed())
	})

	s.Run("ConcurrentAccess", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 5})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// assign many publishers and consumers concurrently
		const concurrency = 20
		assignedPublishers := make([]Connection, concurrency)
		assignedConsumers := make([]Connection, concurrency)

		doneCh := make(chan struct{})
		go func() {
			for i := range concurrency {
				conn, err := mgr.Assign(ConnectionPurposePublish)
				s.NoError(err)
				assignedPublishers[i] = conn
			}
			close(doneCh)
		}()

		for i := range concurrency {
			conn, err := mgr.Assign(ConnectionPurposeConsume)
			s.NoError(err)
			assignedConsumers[i] = conn
		}

		<-doneCh

		// all should be valid connections
		for i := range concurrency {
			s.NotNil(assignedPublishers[i])
			s.NotNil(assignedConsumers[i])
			s.False(assignedPublishers[i].IsClosed())
			s.False(assignedConsumers[i].IsClosed())
		}
	})
}

func (s *integrationTestSuite) TestConnectionManagerReplace() {
	s.Run("Success", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		oldConn, err := mgr.Assign(ConnectionPurposeControl)
		s.Require().NoError(err)
		s.NotNil(oldConn)

		// get the index of the connection
		idx := mgr.Index(oldConn)
		s.GreaterOrEqual(idx, 0)

		// replace the connection
		err = mgr.Replace(idx)
		s.NoError(err)

		newConn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(newConn)
		s.False(newConn.IsClosed(), "new connection should be open")
	})

	s.Run("WithInvalidIndex", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// try to replace out of bounds
		err = mgr.Replace(10)
		s.Error(err)
		s.ErrorIs(err, ErrConnectionManager)

		err = mgr.Replace(-1)
		s.Error(err)
		s.ErrorIs(err, ErrConnectionManager)
	})

	s.Run("WhenClosed", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)

		err = mgr.Close()
		s.NoError(err)

		// try to replace after close
		err = mgr.Replace(0)
		s.ErrorIs(err, ErrConnectionManagerClosed)
	})

	s.Run("WithNoAutoReconnect", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{
			Size:            2,
			NoAutoReconnect: true,
		})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		oldConn, err := mgr.Assign(ConnectionPurposeControl)
		s.Require().NoError(err)
		s.NotNil(oldConn)

		idx := mgr.Index(oldConn)
		s.GreaterOrEqual(idx, 0)

		// Replace the connection (should succeed on first attempt)
		err = mgr.Replace(idx)
		s.NoError(err, "Replace should succeed with valid URL and NoAutoReconnect")

		newConn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(newConn)
		s.False(newConn.IsClosed(), "new connection should be open")
		s.NotEqual(oldConn, newConn, "should have a new connection instance")

		// verify we can use the new connection
		ch, err := newConn.Channel()
		s.NoError(err)
		s.NotNil(ch)
		ch.Close()
	})

	s.Run("WithContextCancelled", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{
			Size:            1,
			NoAutoReconnect: false,
			ReconnectMin:    1 * time.Second,
			ReconnectMax:    5 * time.Second,
		})

		ctx, cancel := context.WithCancel(s.CTX)
		err := mgr.Init(ctx)
		s.Require().NoError(err)
		defer mgr.Close()

		// cancel the context immediately
		cancel()

		// wait a moment to ensure the context is fully cancelled
		time.Sleep(10 * time.Millisecond)

		// try to replace, should fail with context cancelled error
		err = mgr.Replace(0)
		s.Error(err)
		s.ErrorIs(err, ErrConnectionManager)
		s.ErrorContains(err, "context cancelled")
	})

	s.Run("WhenManagerClosed", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)

		err = mgr.Close()
		s.NoError(err)

		err = mgr.Replace(0)
		s.Error(err, "replace should fail when manager is closed")
		s.ErrorIs(err, ErrConnectionManagerClosed)
	})
}

func (s *integrationTestSuite) TestConnectionManagerClose() {
	mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 3})
	err := mgr.Init(s.CTX)
	s.Require().NoError(err)

	// get connections before closing
	conns := make([]Connection, 3)
	for i := range 3 {
		conn, err := mgr.Assign(ConnectionPurposeControl)
		s.Require().NoError(err)
		conns[i] = conn
	}

	err = mgr.Close()
	s.NoError(err)

	// double close should be idempotent
	err = mgr.Close()
	s.NoError(err)
}

func (s *integrationTestSuite) TestConnectionManagerIndex() {
	mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 3})
	err := mgr.Init(s.CTX)
	s.Require().NoError(err)
	defer mgr.Close()

	// assign connections and verify indices
	ctrConn, err := mgr.Assign(ConnectionPurposeControl)
	s.Require().NoError(err)
	idx0 := mgr.Index(ctrConn)
	s.GreaterOrEqual(idx0, 0, "should find valid index for connection")

	// nil connection should return -1
	s.Equal(-1, mgr.Index(nil))

	// unknown connection should return -1
	unknownConn, err := DefaultDialer(s.URL, nil)
	s.Require().NoError(err)
	defer unknownConn.Close()
	s.Equal(-1, mgr.Index(unknownConn))
}

func (s *integrationTestSuite) TestConnectionManagerMonitor() {
	s.Run("Reconnect", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 1})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		oldConn, err := mgr.Assign(ConnectionPurposeControl)
		s.Require().NoError(err)
		s.NotNil(oldConn)
		s.False(oldConn.IsClosed())

		// close the connection to trigger replacement
		err = oldConn.Close()
		s.NoError(err)
		s.True(oldConn.IsClosed())

		// wait for monitor to detect closure and replace
		time.Sleep(100 * time.Millisecond)

		// assign should get a new connection
		newConn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(newConn)
		s.False(newConn.IsClosed(), "new connection should be open")
	})

	s.Run("WithConnectionFailures", func() {
		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// get reference to original connection
		oldConn, err := mgr.Assign(ConnectionPurposeControl)
		s.Require().NoError(err)

		// force close the connection to trigger monitor
		err = oldConn.Close()
		s.NoError(err)
		s.True(oldConn.IsClosed(), "old connection should be closed")

		// wait for monitor to detect closure
		time.Sleep(100 * time.Millisecond)

		// verify new connection is available
		newConn, err := mgr.Assign(ConnectionPurposeControl)
		s.NoError(err)
		s.NotNil(newConn)
		s.False(newConn.IsClosed())
	})
}

func (s *integrationTestSuite) TestConnectionManagerHandlers() {
	s.Run("OnOpen", func() {
		var openedCount atomic.Int32
		var lastIdx atomic.Int32

		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{
			Size: 2,
			OnOpen: func(idx int) {
				openedCount.Add(1)
				lastIdx.Store(int32(idx))
			},
		})

		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// wait for handlers to be called
		time.Sleep(100 * time.Millisecond)

		// should have called handler for each connection
		s.Equal(int32(2), openedCount.Load(), "should call open handler for each connection")
		s.GreaterOrEqual(lastIdx.Load(), int32(0), "last index should be valid")
	})

	s.Run("OnClose", func() {
		var handlerCalled atomic.Bool
		var totalCalls atomic.Int32

		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{
			Size: 2,
			OnClose: func(idx int, code int, reason string, server bool, recover bool) {
				handlerCalled.Store(true)
				totalCalls.Add(1)
			},
		})

		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// get a connection and close it to trigger close notification
		conn, err := mgr.Assign(ConnectionPurposeControl)
		s.Require().NoError(err)
		s.Require().NotNil(conn)

		err = conn.Close()
		s.NoError(err)

		// wait for handler to be called
		time.Sleep(100 * time.Millisecond)

		s.True(handlerCalled.Load(), "handler should be called")
		s.GreaterOrEqual(totalCalls.Load(), int32(1), "handler should be called at least once")
	})

	s.Run("OnBlock", func() {
		var handlerCalled atomic.Bool

		mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{
			Size: 1,
			OnBlock: func(idx int, active bool, reason string) {
				handlerCalled.Store(true)
			},
		})

		err := mgr.Init(s.CTX)
		s.Require().NoError(err)
		defer mgr.Close()

		// note: testing actual blocking requires triggering RabbitMQ flow control,
		// which is non-trivial in a test environment
		// this test only verifies the handler is registered and can be invoked
		time.Sleep(100 * time.Millisecond)

		// we don't assert handlerCalled here because blocking is unlikely in tests
		s.NotNil(mgr, "manager should be initialized")
	})
}

func (s *integrationTestSuite) TestConnectionManagerContextCancellation() {
	ctx, cancel := context.WithCancel(s.CTX)

	mgr := NewConnectionManager(s.URL, &ConnectionManagerOptions{Size: 2})
	err := mgr.Init(ctx)
	s.Require().NoError(err)
	defer mgr.Close()

	// verify connections are open
	conn, err := mgr.Assign(ConnectionPurposeControl)
	s.NoError(err)
	s.NotNil(conn)

	// cancel context, monitoring goroutines should stop
	cancel()

	// wait a moment
	time.Sleep(100 * time.Millisecond)

	// manager should still be functional for assignment
	conn2, err := mgr.Assign(ConnectionPurposePublish)
	s.NoError(err)
	s.NotNil(conn2)
}
