//go:build integration
// +build integration

package transport

import (
	"errors"
)

func (s *integrationTestSuite) TestConnectionAdapterChannel() {
	conn := s.newTestConnection()
	defer conn.Close()

	ch, err := conn.Channel()
	s.Require().NoError(err)
	defer ch.Close()

	s.Run("WithClosedConnection", func() {
		conn := s.newTestConnection()
		conn.Close() // needed to test channel creation failure

		ch, err := conn.Channel()
		s.Error(err)
		s.Nil(ch)
	})
}

func (s *integrationTestSuite) TestDoSafeChannelAction() {
	s.Run("Success", func() {
		conn := s.newTestConnection()
		defer conn.Close()

		ch, err := conn.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		// successful operation should not close channel
		err = DoSafeChannelAction(ch, func(ch Channel) error {
			return nil
		})
		s.NoError(err, "no-op operation should not return error")
		s.False(ch.IsClosed(), "channel should remain open after no-op")

		// successful operation, declare a new exchange
		err = DoSafeChannelAction(ch, func(ch Channel) error {
			return ch.ExchangeDeclare("test-safe-channel-success", "direct", false, true, false, false, nil)
		})

		s.NoError(err, "successful operation should not return error")
		s.False(ch.IsClosed(), "channel should remain open after successful operation")
	})

	s.Run("Failure", func() {
		conn := s.newTestConnection()
		defer conn.Close()

		ch, err := conn.Channel()
		s.Require().NoError(err)

		err = DoSafeChannelAction(ch, func(ch Channel) error {
			return errors.New("intentional error")
		})
		s.Error(err, "operation that returns error should propagate it")
		s.False(ch.IsClosed(), "channel should remain open after operation error")

		// declare an exchange with specific parameters
		err = ch.ExchangeDeclare("test-safe-channel-failure", "direct", true, false, false, false, nil)
		s.Require().NoError(err)

		// try to re-declare with different parameters - this should close the channel (precondition failed)
		err = DoSafeChannelAction(ch, func(ch Channel) error {
			return ch.ExchangeDeclare("test-safe-channel-failure", "topic", false, false, false, false, nil)
		})

		// should have both operation error and channel closure information
		s.Error(err, "conflicting exchange declaration should return error")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed after protocol error")

		ch, err = conn.Channel()
		s.Require().NoError(err)

		// try to passive declare a non-existent exchange
		err = DoSafeChannelAction(ch, func(ch Channel) error {
			return ch.ExchangeDeclarePassive("test-safe-channel-non-existent", "direct", false, false, false, false, nil)
		})

		// should have channel closure error
		s.Error(err, "passive declare of non-existent exchange should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed after not found error")
	})
}

func (s *integrationTestSuite) TestDoSafeChannelActionWithReturn() {
	s.Run("Success", func() {
		conn := s.newTestConnection()
		defer conn.Close()

		ch, err := conn.Channel()
		s.Require().NoError(err)
		defer ch.Close()

		// declare queue and get queue
		queue, err := DoSafeChannelActionWithReturn(ch, func(ch Channel) (Queue, error) {
			return ch.QueueDeclare("test-safe-channel-return-success", true, false, false, false, nil)
		})

		s.NoError(err)
		s.Equal("test-safe-channel-return-success", queue.Name)
		s.Equal(0, queue.Messages)
		s.Equal(0, queue.Consumers)
	})

	s.Run("Failure", func() {
		conn := s.newTestConnection()
		defer conn.Close()

		ch, err := conn.Channel()
		s.Require().NoError(err)

		// try to inspect a non-existent queue (passive declare)
		_, err = DoSafeChannelActionWithReturn(ch, func(ch Channel) (Queue, error) {
			return ch.QueueDeclarePassive("test-safe-channel-return-non-existent", false, false, false, false, nil)
		})
		s.Error(err, "operation on non-existent queue should return error")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch.IsClosed(), "channel should be closed")
	})
}
