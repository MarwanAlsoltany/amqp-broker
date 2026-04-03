//go:build integration
// +build integration

package topology

func (s *integrationTestSuite) TestExchangeDeclare() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	exchange := Exchange{Name: eName, Type: "direct", Durable: true}

	err := exchange.Declare(ch)
	s.NoError(err)

	s.Run("WithTypes", func() {
		eTypes := []string{"direct", "topic", "fanout", "headers"}
		for _, eType := range eTypes {
			ex := Exchange{Name: s.testName("test-exchange-" + eType), Type: eType}
			err = ex.Declare(ch)
			s.NoError(err, "failed to declare %s exchange", eType)
		}
	})

	s.Run("Validation", func() {
		err := Exchange{Name: ""}.Declare(ch)
		s.Error(err, "declaring exchange with empty name should fail")
		s.ErrorIs(err, ErrTopologyExchangeNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch2 := s.newTestChannel()
		defer ch2.Close()

		conflicting := Exchange{Name: eName + "-conflicting", Type: "direct", Durable: true}
		err := conflicting.Declare(ch2)
		s.NoError(err)

		// re-declare with conflicting parameters, should close channel
		conflicting.Durable = false
		err = conflicting.Declare(ch2)
		s.Error(err, "declaring conflicting exchange should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch2.IsClosed(), "channel should be closed")
	})
}

func (s *integrationTestSuite) TestExchangeVerify() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	exchange := Exchange{Name: eName, Type: "direct", Durable: true}

	err := exchange.Declare(ch)
	s.Require().NoError(err)

	err = exchange.Verify(ch)
	s.NoError(err)

	s.Run("Validation", func() {
		err := Exchange{Name: ""}.Verify(ch)
		s.Error(err, "verifying exchange with empty name should fail")
		s.ErrorIs(err, ErrTopologyExchangeNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch2 := s.newTestChannel()
		defer ch2.Close()

		nonExistent := Exchange{Name: eName + "-non-existent", Type: "direct"}
		err := nonExistent.Verify(ch2)
		s.Error(err, "verifying non-existent exchange should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch2.IsClosed(), "channel should be closed")
	})
}

func (s *integrationTestSuite) TestExchangeDelete() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	exchange := Exchange{Name: eName, Type: "direct"}

	err := exchange.Declare(ch)
	s.Require().NoError(err)

	err = exchange.Delete(ch, false)
	s.NoError(err)

	s.Run("Validation", func() {
		err := Exchange{Name: ""}.Delete(ch, false)
		s.Error(err, "deleting exchange with empty name should fail")
		s.ErrorIs(err, ErrTopologyExchangeNameEmpty)
	})
}
