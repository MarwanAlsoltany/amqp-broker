//go:build integration
// +build integration

package topology

func (s *integrationTestSuite) TestQueueDeclare() {
	ch := s.newTestChannel()
	defer ch.Close()

	qName := s.testName("test-queue")
	queue := Queue{Name: qName, Durable: true}

	err := queue.Declare(ch)
	s.NoError(err)

	s.Run("WithArguments", func() {
		q := Queue{
			Name: qName + "-args",
			Arguments: Arguments{
				"x-message-ttl": int32(60000),
				"x-max-length":  int32(1000),
			},
		}
		err := q.Declare(ch)
		s.NoError(err)
		info, err := q.Inspect(ch)
		s.NoError(err)
		s.NotNil(info)
	})

	s.Run("Validation", func() {
		err := Queue{Name: ""}.Declare(ch)
		s.Error(err, "declaring queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch2 := s.newTestChannel()
		defer ch2.Close()

		q := Queue{Name: qName + "-duplicate", Durable: true}
		err := q.Declare(ch2)
		s.NoError(err)

		// re-declare with conflicting parameters, should close channel
		q.Durable = false
		err = q.Declare(ch2)
		s.Error(err, "declaring queue with conflicting parameters should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch2.IsClosed(), "channel should be closed")
	})
}

func (s *integrationTestSuite) TestQueueVerify() {
	ch := s.newTestChannel()
	defer ch.Close()

	qName := s.testName("test-queue")
	queue := Queue{Name: qName, Durable: true}

	err := queue.Declare(ch)
	s.Require().NoError(err)

	err = queue.Verify(ch)
	s.NoError(err)

	s.Run("Validation", func() {
		err := Queue{Name: ""}.Verify(ch)
		s.Error(err, "verifying queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})

	s.Run("WithChannelCloseError", func() {
		ch2 := s.newTestChannel()
		defer ch2.Close()

		nonExistent := Queue{Name: qName + "-non-existent"}
		err := nonExistent.Verify(ch2)
		s.Error(err, "verifying non-existent queue should fail")
		s.Contains(err.Error(), "channel closed", "error should mention channel closure")
		s.True(ch2.IsClosed(), "channel should be closed")
	})
}

func (s *integrationTestSuite) TestQueueDelete() {
	ch := s.newTestChannel()
	defer ch.Close()

	qName := s.testName("test-queue")
	queue := Queue{Name: qName, Durable: true}

	err := queue.Declare(ch)
	s.Require().NoError(err)

	count, err := queue.Delete(ch, false, false)
	s.NoError(err)
	s.GreaterOrEqual(count, 0)

	s.Run("Validation", func() {
		_, err := Queue{Name: ""}.Delete(ch, false, false)
		s.Error(err, "deleting queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})
}

func (s *integrationTestSuite) TestQueuePurge() {
	ch := s.newTestChannel()
	defer ch.Close()

	qName := s.testName("test-queue-purge")
	queue := Queue{Name: qName, Durable: false, AutoDelete: true}

	err := queue.Declare(ch)
	s.Require().NoError(err)

	count, err := queue.Purge(ch)
	s.NoError(err)
	s.Equal(0, count)

	s.Run("Validation", func() {
		_, err := Queue{Name: ""}.Purge(ch)
		s.Error(err, "purging queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})
}

func (s *integrationTestSuite) TestQueueInspect() {
	ch := s.newTestChannel()
	defer ch.Close()

	qName := s.testName("test-queue-inspect")
	queue := Queue{Name: qName, Durable: false, AutoDelete: true}

	err := queue.Declare(ch)
	s.Require().NoError(err)

	info, err := queue.Inspect(ch)
	s.NoError(err)
	s.NotNil(info)
	s.Equal(0, info.Messages)
	s.Equal(0, info.Consumers)

	s.Run("WithChannelCloseError", func() {
		ch := s.newTestChannel()
		defer ch.Close()

		nonExistent := Queue{Name: qName + "-non-existent"}
		_, err := nonExistent.Inspect(ch)
		s.Error(err, "inspecting non-existent queue should fail")
		s.True(ch.IsClosed(), "channel should be closed after inspecting non-existent queue")
	})

	s.Run("Validation", func() {
		_, err := Queue{Name: ""}.Inspect(ch)
		s.Error(err, "inspecting queue with empty name should fail")
		s.ErrorIs(err, ErrTopologyQueueNameEmpty)
	})
}
