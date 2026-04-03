//go:build integration
// +build integration

package topology

func (s *integrationTestSuite) TestBindingDeclare() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	qName := s.testName("test-queue")
	kName := s.testName("test-key")
	exchange := Exchange{Name: eName, Type: "direct"}
	queue := Queue{Name: qName, Durable: false, AutoDelete: true}

	err := exchange.Declare(ch)
	s.Require().NoError(err)
	err = queue.Declare(ch)
	s.Require().NoError(err)

	binding := NewBinding(eName, qName, kName)
	err = binding.Declare(ch)
	s.NoError(err)

	s.Run("Validation", func() {
		err := Binding{}.Declare(ch)
		s.Error(err, "declaring binding with empty fields should fail")
		s.ErrorIs(err, ErrTopologyBindingFieldsEmpty)
	})
}

func (s *integrationTestSuite) TestBindingDelete() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	qName := s.testName("test-queue")
	kName := s.testName("test-key")
	exchange := Exchange{Name: eName, Type: "direct"}
	queue := Queue{Name: qName, Durable: false, AutoDelete: true}

	err := exchange.Declare(ch)
	s.Require().NoError(err)
	err = queue.Declare(ch)
	s.Require().NoError(err)

	binding := NewBinding(eName, qName, kName)
	err = binding.Declare(ch)
	s.Require().NoError(err)

	err = binding.Delete(ch)
	s.NoError(err)

	s.Run("Validation", func() {
		err := Binding{}.Delete(ch)
		s.Error(err, "deleting binding with empty fields should fail")
		s.ErrorIs(err, ErrTopologyBindingFieldsEmpty)
	})
}
