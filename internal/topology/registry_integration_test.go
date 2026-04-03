//go:build integration
// +build integration

package topology

import (
	"sync"
)

func (s *integrationTestSuite) TestRegistryDeclare() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	qName := s.testName("test-queue")
	bKey := s.testName("test-key")

	tm := NewRegistry()
	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: bKey}},
	}

	err := tm.Declare(ch, topology)
	s.NoError(err, "declaring topology should succeed")

	// client state should be updated
	s.NotNil(tm.Exchange(eName), "exchange should be in manager")
	s.NotNil(tm.Queue(qName), "queue should be in manager")
	s.NotNil(tm.Binding(eName, qName, bKey), "binding should be in manager")

	s.Run("IdempotentOnCache", func() {
		// re-declaring the same topology should be a no-op (served from cache)
		err := tm.Declare(ch, topology)
		s.NoError(err)
	})

	s.Run("Validation", func() {
		err := tm.Declare(ch, &Topology{Exchanges: []Exchange{{Name: ""}}})
		s.Error(err, "declaring topology with invalid exchange should fail")

		err = tm.Declare(ch, &Topology{Queues: []Queue{{Name: ""}}})
		s.Error(err, "declaring topology with invalid queue should fail")

		err = tm.Declare(ch, &Topology{Bindings: []Binding{{Source: "", Destination: "", Key: ""}}})
		s.Error(err, "declaring topology with invalid binding should fail")
	})
}

func (s *integrationTestSuite) TestRegistryVerify() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	qName := s.testName("test-queue")
	bKey := s.testName("test-key")

	tm := NewRegistry()
	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
		Bindings:  []Binding{{Source: eName, Destination: qName, Key: bKey}},
	}

	err := tm.Declare(ch, topology)
	s.Require().NoError(err)

	err = tm.Verify(ch, topology)
	s.NoError(err, "verifying declared topology should succeed")

	s.Run("Validation", func() {
		err := tm.Verify(ch, &Topology{Exchanges: []Exchange{{Name: ""}}})
		s.Error(err, "verifying topology with invalid exchange should fail")

		err = tm.Verify(ch, &Topology{Queues: []Queue{{Name: ""}}})
		s.Error(err, "verifying topology with invalid queue should fail")

		err = tm.Verify(ch, &Topology{Bindings: []Binding{{Source: "", Destination: "", Key: ""}}})
		s.Error(err, "verifying topology with invalid binding should fail")
	})
}

func (s *integrationTestSuite) TestRegistryDelete() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	qName := s.testName("test-queue")

	tm := NewRegistry()
	topology := &Topology{
		Exchanges: []Exchange{{Name: eName, Type: "direct"}},
		Queues:    []Queue{{Name: qName}},
	}

	err := tm.Declare(ch, topology)
	s.Require().NoError(err)

	err = tm.Delete(ch, topology)
	s.NoError(err)

	// client state should be cleared
	s.Nil(tm.Exchange(eName), "exchange should be removed from manager")
	s.Nil(tm.Queue(qName), "queue should be removed from manager")

	s.Run("Validation", func() {
		err := tm.Delete(ch, &Topology{Exchanges: []Exchange{{Name: ""}}})
		s.Error(err, "deleting topology with invalid exchange should fail")

		err = tm.Delete(ch, &Topology{Queues: []Queue{{Name: ""}}})
		s.Error(err, "deleting topology with invalid queue should fail")

		err = tm.Delete(ch, &Topology{Bindings: []Binding{{Source: "", Destination: "", Key: ""}}})
		s.Error(err, "deleting topology with invalid binding should fail")
	})
}

func (s *integrationTestSuite) TestRegistrySync() {
	ch := s.newTestChannel()
	defer ch.Close()

	eName := s.testName("test-exchange")
	qName := s.testName("test-queue")
	bKey := s.testName("test-key")

	tm := NewRegistry()

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

	err := tm.Declare(ch, initial)
	s.Require().NoError(err)

	desired := &Topology{
		Exchanges: []Exchange{
			{Name: eName + "-1", Type: "direct", Arguments: Arguments{"x-name": "value"}}, // modified
			{Name: eName + "-3", Type: "topic"},                                           // new
		},
		Queues: []Queue{
			{Name: qName + "-1", Arguments: Arguments{"x-name": "value"}}, // modified
			{Name: qName + "-3"}, // new
		},
		Bindings: []Binding{
			{Source: eName + "-1", Destination: qName + "-1", Key: bKey, Arguments: Arguments{"x-name": "value"}}, // modified
			{Source: eName + "-1", Destination: eName + "-3", Key: bKey, Type: BindingTypeExchange},               // new
		},
	}

	err = tm.Sync(ch, desired)
	s.NoError(err)

	// verify client state reflects desired topology

	s.NotNil(tm.Exchange(eName+"-1"), "exchange 1 should remain")
	s.Nil(tm.Exchange(eName+"-2"), "exchange 2 should be removed")
	s.NotNil(tm.Exchange(eName+"-3"), "exchange 3 should be added")

	s.NotNil(tm.Queue(qName+"-1"), "queue 1 should remain")
	s.Nil(tm.Queue(qName+"-2"), "queue 2 should be removed")
	s.NotNil(tm.Queue(qName+"-3"), "queue 3 should be added")

	s.NotNil(tm.Binding(eName+"-1", qName+"-1", bKey), "binding 1 should remain")
	s.Nil(tm.Binding(eName+"-1", eName+"-2", bKey), "binding 2 should be removed")
	s.NotNil(tm.Binding(eName+"-1", eName+"-3", bKey), "binding 3 should be added")

	// verify server state for entities that should continue to exist
	ex1 := desired.Exchange(eName + "-1")
	s.Require().NotNil(ex1)
	err = ex1.Verify(ch)
	s.NoError(err, "exchange 1 should still exist on server")

	ex3 := desired.Exchange(eName + "-3")
	s.Require().NotNil(ex3)
	err = ex3.Verify(ch)
	s.NoError(err, "exchange 3 should exist on server")

	q1 := desired.Queue(qName + "-1")
	s.Require().NotNil(q1)
	err = q1.Verify(ch)
	s.NoError(err, "queue 1 should still exist on server")

	q3 := desired.Queue(qName + "-3")
	s.Require().NotNil(q3)
	err = q3.Verify(ch)
	s.NoError(err, "queue 3 should exist on server")

	// use fresh channels because verify closes the channel on failure

	ex2Ch := s.newTestChannel()
	defer ex2Ch.Close()
	ex2 := initial.Exchange(eName + "-2")
	s.Require().NotNil(ex2)
	err = ex2.Verify(ex2Ch)
	s.Error(err, "exchange 2 should not exist on server")
	s.True(ex2Ch.IsClosed(), "channel should be closed after verifying non-existent exchange")

	q2Ch := s.newTestChannel()
	defer q2Ch.Close()
	q2 := initial.Queue(qName + "-2")
	s.Require().NotNil(q2)
	err = q2.Verify(q2Ch)
	s.Error(err, "queue 2 should not exist on server")
	s.True(q2Ch.IsClosed(), "channel should be closed after verifying non-existent queue")
}

func (s *integrationTestSuite) TestRegistryConcurrentAccess() {
	ch := s.newTestChannel()
	defer ch.Close()

	tm := NewRegistry()

	var wg sync.WaitGroup
	concurrency := 10

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()

			eName := s.testName("test-exchange-concurrent")
			topology := &Topology{
				Exchanges: []Exchange{{Name: eName, Type: "direct", AutoDelete: true}},
			}

			err := tm.Declare(ch, topology)
			s.NoError(err)

			exchange := tm.Exchange(eName)
			s.NotNil(exchange)
		}()
	}

	wg.Wait()
}
