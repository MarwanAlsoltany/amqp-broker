package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRegistry(t *testing.T) {
	tm := NewRegistry()

	assert.NotNil(t, tm)
	assert.Empty(t, tm.exchanges)
	assert.Empty(t, tm.queues)
	assert.Empty(t, tm.bindings)

	t.Run("ConcurrentAccess", func(t *testing.T) {
		tm := NewRegistry()

		// add initial data
		tm.stateMu.Lock()
		tm.exchanges = []Exchange{{Name: "exchange1", Type: "direct"}}
		tm.queues = []Queue{{Name: "queue1", Durable: true}}
		tm.bindings = []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}}
		tm.stateMu.Unlock()

		// concurrent reads should work
		doneCh := make(chan bool, 10)
		for range 10 {
			go func() {
				e := tm.Exchange("exchange1")
				assert.NotNil(t, e)
				q := tm.Queue("queue1")
				assert.NotNil(t, q)
				b := tm.Binding("exchange1", "queue1", "test")
				assert.NotNil(t, b)
				doneCh <- true
			}()
		}

		for range 10 {
			<-doneCh
		}
	})
}

func TestTopologyManagerExchange(t *testing.T) {
	tm := NewRegistry()

	tm.stateMu.Lock()
	tm.exchanges = []Exchange{
		{Name: "exchange1", Type: "direct"},
		{Name: "exchange2", Type: "topic"},
	}
	tm.stateMu.Unlock()

	t.Run("WithExistingExchange", func(t *testing.T) {
		exchange := tm.Exchange("exchange1")
		assert.NotNil(t, exchange)
		assert.Equal(t, "exchange1", exchange.Name)
		assert.Equal(t, "direct", exchange.Type)
	})

	t.Run("WithNonExistentExchange", func(t *testing.T) {
		exchange := tm.Exchange("non-existent")
		assert.Nil(t, exchange)
	})
}

func TestTopologyManagerQueue(t *testing.T) {
	tm := NewRegistry()

	tm.stateMu.Lock()
	tm.queues = []Queue{
		{Name: "queue1", Durable: true},
		{Name: "queue2", Durable: false},
	}
	tm.stateMu.Unlock()

	t.Run("WithExistingQueue", func(t *testing.T) {
		queue := tm.Queue("queue1")
		assert.NotNil(t, queue)
		assert.Equal(t, "queue1", queue.Name)
		assert.True(t, queue.Durable)
	})

	t.Run("WithNonExistentQueue", func(t *testing.T) {
		queue := tm.Queue("non-existent")
		assert.Nil(t, queue)
	})
}

func TestTopologyManagerBinding(t *testing.T) {
	tm := NewRegistry()

	tm.stateMu.Lock()
	tm.bindings = []Binding{
		{Source: "exchange1", Destination: "queue1", Key: "key1"},
		{Source: "exchange2", Destination: "queue2", Key: "key2"},
	}
	tm.stateMu.Unlock()

	t.Run("WithExistingBinding", func(t *testing.T) {
		binding := tm.Binding("exchange1", "queue1", "key1")
		assert.NotNil(t, binding)
		assert.Equal(t, "exchange1", binding.Source)
		assert.Equal(t, "queue1", binding.Destination)
		assert.Equal(t, "key1", binding.Key)
	})

	t.Run("WithNonExistentBinding", func(t *testing.T) {
		binding := tm.Binding("non-existent", "non-existent", "non-existent")
		assert.Nil(t, binding)
	})
}

func TestTopologyManagerDeclare(t *testing.T) {
	tm := NewRegistry()

	var err error

	err = tm.Declare(nil, &Topology{Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)

	err = tm.Declare(nil, &Topology{Queues: []Queue{{Name: "queue1", Durable: true}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)

	err = tm.Declare(nil, &Topology{Bindings: []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)

	t.Run("WhenCached", func(t *testing.T) {
		exchange := Exchange{Name: "exchange1", Type: "direct"}
		queue := Queue{Name: "queue1", Durable: true}
		binding := Binding{Source: "exchange1", Destination: "queue1", Key: "test"}

		tm.stateMu.Lock()
		tm.exchanges = []Exchange{exchange}
		tm.declarations.Store(hash(exchange), exchange)
		tm.queues = []Queue{queue}
		tm.declarations.Store(hash(queue), queue)
		tm.bindings = []Binding{binding}
		tm.declarations.Store(hash(binding), binding)
		tm.stateMu.Unlock()

		err := tm.Declare(nil, &Topology{
			Exchanges: []Exchange{exchange},
			Queues:    []Queue{queue},
			Bindings:  []Binding{binding},
		})
		assert.NoError(t, err)
	})

	t.Run("WhenInvalid", func(t *testing.T) {
		err := tm.Declare(nil, &Topology{Exchanges: []Exchange{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.Declare(nil, &Topology{Queues: []Queue{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.Declare(nil, &Topology{Bindings: []Binding{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})

	t.Run("WhenEmpty", func(t *testing.T) {
		err := tm.Declare(nil, &Topology{})
		assert.NoError(t, err)
	})
}

func TestTopologyManagerDelete(t *testing.T) {
	tm := NewRegistry()

	var err error

	err = tm.Delete(nil, &Topology{Bindings: []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)

	err = tm.Delete(nil, &Topology{Queues: []Queue{{Name: "queue1", Durable: true}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)

	err = tm.Delete(nil, &Topology{Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)

	t.Run("WhenInvalid", func(t *testing.T) {
		err := tm.Delete(nil, &Topology{Bindings: []Binding{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.Delete(nil, &Topology{Queues: []Queue{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.Delete(nil, &Topology{Exchanges: []Exchange{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})

	t.Run("WhenEmpty", func(t *testing.T) {
		err := tm.Delete(nil, &Topology{})
		assert.NoError(t, err)
	})
}

func TestTopologyManagerVerify(t *testing.T) {
	tm := NewRegistry()

	var err error

	err = tm.Verify(nil, &Topology{Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)

	err = tm.Verify(nil, &Topology{Queues: []Queue{{Name: "queue1", Durable: true}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)

	err = tm.Verify(nil, &Topology{Bindings: []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed) // bindings are not verified, they are redeclared

	t.Run("WhenInvalid", func(t *testing.T) {
		err := tm.Verify(nil, &Topology{Exchanges: []Exchange{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.Verify(nil, &Topology{Queues: []Queue{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.Verify(nil, &Topology{Bindings: []Binding{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})

	t.Run("WhenEmpty", func(t *testing.T) {
		err := tm.Verify(nil, &Topology{})
		assert.NoError(t, err)
	})
}

func TestTopologyManagerSync(t *testing.T) {
	tm := NewRegistry()

	topology := &Topology{
		Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}},
		Queues:    []Queue{{Name: "queue1", Durable: true}},
		Bindings:  []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}},
	}

	err := tm.Sync(nil, topology)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)

	// to trigger diff mechanism and read delete path
	tm.stateMu.Lock()
	tm.exchanges = topology.Exchanges
	tm.queues = topology.Queues
	tm.bindings = topology.Bindings
	tm.stateMu.Unlock()

	err = tm.Sync(nil, &Topology{
		Exchanges: []Exchange{{Name: "exchange2", Type: "direct"}},
		Queues:    []Queue{{Name: "queue2", Durable: true}},
		Bindings:  []Binding{{Source: "exchange2", Destination: "queue2", Key: "test"}},
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)

	t.Run("WhenInvalid", func(t *testing.T) {
		err := tm.Sync(nil, &Topology{Exchanges: []Exchange{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopology)

		err = tm.Sync(nil, &Topology{Queues: []Queue{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopology)

		err = tm.Sync(nil, &Topology{Bindings: []Binding{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopology)
	})

	t.Run("WhenEmpty", func(t *testing.T) {
		err := tm.Delete(nil, &Topology{})
		assert.NoError(t, err)
	})
}

func TestHash(t *testing.T) {
	t.Run("Consistency", func(t *testing.T) {
		h1 := hash(nil)
		h2 := hash(nil)
		h3 := hash(nil)
		assert.Equal(t, h1, h2)
		assert.Equal(t, h2, h3)
	})

	t.Run("Length", func(t *testing.T) {
		h := hash("test")
		assert.Len(t, h, 32) // MD5 hex is 32 chars
	})

	t.Run("SameValue", func(t *testing.T) {
		e1 := Exchange{Name: "test", Type: "direct"}
		e2 := Exchange{Name: "test", Type: "direct"}
		h1 := hash(e1)
		h2 := hash(e2)
		assert.Equal(t, h1, h2)
	})

	t.Run("DifferentValue", func(t *testing.T) {
		e1 := Exchange{Name: "test1", Type: "direct"}
		e2 := Exchange{Name: "test2", Type: "direct"}
		h1 := hash(e1)
		h2 := hash(e2)
		assert.NotEqual(t, h1, h2)
	})

	t.Run("DifferentTypes", func(t *testing.T) {
		e := Exchange{Name: "test", Type: "direct"}
		q := Queue{Name: "test"}
		b := Binding{Source: "test", Destination: "test"}

		he := hash(e)
		hq := hash(q)
		hb := hash(b)

		// all should produce valid hashes
		assert.Len(t, he, 32)
		assert.Len(t, hq, 32)
		assert.Len(t, hb, 32)

		// all should be different
		assert.NotEqual(t, he, hq)
		assert.NotEqual(t, hq, hb)
		assert.NotEqual(t, he, hb)
	})
}
