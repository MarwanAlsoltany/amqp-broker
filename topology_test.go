package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewExchange(t *testing.T) {
	exchange := NewExchange("test-exchange")

	assert.Equal(t, "test-exchange", exchange.Name)
	assert.Equal(t, defaultExchangeType, exchange.Type)
	assert.Equal(t, defaultExchangeDurable, exchange.Durable)
	assert.False(t, exchange.AutoDelete)
	assert.False(t, exchange.Internal)
	assert.Nil(t, exchange.Arguments)
}

func TestExchangeValidate(t *testing.T) {
	tests := []struct {
		name     string
		exchange Exchange
		want     error
	}{
		{
			name:     "DirectExchange",
			exchange: Exchange{Name: "test.direct", Type: "direct", Durable: true},
			want:     nil,
		},
		{
			name:     "TopicExchange",
			exchange: Exchange{Name: "test.topic", Type: "topic", Durable: true},
			want:     nil,
		},
		{
			name:     "FanoutExchange",
			exchange: Exchange{Name: "test.fanout", Type: "fanout"},
			want:     nil,
		},
		{
			name:     "HeadersExchange",
			exchange: Exchange{Name: "test.headers", Type: "headers"},
			want:     nil,
		},
		{
			name:     "WithArguments",
			exchange: Exchange{Name: "test.args", Type: "direct", Arguments: Arguments{"x-delayed-type": "direct"}},
			want:     nil,
		},
		{
			name:     "EmptyFields",
			exchange: Exchange{},
			want:     ErrTopologyExchangeNameEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.exchange.Validate()
			assert.Equal(t, tt.want, err)
			if err != nil {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestExchangeMatches(t *testing.T) {
	exchange1 := Exchange{Name: "test", Type: "direct", Durable: true}
	exchange2 := Exchange{Name: "test", Type: "fanout", Durable: false}
	exchange3 := Exchange{Name: "other", Type: "direct", Durable: true}

	assert.True(t, exchange1.Matches(exchange2), "exchanges with same name should match")
	assert.False(t, exchange1.Matches(exchange3), "exchanges with different names should not match")
}

func TestExchangeDeclare(t *testing.T) {
	exchange := Exchange{Name: "test-exchange", Type: "direct", Durable: true}

	err := exchange.Declare(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)
	assert.ErrorContains(t, err, "exchange")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Exchange{}.Declare(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestExchangeVerify(t *testing.T) {
	exchange := Exchange{Name: "test-exchange", Type: "direct", Durable: true}

	err := exchange.Verify(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)
	assert.ErrorContains(t, err, "exchange")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Exchange{}.Verify(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestExchangeDelete(t *testing.T) {
	exchange := Exchange{Name: "test-exchange", Type: "direct", Durable: true}

	err := exchange.Delete(nil, false)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)
	assert.ErrorContains(t, err, "exchange")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Exchange{}.Delete(nil, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestNewQueue(t *testing.T) {
	queue := NewQueue("test-queue")

	assert.Equal(t, "test-queue", queue.Name)
	assert.Equal(t, defaultQueueDurable, queue.Durable)
	assert.False(t, queue.AutoDelete)
	assert.False(t, queue.Exclusive)
	assert.Nil(t, queue.Arguments)
}

func TestQueueValidate(t *testing.T) {
	tests := []struct {
		name  string
		queue Queue
		want  error
	}{
		{
			name:  "DurableQueue",
			queue: Queue{Name: "test.durable", Durable: true},
			want:  nil,
		},
		{
			name:  "AutoDeleteQueue",
			queue: Queue{Name: "test.autodelete", AutoDelete: true},
			want:  nil,
		},
		{
			name:  "ExclusiveQueue",
			queue: Queue{Name: "test.exclusive", Exclusive: true},
			want:  nil,
		},
		{
			name:  "WithArguments",
			queue: Queue{Name: "test.args", Arguments: Arguments{"x-message-ttl": 60000}},
			want:  nil,
		},
		{
			name:  "EmptyFields",
			queue: Queue{},
			want:  ErrTopologyQueueNameEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.queue.Validate()
			assert.Equal(t, tt.want, err)
			if err != nil {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestQueueMatches(t *testing.T) {
	queue1 := Queue{Name: "test", Durable: true}
	queue2 := Queue{Name: "test", Durable: false}
	queue3 := Queue{Name: "other", Durable: true}

	assert.True(t, queue1.Matches(queue2), "queues with same name should match")
	assert.False(t, queue1.Matches(queue3), "queues with different names should not match")
}

func TestQueueDeclare(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	err := queue.Declare(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)
	assert.ErrorContains(t, err, "queue")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Queue{}.Declare(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestQueueVerify(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	err := queue.Verify(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)
	assert.ErrorContains(t, err, "queue")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Queue{}.Verify(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestQueueDelete(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	_, err := queue.Delete(nil, false, false)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)
	assert.ErrorContains(t, err, "queue")

	t.Run("WhenInvalid", func(t *testing.T) {
		_, err := Queue{}.Delete(nil, false, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestQueuePurge(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	_, err := queue.Purge(nil)
	assert.Error(t, err)              // expected with nil channel
	assert.ErrorIs(t, err, ErrBroker) // has no specific error

	t.Run("WhenInvalid", func(t *testing.T) {
		_, err := Queue{}.Purge(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestQueueInspect(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	_, err := queue.Inspect(nil)
	assert.Error(t, err)              // expected with nil channel
	assert.ErrorIs(t, err, ErrBroker) // has no specific error

	t.Run("WhenInvalid", func(t *testing.T) {
		_, err := Queue{}.Inspect(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestNewBinding(t *testing.T) {
	binding := NewBinding("test-exchange", "test-queue", "test-key")

	assert.Equal(t, "test-exchange", binding.Source)
	assert.Equal(t, "test-queue", binding.Destination)
	assert.Equal(t, "test-key", binding.Key)
	assert.Equal(t, BindingTypeQueue, binding.Type)
	assert.Nil(t, binding.Arguments)
}

func TestBindingValidate(t *testing.T) {
	tests := []struct {
		name    string
		binding Binding
		want    error
	}{
		{
			name:    "WithSourceAndDestination",
			binding: Binding{Source: "exchange1", Destination: "queue1"},
			want:    nil,
		},
		{
			name:    "WithSourceAndDestinationAndKey",
			binding: Binding{Source: "exchange1", Destination: "queue1", Key: "key"},
			want:    nil,
		},
		{
			name:    "WithSourceAndDestinationAndArguments",
			binding: Binding{Source: "exchange1", Destination: "queue1", Arguments: Arguments{"x-match": "all"}},
			want:    nil,
		},
		{
			name:    "EmptyFields",
			binding: Binding{},
			want:    ErrTopologyBindingFieldsEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.binding.Validate()
			assert.Equal(t, tt.want, err)
			if err != nil {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestBindingMatches(t *testing.T) {
	binding1 := Binding{Source: "exchange", Destination: "queue", Key: "test"}
	binding2 := Binding{Source: "exchange", Destination: "queue", Key: "test"}
	binding3 := Binding{Source: "exchange", Destination: "queue", Key: "other"}
	binding4 := Binding{Source: "other", Destination: "queue", Key: "test"}

	assert.True(t, binding1.Matches(binding2), "bindings with same source, destination, key should match")
	assert.False(t, binding1.Matches(binding3), "bindings with different keys should not match")
	assert.False(t, binding1.Matches(binding4), "bindings with different sources should not match")
}

func TestBindingDeclare(t *testing.T) {
	binding := Binding{Source: "test-exchange", Destination: "test-queue", Key: "test-key"}

	err := binding.Declare(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)
	assert.ErrorContains(t, err, "binding")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Binding{}.Declare(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestBindingDelete(t *testing.T) {
	binding := Binding{Source: "test-exchange", Destination: "test-queue", Key: "test-key"}

	err := binding.Delete(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)
	assert.ErrorContains(t, err, "binding")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Binding{}.Delete(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})
}

func TestNewRoutingKey(t *testing.T) {
	key := NewRoutingKey("test.key", nil)
	assert.Equal(t, RoutingKey("test.key"), key)
}

func TestRoutingKeyString(t *testing.T) {
	key := RoutingKey("test.routing.key")
	assert.Equal(t, "test.routing.key", key.String())
}

func TestRoutingKeyReplace(t *testing.T) {
	tests := []struct {
		name string
		key  RoutingKey
		args map[string]string
		want RoutingKey
	}{
		{
			name: "SinglePlaceholder",
			key:  RoutingKey("user.{id}.created"),
			args: map[string]string{"id": "123"},
			want: RoutingKey("user.123.created"),
		},
		{
			name: "MultiplePlaceholders",
			key:  RoutingKey("user.{id}.{action}"),
			args: map[string]string{"id": "123", "action": "updated"},
			want: RoutingKey("user.123.updated"),
		},
		{
			name: "SinglePlaceholder",
			key:  RoutingKey("user.{id}.created"),
			args: map[string]string{"id": "123"},
			want: RoutingKey("user.123.created"),
		},
		{
			name: "MultiplePlaceholders",
			key:  RoutingKey("user.{id}.{action}"),
			args: map[string]string{"id": "123", "action": "updated"},
			want: RoutingKey("user.123.updated"),
		},
		{
			name: "NoPlaceholders",
			key:  RoutingKey("user.created"),
			args: map[string]string{"id": "123"},
			want: RoutingKey("user.created"),
		},
		{
			name: "UnusedArguments",
			key:  RoutingKey("user.{id}.created"),
			args: map[string]string{"id": "123", "unused": "value"},
			want: RoutingKey("user.123.created"),
		},
		{
			name: "EmptyArguments",
			key:  RoutingKey("user.{id}.created"),
			args: map[string]string{},
			want: RoutingKey("user.{id}.created"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.key.Replace(tt.args)
			assert.Equal(t, tt.want, tt.key)
		})
	}
}

func TestRoutingKeyValidate(t *testing.T) {
	tests := []struct {
		name string
		key  RoutingKey
		want error
	}{
		{
			name: "ValidKey",
			key:  RoutingKey("user.created"),
			want: nil,
		},
		{
			name: "ValidTopicPattern",
			key:  RoutingKey("user.*.created"),
			want: nil,
		},
		{
			name: "ValidHashPattern",
			key:  RoutingKey("user.#"),
			want: nil,
		},
		{
			name: "ValidWithPlaceholders",
			key:  RoutingKey("user.{id}.created"),
			want: nil,
		},
		{
			name: "EmptyKey",
			key:  RoutingKey(""),
			want: ErrTopologyRoutingKeyEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.key.Validate()
			assert.Equal(t, tt.want, err)
			if err != nil {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestNewTopology(t *testing.T) {
	exchanges := []Exchange{{Name: "exchange", Type: "direct"}}
	queues := []Queue{{Name: "queue"}}
	bindings := []Binding{{Source: "exchange", Destination: "queue", Key: "test"}}

	topology := NewTopology(exchanges, queues, bindings)

	assert.Len(t, topology.Exchanges, 1)
	assert.Len(t, topology.Queues, 1)
	assert.Len(t, topology.Bindings, 1)
	assert.Equal(t, "exchange", topology.Exchanges[0].Name)
	assert.Equal(t, "queue", topology.Queues[0].Name)
	assert.Equal(t, "exchange", topology.Bindings[0].Source)
	assert.Equal(t, "queue", topology.Bindings[0].Destination)
	assert.Equal(t, "test", topology.Bindings[0].Key)
}

func TestTopologyEmpty(t *testing.T) {
	tests := []struct {
		name     string
		topology *Topology
		want     bool
	}{
		{
			name:     "WhenEmpty",
			topology: &Topology{},
			want:     true,
		},
		{
			name: "WithExchange",
			topology: &Topology{
				Exchanges: []Exchange{{Name: "test"}},
			},
			want: false,
		},
		{
			name: "WithQueue",
			topology: &Topology{
				Queues: []Queue{{Name: "test"}},
			},
			want: false,
		},
		{
			name: "WithBinding",
			topology: &Topology{
				Bindings: []Binding{{Source: "test", Destination: "test", Key: "test"}},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.topology.Empty())
		})
	}
}

func TestTopologyMerge(t *testing.T) {
	topology1 := &Topology{
		Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}},
		Queues:    []Queue{{Name: "queue1"}},
		Bindings:  []Binding{{Source: "exchange1", Destination: "queue1", Key: ""}},
	}

	// covers different identity merging
	topology2 := &Topology{
		Exchanges: []Exchange{{Name: "exchange2", Type: "topic"}},
		Queues:    []Queue{{Name: "queue2"}},
		Bindings:  []Binding{{Source: "exchange2", Destination: "queue2", Key: "test"}},
	}

	// covers same identity merging
	topology3 := &Topology{
		Exchanges: []Exchange{
			// from toplogy1 with different type
			{Name: "exchange1", Type: "fanout"},
		},
		Queues:    []Queue{topology1.Queues[0]},
		Bindings:  []Binding{topology2.Bindings[0]},
	}

	merged := topology1.Merge(topology2).Merge(topology3)

	// exchange1 should be from topology2 (overridden)
	exchange := merged.Exchange("exchange1")
	assert.NotNil(t, exchange)
	assert.Equal(t, "fanout", exchange.Type, "merged exchange should use type from topology 3")

	// should have both exchanges
	assert.NotNil(t, merged.Exchange("exchange2"))

	// should have both queues
	assert.NotNil(t, merged.Queue("queue1"))
	assert.NotNil(t, merged.Queue("queue2"))

	// should have both bindings
	assert.NotNil(t, merged.Binding("exchange1", "queue1", ""))
	assert.NotNil(t, merged.Binding("exchange2", "queue2", "test"))
}

func TestTopologyValidate(t *testing.T) {
	topology := &Topology{
		Exchanges: []Exchange{
			{Name: "exchange1", Type: "direct"},
			{Name: ""},
		},
		Queues: []Queue{
			{Name: "queue1", Durable: true},
			{Name: "", Durable: false},
		},
		Bindings: []Binding{
			{Source: "exchange1", Destination: "queue1", Key: "key1"},
			{Source: "", Destination: "queue2", Key: "key2"},
		},
	}

	err := topology.Validate()
	assert.Error(t, err)
	// joined errors
	assert.ErrorIs(t, err, ErrTopologyValidation)
	assert.ErrorIs(t, err, ErrTopologyExchangeNameEmpty)
	assert.ErrorIs(t, err, ErrTopologyQueueNameEmpty)
	assert.ErrorIs(t, err, ErrTopologyBindingFieldsEmpty)
}

func TestTopologyExchange(t *testing.T) {
	topology := &Topology{
		Exchanges: []Exchange{
			{Name: "exchange1", Type: "direct"},
			{Name: "exchange2", Type: "topic"},
			{Name: "exchange3", Type: "fanout"},
		},
	}

	exchange := topology.Exchange("exchange1")
	assert.NotNil(t, exchange)
	assert.Equal(t, "exchange1", exchange.Name)
	assert.Equal(t, "direct", exchange.Type)

	exchange = topology.Exchange("non-existent")
	assert.Nil(t, exchange, "should return nil for non-existent exchange")

	tests := []struct {
		name     string
		exchange string
		want     *Exchange
	}{
		{
			name:     "ExistingExchange",
			exchange: "exchange1",
			want:     &Exchange{Name: "exchange1", Type: "direct"},
		},
		{
			name:     "NonExistingExchange",
			exchange: "exchange404",
			want:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topology.Exchange(tt.exchange)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestTopologyQueue(t *testing.T) {
	topology := &Topology{
		Queues: []Queue{
			{Name: "queue1", Durable: true},
			{Name: "queue2", Durable: false},
		},
	}

	queue := topology.Queue("queue1")
	assert.NotNil(t, queue)
	assert.Equal(t, "queue1", queue.Name)

	queue = topology.Queue("non-existent")
	assert.Nil(t, queue, "should return nil for non-existent queue")

	tests := []struct {
		name  string
		queue string
		want  *Queue
	}{
		{
			name:  "ExistingQueue",
			queue: "queue1",
			want:  &Queue{Name: "queue1", Durable: true},
		},
		{
			name:  "NonExistingQueue",
			queue: "queue404",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topology.Queue(tt.queue)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestTopologyBinding(t *testing.T) {
	topology := &Topology{
		Bindings: []Binding{
			{Source: "exchange1", Destination: "queue1", Key: "key1"},
			{Source: "exchange2", Destination: "queue2", Key: "key2"},
		},
	}

	binding := topology.Binding("exchange1", "queue1", "key1")
	assert.NotNil(t, binding)
	assert.Equal(t, "exchange1", binding.Source)
	assert.Equal(t, "queue1", binding.Destination)
	assert.Equal(t, "key1", binding.Key)

	binding = topology.Binding("non-existent", "non-existent", "non-existent")
	assert.Nil(t, binding, "should return nil for non-existent binding")
	binding = topology.Binding("non-existent", "queue1", "key1")
	assert.Nil(t, binding, "should return nil for non-existent binding")
	binding = topology.Binding("exchange1", "non-existent", "key1")
	assert.Nil(t, binding, "should return nil for non-existent binding")
	binding = topology.Binding("exchange1", "queue1", "non-existent")
	assert.Nil(t, binding, "should return nil for non-existent binding")

	tests := []struct {
		name        string
		source      string
		destination string
		key         string
		want        *Binding
	}{
		{
			name:        "ExistingBinding",
			source:      "exchange1",
			destination: "queue1",
			key:         "key1",
			want:        &Binding{Source: "exchange1", Destination: "queue1", Key: "key1"},
		},
		{
			name:        "NonExistingBinding",
			source:      "exchange404",
			destination: "queue404",
			key:         "key404",
			want:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topology.Binding(tt.source, tt.destination, tt.key)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestToplogyDeclare(t *testing.T) {
	topology := &Topology{
		Exchanges: []Exchange{
			{Name: "exchange1", Type: "direct"},
		},
		Queues: []Queue{
			{Name: "queue1", Durable: true},
		},
		Bindings: []Binding{
			{Source: "exchange1", Destination: "queue1", Key: "test"},
		},
	}

	err := topology.Declare(nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)
}

func TestTopologyDelete(t *testing.T) {
	topology := &Topology{
		Exchanges: []Exchange{
			{Name: "exchange1", Type: "direct"},
		},
		Queues: []Queue{
			{Name: "queue1", Durable: true},
		},
		Bindings: []Binding{
			{Source: "exchange1", Destination: "queue1", Key: "test"},
		},
	}

	err := topology.Delete(nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)
}

func TestTopologyVerify(t *testing.T) {
	topology := &Topology{
		Exchanges: []Exchange{
			{Name: "exchange1", Type: "direct"},
		},
		Queues: []Queue{
			{Name: "queue1", Durable: true},
		},
		Bindings: []Binding{
			{Source: "exchange1", Destination: "queue1", Key: "test"},
		},
	}

	err := topology.Verify(nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)
}

func TestNewTopologyManager(t *testing.T) {
	tm := newTopologyManager()

	assert.NotNil(t, tm)
	assert.Empty(t, tm.exchanges)
	assert.Empty(t, tm.queues)
	assert.Empty(t, tm.bindings)

	t.Run("ConcurrentAccess", func(t *testing.T) {
		tm := newTopologyManager()

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
				e := tm.exchange("exchange1")
				assert.NotNil(t, e)
				q := tm.queue("queue1")
				assert.NotNil(t, q)
				b := tm.binding("exchange1", "queue1", "test")
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
	tm := newTopologyManager()

	tm.stateMu.Lock()
	tm.exchanges = []Exchange{
		{Name: "exchange1", Type: "direct"},
		{Name: "exchange2", Type: "topic"},
	}
	tm.stateMu.Unlock()

	t.Run("WithExistingExchange", func(t *testing.T) {
		exchange := tm.exchange("exchange1")
		assert.NotNil(t, exchange)
		assert.Equal(t, "exchange1", exchange.Name)
		assert.Equal(t, "direct", exchange.Type)
	})

	t.Run("WithNonExistentExchange", func(t *testing.T) {
		exchange := tm.exchange("non-existent")
		assert.Nil(t, exchange)
	})
}

func TestTopologyManagerQueue(t *testing.T) {
	tm := newTopologyManager()

	tm.stateMu.Lock()
	tm.queues = []Queue{
		{Name: "queue1", Durable: true},
		{Name: "queue2", Durable: false},
	}
	tm.stateMu.Unlock()

	t.Run("WithExistingQueue", func(t *testing.T) {
		queue := tm.queue("queue1")
		assert.NotNil(t, queue)
		assert.Equal(t, "queue1", queue.Name)
		assert.True(t, queue.Durable)
	})

	t.Run("WithNonExistentQueue", func(t *testing.T) {
		queue := tm.queue("non-existent")
		assert.Nil(t, queue)
	})
}

func TestTopologyManagerBinding(t *testing.T) {
	tm := newTopologyManager()

	// add bindings
	tm.stateMu.Lock()
	tm.bindings = []Binding{
		{Source: "exchange1", Destination: "queue1", Key: "key1"},
		{Source: "exchange2", Destination: "queue2", Key: "key2"},
	}
	tm.stateMu.Unlock()

	t.Run("WithExistingBinding", func(t *testing.T) {
		binding := tm.binding("exchange1", "queue1", "key1")
		assert.NotNil(t, binding)
		assert.Equal(t, "exchange1", binding.Source)
		assert.Equal(t, "queue1", binding.Destination)
		assert.Equal(t, "key1", binding.Key)
	})

	t.Run("WithNonExistentBinding", func(t *testing.T) {
		binding := tm.binding("non-existent", "non-existent", "non-existent")
		assert.Nil(t, binding)
	})
}

func TestTopologyManagerDeclare(t *testing.T) {
	tm := newTopologyManager()

	var err error

	err = tm.declare(nil, &Topology{Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)

	err = tm.declare(nil, &Topology{Queues: []Queue{{Name: "queue1", Durable: true}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)

	err = tm.declare(nil, &Topology{Bindings: []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}}})
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

		err := tm.declare(nil, &Topology{
			Exchanges: []Exchange{exchange},
			Queues:    []Queue{queue},
			Bindings:  []Binding{binding},
		})
		assert.NoError(t, err)
	})

	t.Run("WhenInvalid", func(t *testing.T) {
		err := tm.declare(nil, &Topology{Exchanges: []Exchange{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.declare(nil, &Topology{Queues: []Queue{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.declare(nil, &Topology{Bindings: []Binding{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})

	t.Run("WhenEmpty", func(t *testing.T) {
		err := tm.declare(nil, &Topology{})
		assert.NoError(t, err)
	})
}

func TestTopologyManagerDelete(t *testing.T) {
	tm := newTopologyManager()

	var err error

	err = tm.delete(nil, &Topology{Bindings: []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)

	err = tm.delete(nil, &Topology{Queues: []Queue{{Name: "queue1", Durable: true}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)

	err = tm.delete(nil, &Topology{Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)

	t.Run("WhenInvalid", func(t *testing.T) {
		err := tm.delete(nil, &Topology{Bindings: []Binding{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.delete(nil, &Topology{Queues: []Queue{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.delete(nil, &Topology{Exchanges: []Exchange{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})

	t.Run("WhenEmpty", func(t *testing.T) {
		err := tm.delete(nil, &Topology{})
		assert.NoError(t, err)
	})
}

func TestTopologyManagerVerify(t *testing.T) {
	tm := newTopologyManager()

	var err error

	err = tm.verify(nil, &Topology{Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)

	err = tm.verify(nil, &Topology{Queues: []Queue{{Name: "queue1", Durable: true}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)

	err = tm.verify(nil, &Topology{Bindings: []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}}})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed) // binding do not get verified, they get redeclared

	t.Run("WhenInvalid", func(t *testing.T) {
		err := tm.verify(nil, &Topology{Exchanges: []Exchange{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.verify(nil, &Topology{Queues: []Queue{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)

		err = tm.verify(nil, &Topology{Bindings: []Binding{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyValidation)
	})

	t.Run("WhenEmpty", func(t *testing.T) {
		err := tm.verify(nil, &Topology{})
		assert.NoError(t, err)
	})
}

func TestTopologyManagerSync(t *testing.T) {
	tm := newTopologyManager()

	topology := &Topology{
		Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}},
		Queues:    []Queue{{Name: "queue1", Durable: true}},
		Bindings:  []Binding{{Source: "exchange1", Destination: "queue1", Key: "test"}},
	}

	err := tm.sync(nil, topology)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)

	// to trigger diff mechanism and read delete path
	tm.stateMu.Lock()
	tm.exchanges = topology.Exchanges
	tm.queues = topology.Queues
	tm.bindings = topology.Bindings
	tm.stateMu.Unlock()

	err = tm.sync(nil, &Topology{
		Exchanges: []Exchange{{Name: "exchange2", Type: "direct"}},
		Queues:    []Queue{{Name: "queue2", Durable: true}},
		Bindings:  []Binding{{Source: "exchange2", Destination: "queue2", Key: "test"}},
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)

	t.Run("WhenInvalid", func(t *testing.T) {
		err := tm.sync(nil, &Topology{Exchanges: []Exchange{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopology)

		err = tm.sync(nil, &Topology{Queues: []Queue{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopology)

		err = tm.sync(nil, &Topology{Bindings: []Binding{{}}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopology)
	})

	t.Run("WhenEmpty", func(t *testing.T) {
		err := tm.delete(nil, &Topology{})
		assert.NoError(t, err)
	})
}
