package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	exchanges := []Exchange{{Name: "exchange", Type: "direct"}}
	queues := []Queue{{Name: "queue"}}
	bindings := []Binding{{Source: "exchange", Destination: "queue", Key: "test"}}

	topology := New(exchanges, queues, bindings)

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
			// from topology1 with different type
			{Name: "exchange1", Type: "fanout"},
		},
		Queues:   []Queue{topology1.Queues[0]},
		Bindings: []Binding{topology2.Bindings[0]},
	}

	merged := topology1.Merge(topology2).Merge(topology3)

	// exchange1 should be from topology3 (overridden)
	exchange := merged.Exchange("exchange1")
	assert.NotNil(t, exchange)
	assert.Equal(t, "fanout", exchange.Type, "merged exchange should use type from topology3")

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
		Exchanges: []Exchange{{Name: "exchange1", Type: "direct"}},
		Queues:    []Queue{{Name: "queue1", Durable: true}},
		Bindings:  []Binding{{Source: "exchange1", Destination: "queue1", Key: "key1"}},
	}

	err := topology.Validate()
	assert.NoError(t, err)

	t.Run("WhenInvalid", func(t *testing.T) {
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
	})
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
