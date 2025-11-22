package broker

// Exchange represents an AMQP exchange declaration.
type Exchange struct {
	// Exchange name
	Name string
	// Exchange type:direct, fanout, topic, headers (default: direct)
	Type string
	// Survives broker restart
	Durable bool
	// Deleted when no bindings remain
	AutoDelete bool
	// Cannot be published to directly
	Internal bool
	// Don't wait for server confirmation
	NoWait bool
	// Additional AMQP arguments
	Args Args
}

// Queue represents an AMQP queue declaration.
type Queue struct {
	// Queue name
	Name string
	// Survives broker restart
	Durable bool
	// Deleted when no consumers remain
	AutoDelete bool
	// Used by only one connection
	Exclusive bool
	// Don't wait for server confirmation
	NoWait bool
	// Additional AMQP arguments
	Args Args
}

// RoutingKey represents an AMQP routing key for message routing.
type RoutingKey string

// Binding represents a queue-to-exchange binding.
type Binding struct {
	// Type of binding destination,
	// either "queue" or "exchange", defaults to: "queue"
	Type BindingType
	// Source name
	Source string
	// Destination name
	Destination string
	// Pattern for routing keys,
	// dot-separated words, * = single word, # = zero or more words
	Pattern string
	// Additional AMQP arguments
	Args Args
}

// BindingType specifies the type of binding destination.
type BindingType string

const (
	BindingTypeQueue    BindingType = "queue"
	BindingTypeExchange BindingType = "exchange"
)

// Topology represents a complete AMQP topology declaration.
type Topology struct {
	Exchanges []Exchange
	Queues    []Queue
	Bindings  []Binding
}

func (t Topology) Empty() bool {
	return len(t.Exchanges) == 0 && len(t.Queues) == 0 && len(t.Bindings) == 0
}

func (t Topology) GetExchange(name string) *Exchange {
	for _, ex := range t.Exchanges {
		if ex.Name == name {
			return &ex
		}
	}
	return nil
}

func (t Topology) GetQueue(name string) *Queue {
	for _, q := range t.Queues {
		if q.Name == name {
			return &q
		}
	}
	return nil
}

func (t Topology) GetBinding(source, destination string) *Binding {
	for _, b := range t.Bindings {
		if b.Source == source && b.Destination == destination {
			return &b
		}
	}
	return nil
}
