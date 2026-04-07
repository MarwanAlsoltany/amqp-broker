package broker

import (
	"github.com/MarwanAlsoltany/amqp-broker/internal/topology"
)

var (
	// ErrTopology is the base error for topology operations.
	// All topology-related errors wrap this error.
	ErrTopology = topology.ErrTopology

	// ErrTopologyDeclareFailed indicates a topology declaration failed.
	// This occurs when exchange, queue, or binding declaration is rejected by the broker.
	ErrTopologyDeclareFailed = topology.ErrTopologyDeclareFailed

	// ErrTopologyDeleteFailed indicates a topology deletion failed.
	// This occurs when exchange, queue, or binding deletion is rejected by the broker.
	ErrTopologyDeleteFailed = topology.ErrTopologyDeleteFailed

	// ErrTopologyVerifyFailed indicates a topology verification failed.
	// This occurs when passive declaration fails, meaning the entity doesn't exist.
	ErrTopologyVerifyFailed = topology.ErrTopologyVerifyFailed

	// ErrTopologyValidation is the base error for topology validation errors.
	// It wraps specific validation failures like empty names or missing fields.
	ErrTopologyValidation = topology.ErrTopologyValidation

	// ErrTopologyExchangeNameEmpty indicates an exchange name is empty.
	// Exchange names are required for all exchange operations.
	ErrTopologyExchangeNameEmpty = topology.ErrTopologyExchangeNameEmpty

	// ErrTopologyQueueNameEmpty indicates a queue name is empty.
	// Queue names are required for non-exclusive queues.
	ErrTopologyQueueNameEmpty = topology.ErrTopologyQueueNameEmpty

	// ErrTopologyBindingFieldsEmpty indicates binding source or destination is empty.
	// Both source and destination are required for all bindings.
	ErrTopologyBindingFieldsEmpty = topology.ErrTopologyBindingFieldsEmpty

	// ErrTopologyRoutingKeyEmpty indicates a routing key is empty.
	// Some operations require non-empty routing keys.
	ErrTopologyRoutingKeyEmpty = topology.ErrTopologyRoutingKeyEmpty
)

type (
	// Topology is a DTO that groups exchanges, queues, and bindings for bulk operations.
	// It provides methods for declaring, verifying, and deleting complete topologies.
	Topology = topology.Topology

	// Exchange represents an AMQP exchange with configuration and lifecycle methods.
	// Exchanges route messages to queues based on routing rules.
	Exchange = topology.Exchange

	// Queue represents an AMQP queue with configuration and lifecycle methods.
	// Queues store messages until they're consumed.
	Queue = topology.Queue

	// Binding binds a queue to an exchange (or an exchange to another exchange)
	// with a routing key pattern for message filtering.
	Binding = topology.Binding

	// BindingType indicates whether a binding connects to a queue or exchange.
	// Valid values are BindingTypeQueue and BindingTypeExchange.
	BindingType = topology.BindingType

	// RoutingKey represents an AMQP routing key with placeholder substitution.
	// Placeholders like {key} are replaced with values from a map.
	RoutingKey = topology.RoutingKey
)

const (
	// BindingTypeQueue indicates the binding destination is a queue.
	// This is the most common binding type for routing messages to consumers.
	BindingTypeQueue = topology.BindingTypeQueue

	// BindingTypeExchange indicates the binding destination is another exchange.
	// This is used for exchange-to-exchange routing patterns.
	BindingTypeExchange = topology.BindingTypeExchange
)

// NewTopology creates a new [Topology] grouping exchanges, queues, and bindings.
// The topology can be used for bulk declare, verify, delete, and sync operations.
//
// Example:
//
//	topology := NewTopology(
//		[]Exchange{NewExchange("events").WithType("topic")},
//		[]Queue{NewQueue("events.orders")},
//		[]Binding{NewBinding("events", "events.orders", "orders.*")},
//	)
func NewTopology(exchanges []Exchange, queues []Queue, bindings []Binding) Topology {
	return topology.New(exchanges, queues, bindings)
}

// NewExchange creates a new Exchange with the given name and default settings.
// Use the With* methods to configure exchange type, durability, and other options.
//
// Default settings:
//   - Type: "direct"
//   - Durable: true
//   - AutoDelete: false
//   - Internal: false
//
// Example:
//
//	exchange := NewExchange("events").WithType("topic").WithDurable(true)
func NewExchange(name string) Exchange {
	return topology.NewExchange(name)
}

// NewQueue creates a new [Queue] with the given name and default settings.
// Use the With* methods to configure durability, TTL, and other options.
//
// Default settings:
//   - Durable: true
//   - AutoDelete: false
//   - Exclusive: false
//
// Example:
//
//	queue := NewQueue("orders").WithDurable(true).WithArgument("x-message-ttl", 60000)
func NewQueue(name string) Queue {
	return topology.NewQueue(name)
}

// NewBinding creates a new [Binding] connecting a source exchange to a destination.
// The destination can be a queue (default) or another exchange.
// The key parameter is used for routing key pattern matching.
//
// For direct exchanges, key is matched exactly.
// For topic exchanges, key supports wildcards: * (one word), # (zero or more words).
// For fanout exchanges, key is ignored.
// For headers exchanges, use [Arguments] for matching instead of key.
//
// Example:
//
//	// exchange-to-queue binding
//	binding := NewBinding("events", "orders.queue", "orders.created")
//
//	// exchange-to-exchange binding
//	binding := NewBinding("source", "destination", "key").WithType(BindingTypeExchange)
func NewBinding(source, destination, key string) Binding {
	return topology.NewBinding(source, destination, key)
}

// NewRoutingKey creates a RoutingKey with placeholder substitution support.
// Placeholders in the format {name} are replaced with values from the map.
//
// Example:
//
//	// template: "orders.{region}.{action}"
//	rk := NewRoutingKey("orders.{region}.{action}", map[string]string{
//		"region": "us-east",
//		"action": "created",
//	})
//	// result: "orders.us-east.created"
func NewRoutingKey(key string, placeholders map[string]string) RoutingKey {
	return topology.NewRoutingKey(key, placeholders)
}
