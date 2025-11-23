package broker

import (
	"strings"
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

func (t Topology) Exchange(name string) *Exchange {
	for _, ex := range t.Exchanges {
		if ex.Name == name {
			return &ex
		}
	}
	return nil
}

func (t Topology) Queue(name string) *Queue {
	for _, q := range t.Queues {
		if q.Name == name {
			return &q
		}
	}
	return nil
}

func (t Topology) Binding(source, destination string) *Binding {
	for _, b := range t.Bindings {
		if b.Source == source && b.Destination == destination {
			return &b
		}
	}
	return nil
}

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
	Args Arguments
}

// Declare declares this exchange using the provided channel.
func (e Exchange) Declare(ch *Channel) error {
	if e.Name == "" {
		return ErrEmptyExchangeName
	}

	kind := e.Type
	if kind == "" {
		kind = defaultExchangeType
	}

	return ch.ExchangeDeclare(
		e.Name,
		kind,
		e.Durable,
		e.AutoDelete,
		e.Internal,
		e.NoWait,
		e.Args,
	)
}

// DeclarePassive checks if this exchange exists without creating it.
func (e Exchange) DeclarePassive(ch *Channel) error {
	if e.Name == "" {
		return ErrEmptyExchangeName
	}

	kind := e.Type
	if kind == "" {
		kind = defaultExchangeType
	}

	return ch.ExchangeDeclarePassive(
		e.Name,
		kind,
		e.Durable,
		e.AutoDelete,
		e.Internal,
		e.NoWait,
		e.Args,
	)
}

// Delete deletes this exchange.
func (e Exchange) Delete(ch *Channel, ifUnused, noWait bool) error {
	if e.Name == "" {
		return ErrEmptyExchangeName
	}
	return ch.ExchangeDelete(e.Name, ifUnused, noWait)
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
	Args Arguments
}

// Declare declares this queue using the provided channel.
func (q Queue) Declare(ch *Channel) error {
	if q.Name == "" {
		return ErrEmptyQueueName
	}

	_, err := ch.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args,
	)
	return err
}

// DeclarePassive checks if this queue exists without creating it.
func (q Queue) DeclarePassive(ch *Channel) error {
	if q.Name == "" {
		return ErrEmptyQueueName
	}

	_, err := ch.QueueDeclarePassive(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args,
	)
	return err
}

// Delete deletes this queue.
func (q Queue) Delete(ch *Channel, ifUnused, ifEmpty, noWait bool) (int, error) {
	if q.Name == "" {
		return 0, ErrEmptyQueueName
	}
	return ch.QueueDelete(q.Name, ifUnused, ifEmpty, noWait)
}

// Purge removes all messages from this queue.
func (q Queue) Purge(ch *Channel, noWait bool) (int, error) {
	if q.Name == "" {
		return 0, ErrEmptyQueueName
	}
	return ch.QueuePurge(q.Name, noWait)
}

// Inspect retrieves queue information including message and consumer counts.
func (q Queue) Inspect(ch *Channel) (*struct{ Messages, Consumers int }, error) {
	if q.Name == "" {
		return nil, ErrEmptyQueueName
	}

	info, err := ch.QueueDeclarePassive(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args,
	)
	if err != nil {
		return nil, err
	}

	return &struct{ Messages, Consumers int }{
		Messages:  info.Messages,
		Consumers: info.Consumers,
	}, nil
}

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
	Args Arguments
}

// Declare declares this binding using the provided channel.
func (bn Binding) Declare(ch *Channel) error {
	if bn.Source == "" || bn.Destination == "" {
		return ErrEmptyBindingName
	}

	if bn.Type == BindingTypeExchange {
		return ch.ExchangeBind(
			bn.Destination,
			bn.Pattern,
			bn.Source,
			false, // no-wait
			bn.Args,
		)
	}

	return ch.QueueBind(
		bn.Destination,
		bn.Pattern,
		bn.Source,
		false, // no-wait
		bn.Args,
	)
}

// Delete removes (unbinds) this binding using the provided channel.
func (bn Binding) Delete(ch *Channel, args Arguments) error {
	if bn.Source == "" || bn.Destination == "" {
		return ErrEmptyBindingName
	}

	if bn.Type == BindingTypeExchange {
		return ch.ExchangeUnbind(bn.Destination, bn.Pattern, bn.Source, false, args)
	}

	return ch.QueueUnbind(bn.Destination, bn.Pattern, bn.Source, args)
}

// BindingType specifies the type of binding destination.
type BindingType string

const (
	BindingTypeQueue    BindingType = "queue"
	BindingTypeExchange BindingType = "exchange"
)

// RoutingKey represents an AMQP routing key for message routing.
type RoutingKey string

// Inject replaces placeholders in the routing key with values from the provided map.
// Placeholders are in the format {key}. For example, given a routing key "user.{id}.update"
// and args map {"id": "123"}, their Inject method will produce "user.123.update".
func (rk *RoutingKey) Inject(args map[string]string) {
	// replace {} placeholders with values from args
	pattern := string(*rk)
	for k, v := range args {
		placeholder := "{" + k + "}"
		pattern = strings.ReplaceAll(pattern, placeholder, v)
	}
	*rk = RoutingKey(pattern)
}

func (rk RoutingKey) String() string {
	return string(rk)
}
