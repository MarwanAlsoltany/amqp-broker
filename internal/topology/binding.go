package topology

import (
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

var (
	// ErrTopologyBindingFieldsEmpty indicates binding source or destination is empty.
	// Both source and destination are required for all bindings.
	ErrTopologyBindingFieldsEmpty = ErrTopologyValidation.Derive("binding field(s) empty")
)

// BindingType indicates whether a binding connects to a queue or exchange.
type BindingType string

const (
	// BindingTypeQueue indicates the binding destination is a queue.
	// This is the most common binding type for routing messages to consumers.
	BindingTypeQueue BindingType = "queue"
	// BindingTypeExchange indicates the binding destination is another exchange.
	// This is used for exchange-to-exchange routing patterns.
	BindingTypeExchange BindingType = "exchange"
)

// Binding binds a queue to an exchange (or an exchange to another exchange)
// with a routing key pattern for message filtering.
//
// The With* methods use value receivers and return modified copies, leaving the original unchanged.
// This allows for safe, immutable-style configuration:
//
//	b := NewBinding("source", "destination", "key").WithType(BindingTypeExchange)
//	// original from NewBinding is unmodified
type Binding struct {
	// Type of binding destination,
	// either "queue" or "exchange", defaults to: "queue"
	Type BindingType
	// Source exchange name
	Source string
	// Destination queue or exchange name
	Destination string
	// Key used to match against message routing keys for filtering.
	// The matching behavior depends on the source exchange type:
	//   - direct: exact match (e.g. "orders.created")
	//   - topic: pattern match with wildcards (e.g. "orders.*" or "*.created" or "orders.#")
	//     where * matches exactly one word and # matches zero or more words
	//   - fanout: ignored, all messages are routed regardless of key
	//   - headers: ignored, matching is based on message headers and Arguments
	// For queue bindings, this filters which messages from the source exchange
	// are routed to the destination queue. For exchange-to-exchange bindings,
	// this filters which messages are forwarded to the destination exchange.
	Key string
	// Additional AMQP arguments for headers exchange matching or custom behavior
	Arguments Arguments
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
	return Binding{
		Type:        BindingTypeQueue,
		Source:      source,
		Destination: destination,
		Key:         key,
		Arguments:   nil,
	}
}

// Matches returns true if this binding matches another by source, destination, and key.
func (b Binding) Matches(other Binding) bool {
	return b.Source == other.Source && b.Destination == other.Destination && b.Key == other.Key
}

// Validate returns an error if the binding is invalid.
func (b Binding) Validate() error {
	if b.Source == "" || b.Destination == "" {
		return ErrTopologyBindingFieldsEmpty
	}
	return nil
}

// Declare declares (binds) this binding using the provided channel.
// If the operation causes a channel-level error (e.g. source/destination not found),
// the error will include information about the channel closure reason.
func (b Binding) Declare(ch transport.Channel) error {
	if err := b.Validate(); err != nil {
		return err
	}

	err := transport.DoSafeChannelAction(ch, func(ch transport.Channel) error {
		if b.Type == BindingTypeExchange {
			return ch.ExchangeBind(
				b.Destination,
				b.Key,
				b.Source,
				false, // no-wait
				b.Arguments,
			)
		}

		return ch.QueueBind(
			b.Destination,
			b.Key,
			b.Source,
			false, // no-wait
			b.Arguments,
		)
	})

	if err != nil {
		return ErrTopologyDeclareFailed.Detailf("%s binding %q -> %q: %w", b.Type, b.Source, b.Destination, err)
	}

	return nil
}

// Delete removes (unbinds) this binding using the provided channel.
// If the operation causes a channel-level error (e.g. binding not found),
// the error will include information about the channel closure reason.
func (b Binding) Delete(ch transport.Channel) error {
	if err := b.Validate(); err != nil {
		return err
	}

	err := transport.DoSafeChannelAction(ch, func(ch transport.Channel) error {
		if b.Type == BindingTypeExchange {
			return ch.ExchangeUnbind(
				b.Destination,
				b.Key,
				b.Source,
				false, // no-wait
				b.Arguments,
			)
		}

		return ch.QueueUnbind(
			b.Destination,
			b.Key,
			b.Source,
			b.Arguments,
		)
	})

	if err != nil {
		return ErrTopologyDeleteFailed.Detailf("%s binding %q -> %q: %w", b.Type, b.Source, b.Destination, err)
	}

	return nil
}

// WithType sets the binding destination type (queue or exchange).
func (b Binding) WithType(t BindingType) Binding {
	b.Type = t
	return b
}

// WithArguments sets the additional AMQP arguments.
func (b Binding) WithArguments(args Arguments) Binding {
	b.Arguments = args
	return b
}

// WithArgument adds or updates a single argument.
func (b Binding) WithArgument(key string, value any) Binding {
	if b.Arguments == nil {
		b.Arguments = make(Arguments)
	}
	b.Arguments[key] = value
	return b
}
