package topology

import (
	"fmt"

	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

const (
	DefaultExchangeType    = "direct"
	DefaultExchangeDurable = true
)

var (
	// ErrTopologyExchangeNameEmpty indicates an exchange name is empty.
	// Exchange names are required for all exchange operations.
	ErrTopologyExchangeNameEmpty = fmt.Errorf("%w: exchange name empty", ErrTopologyValidation)
)

// Exchange represents an AMQP exchange with configuration and lifecycle methods.
// Exchanges route messages to queues based on routing rules.
//
// The With* methods use value receivers and return modified copies, leaving the original unchanged.
// This allows for safe, immutable-style configuration:
//
//	ex := NewExchange("events").WithType("topic").WithDurable(true)
//	// original from NewExchange is unmodified
type Exchange struct {
	// Exchange name
	Name string
	// Exchange type: direct, fanout, topic, headers (default: direct)
	Type string
	// Survives broker restart
	Durable bool
	// Deleted when no bindings remain
	AutoDelete bool
	// Cannot be published to directly (for inter-exchange topologies only)
	Internal bool
	// Additional AMQP arguments
	Arguments Arguments
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
	return Exchange{
		Name:       name,
		Type:       DefaultExchangeType,
		Durable:    DefaultExchangeDurable,
		AutoDelete: false,
		Internal:   false,
		Arguments:  nil,
	}
}

// Matches returns true if this exchange matches another by name.
func (e Exchange) Matches(other Exchange) bool {
	return e.Name == other.Name
}

// Validate returns an error if the exchange is invalid.
func (e Exchange) Validate() error {
	if e.Name == "" {
		return ErrTopologyExchangeNameEmpty
	}
	return nil
}

// Declare declares this exchange using the provided channel.
// If the operation causes a channel-level error (e.g. precondition failed),
// the error will include information about the channel closure reason.
func (e Exchange) Declare(ch transport.Channel) error {
	if err := e.Validate(); err != nil {
		return err
	}

	kind := e.Type
	if kind == "" {
		kind = DefaultExchangeType
	}

	err := transport.DoSafeChannelAction(ch, func(ch transport.Channel) error {
		return ch.ExchangeDeclare(
			e.Name,
			kind,
			e.Durable,
			e.AutoDelete,
			e.Internal,
			false, // no-wait
			e.Arguments,
		)
	})

	if err != nil {
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyDeclareFailed, e.Name, err)
	}

	return nil
}

// Verify checks if this exchange exists without creating it (passive declaration).
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
func (e Exchange) Verify(ch transport.Channel) error {
	if err := e.Validate(); err != nil {
		return err
	}

	kind := e.Type
	if kind == "" {
		kind = DefaultExchangeType
	}

	err := transport.DoSafeChannelAction(ch, func(ch transport.Channel) error {
		return ch.ExchangeDeclarePassive(
			e.Name,
			kind,
			e.Durable,
			e.AutoDelete,
			e.Internal,
			false, // no-wait
			e.Arguments,
		)
	})

	if err != nil {
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyVerifyFailed, e.Name, err)
	}

	return nil
}

// Delete removes this exchange.
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
//
// When ifUnused is true, the exchange will not be deleted if there are any
// queues or exchanges bound to it. If there are bindings, an error will be
// returned and the channel will be closed.
//
// If this exchange does not exist, the channel will be closed with an error.
func (e Exchange) Delete(ch transport.Channel, ifUnused bool) error {
	if err := e.Validate(); err != nil {
		return err
	}

	err := transport.DoSafeChannelAction(ch, func(ch transport.Channel) error {
		return ch.ExchangeDelete(e.Name, ifUnused, false /* no-wait */)
	})

	if err != nil {
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyDeleteFailed, e.Name, err)
	}

	return nil
}

// WithType sets the exchange type (direct, fanout, topic, headers).
func (e Exchange) WithType(t string) Exchange {
	e.Type = t
	return e
}

// WithDurable sets whether the exchange survives broker restart.
func (e Exchange) WithDurable(durable bool) Exchange {
	e.Durable = durable
	return e
}

// WithAutoDelete sets whether the exchange is deleted when no bindings remain.
func (e Exchange) WithAutoDelete(autoDelete bool) Exchange {
	e.AutoDelete = autoDelete
	return e
}

// WithInternal sets whether the exchange cannot be published to directly.
func (e Exchange) WithInternal(internal bool) Exchange {
	e.Internal = internal
	return e
}

// WithArguments sets the additional AMQP arguments.
func (e Exchange) WithArguments(args Arguments) Exchange {
	e.Arguments = args
	return e
}

// WithArgument adds or updates a single argument.
func (e Exchange) WithArgument(key string, value any) Exchange {
	if e.Arguments == nil {
		e.Arguments = make(Arguments)
	}
	e.Arguments[key] = value
	return e
}
