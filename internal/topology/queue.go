package topology

import (
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

const (
	DefaultQueueDurable = true
)

var (
	// ErrTopologyQueueNameEmpty indicates a queue name is empty.
	// Queue names are required for non-exclusive queues.
	ErrTopologyQueueNameEmpty = ErrTopologyValidation.Derive("queue name empty")
)

// Queue represents an AMQP queue with configuration and lifecycle methods.
// Queues store messages until they're consumed.
//
// The With* methods use value receivers and return modified copies, leaving the original unchanged.
// This allows for safe, immutable-style configuration:
//
//	q := NewQueue("notifications").WithDurable(true).WithArgument("x-message-ttl", 60000)
//	// original from NewQueue is unmodified
type Queue struct {
	// Queue name
	Name string
	// Survives broker restart
	Durable bool
	// Deleted when no consumers remain
	AutoDelete bool
	// Used by only one connection
	Exclusive bool
	// Additional AMQP arguments
	Arguments Arguments
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
	return Queue{
		Name:       name,
		Durable:    DefaultQueueDurable,
		AutoDelete: false,
		Exclusive:  false,
		Arguments:  nil,
	}
}

// Matches returns true if this queue matches another by name.
func (q Queue) Matches(other Queue) bool {
	return q.Name == other.Name
}

// Validate returns an error if the queue is invalid.
func (q Queue) Validate() error {
	if q.Name == "" {
		return ErrTopologyQueueNameEmpty
	}
	return nil
}

// Declare declares this queue using the provided channel.
// If the operation causes a channel-level error (e.g. precondition failed),
// the error will include information about the channel closure reason.
//
// When the error return value is not nil, you can assume the queue could not be
// declared with these parameters, and the channel will be closed.
func (q Queue) Declare(ch transport.Channel) error {
	if err := q.Validate(); err != nil {
		return err
	}

	err := transport.DoSafeChannelAction(ch, func(ch transport.Channel) error {
		_, err := ch.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false, // no-wait
			q.Arguments,
		)
		return err
	})

	if err != nil {
		return ErrTopologyDeclareFailed.Detailf("queue %q: %w", q.Name, err)
	}

	return nil
}

// Verify checks if this queue exists without creating it (passive declaration).
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
func (q Queue) Verify(ch transport.Channel) error {
	if err := q.Validate(); err != nil {
		return err
	}

	err := transport.DoSafeChannelAction(ch, func(ch transport.Channel) error {
		_, err := ch.QueueDeclarePassive(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false, // no-wait
			q.Arguments,
		)
		return err
	})

	if err != nil {
		return ErrTopologyVerifyFailed.Detailf("queue %q: %w", q.Name, err)
	}

	return nil
}

// Delete removes this queue and returns the number of purged messages.
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
//
// When this queue does not exist, the channel will be closed with an error.
//
// When ifUnused is true, the queue will not be deleted if there are any
// consumers on the queue. If there are consumers, an error will be returned and
// the channel will be closed.
//
// When ifEmpty is true, the queue will not be deleted if there are any messages
// remaining on the queue. If there are messages, an error will be returned and
// the channel will be closed.
func (q Queue) Delete(ch transport.Channel, ifUnused, ifEmpty bool) (int, error) {
	if err := q.Validate(); err != nil {
		return 0, err
	}

	count, err := transport.DoSafeChannelActionWithReturn(ch, func(ch transport.Channel) (int, error) {
		return ch.QueueDelete(q.Name, ifUnused, ifEmpty, false /* no-wait */)
	})

	if err != nil {
		return count, ErrTopologyDeleteFailed.Detailf("queue %q: %w", q.Name, err)
	}

	return count, nil
}

// Purge removes all messages from this queue.
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
func (q Queue) Purge(ch transport.Channel) (int, error) {
	if err := q.Validate(); err != nil {
		return 0, err
	}

	return transport.DoSafeChannelActionWithReturn(ch, func(ch transport.Channel) (int, error) {
		return ch.QueuePurge(q.Name, false /* no-wait */)
	})
}

// Inspect retrieves queue information including message and consumer counts.
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
//
// If a queue by this name does not exist, an error will be returned and the channel will be closed.
func (q Queue) Inspect(ch transport.Channel) (*struct{ Messages, Consumers int }, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}

	return transport.DoSafeChannelActionWithReturn(ch, func(ch transport.Channel) (*struct{ Messages, Consumers int }, error) {
		info, err := ch.QueueDeclarePassive(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false, // no-wait
			q.Arguments,
		)
		if err != nil {
			return nil, err
		}
		return &struct{ Messages, Consumers int }{
			Messages:  info.Messages,
			Consumers: info.Consumers,
		}, nil
	})
}

// WithDurable sets whether the queue survives broker restart.
func (q Queue) WithDurable(durable bool) Queue {
	q.Durable = durable
	return q
}

// WithAutoDelete sets whether the queue is deleted when no consumers remain.
func (q Queue) WithAutoDelete(autoDelete bool) Queue {
	q.AutoDelete = autoDelete
	return q
}

// WithExclusive sets whether the queue is used by only one connection.
func (q Queue) WithExclusive(exclusive bool) Queue {
	q.Exclusive = exclusive
	return q
}

// WithArguments sets the additional AMQP arguments.
func (q Queue) WithArguments(args Arguments) Queue {
	q.Arguments = args
	return q
}

// WithArgument adds or updates a single argument.
func (q Queue) WithArgument(key string, value any) Queue {
	if q.Arguments == nil {
		q.Arguments = make(Arguments)
	}
	q.Arguments[key] = value
	return q
}
