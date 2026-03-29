package transport

import (
	"context"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Connection represents an AMQP connection with minimal required methods.
// It provides methods for managing channels and connection lifecycle.
// This interface abstracts the underlying AMQP library for testability.
type Connection interface {
	// See [amqp091.Connection.Channel]
	Channel() (Channel, error)

	// See [amqp091.Connection.Close]
	Close() error
	// See [amqp091.Connection.IsClosed]
	IsClosed() bool
	// See [amqp091.Connection.NotifyClose]
	NotifyClose(receiver chan *Error) chan *Error
	// See [amqp091.Connection.NotifyBlocked]
	NotifyBlocked(receiver chan Blocking) chan Blocking
}

// Channel represents an AMQP channel within a connection.
// It provides methods for publishing, consuming, and topology operations.
// This interface includes only the methods actually used by the broker.
type Channel interface {
	/* Publishing Methods */

	// See [amqp091.Channel.PublishWithContext]
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) error
	// See [amqp091.Channel.PublishWithDeferredConfirmWithContext]
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) (*DeferredConfirmation, error)
	// See [amqp091.Channel.Confirm]
	Confirm(noWait bool) error
	// See [amqp091.Channel.NotifyPublish]
	NotifyPublish(confirm chan Confirmation) chan Confirmation
	// See [amqp091.Channel.NotifyReturn]
	NotifyReturn(returns chan Return) chan Return
	// See [amqp091.Channel.NotifyFlow]
	NotifyFlow(flow chan bool) chan bool

	/* Consumption Methods */

	// See [amqp091.Channel.Consume]
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args Arguments) (<-chan Delivery, error)
	// See [amqp091.Channel.Get]
	Get(queue string, autoAck bool) (msg Delivery, ok bool, err error)
	// See [amqp091.Channel.Qos]
	Qos(prefetchCount, prefetchSize int, global bool) error
	// See [amqp091.Channel.Cancel]
	Cancel(consumer string, noWait bool) error
	// See [amqp091.Channel.NotifyCancel]
	NotifyCancel(cancellations chan string) chan string

	/* Topology Methods */

	// See [amqp091.Channel.ExchangeDeclare]
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args Arguments) error
	// See [amqp091.Channel.ExchangeDeclarePassive]
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args Arguments) error
	// See [amqp091.Channel.ExchangeDelete]
	ExchangeDelete(name string, ifUnused, noWait bool) error
	// See [amqp091.Channel.ExchangeBind]
	ExchangeBind(destination, key, source string, noWait bool, args Arguments) error
	// See [amqp091.Channel.ExchangeUnbind]
	ExchangeUnbind(destination, key, source string, noWait bool, args Arguments) error

	// See [amqp091.Channel.QueueDeclare]
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args Arguments) (Queue, error)
	// See [amqp091.Channel.QueueDeclarePassive]
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args Arguments) (Queue, error)
	// See [amqp091.Channel.QueueDelete]
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	// See [amqp091.Channel.QueueBind]
	QueueBind(name, key, exchange string, noWait bool, args Arguments) error
	// See [amqp091.Channel.QueueUnbind]
	QueueUnbind(name, key, exchange string, args Arguments) error
	// See [amqp091.Channel.QueuePurge]
	QueuePurge(name string, noWait bool) (int, error)

	/* Transaction Methods */

	// See [amqp091.Channel.Tx]
	Tx() error
	// See [amqp091.Channel.TxCommit]
	TxCommit() error
	// See [amqp091.Channel.TxRollback]
	TxRollback() error

	/* Lifecycle Methods */

	// See [amqp091.Channel.Close]
	Close() error
	// See [amqp091.Channel.IsClosed]
	IsClosed() bool
	// See [amqp091.Channel.NotifyClose]
	NotifyClose(receiver chan *Error) chan *Error
}

// Type aliases for compatibility with amqp091-go types.
// These allow us to use the same types without direct dependency in user code.
type (
	Config               = amqp091.Config
	Arguments            = amqp091.Table
	Error                = amqp091.Error
	Queue                = amqp091.Queue
	Publishing           = amqp091.Publishing
	Delivery             = amqp091.Delivery
	Return               = amqp091.Return
	Confirmation         = amqp091.Confirmation
	DeferredConfirmation = amqp091.DeferredConfirmation
	Blocking             = amqp091.Blocking
	Acknowledger         = amqp091.Acknowledger
)
