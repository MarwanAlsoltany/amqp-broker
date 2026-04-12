package transport

import (
	"fmt"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
	"github.com/MarwanAlsoltany/serrors"
)

var (
	// ErrTransport is the base error for transport operations.
	// All transport-related errors wrap this error.
	ErrTransport = internal.ErrDomain.Sentinel("transport")
)

func init() {
	serrors.RegisterTypedFormatFunc(internal.ErrDomain, func(amqpErr *amqp091.Error) string {
		source := "client"
		if amqpErr.Server {
			source = "server"
		}
		return fmt.Sprintf(
			"%s (source=%s, code=%d, recoverable=%v)",
			amqpErr.Reason, source, amqpErr.Code, amqpErr.Recover,
		)
	})
}
