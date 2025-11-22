package broker

import (
	"io"

	amqp "github.com/rabbitmq/amqp091-go"
)

type endpoint interface {
	io.Closer
	// last-known running connection (may change after reconnect)
	Connection() *amqp.Connection
	// last-known running channel (may change after reconnect)
	Channel() *amqp.Channel
}

type releaseFunc func(bad bool)
type resourceWrapper[T any] struct {
	resource *T
	release  releaseFunc
}

var defaultExchangeType = "direct"

func defaultExchangeTypeFallback(kind string) string {
	if kind != "" {
		return kind
	}
	return defaultExchangeType
}
