package broker

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

/* External types */
type (
	Arguments  = amqp.Table
	Config     = amqp.Config
	Connection = amqp.Connection
	Channel    = amqp.Channel
	AMQPError  = amqp.Error
)

// See: github.com/rabbitmq/amqp091-go/types.go
