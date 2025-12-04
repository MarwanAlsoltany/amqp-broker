package broker

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// noCopy may be embedded into structs which must not be copied after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527 for details.
type noCopy struct{}

var _ sync.Locker = (*noCopy)(nil)

// Lock is a no-op used by -copylocks checker from "go vet".
func (*noCopy) Lock() {}

// Unlock is a no-op used by -copylocks checker from "go vet".
func (*noCopy) Unlock() {}

/* External types, aliased for convenience */

// See: github.com/rabbitmq/amqp091-go
type (
	Arguments  = amqp.Table
	Config     = amqp.Config
	Connection = amqp.Connection
	Channel    = amqp.Channel
	AMQPError  = amqp.Error
)
