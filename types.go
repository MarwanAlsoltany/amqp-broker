package broker

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RoutingKey string

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type Binding struct {
	Exchange   string
	Queue      string
	RoutingKey string
	Args       amqp.Table
}

type Topology struct {
	Exchanges []Exchange
	Queues    []Queue
	Bindings  []Binding
}

// PublishOptions controls publish behaviour. If WaitForConfirm is set true,
// use NewPublisher with confirm mode for best results (Broker.Publish will
// use a short-lived confirm publisher when WaitForConfirm=true).
type PublishOptions struct {
	DeliveryMode   uint8
	ContentType    string
	Headers        amqp.Table
	Mandatory      bool
	Immediate      bool
	WaitForConfirm bool
	ConfirmTimeout time.Duration
}

// ConsumeOptions controls consume behavior.
type ConsumeOptions struct {
	AutoAck       bool
	PrefetchCount int
	RequeueOnErr  bool
	Bindings      []Binding
}

// AckAction is ack control
type AckAction int

const (
	AckActionAck AckAction = iota
	AckActionNackRequeue
	AckActionNackDiscard
	AckActionNoAction // handler already acked/nacked
)

type Message struct {
	Body        []byte
	Headers     amqp.Table
	ContentType string

	Delivery *amqp.Delivery

	Ack    func() error
	Nack   func(requeue bool) error
	Reject func() error

	Broker *Broker
}

type Handler func(ctx context.Context, msg *Message) (AckAction, error)
