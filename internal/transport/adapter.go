package transport

import (
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// connectionAdapter wraps [amqp091.Connection] to implement the Connection interface.
type connectionAdapter struct {
	// uses struct embedding to automatically inherit all methods
	*amqp091.Connection
}

var _ Connection = (*connectionAdapter)(nil)

// Channel opens a new channel and returns it wrapped in our Channel interface.
// This override is necessary to return the interface type instead of the concrete type.
func (c *connectionAdapter) Channel() (Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return &channelAdapter{Channel: ch}, nil
}

// channelAdapter wraps [amqp091.Channel] to implement the Channel interface.
type channelAdapter struct {
	// uses struct embedding to automatically inherit all methods.
	*amqp091.Channel
}

var _ Channel = (*channelAdapter)(nil)
