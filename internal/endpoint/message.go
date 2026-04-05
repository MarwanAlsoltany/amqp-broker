package endpoint

import (
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

// messageToPublishing converts a message.Message to an AMQP Publishing for sending.
func messageToPublishing(m *message.Message) transport.Publishing {
	return transport.Publishing{
		Headers:         m.Headers,
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode:    m.DeliveryMode,
		Priority:        m.Priority,
		CorrelationId:   m.CorrelationID,
		ReplyTo:         m.ReplyTo,
		Expiration:      m.Expiration,
		MessageId:       m.MessageID,
		Timestamp:       m.Timestamp,
		Type:            m.Type,
		UserId:          m.UserID,
		AppId:           m.AppID,
		Body:            m.Body,
	}
}

// deliveryToMessage converts an AMQP Delivery to a consumed message.Message.
// d is passed as a pointer so it can be used as an Acknowledger (*amqp091.Delivery implements message.Acknowledger).
func deliveryToMessage(d *transport.Delivery) message.Message {
	msg := message.Message{
		Body:            d.Body,
		Headers:         d.Headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationID:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageID:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserID:          d.UserId,
		AppID:           d.AppId,
	}

	info := message.DeliveryInfo{
		DeliveryTag:  d.DeliveryTag,
		ConsumerTag:  d.ConsumerTag,
		Exchange:     d.Exchange,
		RoutingKey:   d.RoutingKey,
		Redelivered:  d.Redelivered,
		MessageCount: d.MessageCount,
	}

	return message.NewConsumedMessage(msg, d, info)
}

// returnToMessage converts an AMQP Return to a returned message.Message.
func returnToMessage(r *transport.Return) message.Message {
	msg := message.Message{
		Body:            r.Body,
		Headers:         r.Headers,
		ContentType:     r.ContentType,
		ContentEncoding: r.ContentEncoding,
		DeliveryMode:    r.DeliveryMode,
		Priority:        r.Priority,
		CorrelationID:   r.CorrelationId,
		ReplyTo:         r.ReplyTo,
		Expiration:      r.Expiration,
		MessageID:       r.MessageId,
		Timestamp:       r.Timestamp,
		Type:            r.Type,
		UserID:          r.UserId,
		AppID:           r.AppId,
	}

	info := message.ReturnInfo{
		ReplyCode:  r.ReplyCode,
		ReplyText:  r.ReplyText,
		Exchange:   r.Exchange,
		RoutingKey: r.RoutingKey,
	}

	return message.NewReturnedMessage(msg, info)
}
