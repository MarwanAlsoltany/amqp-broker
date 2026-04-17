package main

import (
	"context"
	"log"
	"time"

	broker "github.com/MarwanAlsoltany/amqp-broker"
)

func main() {
	ctx := context.Background()

	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	exchange := broker.NewExchange("example.message").
		WithType("direct").
		WithDurable(true)

	queue := broker.NewQueue("example.message.queue").
		WithDurable(true)

	binding := broker.NewBinding(exchange.Name, queue.Name, "message").
		WithType(broker.BindingTypeQueue)

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{binding},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	publish := func(msg broker.Message) {
		if err := b.Publish(ctx, exchange.Name, "message", msg); err != nil {
			log.Fatal(err)
		}
	}

	// NewMessageBuilder() provides a fluent API for constructing messages,
	// call Build() to produce the Message value; Reset() to reuse the builder
	log.Println("--- message builder ---")
	{
		// plain text
		msg1, _ := broker.NewMessageBuilder().BodyString("hello, world").Text().Build()
		publish(msg1)
		log.Printf("\ttext: content-type=%s", msg1.ContentType)

		// binary body
		msg2, _ := broker.NewMessageBuilder().Body([]byte{0x01, 0x02, 0x03}).Binary().Build()
		publish(msg2)
		log.Printf("\tbinary: content-type=%s len=%d", msg2.ContentType, len(msg2.Body))

		// with custom headers
		msg3, _ := broker.NewMessageBuilder().
			BodyString("message with headers").
			Header("tenant-id", "tenant-123").
			Header("trace-id", "trace-456").
			Build()
		publish(msg3)
		log.Printf("\theaders: %v", msg3.Headers)

		// with priority (0–255)
		msg4, _ := broker.NewMessageBuilder().BodyString("high priority").Priority(9).Build()
		publish(msg4)
		log.Printf("\tpriority: %d", msg4.Priority)

		// with TTL
		msg5, _ := broker.NewMessageBuilder().BodyString("expires in 30s").ExpirationDuration(30 * time.Second).Build()
		publish(msg5)
		log.Printf("\texpiration: %s ms", msg5.Expiration)

		// persistent + timestamp
		msg6, _ := broker.NewMessageBuilder().BodyString("persistent").Persistent().Now().Build()
		publish(msg6)
		log.Printf("\tpersistent: delivery-mode=%d timestamp=%v", msg6.DeliveryMode, msg6.Timestamp.Format(time.RFC3339))

		// all properties at once
		msg7, _ := broker.NewMessageBuilder().
			BodyString("full message").
			ContentType("text/plain").
			DeliveryMode(2).
			Priority(5).
			CorrelationID("corr-123").
			ReplyTo("reply.queue").
			MessageID("msg-456").
			Type("notification").
			AppID("example-app").
			Now().
			Header("version", "1.0").
			Build()
		publish(msg7)
		log.Printf("\tfull message: id=%s type=%s app=%s corr=%s reply-to=%s",
			msg7.MessageID, msg7.Type, msg7.AppID, msg7.CorrelationID, msg7.ReplyTo)

		// reusing builder with Reset()
		builder := broker.NewMessageBuilder()
		for i := 1; i <= 3; i++ {
			m, _ := builder.BodyString("reused builder").MessageID("reused-" + string(rune('0'+i))).Build()
			publish(m)
			builder.Reset()
		}
		log.Println("\treused builder: 3 messages published")
	}

	// BodyJSON() marshals any value to JSON and sets content-type to application/json;
	// JSON() sets content-type without marshalling (use when body is already JSON bytes)
	log.Println("--- json messages ---")
	{
		type User struct {
			ID    int    `json:"id"`
			Name  string `json:"name"`
			Email string `json:"email"`
		}

		type Order struct {
			OrderID string   `json:"order_id"`
			Total   float64  `json:"total"`
			Items   []string `json:"items"`
		}

		// struct -> JSON
		user := User{ID: 1, Name: "John Doe", Email: "john.doe@example.com"}
		msg1, _ := broker.NewMessageBuilder().BodyJSON(user).Build()
		publish(msg1)
		log.Printf("\tstruct: content-type=%s body=%s", msg1.ContentType, string(msg1.Body))

		// nested struct
		order := Order{OrderID: "ORD-001", Total: 99.99, Items: []string{"a", "b"}}
		msg2, _ := broker.NewMessageBuilder().BodyJSON(order).Build()
		publish(msg2)
		log.Printf("\tnested struct: %s", string(msg2.Body))

		// map
		object := map[string]any{"event": "user.created", "ts": "2026-01-01T00:00:00Z"}
		msg3, _ := broker.NewMessageBuilder().BodyJSON(object).Build()
		publish(msg3)
		log.Printf("\tmap: %s", string(msg3.Body))

		// array
		array := []string{"apple", "banana", "cherry"}
		msg4, _ := broker.NewMessageBuilder().BodyJSON(array).Build()
		publish(msg4)
		log.Printf("\tslice: %s", string(msg4.Body))

		// pre-encoded JSON bytes
		json := []byte(`{"message":"pre-encoded JSON"}`)
		msg5, _ := broker.NewMessageBuilder().Body(json).JSON().Build()
		publish(msg5)
		log.Printf("\tpre-encoded: content-type=%s", msg5.ContentType)
	}

	// - delivery mode 1 (transient): lost if broker restarts
	// - delivery mode 2 (persistent): survives restart when queue is also durable
	// - priority: higher value = delivered first (requires x-max-priority queue arg)
	// - expiration: message removed from queue after this many milliseconds
	// - headers: arbitrary key/value pairs passed with the message
	log.Println("--- message properties ---")
	{
		// transient vs persistent
		pt, _ := broker.NewMessageBuilder().BodyString("transient").Transient().Build()
		pp, _ := broker.NewMessageBuilder().BodyString("persistent").Persistent().Build()
		publish(pt)
		publish(pp)
		log.Printf("\ttransient: delivery-mode=%d", pt.DeliveryMode)
		log.Printf("\tpersistent: delivery-mode=%d", pp.DeliveryMode)

		// priority levels
		plo, _ := broker.NewMessageBuilder().BodyString("low priority").Priority(1).Build()
		phi, _ := broker.NewMessageBuilder().BodyString("high priority").Priority(9).Build()
		publish(plo)
		publish(phi)
		log.Printf("\tpriority low=%d high=%d", plo.Priority, phi.Priority)

		// expiration: milliseconds string
		pe1, _ := broker.NewMessageBuilder().BodyString("expires 10s").Expiration("10000").Build()
		pe2, _ := broker.NewMessageBuilder().BodyString("expires 1m").ExpirationDuration(1 * time.Minute).Build()
		publish(pe1)
		publish(pe2)
		log.Printf("\texpiration: %s ms, %s ms", pe1.Expiration, pe2.Expiration)

		// multiple headers at once
		ph, _ := broker.NewMessageBuilder().
			BodyString("multi-header").
			Headers(broker.Arguments{"format": "pdf", "version": "2.0", "encrypted": true}).
			Build()
		publish(ph)
		log.Printf("\theaders: %v", ph.Headers)
	}

	// - correlation-id + reply-to: used in request-reply (RPC) patterns
	// - message-id: unique identifier set by the publisher
	// - timestamp: when the message was created
	// - type + app-id: application-level message classification and source
	log.Println("--- message metadata ---")
	{
		// request-reply metadata
		msg1, _ := broker.NewMessageBuilder().
			BodyString("request data").
			CorrelationID("req-abc-123").
			ReplyTo("response.queue").
			Build()
		publish(msg1)
		log.Printf("\trequest: correlation-id=%s reply-to=%s", msg1.CorrelationID, msg1.ReplyTo)

		// unique message ID
		msg2, _ := broker.NewMessageBuilder().
			BodyString("event notification").
			MessageID("evt-" + time.Now().Format("20060102150405")).
			Build()
		publish(msg2)
		log.Printf("\tmessage-id: %s", msg2.MessageID)

		// explicit timestamp vs Now()
		msg3, _ := broker.NewMessageBuilder().BodyString("explicit ts").Timestamp(time.Now()).Build()
		msg4, _ := broker.NewMessageBuilder().BodyString("now helper").Now().Build()
		publish(msg3)
		publish(msg4)
		log.Printf("\ttimestamp: %s", msg3.Timestamp.Format(time.RFC3339))

		// type + app-id
		msg5, _ := broker.NewMessageBuilder().
			BodyString("order created").
			Type("order.created").
			AppID("order-service").
			Build()
		publish(msg5)
		log.Printf("\ttype=%s app-id=%s", msg5.Type, msg5.AppID)

		// complete RPC-style metadata
		msg6, _ := broker.NewMessageBuilder().
			BodyString("calculate fibonacci(10)").
			CorrelationID("fib-"+time.Now().Format("150405")).
			ReplyTo("fibonacci.reply").
			MessageID("msg-fib-123").
			Type("rpc.request").
			AppID("fib-client").
			Now().
			Header("method", "fibonacci").
			Header("parameter", 10).
			Build()
		publish(msg6)
		log.Printf("\trpc request: id=%s corr=%s reply-to=%s type=%s app=%s",
			msg6.MessageID, msg6.CorrelationID, msg6.ReplyTo, msg6.Type, msg6.AppID)
	}

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
