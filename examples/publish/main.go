package main

import (
	"context"
	"log"

	broker "github.com/MarwanAlsoltany/amqp-broker"
)

func main() {
	ctx := context.Background()

	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	exchange := broker.NewExchange("example.publish").
		WithType("direct").
		WithDurable(true)

	queue := broker.NewQueue("example.publish.queue").
		WithDurable(true)

	binding := broker.NewBinding(exchange.Name, queue.Name, "publish")

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{binding},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	// b.Publish() creates a short-lived publisher internally;
	// simple and convenient for occasional or low-frequency publishing
	log.Println("--- one-off publish ---")

	msg := broker.NewMessage([]byte("Hello, World!"))

	if err := b.Publish(ctx, exchange.Name, "publish", msg); err != nil {
		log.Fatal(err)
	}

	log.Println("message published successfully")

	// b.NewPublisher() returns a long-lived publisher that owns its channel;
	// use when publishing frequently, it avoids per-call channel setup overhead
	// and supports publisher confirms via PublisherOptions.ConfirmMode
	log.Println("--- managed publisher ---")

	publisher, err := b.NewPublisher(
		&broker.PublisherOptions{
			EndpointOptions: broker.EndpointOptions{
				NoAutoDeclare: true, // topology already declared above
			},
		},
		exchange,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	for i := 1; i <= 5; i++ {
		msg := broker.NewMessage([]byte("message " + string(rune('0'+i))))
		if err := publisher.Publish(ctx, "publish", msg); err != nil {
			log.Fatal(err)
		}
		log.Printf("\tpublished: message %d", i)
	}

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
