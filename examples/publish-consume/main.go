package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
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

	exchange := broker.NewExchange("example.bidirectional").
		WithType("direct").
		WithDurable(true)

	queue := broker.NewQueue("example.bidirectional.queue").
		WithDurable(true)

	binding := broker.NewBinding(exchange.Name, queue.Name, "bidirectional")

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{binding},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	var consumed atomic.Int32

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{
				NoAutoDeclare: true,
			},
			PrefetchCount: 1,
		},
		queue,
		func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			n := consumed.Add(1)
			log.Printf("\tconsumed  %d: %s", n, string(msg.Body))
			return broker.HandlerActionAck, nil
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	go func() {
		if err := consumer.Consume(ctx); err != nil {
			log.Printf("consumer error: %v", err)
		}
	}()

	publisher, err := b.NewPublisher(
		&broker.PublisherOptions{
			EndpointOptions: broker.EndpointOptions{
				NoAutoDeclare: true,
			},
		},
		exchange,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	const maxMessages = 5
	var published int

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-sigCh:
			goto cleanup

		case <-ticker.C:
			if published >= maxMessages {
				time.Sleep(500 * time.Millisecond)
				goto cleanup
			}

			published++
			msg := broker.NewMessage([]byte("message " + string(rune('0'+published))))

			if err := publisher.Publish(ctx, "bidirectional", msg); err != nil {
				log.Printf("publish error: %v", err)
				continue
			}

			log.Printf("\tpublished %d: %s", published, string(msg.Body))
		}
	}

cleanup:
	log.Printf("summary: published=%d consumed=%d", published, consumed.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
