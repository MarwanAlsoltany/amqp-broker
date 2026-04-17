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

	exchange := broker.NewExchange("example.consume").
		WithType("direct").
		WithDurable(true)

	queue := broker.NewQueue("example.consume.queue").
		WithDurable(true)

	binding := broker.NewBinding(exchange.Name, queue.Name, "consume")

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{binding},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	// b.Consume() uses a short-lived consumer internally,
	// the call blocks until the context is cancelled or an error occurs
	log.Println("--- one-off consume ---")

	msg := broker.NewMessage([]byte("hello, consumer!"))
	if err := b.Publish(ctx, exchange.Name, "consume", msg); err != nil {
		log.Fatal(err)
	}

	consumeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := b.Consume(consumeCtx, queue.Name, func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		log.Printf("\treceived: %s", string(msg.Body))
		return broker.HandlerActionAck, nil
	}); err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		log.Fatal(err)
	}

	log.Println("consumer stopped")

	// b.NewConsumer() returns a long-lived consumer with its own channel,
	// which supports prefetch, concurrent handlers, and lifecycle management;
	// run Consume() in a goroutine, it blocks until ctx is cancelled or Close() is called
	log.Println("--- managed consumer ---")

	for i := 1; i <= 5; i++ {
		m := broker.NewMessage([]byte("message " + string(rune('0'+i))))
		if err := b.Publish(ctx, exchange.Name, "consume", m); err != nil {
			log.Fatal(err)
		}
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
			log.Printf("\tconsumed %d: %s", n, string(msg.Body))
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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-ticker.C:
			if consumed.Load() >= 5 {
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: consumed=%d", consumed.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
