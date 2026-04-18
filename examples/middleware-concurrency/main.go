package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	exchange := broker.NewExchange("example.concurrency").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.concurrency.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "concurrency").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	messageCount := 10

	for i := 1; i <= messageCount; i++ {
		body := fmt.Sprintf(`{"id":%d}`, i)
		if err := b.Publish(ctx, exchange.Name, "concurrency", broker.NewMessage([]byte(body))); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages - max 3 concurrent workers", messageCount)

	var (
		processed     atomic.Int32
		current       atomic.Int32
		maxConcurrent atomic.Int32
	)

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		var payload struct{ ID int }
		_ = json.Unmarshal(msg.Body, &payload)

		n := current.Add(1)
		if n > maxConcurrent.Load() {
			maxConcurrent.Store(n)
		}

		log.Printf("\t%-12s  %-9s  concurrent=%d", fmt.Sprintf("[worker-%02d]", payload.ID), "started", n)
		time.Sleep(300 * time.Millisecond)

		current.Add(-1)
		processed.Add(1)
		log.Printf("\t%-12s  done", fmt.Sprintf("[worker-%02d]", payload.ID))

		return broker.HandlerActionAck, nil
	}

	// ConcurrencyMiddleware limits how many handlers run simultaneously;
	// messages beyond Max are buffered until a slot is free,
	// OnWait is called when a message is waiting for a slot,
	// OnAcquire is called when the message acquires a slot
	wrapped := broker.WrapHandler(
		handler,
		broker.ConcurrencyMiddleware(&broker.ConcurrencyMiddlewareConfig{
			Max: 3,
			OnWait: func(ctx context.Context, msg *broker.Message) {
				log.Printf("\t%-12s  waiting for concurrency slot", "[wait]")
			},
			OnAcquire: func(ctx context.Context, msg *broker.Message) {
				log.Printf("\t%-12s  current=%d", "[acquired]", current.Load())
			},
		}),
	)

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
			PrefetchCount:   messageCount,
		},
		queue,
		wrapped,
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
	timeout := time.After(15 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if processed.Load() >= int32(messageCount) {
				time.Sleep(100 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: processed=%d max-concurrent=%d", processed.Load(), maxConcurrent.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
