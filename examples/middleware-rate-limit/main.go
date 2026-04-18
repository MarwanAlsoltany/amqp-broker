package main

import (
	"context"
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

	exchange := broker.NewExchange("example.ratelimit").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.ratelimit.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "ratelimit").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	messageCount := 15

	for i := 1; i <= messageCount; i++ {
		body := fmt.Sprintf(`{"id":%d}`, i)
		if err := b.Publish(ctx, exchange.Name, "ratelimit", broker.NewMessage([]byte(body))); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages - expect ~%ds processing at RPS=5", messageCount, messageCount/5)

	start := time.Now()
	var processed atomic.Int32

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		n := processed.Add(1)
		log.Printf("\t[msg %02d]  t=%v", n, time.Since(start).Round(time.Millisecond))
		return broker.HandlerActionAck, nil
	}

	// RateLimitMiddleware throttles message processing to the given rate (RPS);
	// it requires a context for lifecycle management (stops the rate limiter when cancelled),
	// set PrefetchCount to messageCount so all messages are delivered immediately,
	// making the rate-limiting effect clearly visible in the log timestamps
	wrapped := broker.WrapHandler(
		handler,
		broker.RateLimitMiddleware(ctx, &broker.RateLimitMiddlewareConfig{
			RPS: 5,
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
	timeout := time.After(20 * time.Second)
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
	elapsed := time.Since(start).Round(time.Millisecond)
	log.Printf("summary: processed=%d elapsed=%v", processed.Load(), elapsed)

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
