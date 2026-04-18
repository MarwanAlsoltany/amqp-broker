package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
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

	exchange := broker.NewExchange("example.recovery").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.recovery.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "recovery").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	messages := []string{
		"ok",
		"panic-string",
		"ok",
		"panic-error",
		"ok",
	}

	for _, body := range messages {
		if err := b.Publish(ctx, exchange.Name, "recovery", broker.NewMessage([]byte(body))); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages", len(messages))

	var (
		processed atomic.Int32
		recovered atomic.Int32
	)

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		processed.Add(1)
		body := string(msg.Body)

		switch body {
		case "panic-string":
			panic("simulated string panic")
		case "panic-error":
			panic(errors.New("simulated error panic"))
		}

		log.Printf("\t%-11s  %s", "[processed]", body)
		return broker.HandlerActionAck, nil
	}

	// RecoveryMiddleware catches panics in the handler, logs them, and returns an error;
	// Action controls what happens to the message after recovery (default: NackRequeue),
	// Logger defaults to slog.DiscardHandler when nil, set it to capture panic details,
	// OnPanic is called after logging; use it for alerting or metrics
	wrapped := broker.WrapHandler(
		handler,
		broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{
			Logger: slog.Default(),
			Level:  slog.LevelError,
			Action: broker.HandlerActionNackDiscard,
			OnPanic: func(ctx context.Context, msg *broker.Message, panicValue any) {
				recovered.Add(1)
				log.Printf("\t%-11s  %v", "[recovered]", panicValue)
			},
		}),
	)

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
			PrefetchCount:   1,
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
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if processed.Load() >= int32(len(messages)) {
				time.Sleep(100 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: processed=%d recovered=%d", processed.Load(), recovered.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
