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

	exchange := broker.NewExchange("example.logging").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.logging.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "logging").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	messages := []string{"success", "trigger error", "another success"}

	for _, body := range messages {
		if err := b.Publish(ctx, exchange.Name, "logging", broker.NewMessage([]byte(body))); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages", len(messages))

	var processed atomic.Int32

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		processed.Add(1)

		if string(msg.Body) == "trigger error" {
			return broker.HandlerActionNackDiscard, errors.New("simulated handler error")
		}

		return broker.HandlerActionAck, nil
	}

	// LoggingMiddleware logs each message lifecycle event using structured logging;
	// StartLevel, SuccessLevel, ErrorLevel default to slog.LevelInfo (zero value);
	// NoStart/NoSuccess/NoError can be set to suppress specific events;
	// Fields adds extra attributes to every log line from the message
	wrapped := broker.WrapHandler(
		handler,
		broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{
			Logger:       slog.Default(),
			StartLevel:   slog.LevelDebug,
			SuccessLevel: slog.LevelInfo,
			ErrorLevel:   slog.LevelWarn,
			Fields: func(msg *broker.Message) []any {
				return []any{slog.String("body", string(msg.Body))}
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
	log.Printf("summary: processed=%d", processed.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
