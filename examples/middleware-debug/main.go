package main

import (
	"context"
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

	exchange := broker.NewExchange("example.debug").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.debug.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "debug").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	messages := []struct {
		body        string
		contentType string
		headers     map[string]any
	}{
		{
			body:        "plain text message",
			contentType: "text/plain",
			headers:     map[string]any{"priority": "high"},
		},
		{
			body:        `{"order_id":"12345","amount":99.99}`,
			contentType: "application/json",
			headers:     map[string]any{"source": "api", "version": "v1"},
		},
		{
			body:        "a message without custom headers",
			contentType: "text/plain",
			headers:     nil,
		},
	}

	for _, m := range messages {
		msg := broker.NewMessage([]byte(m.body))
		msg.ContentType = m.contentType
		if len(m.headers) > 0 {
			msg.Headers = m.headers
		}
		if err := b.Publish(ctx, exchange.Name, "debug", msg); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages", len(messages))

	var processed atomic.Int32

	// DebugMiddleware logs all message fields before the handler runs;
	// Level controls the slog level (zero value = slog.LevelInfo),
	// Fields extracts the slog attributes to log; defaults to id, content-type, headers, body
	handler := broker.WrapHandler(
		func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			n := processed.Add(1)
			log.Printf("\thandler: message %d done", n)
			return broker.HandlerActionAck, nil
		},
		broker.DebugMiddleware(&broker.DebugMiddlewareConfig{
			Logger: slog.Default(),
			Level:  slog.LevelDebug,
			Fields: func(msg *broker.Message) []any {
				body := string(msg.Body)
				if len(body) > 128 {
					body = body[:128] + "...(truncated)"
				}
				return []any{
					slog.String("id", msg.MessageID),
					slog.String("contentType", msg.ContentType),
					slog.Any("headers", msg.Headers),
					slog.String("body", body),
				}
			},
		}),
	)

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
			PrefetchCount:   1,
		},
		queue,
		handler,
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
