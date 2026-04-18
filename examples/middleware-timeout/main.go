package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
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

	exchange := broker.NewExchange("example.timeout").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.timeout.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "timeout").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	type task struct {
		name  string
		sleep int
	}

	tasks := []task{
		{"fast", 100},
		{"medium", 500},
		{"slow", 1500},
		{"fast", 200},
	}

	for i, t := range tasks {
		body := fmt.Sprintf(`{"id":%d,"task":"%s","sleep":%d}`, i+1, t.name, t.sleep)
		if err := b.Publish(ctx, exchange.Name, "timeout", broker.NewMessage([]byte(body))); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d tasks", len(tasks))

	var (
		processed atomic.Int32
		timedOut  atomic.Int32
	)

	// TimeoutMiddleware enforces a per-message time limit,
	// when the deadline is exceeded the message handler context is cancelled,
	// the handler should respect ctx.Done() for early exit;
	// messages that exceed the timeout are nack-discarded by default (via Action)
	timeoutHandler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		processed.Add(1)
		var payload struct {
			ID    int    `json:"id"`
			Task  string `json:"task"`
			Sleep int    `json:"sleep"`
		}
		_ = json.Unmarshal(msg.Body, &payload)

		start := time.Now()
		select {
		case <-time.After(time.Duration(payload.Sleep) * time.Millisecond):
			log.Printf("\ttask %d  %-8s  %-10s  %v", payload.ID, payload.Task, "completed", time.Since(start).Round(time.Millisecond))
			return broker.HandlerActionAck, nil
		case <-ctx.Done():
			log.Printf("\ttask %d  %-8s  %-10s  %v", payload.ID, payload.Task, "timed-out", time.Since(start).Round(time.Millisecond))
			return broker.HandlerActionNackDiscard, fmt.Errorf("timeout: %w", ctx.Err())
		}
	}

	// outer wrapper counts timed-out messages by inspecting the returned action
	actualHandler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		action, err := timeoutHandler(ctx, msg)
		if err != nil && strings.Contains(err.Error(), "timeout") {
			timedOut.Add(1)
		}
		return action, err
	}

	wrapped := broker.WrapHandler(
		actualHandler,
		broker.TimeoutMiddleware(&broker.TimeoutMiddlewareConfig{
			Timeout: 1 * time.Second,
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
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if processed.Load() >= int32(len(tasks)) {
				time.Sleep(500 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: processed=%d timed-out=%d", processed.Load(), timedOut.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
