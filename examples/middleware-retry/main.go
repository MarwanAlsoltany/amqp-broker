package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
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

	exchange := broker.NewExchange("example.retry").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.retry.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "retry").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	messages := []string{
		"ok",
		"transient-error",
		"ok",
	}

	for _, body := range messages {
		if err := b.Publish(ctx, exchange.Name, "retry", broker.NewMessage([]byte(body))); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages", len(messages))

	var (
		mu       sync.Mutex
		attempts = map[string]int{}
		total    atomic.Int32
		retried  atomic.Int32
	)

	// RetryMiddleware re-delivers failed messages with exponential backoff,
	// it is transparent: the handler just returns an error and the middleware handles retries;
	// MaxAttempts includes the original attempt, so 3 = 1 original + 2 retries
	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		body := string(msg.Body)

		mu.Lock()
		attempts[body]++
		n := attempts[body]
		mu.Unlock()

		if body == "transient-error" {
			if n < 3 {
				log.Printf("\t%-15s  attempt %d  failed", body, n)
				retried.Add(1)
				return broker.HandlerActionNackDiscard, errors.New("transient error")
			}
			log.Printf("\t%-15s  attempt %d  succeeded", body, n)
		} else {
			log.Printf("\t%-15s  ok", body)
		}

		total.Add(1)
		return broker.HandlerActionAck, nil
	}

	wrapped := broker.WrapHandler(
		handler,
		broker.RetryMiddleware(&broker.RetryMiddlewareConfig{
			MaxAttempts:       3,
			MinBackoff:        500 * time.Millisecond,
			MaxBackoff:        5 * time.Second,
			BackoffMultiplier: 2.0,
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
	timeout := time.After(30 * time.Second) // each retry adds backoff; allow generous timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if total.Load() >= int32(len(messages)) {
				time.Sleep(100 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	mu.Lock()
	var attemptStr strings.Builder
	for body, n := range attempts {
		attemptStr.WriteString(fmt.Sprintf("%s=%d ", body, n))
	}
	mu.Unlock()

	log.Printf("summary: completed=%d retried=%d attempts=[%s]",
		total.Load(), retried.Load(), strings.TrimRight(attemptStr.String(), " "))

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
