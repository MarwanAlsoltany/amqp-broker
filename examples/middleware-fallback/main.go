package main

import (
	"context"
	"errors"
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

	exchange := broker.NewExchange("example.fallback").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.fallback.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "fallback").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	msgs := []string{
		"ok",
		"trigger-error",
		"ok",
		"trigger-error",
		"ok",
	}

	for _, body := range msgs {
		if err := b.Publish(ctx, exchange.Name, "fallback", broker.NewMessage([]byte(body))); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages", len(msgs))

	var (
		primaryOK    atomic.Int32
		fallbackUsed atomic.Int32
	)

	primary := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		body := string(msg.Body)
		if body == "trigger-error" {
			log.Printf("\t%-18s  %s", "[primary-failure]", body)
			return broker.HandlerActionNoAction, errors.New("primary error")
		}
		primaryOK.Add(1)
		log.Printf("\t%-18s  %s", "[primary-success]", body)
		return broker.HandlerActionAck, nil
	}

	fallbackHandler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		fallbackUsed.Add(1)
		log.Printf("\t%-18s  %s", "[fallback]", string(msg.Body))
		return broker.HandlerActionNackDiscard, nil
	}

	// FallbackMiddleware calls a secondary handler when the primary returns an error;
	// ShouldFallback decides whether to invoke the fallback based on action + error;
	// the fallback's (action, error) replaces the primary's result
	handler := broker.WrapHandler(
		primary,
		broker.FallbackMiddleware(&broker.FallbackMiddlewareConfig{
			Fallback:       fallbackHandler,
			ShouldFallback: func(err error, action broker.HandlerAction) bool { return err != nil },
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
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if primaryOK.Load()+fallbackUsed.Load() >= int32(len(msgs)) {
				time.Sleep(50 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: primary-passed=%d fallback-used=%d", primaryOK.Load(), fallbackUsed.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
