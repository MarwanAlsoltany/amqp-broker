package main

import (
	"context"
	"encoding/json"
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

	exchange := broker.NewExchange("example.validation").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.validation.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "validation").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	type Order struct {
		ID       string  `json:"id"`
		Product  string  `json:"product"`
		Quantity int     `json:"quantity"`
		Price    float64 `json:"price"`
	}

	orders := []Order{
		{ID: "1", Product: "Laptop", Quantity: 1, Price: 999.99},
		{ID: "", Product: "Mouse", Quantity: 2, Price: 29.99},     // missing id
		{ID: "3", Product: "", Quantity: 1, Price: 49.99},         // missing product
		{ID: "4", Product: "Keyboard", Quantity: 0, Price: 79.99}, // zero quantity
		{ID: "5", Product: "Monitor", Quantity: 1, Price: -1},     // negative price
		{ID: "6", Product: "Headphones", Quantity: 1, Price: 149.99},
	}

	for _, o := range orders {
		data, _ := json.Marshal(o)
		msg := broker.NewMessage(data)
		msg.ContentType = "application/json"
		if err := b.Publish(ctx, exchange.Name, "validation", msg); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d orders", len(orders))

	var (
		accepted atomic.Int32
		rejected atomic.Int32
	)

	// ValidationMiddleware rejects messages that fail a custom predicate;
	// invalid messages receive Action (default: NackDiscard) and never reach the handler
	handler := broker.WrapHandler(
		func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			accepted.Add(1)
			var o Order
			_ = json.Unmarshal(msg.Body, &o)
			log.Printf("\t%-10s  %-7s  %-14s  quantity=%d  price=%.2f", "[valid]", o.ID, o.Product, o.Quantity, o.Price)
			return broker.HandlerActionAck, nil
		},
		broker.ValidationMiddleware(&broker.ValidationMiddlewareConfig{
			Validate: func(msg *broker.Message) error {
				if msg.ContentType != "application/json" {
					return errors.New("invalid content type")
				}
				var o Order
				if err := json.Unmarshal(msg.Body, &o); err != nil {
					return errors.New("invalid json")
				}
				if o.ID == "" {
					return errors.New("id required")
				}
				if o.Product == "" {
					return errors.New("product required")
				}
				if o.Quantity <= 0 {
					return errors.New("quantity must be positive")
				}
				if o.Price < 0 {
					return errors.New("price cannot be negative")
				}
				return nil
			},
			Action: broker.HandlerActionNackDiscard,
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
			if accepted.Load()+rejected.Load() >= int32(len(orders)) {
				time.Sleep(50 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	// rejected count is inferred: messages discarded by middleware do not pass through the handler
	log.Printf("summary: accepted=%d rejected=%d (inferred)", accepted.Load(), int32(len(orders))-accepted.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
