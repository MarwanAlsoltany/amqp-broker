package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"os"
	"os/signal"
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

	exchange := broker.NewExchange("example.composition").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.composition.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "composition").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	testOrders := []struct {
		order     Order
		duplicate bool
		invalid   bool
	}{
		{Order{ID: "ORD-001", Product: "Laptop", Quantity: 1, Price: 999.99}, false, false},
		{Order{ID: "ORD-001", Product: "Laptop", Quantity: 1, Price: 999.99}, true, false}, // duplicate
		{Order{ID: "ORD-002", Product: "", Quantity: 1, Price: 49.99}, false, true},        // invalid: no product
		{Order{ID: "ORD-003", Product: "Mouse", Quantity: 2, Price: 29.99}, false, false},
		{Order{ID: "ORD-004", Product: "Keyboard", Quantity: 1, Price: 79.99}, false, false},
	}

	for _, t := range testOrders {
		data, _ := json.Marshal(t.order)
		msg := broker.NewMessage(data)
		msg.ContentType = "application/json"
		msg.MessageID = t.order.ID
		if err := b.Publish(ctx, exchange.Name, "composition", msg); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages (1 duplicate, 1 invalid)", len(testOrders))

	var (
		processed atomic.Int32
		succeeded atomic.Int32
	)

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		processed.Add(1)

		var order Order
		_ = json.Unmarshal(msg.Body, &order)

		// simulate a transient error on the last order's first attempt
		if order.ID == "ORD-004" && processed.Load() == 4 {
			log.Printf("\t[handler]  %-9s  %s (will retry)", "transient", order.ID)
			return broker.HandlerActionNoAction, errors.New("temporary connection error")
		}

		succeeded.Add(1)
		log.Printf("\t[handler]  %-9s  %s %s qty=%d price=%.2f",
			"processed", order.ID, order.Product, order.Quantity, order.Price)
		return broker.HandlerActionAck, nil
	}

	validateOrder := func(msg *broker.Message) error {
		if msg.ContentType != "application/json" {
			return errors.New("invalid content type")
		}
		var o Order
		if err := json.Unmarshal(msg.Body, &o); err != nil {
			return errors.New("invalid JSON")
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
	}

	// WrapHandler applies middleware in declaration order (outermost first), the call chain is:
	//    debug -> metrics -> logging -> recovery -> timeout -> concurrency -> rate-limit ->
	//    deduplication -> validation -> retry -> HANDLER
	// each layer has a single responsibility; compose them to build a robust processing pipeline
	wrapped := broker.WrapHandler(
		handler,
		broker.DebugMiddleware(&broker.DebugMiddlewareConfig{
			Logger: slog.Default(),
			Level:  slog.LevelDebug,
		}),
		broker.MetricsMiddleware(&broker.MetricsMiddlewareConfig{
			Logger: slog.Default(),
			Level:  slog.LevelDebug,
		}),
		broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{
			Logger: slog.Default(),
		}),
		broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{
			Logger: slog.Default(),
			Action: broker.HandlerActionNackDiscard,
		}),
		broker.TimeoutMiddleware(&broker.TimeoutMiddlewareConfig{
			Timeout: 5 * time.Second,
		}),
		broker.ConcurrencyMiddleware(&broker.ConcurrencyMiddlewareConfig{
			Max: 3,
		}),
		broker.RateLimitMiddleware(ctx, &broker.RateLimitMiddlewareConfig{
			RPS: 10,
		}),
		broker.DeduplicationMiddleware(&broker.DeduplicationMiddlewareConfig{
			Cache:    newMemoryCache(),
			Identify: func(msg *broker.Message) string { return msg.MessageID },
			Action:   broker.HandlerActionAck,
		}),
		broker.ValidationMiddleware(&broker.ValidationMiddlewareConfig{
			Validate: validateOrder,
			Action:   broker.HandlerActionNackDiscard,
		}),
		broker.RetryMiddleware(&broker.RetryMiddlewareConfig{
			MaxAttempts: 2,
			MinBackoff:  500 * time.Millisecond,
			MaxBackoff:  2 * time.Second,
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
	timeout := time.After(20 * time.Second) // extra time for retries
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if succeeded.Load() >= 3 { // 3 valid unique orders
				time.Sleep(100 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: published=%d handler-calls=%d succeeded=%d filtered=%d",
		len(testOrders), processed.Load(), succeeded.Load(), int32(len(testOrders))-succeeded.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

// Order represents a business message.
type Order struct {
	ID       string  `json:"id"`
	Product  string  `json:"product"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// MemoryCache is a thread-safe in-memory deduplication cache.
type MemoryCache struct {
	m  map[string]bool
	mu sync.Mutex
}

func newMemoryCache() *MemoryCache {
	return &MemoryCache{m: make(map[string]bool)}
}

// Seen returns true the first time an ID is seen and records it; false for duplicates.
func (c *MemoryCache) Seen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[id] {
		return false
	}
	c.m[id] = true
	return true
}
