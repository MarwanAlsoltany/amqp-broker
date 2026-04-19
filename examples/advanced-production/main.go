package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics := &Metrics{}

	b, err := broker.New(
		broker.WithURL("amqp://guest:guest@localhost:5672/"),
		broker.WithConnectionPoolSize(3),
		broker.WithConnectionReconnectConfig(false, 1*time.Second, 10*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	// durable topic exchange + queue with message TTL for production use
	exchange := broker.NewExchange("production.orders").WithType("topic").WithDurable(true)
	queue := broker.NewQueue("production.orders.processing").
		WithDurable(true).
		WithArgument("x-message-ttl", 3600000) // 1h TTL

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{
			broker.NewBinding(exchange.Name, queue.Name, "order.*").WithType(broker.BindingTypeQueue),
		},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	// deduplication cache with TTL expiry; see the advanced-idempotency example for details.
	// in production, replace with a distributed cache (Redis, Memcached) for multi-instance deployments
	dedupCache := newTTLCache(1*time.Hour, 5*time.Minute)

	processOrder := func(order *Order) error {
		log.Printf("\t[business] %s: %s qty=%d", order.ID, order.Product, order.Quantity)
		time.Sleep(100 * time.Millisecond)
		if order.Quantity < 0 {
			return fmt.Errorf("invalid quantity: %d", order.Quantity)
		}
		return nil
	}

	baseHandler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		metrics.received.Add(1)

		var order Order
		if err := json.Unmarshal(msg.Body, &order); err != nil {
			metrics.failed.Add(1)
			return broker.HandlerActionNackDiscard, err
		}

		if err := processOrder(&order); err != nil {
			metrics.failed.Add(1)
			return broker.HandlerActionNackDiscard, err
		}

		metrics.processed.Add(1)
		return broker.HandlerActionAck, nil
	}

	// production middleware stack: recovery -> timeout -> deduplication -> validation -> HANDLER
	handler := broker.WrapHandler(
		baseHandler,
		broker.DeduplicationMiddleware(&broker.DeduplicationMiddlewareConfig{
			// wrap the TTLCache so duplicates increment the metrics counter
			Cache:    &countingCache{inner: dedupCache, onDup: func() { metrics.duplicates.Add(1) }},
			Identify: func(msg *broker.Message) string { return msg.MessageID },
			Action:   broker.HandlerActionAck,
		}),
		broker.LoggingMiddleware(&broker.LoggingMiddlewareConfig{
			Logger: slog.Default(),
		}),
		broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{
			Logger: slog.Default(),
			Action: broker.HandlerActionNackDiscard,
			OnPanic: func(ctx context.Context, msg *broker.Message, recovered any) {
				log.Printf("\t[panic] %v", recovered)
				metrics.panics.Add(1)
			},
		}),
		broker.TimeoutMiddleware(&broker.TimeoutMiddlewareConfig{
			Timeout: 5 * time.Second,
		}),
		broker.ValidationMiddleware(&broker.ValidationMiddlewareConfig{
			Validate: func(msg *broker.Message) error {
				var o Order
				if err := json.Unmarshal(msg.Body, &o); err != nil {
					return fmt.Errorf("invalid JSON: %w", err)
				}
				if o.ID == "" {
					return fmt.Errorf("id required")
				}
				if o.Quantity == 0 {
					return fmt.Errorf("quantity must be non-zero")
				}
				return nil
			},
		}),
	)

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{PrefetchCount: 5},
		queue,
		handler,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Consume(ctx); err != nil && ctx.Err() == nil {
			log.Printf("consumer error: %v", err)
		}
	}()

	time.Sleep(300 * time.Millisecond)

	orders := []Order{
		{ID: "ORD-001", Product: "Laptop", Quantity: 1, UserID: "user-1", Created: time.Now()},
		{ID: "ORD-002", Product: "Mouse", Quantity: 2, UserID: "user-2", Created: time.Now()},
		{ID: "ORD-001", Product: "Laptop", Quantity: 1, UserID: "user-1", Created: time.Now()}, // duplicate
		{ID: "ORD-003", Product: "Keyboard", Quantity: 1, UserID: "user-3", Created: time.Now()},
		{ID: "ORD-004", Product: "Monitor", Quantity: -1, UserID: "user-4", Created: time.Now()}, // invalid
		{ID: "ORD-005", Product: "Headphones", Quantity: 1, UserID: "user-5", Created: time.Now()},
	}

	log.Printf("publishing %d orders (1 duplicate, 1 invalid)", len(orders))
	for i, o := range orders {
		data, _ := json.Marshal(o)
		msg, err := broker.NewMessageBuilder().
			Body(data).
			MessageID(o.ID).
			JSON().
			Persistent().
			Priority(5).
			Build()
		if err != nil {
			log.Fatal(err)
		}
		if err := b.Publish(ctx, exchange.Name, "order.created", msg); err != nil {
			log.Printf("\tpublish error: %v", err)
		} else {
			log.Printf("\t[%02d] %s %s qty=%d", i+1, o.ID, o.Product, o.Quantity)
		}
		time.Sleep(100 * time.Millisecond)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	timeout := time.After(12 * time.Second)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case sig := <-sigCh:
			log.Printf("signal: %v", sig)
			goto shutdown
		case <-timeout:
			goto shutdown
		case <-ticker.C:
			log.Println("--- metrics ---")
			stats := metrics.Report()
			log.Printf("\treceived=%d processed=%d failed=%d duplicates=%d panics=%d",
				stats.Received, stats.Processed, stats.Failed, stats.Duplicates, stats.Panics)
		}
	}

shutdown:
	log.Println("shutting down ...")
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("consumer stopped")
	case <-time.After(10 * time.Second):
		log.Println("shutdown timeout")
	}

	log.Println("--- final metrics ---")

	stats := metrics.Report()

	log.Printf("\treceived=%d processed=%d failed=%d duplicates=%d panics=%d",
		stats.Received, stats.Processed, stats.Failed, stats.Duplicates, stats.Panics)

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

// countingCache wraps a TTLCache and calls onDup whenever a duplicate is detected.
type countingCache struct {
	inner *TTLCache
	onDup func()
}

func (c *countingCache) Seen(id string) bool {
	ok := c.inner.Seen(id)
	if !ok {
		c.onDup()
	}
	return ok
}

// TTLCache is a thread-safe deduplication cache with per-entry TTL expiry.
// it satisfies the DeduplicationMiddlewareConfig.Cache interface.
// see the advanced-idempotency example for full documentation.
type TTLCache struct {
	mu      sync.RWMutex
	entries map[string]time.Time
	ttl     time.Duration
}

func newTTLCache(ttl, gcInterval time.Duration) *TTLCache {
	c := &TTLCache{entries: make(map[string]time.Time), ttl: ttl}
	go func() {
		ticker := time.NewTicker(gcInterval)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			c.mu.Lock()
			for id, exp := range c.entries {
				if now.After(exp) {
					delete(c.entries, id)
				}
			}
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *TTLCache) Seen(id string) bool {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if exp, ok := c.entries[id]; ok && now.Before(exp) {
		return false
	}
	c.entries[id] = now.Add(c.ttl)
	return true
}

// Order represents a business domain object.
type Order struct {
	ID       string    `json:"id"`
	Product  string    `json:"product"`
	Quantity int       `json:"quantity"`
	UserID   string    `json:"user_id"`
	Created  time.Time `json:"created"`
}

// Metrics tracks system health counters.
type Metrics struct {
	received   atomic.Int64
	processed  atomic.Int64
	failed     atomic.Int64
	duplicates atomic.Int64
	panics     atomic.Int64
}

type MetricsReport struct {
	Received   int64 `json:"received"`
	Processed  int64 `json:"processed"`
	Failed     int64 `json:"failed"`
	Duplicates int64 `json:"duplicates"`
	Panics     int64 `json:"panics"`
}

func (m *Metrics) Report() MetricsReport {
	return MetricsReport{
		Received:   m.received.Load(),
		Processed:  m.processed.Load(),
		Failed:     m.failed.Load(),
		Duplicates: m.duplicates.Load(),
		Panics:     m.panics.Load(),
	}
}
