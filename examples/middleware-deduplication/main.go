package main

import (
	"context"
	"log"
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

	exchange := broker.NewExchange("example.deduplication").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.deduplication.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "deduplication").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	msgs := []struct{ id, body string }{
		{"msg-1", "first"},
		{"msg-2", "second"},
		{"msg-1", "duplicate of first"},
		{"msg-3", "third"},
		{"msg-2", "duplicate of second"},
		{"msg-4", "fourth"},
		{"msg-1", "another duplicate of first"},
	}

	for _, m := range msgs {
		msg := broker.NewMessage([]byte(m.body))
		msg.MessageID = m.id
		if err := b.Publish(ctx, exchange.Name, "deduplication", msg); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages (%d unique ids, %d duplicates)", len(msgs), 4, len(msgs)-4)

	var unique atomic.Int32

	cache := newMemoryCache()

	// DeduplicationMiddleware skips messages whose id has been seen before;
	// provide a Cache implementation and an optional Identify function (default: msg.MessageID);
	// duplicates receive Action (default: Ack) and are not forwarded to the handler
	handler := broker.WrapHandler(
		func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			unique.Add(1)
			log.Printf("\t%-10s  id=%-7s  %s", "[unique]", msg.MessageID, string(msg.Body))
			return broker.HandlerActionAck, nil
		},
		broker.DeduplicationMiddleware(&broker.DeduplicationMiddlewareConfig{
			Cache:    cache,
			Identify: func(msg *broker.Message) string { return msg.MessageID },
			Action:   broker.HandlerActionAck,
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
			if unique.Load() >= 4 {
				time.Sleep(50 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: unique=%d duplicates=%d (inferred)", unique.Load(), int32(len(msgs))-unique.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

// MemoryCache is a thread-safe in-memory deduplication cache with no expiry.
// for a TTL-based implementation see the advanced-idempotency example.
type MemoryCache struct {
	mu sync.Mutex
	m  map[string]bool
}

func newMemoryCache() *MemoryCache {
	return &MemoryCache{m: make(map[string]bool)}
}

// Seen returns true the first time an id is seen and records it; false for duplicates.
func (c *MemoryCache) Seen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[id] {
		return false
	}
	c.m[id] = true
	return true
}
