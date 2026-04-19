package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	exchange := broker.NewExchange("example.idempotency").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.idempotency.queue").WithDurable(true)
	binding := broker.NewBinding(exchange.Name, queue.Name, "idempotent")

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{binding},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	// TTLCache is the custom DeduplicationMiddleware.Cache implementation defined below;
	// it tracks message ids with per-entry expiry and a background GC goroutine
	cache := newTTLCache(5*time.Minute, 1*time.Minute)

	var (
		received  atomic.Int32
		processed atomic.Int32
	)

	// DeduplicationMiddleware checks the cache before forwarding to the handler;
	// the Identify func prefers the explicit MessageID, falling back to a SHA-256
	// content hash for publishers that do not set it, both strategies give the
	// same idempotency guarantee for at-least-once delivery
	handler := broker.WrapHandler(
		func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			n := processed.Add(1)

			// business logic
			log.Printf("\t[%02d] processing: %q", n, string(msg.Body))
			time.Sleep(50 * time.Millisecond)
			log.Printf("\t[%02d] done", n)

			return broker.HandlerActionAck, nil
		},
		broker.DeduplicationMiddleware(&broker.DeduplicationMiddlewareConfig{
			Cache: cache,
			Identify: func(msg *broker.Message) string {
				received.Add(1)
				if msg.MessageID != "" {
					return msg.MessageID
				}
				// fall back to content hash when the publisher does not set MessageID
				id := hash(msg.Body)
				log.Printf("\t     no message-id, using content hash: %s...", id[:16])
				return id
			},
			Action: broker.HandlerActionAck,
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
			log.Printf("consumer: %v", err)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	orders := []struct {
		id, content string
	}{
		{"order-001", "product-A x 2"},
		{"order-002", "product-B x 1"},
		{"order-001", "product-A x 2"}, // duplicate id
		{"order-003", "product-C x 3"},
		{"", "product-D x 1"},          // no id: content hash used
		{"", "product-D x 1"},          // duplicate content
		{"order-002", "product-B x 1"}, // duplicate id
		{"order-004", "product-E x 5"},
		{"", "product-F x 1"},
		{"", "product-F x 1"}, // duplicate content
	}

	log.Printf("publishing %d orders (includes duplicates)", len(orders))

	for i, o := range orders {
		msg := broker.NewMessage([]byte(o.content))
		if o.id != "" {
			msg.MessageID = o.id
			log.Printf("\t[%02d] id=%-9s  content=%q", i+1, o.id, o.content)
		} else {
			log.Printf("\t[%02d] id=%-9s  content=%q", i+1, "none", o.content)
		}
		if err := b.Publish(ctx, exchange.Name, "idempotent", msg); err != nil {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	timeout := time.After(15 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if received.Load() >= int32(len(orders)) {
				time.Sleep(100 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	time.Sleep(200 * time.Millisecond)
	log.Printf("summary: received=%d processed=%d duplicates=%d cache-size=%d",
		received.Load(), processed.Load(), received.Load()-processed.Load(), cache.Size())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

// hash returns the SHA-256 hex digest of b.
// used as a fallback deduplication id when the publisher does not set MessageID.
func hash(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// TTLCache is a thread-safe deduplication cache with per-entry TTL expiry.
// it satisfies the DeduplicationMiddlewareConfig.Cache interface:
//
//	Seen(id string) bool
//
// entries expire after ttl; a background goroutine evicts expired entries every gc interval.
// for a simpler no-expiry implementation see the middleware-deduplication example.
type TTLCache struct {
	mu      sync.RWMutex
	entries map[string]time.Time
	ttl     time.Duration
}

func newTTLCache(ttl, gcInterval time.Duration) *TTLCache {
	c := &TTLCache{
		entries: make(map[string]time.Time),
		ttl:     ttl,
	}
	go c.gc(gcInterval)
	return c
}

// Seen returns true the first time a non-expired id is seen and records it.
// returns false for duplicates (id already present and not yet expired).
func (c *TTLCache) Seen(id string) bool {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if exp, ok := c.entries[id]; ok && now.Before(exp) {
		return false // duplicate
	}
	c.entries[id] = now.Add(c.ttl)
	return true
}

// Size returns the number of entries currently in the cache (including potentially expired ones).
func (c *TTLCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// gc removes expired entries on the given interval.
func (c *TTLCache) gc(interval time.Duration) {
	ticker := time.NewTicker(interval)
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
}
