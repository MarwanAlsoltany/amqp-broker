package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
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

	// a topic exchange routes by pattern matching on dot-separated routing keys;
	// '*' matches exactly one word, '#' matches zero or more words
	exchange := broker.NewExchange("metrics.topic").WithType("topic").WithDurable(false)

	allMetrics := broker.NewQueue("all.metrics").WithDurable(false).WithAutoDelete(true)
	webMetrics := broker.NewQueue("web.metrics").WithDurable(false).WithAutoDelete(true)
	criticalMetrics := broker.NewQueue("critical.metrics").WithDurable(false).WithAutoDelete(true)

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{allMetrics, webMetrics, criticalMetrics},
		[]broker.Binding{
			broker.NewBinding(exchange.Name, allMetrics.Name, "#").WithType(broker.BindingTypeQueue),
			broker.NewBinding(exchange.Name, webMetrics.Name, "web.*.*").WithType(broker.BindingTypeQueue),
			broker.NewBinding(exchange.Name, criticalMetrics.Name, "*.*.critical").WithType(broker.BindingTypeQueue),
		},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	var totalReceived atomic.Int32

	log.Println("starting metric consumers")
	for _, c := range []struct {
		name  string
		queue broker.Queue
	}{
		{"all-metrics", allMetrics},
		{"web-metrics", webMetrics},
		{"critical-metrics", criticalMetrics},
	} {
		wg.Add(1)
		name, queue := c.name, c.queue
		go func() {
			defer wg.Done()
			startMetricsConsumer(ctx, b, queue, name, &totalReceived)
		}()
	}

	time.Sleep(300 * time.Millisecond)

	metrics := []struct {
		topic string
		msg   string
	}{
		{"web.requests.normal", "200 OK: 1543 responses"},
		{"web.latency.critical", "response time >5s: 23 requests"},
		{"db.connections.normal", "active connections: 45"},
		{"db.queries.critical", "slow queries: 12"},
		{"web.errors.critical", "5xx errors: 8"},
		{"cache.hits.normal", "cache hit ratio: 87%"},
		{"web.bandwidth.normal", "bandwidth: 2.3 GB"},
		{"api.rate.critical", "rate limit exceeded: 156 times"},
	}

	// expected: all=8, web.*.*=4, *.*.critical=4 -> total=16
	log.Printf("publishing %d metrics (expected total deliveries: 16)", len(metrics))
	for i, m := range metrics {
		if err := b.Publish(ctx, exchange.Name, m.topic, broker.NewMessage([]byte(m.msg))); err != nil {
			log.Fatal(err)
		}
		log.Printf("\t[%02d] [%s] %s", i+1, m.topic, m.msg)
		time.Sleep(100 * time.Millisecond)
	}

	expected := 16

	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if totalReceived.Load() >= int32(expected) {
				time.Sleep(200 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: published=%d received=%d/%d", len(metrics), totalReceived.Load(), expected)

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

func startMetricsConsumer(ctx context.Context, b *broker.Broker, queue broker.Queue, name string, total *atomic.Int32) {
	var received atomic.Int32

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		n := received.Add(1)
		log.Printf("\t[%s] #%d: %s", name, n, string(msg.Body))
		total.Add(1)
		return broker.HandlerActionAck, nil
	}

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
		},
		queue, handler,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	log.Printf("\t[%s] started", name)
	if err := consumer.Consume(ctx); err != nil {
		log.Printf("\t[%s] stopped: %v", name, err)
	}
	log.Printf("\t[%s] done: received=%d", name, received.Load())
}
