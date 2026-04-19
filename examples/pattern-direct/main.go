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

	// a direct exchange routes messages to the queue whose binding key exactly
	// matches the routing key; use it when you need precise per-severity routing
	exchange := broker.NewExchange("logs.direct").WithType("direct").WithDurable(false)
	errorQueue := broker.NewQueue("logs.error").WithDurable(false).WithAutoDelete(true)
	warningQueue := broker.NewQueue("logs.warning").WithDurable(false).WithAutoDelete(true)
	infoQueue := broker.NewQueue("logs.info").WithDurable(false).WithAutoDelete(true)

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{errorQueue, warningQueue, infoQueue},
		[]broker.Binding{
			broker.NewBinding(exchange.Name, errorQueue.Name, "error").WithType(broker.BindingTypeQueue),
			broker.NewBinding(exchange.Name, warningQueue.Name, "warning").WithType(broker.BindingTypeQueue),
			broker.NewBinding(exchange.Name, infoQueue.Name, "info").WithType(broker.BindingTypeQueue),
		},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	var totalReceived atomic.Int32

	log.Println("starting log consumers")
	for _, c := range []struct {
		severity string
		queue    broker.Queue
	}{
		{"error", errorQueue},
		{"warning", warningQueue},
		{"info", infoQueue},
	} {
		wg.Add(1)
		severity, queue := c.severity, c.queue
		go func() {
			defer wg.Done()
			startLogConsumer(ctx, b, queue, severity, &totalReceived)
		}()
	}

	time.Sleep(300 * time.Millisecond)

	logs := []struct {
		key string
		msg string
	}{
		{"error", "database connection failed"},
		{"warning", "high memory usage detected"},
		{"info", "application started"},
		{"error", "failed to process payment"},
		{"info", "user logged in"},
		{"warning", "slow query detected"},
		{"info", "request processed in 100ms"},
		{"error", "invalid configuration"},
	}

	log.Printf("publishing %d log messages", len(logs))
	for i, l := range logs {
		if err := b.Publish(ctx, exchange.Name, l.key, broker.NewMessage([]byte(l.msg))); err != nil {
			log.Fatal(err)
		}
		log.Printf("\t[%02d] [%s] %s", i+1, l.key, l.msg)
		time.Sleep(100 * time.Millisecond)
	}

	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if totalReceived.Load() >= int32(len(logs)) {
				time.Sleep(200 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: published=%d received=%d", len(logs), totalReceived.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

func startLogConsumer(ctx context.Context, b *broker.Broker, queue broker.Queue, severity string, total *atomic.Int32) {
	var received atomic.Int32

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		n := received.Add(1)
		log.Printf("\t[%s] [%02d]: %s", severity, n, string(msg.Body))
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

	log.Printf("\t[%s consumer] started", severity)
	if err := consumer.Consume(ctx); err != nil {
		log.Printf("\t[%s consumer] stopped: %v", severity, err)
	}
	log.Printf("\t[%s consumer] done: received=%d", severity, received.Load())
}
