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

	// a headers exchange routes by matching message headers against binding arguments:
	// - x-match: "all" requires every specified header to match
	// - x-match: "any" requires at least one header to match
	exchange := broker.NewExchange("orders.headers").WithType("headers").WithDurable(false)

	uQueue := broker.NewQueue("urgent.orders").WithDurable(false).WithAutoDelete(true)
	iQueue := broker.NewQueue("international.orders").WithDurable(false).WithAutoDelete(true)
	pQueue := broker.NewQueue("premium.orders").WithDurable(false).WithAutoDelete(true)

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{uQueue, iQueue, pQueue},
		[]broker.Binding{
			broker.NewBinding(exchange.Name, uQueue.Name, "").
				WithType(broker.BindingTypeQueue).
				WithArguments(broker.Arguments{"x-match": "all", "priority": "urgent"}),
			broker.NewBinding(exchange.Name, iQueue.Name, "").
				WithType(broker.BindingTypeQueue).
				WithArguments(broker.Arguments{"x-match": "all", "region": "international"}),
			broker.NewBinding(exchange.Name, pQueue.Name, "").
				WithType(broker.BindingTypeQueue).
				WithArguments(broker.Arguments{"x-match": "any", "priority": "urgent", "tier": "premium", "vip": true}),
		},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	var totalReceived atomic.Int32

	log.Println("starting order consumers")
	for _, c := range []struct {
		group string
		queue broker.Queue
	}{
		{"urgent-orders", uQueue},
		{"international-orders", iQueue},
		{"premium-orders", pQueue},
	} {
		wg.Add(1)
		group, queue := c.group, c.queue
		go func() {
			defer wg.Done()
			startOrderConsumer(ctx, b, queue, group, &totalReceived)
		}()
	}

	time.Sleep(300 * time.Millisecond)

	orders := []struct {
		headers broker.Arguments
		msg     string
	}{
		{
			broker.Arguments{"priority": "urgent", "region": "domestic", "tier": "standard"},
			"order #1001 - urgent domestic",
		},
		{
			broker.Arguments{"priority": "normal", "region": "international", "tier": "standard"},
			"order #1002 - international",
		},
		{
			broker.Arguments{"priority": "urgent", "region": "international", "tier": "premium"},
			"order #1003 - urgent international premium",
		},
		{
			broker.Arguments{"priority": "normal", "region": "domestic", "tier": "premium"},
			"order #1004 - domestic premium",
		},
		{
			broker.Arguments{"priority": "normal", "region": "domestic", "tier": "standard", "vip": true},
			"order #1005 - VIP",
		},
		{
			broker.Arguments{"priority": "urgent", "region": "domestic", "tier": "standard"},
			"order #1006 - urgent domestic",
		},
	}

	// expected deliveries per queue: urgent=3, international=2, premium=5 -> total=10
	log.Printf("publishing %d orders (expected total deliveries: 10)", len(orders))
	for i, o := range orders {
		msg := broker.NewMessage([]byte(o.msg))
		msg.Headers = o.headers
		if err := b.Publish(ctx, exchange.Name, "", msg); err != nil {
			log.Fatal(err)
		}
		log.Printf("\t[%02d] %s headers=%v", i+1, o.msg, o.headers)
		time.Sleep(100 * time.Millisecond)
	}

	expected := 10

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
	log.Printf("summary: published=%d received=%d/%d", len(orders), totalReceived.Load(), expected)

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

func startOrderConsumer(ctx context.Context, b *broker.Broker, queue broker.Queue, group string, total *atomic.Int32) {
	var received atomic.Int32

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		n := received.Add(1)
		log.Printf("\t[%s] #%d: %s", group, n, string(msg.Body))
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

	log.Printf("\t[%s] started", group)
	if err := consumer.Consume(ctx); err != nil {
		log.Printf("\t[%s] stopped: %v", group, err)
	}
	log.Printf("\t[%s] done: received=%d", group, received.Load())
}
