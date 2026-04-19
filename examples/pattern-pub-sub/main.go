package main

import (
	"context"
	"fmt"
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

	now := time.Now().UnixNano()

	// a fanout exchange broadcasts every message to all bound queues,
	// each consumer/subscriber gets its own exclusive, auto-delete queue
	exchange := broker.NewExchange("events.broadcast").
		WithType("fanout").WithDurable(false)
	sub1 := broker.NewQueue(fmt.Sprintf("events.sub1.%d", now)).
		WithExclusive(true).WithAutoDelete(true)
	sub2 := broker.NewQueue(fmt.Sprintf("events.sub2.%d", now)).
		WithExclusive(true).WithAutoDelete(true)
	sub3 := broker.NewQueue(fmt.Sprintf("events.sub3.%d", now)).
		WithExclusive(true).WithAutoDelete(true)

	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{sub1, sub2, sub3},
		[]broker.Binding{
			broker.NewBinding(exchange.Name, sub1.Name, "").WithType(broker.BindingTypeQueue),
			broker.NewBinding(exchange.Name, sub2.Name, "").WithType(broker.BindingTypeQueue),
			broker.NewBinding(exchange.Name, sub3.Name, "").WithType(broker.BindingTypeQueue),
		},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	var totalReceived atomic.Int32

	log.Printf("starting %d subscribers", len(topology.Queues))
	for i, q := range topology.Queues {
		wg.Add(1)
		id := i + 1
		queue := q
		go func() {
			defer wg.Done()
			startSubscriber(ctx, b, queue, id, &totalReceived)
		}()
	}

	time.Sleep(300 * time.Millisecond)

	events := []string{
		"user logged in",
		"order placed",
		"payment processed",
		"inventory updated",
		"email sent",
	}

	log.Printf("publishing %d events to fanout exchange", len(events))
	for i, event := range events {
		if err := b.Publish(ctx, exchange.Name, "", broker.NewMessage([]byte(event))); err != nil {
			log.Fatal(err)
		}
		log.Printf("\t[%02d] published: %s", i+1, event)
		time.Sleep(100 * time.Millisecond)
	}

	expected := len(events) * len(topology.Queues) // each subscriber receives every event

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
	log.Printf("summary: events=%d subscribers=3 total-received=%d/%d",
		len(events), totalReceived.Load(), expected)

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

func startSubscriber(ctx context.Context, b *broker.Broker, queue broker.Queue, id int, total *atomic.Int32) {
	var received atomic.Int32

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		n := received.Add(1)
		log.Printf("\t[subscriber-%d] event %d: %s", id, n, string(msg.Body))
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

	log.Printf("\t[subscriber-%d] started", id)

	if err := consumer.Consume(ctx); err != nil {
		log.Printf("\t[subscriber-%d] stopped: %v", id, err)
	}

	log.Printf("\t[subscriber-%d] done: received=%d", id, received.Load())
}
