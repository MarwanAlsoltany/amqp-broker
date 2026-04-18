package main

import (
	"context"
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

	exchange := broker.NewExchange("example.deadline").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.deadline.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "deadline").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	now := time.Now()

	msgs := []struct {
		body     string
		deadline time.Time
	}{
		{"future-1", now.Add(10 * time.Second)},
		{"past", now.Add(-5 * time.Second)},
		{"future-2", now.Add(20 * time.Second)},
		{"soon-to-expire", now.Add(500 * time.Millisecond)},
		{"no-deadline", time.Time{}},
	}

	for _, m := range msgs {
		msg := broker.NewMessage([]byte(m.body))
		if !m.deadline.IsZero() {
			// store deadline as time.Time in a custom header; the default Timestamp
			// extractor reads this same key, so no custom Timestamp func is needed
			msg.Headers = broker.Arguments{"x-deadline": m.deadline}
		}
		if err := b.Publish(ctx, exchange.Name, "deadline", msg); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages; waiting 1s so some deadlines pass ...", len(msgs))
	time.Sleep(1 * time.Second)

	var (
		accepted atomic.Int32
		expired  atomic.Int32
	)

	// DeadlineMiddleware discards messages whose deadline has passed;
	// default Timestamp extractor reads msg.Headers["x-deadline"] as time.Time;
	// provide a custom Timestamp func for other deadline formats (unix epoch, RFC3339 header, etc.)
	handler := broker.WrapHandler(
		func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			accepted.Add(1)
			deadline, _ := msg.Headers["x-deadline"].(time.Time)
			if deadline.IsZero() {
				log.Printf("\t%-10s  %s  (no deadline)", "[accepted]", string(msg.Body))
			} else {
				log.Printf("\t%-10s  %s  (deadline in %v)", "[accepted]", string(msg.Body), time.Until(deadline).Round(time.Second))
			}
			return broker.HandlerActionAck, nil
		},
		broker.DeadlineMiddleware(&broker.DeadlineMiddlewareConfig{
			Timestamp: nil, // nil: use default x-deadline header extractor
			Action:    broker.HandlerActionNackDiscard,
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

	// 3 messages pass (future-1 + future-2 + no-deadline); 2 expire (past + soon-to-expire)
	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if accepted.Load()+expired.Load() >= int32(len(msgs)) {
				time.Sleep(50 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: accepted=%d expired=%d (inferred)", accepted.Load(), int32(len(msgs))-accepted.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
