package main

import (
	"context"
	"errors"
	"fmt"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("--- connection-pool ---")
	{
		// WithConnectionPoolSize creates N underlying AMQP connections;
		// concurrent publishers each obtain their own connection from the pool,
		// eliminating head-of-line blocking on a single shared channel
		b, err := broker.New(
			broker.WithURL("amqp://guest:guest@localhost:5672/"),
			broker.WithConnectionPoolSize(5),
		)
		if err != nil {
			log.Fatal(err)
		}

		q := broker.NewQueue("config.pool.test").WithDurable(false).WithAutoDelete(true)
		t := broker.NewTopology(nil, []broker.Queue{q}, nil)
		if err := b.Declare(&t); err != nil {
			log.Fatal(err)
		}

		var wg sync.WaitGroup
		start := time.Now()

		for i := 1; i <= 10; i++ {
			wg.Add(1)
			id := i
			go func() {
				defer wg.Done()
				for j := 1; j <= 5; j++ {
					if err := b.Publish(ctx, "", q.Name, broker.NewMessage([]byte("msg"))); err != nil {
						log.Printf("\t[publisher-%02d] error: %v", id, err)
						return
					}
				}
				log.Printf("\t[publisher-%02d] done", id)
			}()
		}

		wg.Wait()
		elapsed := time.Since(start)
		log.Printf("\tpublished 50 messages via 10 concurrent publishers in %v", elapsed.Round(time.Millisecond))

		_ = b.Delete(&t)
		b.Close()
	}

	log.Println("--- connection-lifecycle ---")
	{
		// WithConnectionOnOpen / OnClose / OnBlocked let you react to connection
		// state changes: log, alert, adjust monitoring dashboards, etc.
		b, err := broker.New(
			broker.WithURL("amqp://guest:guest@localhost:5672/"),
			broker.WithConnectionOnOpen(func(idx int) {
				log.Printf("\tconnection #%d opened", idx)
			}),
			broker.WithConnectionOnClose(func(idx int, code int, reason string, server, recover bool) {
				if code != 0 {
					log.Printf("\tconnection #%d closed: code=%d reason=%s", idx, code, reason)
				} else {
					log.Printf("\tconnection #%d closed gracefully", idx)
				}
			}),
			broker.WithConnectionOnBlocked(func(idx int, active bool, reason string) {
				if active {
					log.Printf("\tconnection #%d blocked: %s", idx, reason)
				} else {
					log.Printf("\tconnection #%d unblocked", idx)
				}
			}),
		)
		if err != nil {
			log.Fatal(err)
		}

		q := broker.NewQueue("config.lifecycle.test").WithDurable(false).WithAutoDelete(true)
		t := broker.NewTopology(nil, []broker.Queue{q}, nil)
		if err := b.Declare(&t); err != nil {
			log.Fatal(err)
		}

		for i := 1; i <= 3; i++ {
			if err := b.Publish(ctx, "", q.Name, broker.NewMessage([]byte("test"))); err != nil {
				log.Fatal(err)
			}
			log.Printf("\tpublished message %d", i)
			time.Sleep(100 * time.Millisecond)
		}

		_ = b.Delete(&t)
		b.Close()
	}

	log.Println("--- reconnection ---")
	{
		// WithConnectionReconnectConfig(disabled, minBackoff, maxBackoff) enables automatic
		// reconnection with exponential back-off; pair with persistent messages and durable
		// queues so in-flight messages survive a broker restart
		var reconnects atomic.Int32

		b, err := broker.New(
			broker.WithURL("amqp://guest:guest@localhost:5672/"),
			broker.WithConnectionReconnectConfig(false, 2*time.Second, 10*time.Second),
			broker.WithConnectionOnOpen(func(idx int) {
				n := reconnects.Add(1)
				if n == 1 {
					log.Printf("\tconnection #%d established", idx)
				} else {
					log.Printf("\tconnection #%d reconnected (attempt #%d)", idx, n)
				}
			}),
		)
		if err != nil {
			log.Fatal(err)
		}

		q := broker.NewQueue("config.reconnect.test").WithDurable(true)
		t := broker.NewTopology(nil, []broker.Queue{q}, nil)
		if err := b.Declare(&t); err != nil {
			log.Fatal(err)
		}

		var received atomic.Int32
		consumer, err := b.NewConsumer(
			&broker.ConsumerOptions{EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true}},
			q,
			func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
				received.Add(1)
				return broker.HandlerActionAck, nil
			},
		)
		if err != nil {
			log.Fatal(err)
		}
		defer consumer.Close()

		go func() {
			if err := consumer.Consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("\tconsumer: %v", err)
			}
		}()

		for i := 1; i <= 5; i++ {
			msg := broker.NewMessage([]byte("resilient"))
			msg.DeliveryMode = 2
			if err := b.Publish(ctx, "", q.Name, msg); err != nil {
				log.Printf("\tpublish error: %v", err)
			} else {
				log.Printf("\tpublished %d", i)
			}
			time.Sleep(200 * time.Millisecond)
		}

		time.Sleep(500 * time.Millisecond)
		log.Printf("\treceived=%d/5", received.Load())

		_ = b.Delete(&t)
		b.Close()
	}

	log.Println("--- publisher-confirms ---")
	{
		// ConfirmMode=true makes Publish() block until the broker acknowledges receipt;
		// more reliable than fire-and-forget, at the cost of throughput
		b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
		if err != nil {
			log.Fatal(err)
		}

		q := broker.NewQueue("config.confirms.test").WithDurable(true)
		t := broker.NewTopology(nil, []broker.Queue{q}, nil)
		if err := b.Declare(&t); err != nil {
			log.Fatal(err)
		}

		publisher, err := b.NewPublisher(
			&broker.PublisherOptions{
				EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
				ConfirmMode:     true,
			},
			broker.NewExchange(""),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer publisher.Close()

		messages := []string{
			"critical event A",
			"critical event B",
			"critical event C",
		}
		var confirmed int

		for i, text := range messages {
			start := time.Now()
			msg := broker.NewMessage([]byte(text))
			msg.DeliveryMode = 2
			if err := publisher.Publish(ctx, broker.NewRoutingKey(q.Name, nil), msg); err != nil {
				log.Printf("\t[%02d] not confirmed: %v", i+1, err)
			} else {
				confirmed++
				log.Printf("\t[%02d] confirmed in %v: %s", i+1, time.Since(start).Round(time.Millisecond), text)
			}
			time.Sleep(50 * time.Millisecond)
		}

		log.Printf("\tconfirmed=%d/%d", confirmed, len(messages))

		_ = b.Delete(&t)
		b.Close()
	}

	log.Println("--- consumer-qos ---")
	{
		// PrefetchCount limits how many unacknowledged messages the broker delivers to
		// the consumer at once, MaxConcurrentHandlers caps parallel handler goroutines
		b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
		if err != nil {
			log.Fatal(err)
		}

		q := broker.NewQueue("config.qos.test").WithDurable(false).WithAutoDelete(false)
		t := broker.NewTopology(nil, []broker.Queue{q}, nil)
		if err := b.Declare(&t); err != nil {
			log.Fatal(err)
		}

		for i := 1; i <= 20; i++ {
			if err := b.Publish(ctx, "", q.Name, broker.NewMessage(fmt.Appendf(nil, "task-%d", i))); err != nil {
				log.Fatal(err)
			}
		}

		var r1, r2 atomic.Int32

		log.Println("\tconsumer 1: prefetch-count=3")
		con1, err := b.NewConsumer(
			&broker.ConsumerOptions{
				EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
				PrefetchCount:   3,
			},
			q,
			func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
				r1.Add(1)
				time.Sleep(100 * time.Millisecond)
				return broker.HandlerActionAck, nil
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		con1Ctx, con1Cancel := context.WithTimeout(ctx, 5*time.Second)
		defer con1Cancel()

		if err := con1.Consume(con1Ctx); err != nil && err != context.DeadlineExceeded {
			log.Printf("\tconsumer 1: %v", err)
		}
		con1.Close()
		log.Printf("\tprocessed %d messages with PrefetchCount=3", r1.Load())

		for i := 21; i <= 40; i++ {
			if err := b.Publish(ctx, "", q.Name, broker.NewMessage(fmt.Appendf(nil, "task-%d", i))); err != nil {
				log.Fatal(err)
			}
		}

		log.Println("\tconsumer 2: max-concurrent-handlers=5")

		var maxConcurrent atomic.Int32
		var current atomic.Int32

		con2, err := b.NewConsumer(
			&broker.ConsumerOptions{
				EndpointOptions:       broker.EndpointOptions{NoAutoDeclare: true},
				MaxConcurrentHandlers: 5,
			},
			q,
			func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
				n := current.Add(1)
				defer current.Add(-1)
				for {
					m := maxConcurrent.Load()
					if n <= m || maxConcurrent.CompareAndSwap(m, n) {
						break
					}
				}
				r2.Add(1)
				time.Sleep(100 * time.Millisecond)
				return broker.HandlerActionAck, nil
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		con2Ctx, con2Cancel := context.WithTimeout(ctx, 5*time.Second)
		defer con2Cancel()

		if err := con2.Consume(con2Ctx); err != nil && err != context.DeadlineExceeded {
			log.Printf("\tconsumer 2: %v", err)
		}
		con2.Close()
		log.Printf("\tprocessed %d messages with MaxConcurrentHandlers=5 (max-observed=%d)", r2.Load(), maxConcurrent.Load())

		_ = b.Delete(&t)
		b.Close()
	}

	log.Println("--- endpoint-caching ---")
	{
		// WithCache(ttl) keeps publisher and consumer channel objects alive for ttl
		// after their last use, subsequent calls with the same (exchange, routing key)
		// reuse the cached endpoint instead of opening a new channel each time
		bWithCached, err := broker.New(
			broker.WithURL("amqp://guest:guest@localhost:5672/"),
			broker.WithCache(1*time.Minute),
		)
		if err != nil {
			log.Fatal(err)
		}

		bWithNoCache, err := broker.New(
			broker.WithURL("amqp://guest:guest@localhost:5672/"),
			broker.WithCache(0), // disabled
		)
		if err != nil {
			log.Fatal(err)
		}

		q := broker.NewQueue("config.cache.test").WithDurable(false).WithAutoDelete(true)
		t := broker.NewTopology(nil, []broker.Queue{q}, nil)
		if err := bWithNoCache.Declare(&t); err != nil {
			log.Fatal(err)
		}

		n := 10

		t1 := time.Now()
		for range n {
			if err := bWithNoCache.Publish(ctx, "", q.Name, broker.NewMessage([]byte("x"))); err != nil {
				log.Fatal(err)
			}
		}
		d1 := time.Since(t1)

		t2 := time.Now()
		for range n {
			if err := bWithCached.Publish(ctx, "", q.Name, broker.NewMessage([]byte("x"))); err != nil {
				log.Fatal(err)
			}
		}
		d2 := time.Since(t2)

		log.Printf("\tno-cache: %v (avg %v/msg)", d1.Round(time.Millisecond), (d1 / time.Duration(n)).Round(time.Microsecond))
		log.Printf("\twith-cache: %v (avg %v/msg)", d2.Round(time.Millisecond), (d2 / time.Duration(n)).Round(time.Microsecond))
		if d2 < d1 {
			log.Printf("\tspeedup: %.1fx", float64(d1)/float64(d2))
		}

		_ = bWithNoCache.Delete(&t)
		bWithNoCache.Close()
		bWithCached.Close()
	}

	log.Println("--- custom-context ---")
	{
		// WithContext(ctx) passes a root context to the broker, when the context
		// is cancelled all broker operations (Publish, Consume) stop gracefully
		ctxCustom, cancelCustom := context.WithCancel(context.Background())

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		go func() {
			select {
			case <-sigCh:
				cancelCustom()
			case <-ctxCustom.Done():
			}
		}()

		b, err := broker.New(
			broker.WithURL("amqp://guest:guest@localhost:5672/"),
			broker.WithContext(ctxCustom),
		)
		if err != nil {
			log.Fatal(err)
		}

		q := broker.NewQueue("config.context.test").WithDurable(false).WithAutoDelete(true)
		t := broker.NewTopology(nil, []broker.Queue{q}, nil)
		if err := b.Declare(&t); err != nil {
			log.Fatal(err)
		}

		var processed atomic.Int32

		consumer, err := b.NewConsumer(
			&broker.ConsumerOptions{EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true}},
			q,
			func(hCtx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
				select {
				case <-time.After(400 * time.Millisecond):
					processed.Add(1)
					return broker.HandlerActionAck, nil
				case <-hCtx.Done():
					return broker.HandlerActionNackRequeue, hCtx.Err()
				}
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		consumeDone := make(chan struct{})
		go func() {
			defer close(consumeDone)
			if err := consumer.Consume(ctxCustom); err != nil && err != context.Canceled {
				log.Printf("\tconsumer: %v", err)
			}
		}()

		publishDone := make(chan struct{})
		go func() {
			defer close(publishDone)
			for i := 1; i <= 5; i++ {
				select {
				case <-ctxCustom.Done():
					return
				default:
					if err := b.Publish(ctxCustom, "", q.Name, broker.NewMessage([]byte("msg"))); err != nil {
						return
					}
					log.Printf("\tpublished %d", i)
					time.Sleep(600 * time.Millisecond)
				}
			}
		}()

		// auto-cancel after processing a few messages
		go func() {
			time.Sleep(3 * time.Second)
			cancelCustom()
		}()

		<-ctxCustom.Done()
		<-consumeDone
		<-publishDone

		consumer.Close()
		log.Printf("\tprocessed=%d", processed.Load())

		_ = b.Delete(&t)
		b.Close()
	}

	log.Println("done")
}
