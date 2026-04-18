package main

import (
	"context"
	"errors"
	"iter"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	broker "github.com/MarwanAlsoltany/amqp-broker"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	exchange := broker.NewExchange("example.batch").WithType("direct").WithDurable(true)
	queueSync := broker.NewQueue("example.batch.sync").WithDurable(true)
	queueAsync := broker.NewQueue("example.batch.async").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queueSync, queueAsync},
		[]broker.Binding{
			broker.NewBinding(exchange.Name, queueSync.Name, "sync").WithType(broker.BindingTypeQueue),
			broker.NewBinding(exchange.Name, queueAsync.Name, "async").WithType(broker.BindingTypeQueue),
		},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	const msgCount = 30

	for range msgCount {
		body := []byte("sync-msg")
		msg := broker.NewMessage(body)
		msg.MessageID = log.Prefix() // reuse prefix as placeholder ID
		if err := b.Publish(ctx, exchange.Name, "sync", broker.NewMessage([]byte("sync-msg"))); err != nil {
			log.Fatal(err)
		}
		if err := b.Publish(ctx, exchange.Name, "async", broker.NewMessage([]byte("async-msg"))); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("published %d messages to each queue", msgCount)

	var (
		syncBatches  atomic.Int32
		asyncBatches atomic.Int32
		syncMsgs     atomic.Int32
		asyncMsgs    atomic.Int32
	)

	// --- sync / blocking BatchMiddleware ---
	//
	// consumer goroutines block until Size messages accumulate or FlushTimeout fires;
	// all goroutines in a batch receive the same action simultaneously,
	// PrefetchCount MUST be >= Size to avoid a deadlock
	syncWrapped := broker.WrapHandler(
		broker.ActionHandler(broker.HandlerActionNoAction),
		broker.BatchMiddleware(ctx,
			func(batchCtx context.Context, msgs iter.Seq2[int, *broker.Message]) (broker.HandlerAction, error) {
				n := 0
				for i, msg := range msgs {
					n++
					log.Printf("\t%-7s  %3d  %3d  %s", "[sync]", syncBatches.Load()+1, i, string(msg.Body))
				}
				syncMsgs.Add(int32(n))
				syncBatches.Add(1)
				return broker.HandlerActionAck, nil
			},
			&broker.BatchConfig{
				Size:         3,
				FlushTimeout: 500 * time.Millisecond,
				ErrorAction:  broker.HandlerActionNackRequeue,
			},
		),
	)

	syncConsumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
			PrefetchCount:   3, // must be >= Size
		},
		queueSync,
		syncWrapped,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer syncConsumer.Close()

	go func() {
		if err := syncConsumer.Consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("sync consumer error: %v", err)
		}
	}()

	// --- async BatchMiddleware (at-most-once, fire-and-forget) ---
	//
	// consumer goroutines return ActionAck immediately on enqueue,
	// a background goroutine accumulates messages and calls the handler in batches;
	// no PrefetchCount constraint; suitable for logging, analytics, metrics
	asyncWrapped := broker.WrapHandler(
		broker.ActionHandler(broker.HandlerActionNoAction),
		broker.BatchMiddleware(ctx,
			func(batchCtx context.Context, msgs iter.Seq2[int, *broker.Message]) (broker.HandlerAction, error) {
				n := 0
				for i, msg := range msgs {
					n++
					log.Printf("\t%-7s  %3d  %3d  %s", "[async]", asyncBatches.Load()+1, i, string(msg.Body))
				}
				asyncMsgs.Add(int32(n))
				asyncBatches.Add(1)
				return broker.HandlerActionAck, nil
			},
			&broker.BatchConfig{
				Async:        true,
				Size:         3,
				FlushTimeout: 500 * time.Millisecond,
				OnError: func(ctx context.Context, err error, count int) {
					slog.ErrorContext(ctx, "async batch error", "count", count, "err", err)
				},
			},
		),
	)

	asyncConsumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
			PrefetchCount:   10,
		},
		queueAsync,
		asyncWrapped,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer asyncConsumer.Close()

	go func() {
		if err := asyncConsumer.Consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("async consumer error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if syncMsgs.Load() >= msgCount && asyncMsgs.Load() >= msgCount {
				time.Sleep(100 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: [sync] batches=%d msgs=%d | [async] batches=%d msgs=%d",
		syncBatches.Load(), syncMsgs.Load(),
		asyncBatches.Load(), asyncMsgs.Load(),
	)

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
