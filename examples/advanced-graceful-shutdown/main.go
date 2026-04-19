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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	exchange := broker.NewExchange("example.shutdown").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.shutdown.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "task").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	var (
		published atomic.Int32
		processed atomic.Int32
		wg        sync.WaitGroup
	)

	// each handler simulates slow work (5000ms) to demonstrate graceful shutdown with in-flight handlers;
	// handlers run with the broker's internal context (b.ctx), not the user's cancel context,
	// so cancelling the user ctx stops new message delivery but lets each in-flight handler
	// finish its current task before the program exits
	handler := func(_ context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		defer processed.Add(1)
		task := string(msg.Body)
		log.Printf("\tprocessing: %s (will take 5000ms)", task)
		time.Sleep(5 * time.Second)
		log.Printf("\tcompleted:  %s", task)
		return broker.HandlerActionAck, nil
	}

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{
				NoAutoDeclare:   true,
				NoAutoReconnect: true, // no reconnect on shutdown - prevents re-delivery loop
			},
			PrefetchCount: 2,
		},
		queue,
		handler,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Consume(ctx); err != nil && ctx.Err() == nil {
			log.Printf("consumer error: %v", err)
		}
	}()

	time.Sleep(300 * time.Millisecond)

	for i := 1; i <= 5; i++ {
		msg := broker.NewMessage([]byte("task-" + string(rune('0'+i))))
		if err := b.Publish(ctx, exchange.Name, "task", msg); err != nil {
			log.Fatal(err)
		}
		published.Add(1)
		time.Sleep(50 * time.Millisecond)
	}

	log.Printf("published %d tasks", published.Load())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received signal: %v", sig)
	case <-time.After(8 * time.Second):
		log.Println("auto-triggering shutdown - in-flight tasks will complete")
	}

	log.Println("cancelling context - stopping new message delivery ...")
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("all handlers finished")
	case <-time.After(15 * time.Second):
		log.Println("shutdown timeout - forcing exit")
	}

	log.Printf("summary: published=%d processed=%d (any unstarted tasks remain in the queue)",
		published.Load(), processed.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
