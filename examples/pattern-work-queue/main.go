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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	// a durable work queue retains tasks across broker restarts,
	// with PrefetchCount=1, each worker takes one task at a time (fair dispatch)
	workQueue := broker.NewQueue("work.tasks").WithDurable(true)
	topology := broker.NewTopology(nil, []broker.Queue{workQueue}, nil)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	tasks := []string{
		"process order #1001",
		"send email to user@example.com",
		"generate report for march",
		"process order #1002",
		"backup database",
		"process order #1003",
		"clean up temp files",
		"process order #1004",
		"send notification",
		"process order #1005",
	}

	log.Printf("publishing %d tasks to work queue", len(tasks))
	for i, task := range tasks {
		msg := broker.NewMessage([]byte(task))
		msg.DeliveryMode = 2 // persistent
		if err := b.Publish(ctx, "", workQueue.Name, msg); err != nil {
			log.Fatal(err)
		}
		log.Printf("\t[%02d] queued: %s", i+1, task)
	}

	workers := 3
	var wg sync.WaitGroup
	var totalProcessed atomic.Int32

	log.Printf("starting %d workers", workers)
	for i := 1; i <= workers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			startWorker(ctx, b, workQueue, workerID, &totalProcessed)
		}()
	}

	time.Sleep(300 * time.Millisecond)

	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if totalProcessed.Load() >= int32(len(tasks)) {
				time.Sleep(300 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	cancel()
	wg.Wait()

	log.Printf("summary: processed=%d/%d", totalProcessed.Load(), len(tasks))

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

func startWorker(ctx context.Context, b *broker.Broker, queue broker.Queue, id int, total *atomic.Int32) {
	var processed atomic.Int32

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		task := string(msg.Body)
		n := processed.Add(1)

		log.Printf("\t[worker-%d] task %d: %s", id, n, task)
		time.Sleep(time.Duration(100+id*50) * time.Millisecond)
		total.Add(1)

		return broker.HandlerActionAck, nil
	}

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
			PrefetchCount:   1,
		},
		queue, handler,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	log.Printf("\t[worker-%d] started", id)

	if err := consumer.Consume(ctx); err != nil {
		log.Printf("\t[worker-%d] stopped: %v", id, err)
	}

	log.Printf("\t[worker-%d] done: processed=%d", id, processed.Load())
}
