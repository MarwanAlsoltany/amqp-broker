package main

import (
	"context"
	"errors"
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

	// a handler returns a HandlerAction that tells the broker what to do with the message:
	//   HandlerActionAck         - acknowledge; remove from queue.
	//   HandlerActionNackRequeue - nack with requeue; message is redelivered.
	//   HandlerActionNackDiscard - nack without requeue; routed to DLX if configured.
	//   HandlerActionNoAction    - no broker-level ack/nack; call manually if needed.
	log.Println("--- handler actions ---")
	{
		exchange := broker.NewExchange("example.actions").WithType("direct").WithDurable(true)
		queue := broker.NewQueue("example.actions.queue").WithDurable(true)
		topology := broker.NewTopology(
			[]broker.Exchange{exchange},
			[]broker.Queue{queue},
			[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "actions").WithType(broker.BindingTypeQueue)},
		)

		if err := b.Declare(&topology); err != nil {
			log.Fatal(err)
		}

		messages := []string{
			"ack",
			"nack-requeue",
			"nack-discard",
			"ack",
			"nack-discard",
		}

		for _, body := range messages {
			if err := b.Publish(ctx, exchange.Name, "actions", broker.NewMessage([]byte(body))); err != nil {
				log.Fatal(err)
			}
		}

		var (
			processed atomic.Int32
			retryMu   sync.Mutex
			retries   = make(map[string]int)
		)

		consumer, err := b.NewConsumer(
			&broker.ConsumerOptions{
				EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
				PrefetchCount:   1,
			},
			queue,
			func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
				processed.Add(1)
				body := string(msg.Body)

				switch body {
				case "ack":
					log.Printf("\t[ack] processed: %s", body)
					return broker.HandlerActionAck, nil

				case "nack-requeue":
					retryMu.Lock()
					retries[body]++
					n := retries[body]
					retryMu.Unlock()

					if n <= 2 {
						log.Printf("\t[nack-requeue] attempt %d, will retry: %s", n, body)
						return broker.HandlerActionNackRequeue, nil
					}
					log.Printf("\t[nack-discard] max retries reached: %s", body)
					return broker.HandlerActionNackDiscard, nil

				case "nack-discard":
					log.Printf("\t[nack-discard] permanent failure: %s", body)
					return broker.HandlerActionNackDiscard, nil

				default:
					log.Printf("\t[no-action] unhandled: %s", body)
					return broker.HandlerActionNoAction, nil
				}
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			if err := consumer.Consume(ctx); err != nil {
				log.Printf("consumer error: %v", err)
			}
		}()

		// 5 original + 2 requeue retries = 7 total invocations
		waitForMessages(7, &processed, 10*time.Second)
		consumer.Close()

		log.Printf("\tsummary: %d handler invocations for %d messages", processed.Load(), len(messages))

		if err := b.Delete(&topology); err != nil {
			log.Printf("cleanup: %v", err)
		}
	}

	// handlers return (HandlerAction, error), returning a non-nil error does not
	// automatically control the action, the two are independent; set both explicitly
	//
	// common patterns:
	//   validation error -> (NackDiscard, err) - permanent, don't retry
	//   transient error  -> (NackRequeue, err) - temporary, retry later
	//   no-action        -> (NoAction, nil) - caller manages ack/nack manually
	log.Println("--- error handling ---")
	{
		exchange := broker.NewExchange("example.errors").WithType("direct").WithDurable(true)
		queue := broker.NewQueue("example.errors.queue").WithDurable(true)
		topology := broker.NewTopology(
			[]broker.Exchange{exchange},
			[]broker.Queue{queue},
			[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "errors").WithType(broker.BindingTypeQueue)},
		)

		if err := b.Declare(&topology); err != nil {
			log.Fatal(err)
		}

		messages := []string{
			"valid",
			"validation-error",
			"transient-error",
			"valid",
			"no-action",
		}

		for _, body := range messages {
			if err := b.Publish(ctx, exchange.Name, "errors", broker.NewMessage([]byte(body))); err != nil {
				log.Fatal(err)
			}
		}

		var (
			processed atomic.Int32
			retryMu   sync.Mutex
			retries   = make(map[string]int)
		)

		var ErrValidation = errors.New("validation error")
		var ErrTransient = errors.New("transient error")

		handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			// note: no-action messages are not acked so they are not counted as fully processed;
			// the counter tracks handler invocations, not message completion
			processed.Add(1)
			body := string(msg.Body)

			switch body {
			case "validation-error":
				// permanent, do not retry
				log.Printf("\t[nack-discard] validation failed: %s", body)
				return broker.HandlerActionNackDiscard, ErrValidation

			case "transient-error":
				retryMu.Lock()
				retries[body]++
				n := retries[body]
				retryMu.Unlock()

				if n <= 2 {
					log.Printf("\t[nack-requeue] transient error, attempt %d: %s", n, body)
					return broker.HandlerActionNackRequeue, ErrTransient
				}
				log.Printf("\t[nack-discard] gave up after %d attempts: %s", n, body)
				return broker.HandlerActionNackDiscard, ErrTransient

			case "no-action":
				// caller takes full responsibility; ack/nack must be sent manually
				// (in this example the message is left unacked intentionally)
				log.Printf("\t[no-action] manual handling: %s", body)
				return broker.HandlerActionNoAction, nil

			default:
				log.Printf("\t[ack] success: %s", body)
				return broker.HandlerActionAck, nil
			}
		}

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

		go func() {
			if err := consumer.Consume(ctx); err != nil {
				log.Printf("consumer error: %v", err)
			}
		}()

		// 5 original + 2 transient retries = 7 handler invocations (no-action is counted but not acked)
		waitForMessages(7, &processed, 10*time.Second)
		consumer.Close()

		log.Printf("\tsummary: %d handler invocations for %d messages", processed.Load(), len(messages))

		if err := b.Delete(&topology); err != nil {
			log.Printf("cleanup: %v", err)
		}
	}

	// the handler receives the same context passed to consumer.Consume().
	// use it to:
	//   - check for cancellation before starting expensive work (ctx.Done())
	//   - propagate deadlines to downstream calls
	//   - read request-scoped values injected by the caller
	// returning NackRequeue on cancellation allows the message to be reprocessed
	// by another consumer after a restart.
	log.Println("--- context usage ---")
	{
		exchange := broker.NewExchange("example.context").WithType("direct").WithDurable(true)
		queue := broker.NewQueue("example.context.queue").WithDurable(true)
		topology := broker.NewTopology(
			[]broker.Exchange{exchange},
			[]broker.Queue{queue},
			[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "context").WithType(broker.BindingTypeQueue)},
		)

		if err := b.Declare(&topology); err != nil {
			log.Fatal(err)
		}

		type task struct {
			body  string
			sleep time.Duration
		}

		tasks := []task{
			{"fast", 100 * time.Millisecond},
			{"moderate", 600 * time.Millisecond},
			{"normal", 300 * time.Millisecond},
			{"another-fast", 150 * time.Millisecond},
		}

		for _, t := range tasks {
			if err := b.Publish(ctx, exchange.Name, "context", broker.NewMessage([]byte(t.body))); err != nil {
				log.Fatal(err)
			}
		}

		var (
			processed atomic.Int32
			cancelled atomic.Int32
		)

		handler := func(handlerCtx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			processed.Add(1)
			body := string(msg.Body)

			// pattern 1: check cancellation before starting work
			select {
			case <-handlerCtx.Done():
				cancelled.Add(1)
				log.Printf("\t[cancelled] before start: %s", body)
				return broker.HandlerActionNackRequeue, handlerCtx.Err()
			default:
			}

			// find processing time
			var sleep time.Duration
			for _, t := range tasks {
				if t.body == body {
					sleep = t.sleep
					break
				}
			}

			log.Printf("\tprocessing %s for %dms", body, sleep.Milliseconds())

			timer := time.NewTimer(sleep)
			defer timer.Stop()

			// pattern 2: respect cancellation during work
			select {
			case <-timer.C:
				log.Printf("\t[ack] completed: %s", body)
				return broker.HandlerActionAck, nil

			case <-handlerCtx.Done():
				err := handlerCtx.Err()
				cancelled.Add(1)
				log.Printf("\t[nack-requeue] interrupted: %s (%v)", body, err)
				return broker.HandlerActionNackRequeue, err
			}
		}

		// wrap with a 500ms per-message timeout to show deadline propagation
		wrapped := broker.WrapHandler(
			handler,
			broker.TimeoutMiddleware(&broker.TimeoutMiddlewareConfig{
				Timeout: 500 * time.Millisecond,
				Action:  broker.HandlerActionNackDiscard,
			}),
		)

		consumer, err := b.NewConsumer(
			&broker.ConsumerOptions{
				EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
				PrefetchCount:   1,
			},
			queue,
			wrapped,
		)
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			if err := consumer.Consume(ctx); err != nil {
				log.Printf("consumer error: %v", err)
			}
		}()

		waitForMessages(int32(len(tasks)), &processed, 10*time.Second)
		consumer.Close()

		log.Printf("\tsummary: processed=%d cancelled/timedout=%d", processed.Load(), cancelled.Load())

		if err := b.Delete(&topology); err != nil {
			log.Printf("cleanup: %v", err)
		}
	}

	log.Println("done")
}

// waitForMessages blocks until n messages have been processed or timeout/signal fires.
func waitForMessages(n int32, processed *atomic.Int32, timeout time.Duration) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	deadline := time.After(timeout)

	for {
		select {
		case <-sigCh:
			return
		case <-deadline:
			return
		case <-ticker.C:
			if processed.Load() >= n {
				time.Sleep(50 * time.Millisecond)
				return
			}
		}
	}
}
