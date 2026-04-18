package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
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

	exchange := broker.NewExchange("example.circuit-breaker").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.circuit-breaker.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "circuit-breaker").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	var (
		received  atomic.Int32
		processed atomic.Int32
		failures  atomic.Int32
		rejected  atomic.Int32
		successes atomic.Int32
		mu        sync.Mutex
		failCount int
	)

	// CircuitBreakerMiddleware stops forwarding messages when the backend fails repeatedly.
	//
	// states:
	//   closed    – normal operation, all messages processed
	//   open      – backend unhealthy; messages are rejected immediately
	//   half-open – backend may be recovering; only MaxProbes messages are
	//               forwarded concurrently as probes. Excess arrivals are rejected (gated)
	//               just like in open state until a probe slot is freed. Once MinSuccesses
	//               consecutive probes succeed the circuit closes again.
	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		processed.Add(1)
		body := string(msg.Body)

		if body == "fail" {
			mu.Lock()
			failCount++
			n := failCount
			mu.Unlock()

			failures.Add(1)
			log.Printf("\t%-12s  attempt %d", "[fail]", n)
			if n == 3 {
				log.Printf("\t%-12s  %s", "[circuit]", "opened")
			}
			return broker.HandlerActionNackDiscard, errors.New("service unavailable")
		}

		mu.Lock()
		failCount = 0
		mu.Unlock()

		n := successes.Add(1)
		log.Printf("\t%-12s  %s", "[success]", body)
		switch n {
		case 1:
			// first probe entered half-open state (lazy transition on first arrival after cooldown)
			log.Printf("\t%-12s  %s", "[circuit]", "half-open")
		case 2:
			// MinSuccesses reached, circuit breaker closes the circuit
			log.Printf("\t%-12s  %s", "[circuit]", "closed")
		}
		return broker.HandlerActionAck, nil
	}

	cbHandler := broker.WrapHandler(
		handler,
		broker.CircuitBreakerMiddleware(&broker.CircuitBreakerMiddlewareConfig{
			Threshold:    3,               // open after 3 consecutive failures
			Cooldown:     3 * time.Second, // stay open for 3s
			MinSuccesses: 2,               // 2 probe successes close the circuit
			MaxProbes:    1,               // 1 probe in flight at a time (default, classic CB)
			Action:       broker.HandlerActionNackDiscard,
			ShouldCount:  func(err error) bool { return err != nil },
		}),
	)

	// outer func counts messages rejected by the circuit breaker (those that never reached
	// the inner handler) and distinguishes open-state from half-open gate rejections
	wrappedHandler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		received.Add(1)
		before := processed.Load()
		action, err := cbHandler(ctx, msg)
		if processed.Load() == before {
			rejected.Add(1)
			if err != nil && strings.Contains(err.Error(), "half-open") {
				log.Printf("\t%-12s  %s", "[gated]", string(msg.Body))
			} else {
				log.Printf("\t%-12s  %s", "[rejected]", string(msg.Body))
			}
		}
		return action, err
	}

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
			PrefetchCount:   1,
		},
		queue,
		wrappedHandler,
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

	time.Sleep(300 * time.Millisecond)

	var messages []string

	log.Println("--- batch 1: 3 failures to open circuit ---")
	for range 3 {
		if err := b.Publish(ctx, exchange.Name, "circuit-breaker", broker.NewMessage([]byte("fail"))); err != nil {
			log.Fatal(err)
		}
		messages = append(messages, "fail")
		time.Sleep(50 * time.Millisecond)
	}

	log.Println("--- batch 2: messages while circuit is open (all rejected) ---")
	for range 3 {
		if err := b.Publish(ctx, exchange.Name, "circuit-breaker", broker.NewMessage([]byte("rejected"))); err != nil {
			log.Fatal(err)
		}
		messages = append(messages, "rejected")
		time.Sleep(50 * time.Millisecond)
	}

	log.Println("\twaiting 3.5s for cooldown ...")
	time.Sleep(3500 * time.Millisecond)

	// first message of this batch triggers the lazy Open->HalfOpen transition;
	// MaxProbes=1 means only one probe executes at a time; with PrefetchCount=1
	// the consumer is already serial so the gate is not visibly exercised here, but with
	// higher concurrency excess arrivals would be gated with "circuit half-open" errors
	log.Println("--- batch 3: half-open probes (first 2 close the circuit), then normal ---")
	for range 4 {
		if err := b.Publish(ctx, exchange.Name, "circuit-breaker", broker.NewMessage([]byte("success"))); err != nil {
			log.Fatal(err)
		}
		messages = append(messages, "success")
		time.Sleep(50 * time.Millisecond)
	}

	log.Printf("published %d messages total", len(messages))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	timeout := time.After(20 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if received.Load() >= int32(len(messages)) {
				time.Sleep(100 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: received=%d processed=%d failures=%d rejected=%d successes=%d",
		received.Load(), processed.Load(), failures.Load(), rejected.Load(), successes.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
