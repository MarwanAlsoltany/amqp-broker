package main

import (
	"context"
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
	timeout := 60 * time.Second

	ctx := context.Background()

	// WithConnectionManagerOptions configures reconnection at the lower-level ConnectionManagerOptions
	// struct, giving access to ReconnectMin, ReconnectMax, OnOpen and OnClose in one place;
	// for a simpler declarative approach use WithConnectionReconnectConfig
	var reconnects atomic.Int32

	b, err := broker.New(
		broker.WithURL("amqp://guest:guest@localhost:5672/"),
		broker.WithConnectionManagerOptions(broker.ConnectionManagerOptions{
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 10 * time.Second,
			OnOpen: func(idx int) {
				n := reconnects.Add(1)
				if n == 1 {
					log.Printf("connection #%d established", idx)
				} else {
					log.Printf("connection #%d reconnected (attempt %d)", idx, n)
				}
			},
			OnClose: func(idx int, code int, reason string, server, recover bool) {
				if code != 0 {
					log.Printf("connection #%d lost: code=%d reason=%s", idx, code, reason)
					if recover {
						log.Println("\tauto-reconnect will attempt to restore")
					}
				}
			},
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	exchange := broker.NewExchange("example.error-recovery").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.error-recovery.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "resilient").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	var processed atomic.Int32

	// RecoveryMiddleware catches panics in the handler so the consumer goroutine
	// itself never crashes; combined with reconnect config the whole pipeline
	// is resilient to both panics and broker disconnects
	handler := broker.WrapHandler(
		func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			processed.Add(1)
			log.Printf("\tprocessed: %s", string(msg.Body))
			return broker.HandlerActionAck, nil
		},
		broker.RecoveryMiddleware(&broker.RecoveryMiddlewareConfig{
			Logger: slog.Default(),
			Level:  slog.LevelError,
			Action: broker.HandlerActionNackDiscard,
			OnPanic: func(ctx context.Context, msg *broker.Message, recovered any) {
				log.Printf("\tpanic recovered: %v", recovered)
			},
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

	// retry loop ensures the consumer reconnects if Consume() exits unexpectedly
	go func() {
		for {
			log.Println("consumer starting ...")
			if err := consumer.Consume(ctx); err != nil {
				log.Printf("consumer stopped: %v - retrying in 2s", err)
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}
	}()

	time.Sleep(300 * time.Millisecond)

	// continuous publisher; in a real scenario this runs independently (different service)
	go func() {
		ticker := time.NewTicker(1500 * time.Millisecond)
		defer ticker.Stop()
		i := 1
		for {
			select {
			case <-ticker.C:
				msg := broker.NewMessage([]byte("message"))
				msg.DeliveryMode = 2 // persistent
				if err := b.Publish(ctx, exchange.Name, "resilient", msg); err != nil {
					log.Printf("\tpublish error: %v", err)
				} else {
					log.Printf("\tpublished %d", i)
					i++
				}
			}
		}
	}()

	log.Println("running - stop/restart RabbitMQ to observe recovery:")
	log.Println("\tdocker stop <rabbitmq>; docker start <rabbitmq>")
	log.Println("press Ctrl+C to exit")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-time.After(timeout):
	}

	log.Printf("summary: reconnects=%d processed=%d", reconnects.Load(), processed.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
