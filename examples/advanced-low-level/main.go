package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	broker "github.com/MarwanAlsoltany/amqp-broker"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx := context.Background()

	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	// b.Connection() returns the raw AMQP connection used for control operations;
	// useful for inspecting connection properties or creating channels manually
	log.Println("--- connection ---")
	{
		conn, err := b.Connection()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("\tconnection ok  closed=%v", conn.IsClosed())
		// connection is managed by the broker; do not close it manually
	}

	// b.Channel() returns a raw AMQP channel from the control connection;
	// the caller is responsible for closing it when done
	log.Println("--- channel ---")
	{
		ch, err := b.Channel()
		if err != nil {
			log.Fatal(err)
		}
		defer ch.Close()

		// use the raw channel to declare topology directly (same as amqp091)
		if err := ch.ExchangeDeclare(
			"example.low-level",
			"direct",
			true,  // durable
			false, // auto-delete
			false, // internal
			false, // no-wait
			nil,
		); err != nil {
			log.Fatal(err)
		}

		if _, err := ch.QueueDeclare(
			"example.low-level.queue",
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,
		); err != nil {
			log.Fatal(err)
		}

		if err := ch.QueueBind("example.low-level.queue", "low", "example.low-level", false, nil); err != nil {
			log.Fatal(err)
		}

		// publish a message directly on the raw channel
		if err := ch.PublishWithContext(ctx, "example.low-level", "low", false, false, amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte("hello from raw channel"),
			DeliveryMode: amqp.Persistent,
		}); err != nil {
			log.Fatal(err)
		}

		log.Printf("\tdeclared topology and published via raw channel  closed=%v", ch.IsClosed())

		ch.Close()
	}

	// b.Transaction() executes a function inside an AMQP transaction;
	// the transaction commits when fn returns nil, otherwise rolls back;
	// NOTE: AMQP transactions carry ~2x overhead; publisher confirms are preferred for production
	log.Println("--- transaction ---")
	{
		// commit: fn returns nil → both publishes are committed atomically
		if err := b.Transaction(ctx, func(ch broker.Channel) error {
			for i := 1; i <= 3; i++ {
				if err := ch.PublishWithContext(ctx, "example.low-level", "low", false, false, amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(fmt.Sprintf("tx-commit-%d", i)),
				}); err != nil {
					return err
				}
			}
			log.Printf("\t[commit]   published 3 messages inside transaction")
			return nil // commit
		}); err != nil {
			log.Printf("\ttransaction commit error: %v", err)
		} else {
			log.Printf("\t[commit]   transaction committed")
		}

		// rollback: fn returns an error → broker calls TxRollback automatically
		if err := b.Transaction(ctx, func(ch broker.Channel) error {
			if err := ch.PublishWithContext(ctx, "example.low-level", "low", false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("tx-rollback"),
			}); err != nil {
				return err
			}
			log.Printf("\t[rollback] published inside transaction, returning error to trigger rollback")
			return fmt.Errorf("simulated error")
		}); err != nil {
			log.Printf("\t[rollback] transaction rolled back: %v", err)
		}
	}

	// b.Release() closes a managed publisher or consumer and removes it from the
	// broker's internal registry; distinct from endpoint.Close() which only closes
	// the resource but leaves its registry entry intact
	log.Println("--- release ---")
	{
		exchange := broker.NewExchange("example.low-level").WithType("direct").WithDurable(true)

		publisher, err := b.NewPublisher(nil, exchange)
		if err != nil {
			log.Fatal(err)
		}

		if err := publisher.Publish(ctx, "low", broker.NewMessage([]byte("published before release"))); err != nil {
			log.Printf("\tpublish error: %v", err)
		} else {
			log.Printf("\tpublished message via managed publisher")
		}

		// Release closes the publisher and deregisters it from the broker;
		// subsequent calls to b.Close() will not attempt to close it again
		if err := b.Release(publisher); err != nil {
			log.Printf("\trelease error: %v", err)
		} else {
			log.Printf("\tpublisher released (closed + deregistered)")
		}
	}

	// cleanup topology declared via raw channel above
	if ch, err := b.Channel(); err == nil {
		_, _ = ch.QueueDelete("example.low-level.queue", false, false, false)
		_ = ch.ExchangeDelete("example.low-level", false, false)
		ch.Close()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
	default:
	}

	log.Println("done")
}
