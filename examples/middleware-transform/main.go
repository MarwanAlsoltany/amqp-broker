package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
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

	exchange := broker.NewExchange("example.transform").WithType("direct").WithDurable(true)
	queue := broker.NewQueue("example.transform.queue").WithDurable(true)
	topology := broker.NewTopology(
		[]broker.Exchange{exchange},
		[]broker.Queue{queue},
		[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "transform").WithType(broker.BindingTypeQueue)},
	)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	type encoded struct {
		raw      string
		encoding string
		encode   func([]byte) []byte
	}

	messages := []encoded{
		{raw: "plain text message", encoding: "none", encode: func(b []byte) []byte {
			return b
		}},
		{raw: "base64 encoded message", encoding: "base64", encode: func(b []byte) []byte {
			return []byte(base64.StdEncoding.EncodeToString(b))
		}},
		{raw: "gzip compressed message", encoding: "gzip", encode: func(b []byte) []byte {
			var buf bytes.Buffer
			w := gzip.NewWriter(&buf)
			_, _ = w.Write(b)
			_ = w.Close()
			return buf.Bytes()
		}},
		{raw: "uppercase message", encoding: "uppercase", encode: func(b []byte) []byte {
			return []byte(strings.ToUpper(string(b)))
		}},
	}

	for _, m := range messages {
		msg := broker.NewMessage(m.encode([]byte(m.raw)))
		msg.Headers = map[string]any{"encoding": m.encoding}
		if err := b.Publish(ctx, exchange.Name, "transform", msg); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("published %d messages with encodings: none, base64, gzip, uppercase", len(messages))

	var processed atomic.Int32

	// TransformMiddleware rewrites the message body before the handler sees it;
	// stack multiple transforms, they are applied innermost-first (last listed = first run),
	// in this example: gzip -> base64 -> lowercase -> HANDLER
	base64Decoder := broker.TransformMiddleware(&broker.TransformMiddlewareConfig{
		Transform: func(ctx context.Context, body []byte) ([]byte, error) {
			decoded, err := base64.StdEncoding.DecodeString(string(body))
			if err != nil {
				return body, nil // not base64, pass through
			}
			log.Printf("\t%-12s  decoded base64", "[transform]")
			return decoded, nil
		},
		Action: broker.HandlerActionNackDiscard,
	})

	gzipDecompressor := broker.TransformMiddleware(&broker.TransformMiddlewareConfig{
		Transform: func(ctx context.Context, body []byte) ([]byte, error) {
			if len(body) < 2 || body[0] != 0x1f || body[1] != 0x8b {
				return body, nil // not gzip, pass through
			}
			r, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				return body, nil
			}
			defer r.Close()
			out, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			log.Printf("\t%-12s  decompressed gzip", "[transform]")
			return out, nil
		},
		Action: broker.HandlerActionNackDiscard,
	})

	lowercaser := broker.TransformMiddleware(&broker.TransformMiddlewareConfig{
		Transform: func(ctx context.Context, body []byte) ([]byte, error) {
			result := bytes.ToLower(body)
			if !bytes.Equal(result, body) {
				log.Printf("\t%-12s  lowercased", "[transform]")
			}
			return result, nil
		},
		Action: broker.HandlerActionNackDiscard,
	})

	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		n := processed.Add(1)
		log.Printf("\t[message %d]  %s", n, string(msg.Body))
		return broker.HandlerActionAck, nil
	}

	wrapped := broker.WrapHandler(
		handler,
		gzipDecompressor,
		base64Decoder,
		lowercaser,
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
	defer consumer.Close()

	go func() {
		if err := consumer.Consume(ctx); err != nil {
			log.Printf("consumer error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			goto cleanup
		case <-timeout:
			goto cleanup
		case <-ticker.C:
			if processed.Load() >= int32(len(messages)) {
				time.Sleep(100 * time.Millisecond)
				goto cleanup
			}
		}
	}

cleanup:
	log.Printf("summary: processed=%d", processed.Load())

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}
