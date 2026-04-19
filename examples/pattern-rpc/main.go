package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	broker "github.com/MarwanAlsoltany/amqp-broker"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	// the RPC server listens on this queue; the client publishes requests to it
	rpcQueue := broker.NewQueue("rpc.calculator").WithDurable(false).WithAutoDelete(true)
	topology := broker.NewTopology(nil, []broker.Queue{rpcQueue}, nil)

	if err := b.Declare(&topology); err != nil {
		log.Fatal(err)
	}

	// start the RPC server
	go startRPCServer(ctx, b, rpcQueue)
	time.Sleep(300 * time.Millisecond)

	client := NewRPCClient(b)
	defer client.Close()

	calls := []struct {
		op string
		a  int
		b  int
	}{
		{"add", 10, 5},
		{"subtract", 10, 5},
		{"multiply", 10, 5},
		{"divide", 10, 5},
		{"divide", 10, 0}, // error case: division by zero
	}

	log.Printf("--- rpc calls ---")
	for _, c := range calls {
		req := Request{Operation: c.op, A: c.a, B: c.b}
		resp, err := client.Call(ctx, rpcQueue.Name, req, 5*time.Second)
		if err != nil {
			log.Printf("\t%s(%d,%d): error: %v", req.Operation, req.A, req.B, err)
			continue
		}
		if resp.Error != "" {
			log.Printf("\t%s(%d,%d): server error: %s", req.Operation, req.A, req.B, resp.Error)
		} else {
			log.Printf("\t%s(%d,%d)=%d", req.Operation, req.A, req.B, resp.Result)
		}
	}

	if err := b.Delete(&topology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	log.Println("done")
}

// Request is an RPC Request payload.
type Request struct {
	Operation string `json:"operation"`
	A         int    `json:"a"`
	B         int    `json:"b"`
}

// Response is an RPC Response payload.
type Response struct {
	Result int    `json:"result"`
	Error  string `json:"error,omitempty"`
}

// RPCClient handles client-side RPC calls over AMQP.
// The client declares a private reply queue and correlates responses using CorrelationID.
type RPCClient struct {
	broker       *broker.Broker
	replyQueue   broker.Queue
	consumer     broker.Consumer
	pending      map[string]chan Response
	pendingMu    sync.Mutex
	closeOnce    sync.Once
	ctxCancel    context.CancelFunc
	consumerDone chan struct{}
}

// NewRPCClient creates an RPC client and starts consuming replies.
func NewRPCClient(b *broker.Broker) *RPCClient {
	queueName := fmt.Sprintf("rpc.reply.%d", time.Now().UnixNano())
	replyQueue := broker.NewQueue(queueName).WithExclusive(true).WithAutoDelete(true)

	ctx, cancel := context.WithCancel(context.Background())

	c := &RPCClient{
		broker:       b,
		replyQueue:   replyQueue,
		pending:      make(map[string]chan Response),
		ctxCancel:    cancel,
		consumerDone: make(chan struct{}),
	}

	consumer, err := b.NewConsumer(nil, replyQueue, c.handleReply)
	if err != nil {
		log.Fatal(err)
	}
	c.consumer = consumer

	go func() {
		defer close(c.consumerDone)
		if err := consumer.Consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("client consumer: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	return c
}

// Call publishes a request and blocks until the response arrives or timeout elapses.
func (c *RPCClient) Call(ctx context.Context, queue string, req Request, timeout time.Duration) (*Response, error) {
	corrID := fmt.Sprintf("rpc-%d", time.Now().UnixNano())
	resCh := make(chan Response, 1)

	c.pendingMu.Lock()
	c.pending[corrID] = resCh
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, corrID)
		c.pendingMu.Unlock()
		close(resCh)
	}()

	body, _ := json.Marshal(req)
	msg := broker.NewMessage(body)
	msg.ContentType = "application/json"
	msg.CorrelationID = corrID
	msg.ReplyTo = c.replyQueue.Name

	if err := c.broker.Publish(ctx, "", queue, msg); err != nil {
		return nil, err
	}

	tCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case res := <-resCh:
		return &res, nil
	case <-tCtx.Done():
		return nil, fmt.Errorf("rpc timeout: %w", tCtx.Err())
	}
}

func (c *RPCClient) handleReply(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
	var res Response
	if err := json.Unmarshal(msg.Body, &res); err != nil {
		return broker.HandlerActionAck, nil
	}
	c.pendingMu.Lock()
	ch, ok := c.pending[msg.CorrelationID]
	c.pendingMu.Unlock()
	if ok {
		ch <- res
	}
	return broker.HandlerActionAck, nil
}

// Close shuts down the reply consumer.
func (c *RPCClient) Close() {
	c.closeOnce.Do(func() {
		c.ctxCancel()
		if c.consumer != nil {
			c.consumer.Close()
			<-c.consumerDone
		}
	})
}

func startRPCServer(ctx context.Context, b *broker.Broker, queue broker.Queue) {
	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
		var req Request
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			log.Printf("\tserver: invalid request: %v", err)
			return broker.HandlerActionAck, nil
		}

		log.Printf("\tserver: %s(%d,%d)", req.Operation, req.A, req.B)
		resp := processRequest(req)

		if msg.ReplyTo != "" {
			body, _ := json.Marshal(resp)
			reply := broker.NewMessage(body)
			reply.ContentType = "application/json"
			reply.CorrelationID = msg.CorrelationID
			if err := b.Publish(ctx, "", msg.ReplyTo, reply); err != nil {
				log.Printf("\tserver: failed to send reply: %v", err)
			}
		}
		return broker.HandlerActionAck, nil
	}

	consumer, err := b.NewConsumer(
		&broker.ConsumerOptions{
			EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
		},
		queue, handler,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	log.Println("\tserver: started")
	if err := consumer.Consume(ctx); err != nil {
		log.Printf("\tserver: %v", err)
	}
}

func processRequest(req Request) Response {
	switch strings.ToLower(req.Operation) {
	case "add":
		return Response{Result: req.A + req.B}
	case "subtract":
		return Response{Result: req.A - req.B}
	case "multiply":
		return Response{Result: req.A * req.B}
	case "divide":
		if req.B == 0 {
			return Response{Error: "division by zero"}
		}
		return Response{Result: req.A / req.B}
	default:
		return Response{Error: fmt.Sprintf("unknown operation: %s", req.Operation)}
	}
}
