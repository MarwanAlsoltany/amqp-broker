package main

import (
	"context"
	"log"

	broker "github.com/MarwanAlsoltany/amqp-broker"
)

func main() {
	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	ctx := context.Background()

	// - direct: routes by exact routing key match.
	// - topic: routes by wildcard patterns (* = one word, # = zero or more words).
	// - fanout: broadcasts to all bound queues, routing key ignored.
	// - headers: routes by message header attributes (x-match: all|any).
	log.Println("--- exchange types ---")

	eTopology := broker.NewTopology(
		[]broker.Exchange{
			broker.NewExchange("exchange.direct").
				WithType("direct").WithDurable(true),
			broker.NewExchange("exchange.topic").
				WithType("topic").WithDurable(true),
			broker.NewExchange("exchange.fanout").
				WithType("fanout").WithDurable(true),
			broker.NewExchange("exchange.headers").
				WithType("headers").WithDurable(true),
			broker.NewExchange("exchange.autodelete").
				WithType("direct").WithDurable(false).WithAutoDelete(true),
			broker.NewExchange("exchange.internal").
				WithType("direct").WithDurable(true).WithInternal(true),
			broker.NewExchange("exchange.args").
				WithType("direct").WithDurable(true).WithArguments(broker.Arguments{"x-custom": "value"}),
		},
		nil, nil,
	)

	if err := b.Declare(&eTopology); err != nil {
		log.Fatal(err)
	}

	for _, e := range eTopology.Exchanges {
		log.Printf("\tdeclared exchange: %s (type=%s durable=%v auto-delete=%v internal=%v)",
			e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal)
	}

	if err := b.Delete(&eTopology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	// - durable: survives broker restart.
	// - auto-delete: deleted when the last consumer disconnects.
	// - exclusive: bound to one connection; deleted when it closes.
	// - with x-message-ttl: messages expire after the given milliseconds.
	// - with x-max-length: queue holds at most N messages (oldest dropped first).
	// - with x-dead-letter-exchange: rejected/expired messages routed to DLX.
	// - with x-max-priority: enables priority queuing (0–N).
	log.Println("--- queue types ---")

	qTopology := broker.NewTopology(
		nil,
		[]broker.Queue{
			broker.NewQueue("queue.durable").
				WithDurable(true),
			broker.NewQueue("queue.autodelete").
				WithDurable(false).WithAutoDelete(true),
			broker.NewQueue("queue.exclusive").
				WithDurable(false).WithExclusive(true),
			broker.NewQueue("queue.ttl").
				WithDurable(true).WithArguments(broker.Arguments{"x-message-ttl": int32(60000)}),
			broker.NewQueue("queue.maxlen").
				WithDurable(true).WithArguments(broker.Arguments{"x-max-length": int32(1000)}),
			broker.NewQueue("queue.dlx").
				WithDurable(true).WithArguments(broker.Arguments{"x-dead-letter-exchange": "example.dlx"}),
			broker.NewQueue("queue.priority").
				WithDurable(true).WithArguments(broker.Arguments{"x-max-priority": int32(10)}),
		},
		nil,
	)

	if err := b.Declare(&qTopology); err != nil {
		log.Fatal(err)
	}

	for _, q := range qTopology.Queues {
		log.Printf("\tdeclared queue: %s (durable=%v auto-delete=%v exclusive=%v args=%v)",
			q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.Arguments)
	}

	if err := b.Delete(&qTopology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	// queue binding:    links an exchange to a queue with an optional routing key.
	// exchange binding: links an exchange to another exchange (message chaining).
	// headers binding:  matches on message header values instead of routing key.
	//                   x-match:"all" requires every header to match;
	//                   x-match:"any" requires at least one.
	log.Println("--- binding types ---")

	be1 := broker.NewExchange("exchnage.direct").WithType("direct").WithDurable(true)
	be2 := broker.NewExchange("exchnage.topic").WithType("topic").WithDurable(true)
	be3 := broker.NewExchange("exchnage.fanout").WithType("fanout").WithDurable(true)
	be4 := broker.NewExchange("exchnage.routing").WithType("direct").WithDurable(true)
	be5 := broker.NewExchange("exchnage.headers").WithType("headers").WithDurable(true)

	bq1 := broker.NewQueue("exchnage.direct.queue").WithDurable(true)
	bq2 := broker.NewQueue("exchnage.topic.queue").WithDurable(true)
	bq3 := broker.NewQueue("exchnage.fanout.queue1").WithDurable(true)
	bq4 := broker.NewQueue("exchnage.fanout.queue2").WithDurable(true)
	bq5 := broker.NewQueue("exchnage.headers.queue").WithDurable(true)

	bTopology := broker.NewTopology(
		[]broker.Exchange{be1, be2, be3, be4, be5},
		[]broker.Queue{bq1, bq2, bq3, bq4, bq5},
		[]broker.Binding{
			// exact routing key
			broker.NewBinding(be1.Name, bq1.Name, "direct.key").
				WithType(broker.BindingTypeQueue),
			// wildcard pattern: events.<any-word>.created
			broker.NewBinding(be2.Name, bq2.Name, "events.*.created").
				WithType(broker.BindingTypeQueue),
			// fanout: routing key ignored
			broker.NewBinding(be3.Name, bq3.Name, "").
				WithType(broker.BindingTypeQueue),
			broker.NewBinding(be3.Name, bq4.Name, "").
				WithType(broker.BindingTypeQueue),
			// exchange-to-exchange binding
			broker.NewBinding(be4.Name, be1.Name, "route.key").
				WithType(broker.BindingTypeExchange),
			// headers binding: all headers must match
			broker.NewBinding(be5.Name, bq5.Name, "").
				WithType(broker.BindingTypeQueue).
				WithArguments(broker.Arguments{"x-match": "all", "format": "pdf", "type": "report"}),
		},
	)

	if err := b.Declare(&bTopology); err != nil {
		log.Fatal(err)
	}

	for _, bind := range bTopology.Bindings {
		log.Printf("\tdeclared binding: %s -> %s (key=%q type=%s)", bind.Source, bind.Destination, bind.Key, bind.Type)
	}

	if err := b.Delete(&bTopology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	// NewRoutingKey() builds a routing key from a template with named placeholders;
	// placeholders like {service} are replaced with values from the provided map
	log.Println("--- routing keys ---")

	rkExchange := broker.NewExchange("rk.topic").WithType("topic").WithDurable(true)
	rkQueue := broker.NewQueue("rk.queue").WithDurable(true)
	rkTopology := broker.NewTopology(
		[]broker.Exchange{rkExchange},
		[]broker.Queue{rkQueue},
		nil,
	)

	if err := b.Declare(&rkTopology); err != nil {
		log.Fatal(err)
	}

	template := "service.{service}.{action}"

	userKey := broker.NewRoutingKey(template, map[string]string{"service": "user", "action": "*"})
	orderKey := broker.NewRoutingKey(template, map[string]string{"service": "order", "action": "created"})

	log.Printf("\ttemplate:      %s", template)
	log.Printf("\tuser binding:  %s", userKey)
	log.Printf("\torder binding: %s", orderKey)

	bindT := broker.NewTopology(nil, nil, []broker.Binding{
		broker.NewBinding(rkExchange.Name, rkQueue.Name, userKey.String()).WithType(broker.BindingTypeQueue),
		broker.NewBinding(rkExchange.Name, rkQueue.Name, orderKey.String()).WithType(broker.BindingTypeQueue),
	})

	if err := b.Declare(&bindT); err != nil {
		log.Fatal(err)
	}

	publishCases := []struct{ key, body string }{
		{
			broker.NewRoutingKey(template, map[string]string{"service": "user", "action": "created"}).String(),
			"user created",
		},
		{
			broker.NewRoutingKey(template, map[string]string{"service": "order", "action": "created"}).String(),
			"order created",
		},
		{
			broker.NewRoutingKey(template, map[string]string{"service": "order", "action": "cancelled"}).String(),
			"order cancelled (unmatched)",
		},
	}

	for _, pc := range publishCases {
		m := broker.NewMessage([]byte(pc.body))
		if err := b.Publish(ctx, rkExchange.Name, pc.key, m); err != nil {
			log.Fatal(err)
		}
		log.Printf("\tpublished: %q routing-key=%s", pc.body, pc.key)
	}

	if err := b.Delete(&bindT); err != nil {
		log.Printf("cleanup: %v", err)
	}

	if err := b.Delete(&rkTopology); err != nil {
		log.Printf("cleanup: %v", err)
	}

	// b.Sync() reconciles declared resources with a desired topology snapshot:
	// - new entries in the snapshot are declared on the broker
	// - entries absent from the snapshot are deleted from the broker
	log.Println("--- topology sync ---")

	desired := broker.NewTopology(
		[]broker.Exchange{
			broker.NewExchange("sync.exchange1").WithType("direct").WithDurable(true),
			broker.NewExchange("sync.exchange2").WithType("topic").WithDurable(true),
		},
		[]broker.Queue{
			broker.NewQueue("sync.queue1").WithDurable(true),
			broker.NewQueue("sync.queue2").WithDurable(true),
		},
		[]broker.Binding{
			broker.NewBinding("sync.exchange1", "sync.queue1", "key1").WithType(broker.BindingTypeQueue),
			broker.NewBinding("sync.exchange2", "sync.queue2", "key2.*").WithType(broker.BindingTypeQueue),
		},
	)

	if err := b.Declare(&desired); err != nil {
		log.Fatal(err)
	}

	log.Printf("\tdeclared: 2 exchanges, 2 queues, 2 bindings")

	// add a third exchange, queue, and binding to desired state
	desired.Exchanges = append(desired.Exchanges, broker.NewExchange("sync.exchange3").WithType("fanout").WithDurable(true))
	desired.Queues = append(desired.Queues, broker.NewQueue("sync.queue3").WithDurable(true))
	desired.Bindings = append(desired.Bindings, broker.NewBinding("sync.exchange3", "sync.queue3", "").WithType(broker.BindingTypeQueue))

	if err := b.Sync(&desired); err != nil {
		log.Fatal(err)
	}

	log.Printf("\tsynced (add): 3 exchanges, 3 queues, 3 bindings")

	// remove the third resources, Sync() will delete them from the broker
	desired.Exchanges = desired.Exchanges[:2]
	desired.Queues = desired.Queues[:2]
	desired.Bindings = desired.Bindings[:2]

	if err := b.Sync(&desired); err != nil {
		log.Fatal(err)
	}

	log.Printf("\tsynced (remove): back to 2 exchanges, 2 queues, 2 bindings")

	if err := b.Delete(&desired); err != nil {
		log.Printf("cleanup: %v", err)
	}

	// b.Declare() - creates resources if they don't exist
	// b.Verify() - passive declare; fails if a resource doesn't exist or mismatches config
	// b.Exchange() / b.Queue() / b.Binding() - query locally tracked topology
	// b.Delete() - removes resources from the broker and local tracking
	log.Println("--- topology operations ---")

	opExchange := broker.NewExchange("ops.exchange").WithType("direct").WithDurable(true)
	opQueue := broker.NewQueue("ops.queue").WithDurable(true)
	opBinding := broker.NewBinding(opExchange.Name, opQueue.Name, "ops.key").WithType(broker.BindingTypeQueue)
	opTopology := broker.NewTopology(
		[]broker.Exchange{opExchange},
		[]broker.Queue{opQueue},
		[]broker.Binding{opBinding},
	)

	if err := b.Declare(&opTopology); err != nil {
		log.Fatal(err)
	}

	log.Println("\tdeclared topology")

	if err := b.Verify(&opTopology); err != nil {
		log.Fatalf("verify failed: %v", err)
	}

	log.Println("\tverified: topology exists on broker")

	if e := b.Exchange(opExchange.Name); e != nil {
		log.Printf("\tquery exchange: %s (type=%s durable=%v)", e.Name, e.Type, e.Durable)
	}

	if q := b.Queue(opQueue.Name); q != nil {
		log.Printf("\tquery queue: %s (durable=%v)", q.Name, q.Durable)
	}

	if bind := b.Binding(opExchange.Name, opQueue.Name, "ops.key"); bind != nil {
		log.Printf("\tquery binding: %s -> %s (key=%s)", bind.Source, bind.Destination, bind.Key)
	}

	if err := b.Delete(&opTopology); err != nil {
		log.Fatal(err)
	}

	log.Println("\tdeleted topology")

	if b.Exchange(opExchange.Name) == nil {
		log.Printf("\tquery after delete: exchange %s not found (expected)", opExchange.Name)
	}

	if err := b.Verify(&opTopology); err != nil {
		log.Printf("\tverify after delete: failed as expected (%v)", err)
	}

	log.Println("done")
}
