package main

import (
	"context"
	"fmt"
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
	ctx := context.Background()

	b, err := broker.New(broker.WithURL("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	// --- basic MetricsMiddleware ---
	//
	// MetricsMiddleware logs processing duration via structured logging and optionally
	// invokes a Record callback for integration with external metrics systems;
	// Level defaults to slog.LevelInfo, set it explicitly to change severity
	log.Println("--- basic metrics ---")
	{
		exchange := broker.NewExchange("example.metrics").WithType("direct").WithDurable(true)
		queue := broker.NewQueue("example.metrics.queue").WithDurable(true)
		topology := broker.NewTopology(
			[]broker.Exchange{exchange},
			[]broker.Queue{queue},
			[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "metrics").WithType(broker.BindingTypeQueue)},
		)

		if err := b.Declare(&topology); err != nil {
			log.Fatal(err)
		}

		type task struct {
			body  string
			sleep time.Duration
		}

		tasks := []task{
			{"fast", 10 * time.Millisecond},
			{"normal", 50 * time.Millisecond},
			{"slow", 150 * time.Millisecond},
			{"instant", 0},
		}

		for _, t := range tasks {
			if err := b.Publish(ctx, exchange.Name, "metrics", broker.NewMessage([]byte(t.body))); err != nil {
				log.Fatal(err)
			}
		}

		log.Printf("\tpublished %d messages", len(tasks))

		var processed atomic.Int32

		// custom aggregated stats accumulate inside the Record callback
		type stats struct {
			count         int
			totalDuration time.Duration
			minDuration   time.Duration
			maxDuration   time.Duration
		}
		var s stats
		s.minDuration = time.Hour

		handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			processed.Add(1)
			for _, t := range tasks {
				if t.body == string(msg.Body) {
					time.Sleep(t.sleep)
					break
				}
			}
			return broker.HandlerActionAck, nil
		}

		wrapped := broker.WrapHandler(
			handler,
			broker.MetricsMiddleware(&broker.MetricsMiddlewareConfig{
				Logger: slog.Default(),
				Level:  slog.LevelDebug,
				Record: func(ctx context.Context, msg *broker.Message, action broker.HandlerAction, err error, duration time.Duration) {
					s.count++
					s.totalDuration += duration
					if duration < s.minDuration {
						s.minDuration = duration
					}
					if duration > s.maxDuration {
						s.maxDuration = duration
					}
					log.Printf("\t[record]  body=%-12s  action=%-12s  duration=%-12v  err=%v",
						string(msg.Body), action, duration.Round(time.Microsecond), err)

				},
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

		waitUntil(func() bool { return processed.Load() >= int32(len(tasks)) }, 5*time.Second)
		consumer.Close()

		if s.count > 0 {
			avg := s.totalDuration / time.Duration(s.count)
			log.Printf("\tsummary: count=%d total=%v avg=%v min=%v max=%v",
				s.count,
				s.totalDuration.Round(time.Microsecond),
				avg.Round(time.Microsecond),
				s.minDuration.Round(time.Microsecond),
				s.maxDuration.Round(time.Microsecond))
		}

		if err := b.Delete(&topology); err != nil {
			log.Printf("cleanup: %v", err)
		}
	}

	// --- Metrics struct with success/failure tracking and periodic reporting ---
	//
	// the Record callback can drive a more structured Metrics type that separates
	// success from failure counts and reports periodically rather than only at shutdown;
	// this pattern is useful when you want live insight into handler health
	log.Println("--- metrics struct ---")
	{
		exchange := broker.NewExchange("example.metrics.struct").WithType("direct").WithDurable(true)
		queue := broker.NewQueue("example.metrics.struct.queue").WithDurable(true)
		topology := broker.NewTopology(
			[]broker.Exchange{exchange},
			[]broker.Queue{queue},
			[]broker.Binding{broker.NewBinding(exchange.Name, queue.Name, "struct").WithType(broker.BindingTypeQueue)},
		)

		if err := b.Declare(&topology); err != nil {
			log.Fatal(err)
		}

		tasks := []struct {
			body string
			fail bool
			ms   int
		}{
			{"fast", false, 20},
			{"medium", false, 80},
			{"slow", false, 200},
			{"error", true, 30},
			{"fast2", false, 15},
			{"error2", true, 10},
		}

		for _, t := range tasks {
			if err := b.Publish(ctx, exchange.Name, "struct", broker.NewMessage([]byte(t.body))); err != nil {
				log.Fatal(err)
			}
		}

		log.Printf("\tpublished %d messages", len(tasks))

		m := &Metrics{}

		handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
			for _, t := range tasks {
				if t.body == string(msg.Body) {
					time.Sleep(time.Duration(t.ms) * time.Millisecond)
					if t.fail {
						return broker.HandlerActionNackDiscard, fmt.Errorf("simulated failure: %s", msg.Body)
					}
					return broker.HandlerActionAck, nil
				}
			}
			return broker.HandlerActionAck, nil
		}

		wrapped := broker.WrapHandler(
			handler,
			broker.MetricsMiddleware(&broker.MetricsMiddlewareConfig{
				Logger: slog.Default(),
				Level:  slog.LevelDebug,
				Record: func(ctx context.Context, msg *broker.Message, action broker.HandlerAction, err error, duration time.Duration) {
					m.Record(action, err, duration)
				},
			}),
		)

		consumer, err := b.NewConsumer(
			&broker.ConsumerOptions{
				EndpointOptions: broker.EndpointOptions{NoAutoDeclare: true},
				PrefetchCount:   3,
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

		// periodic reporter: prints a snapshot every second while messages arrive
		stopReport := make(chan struct{})
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopReport:
					return
				case <-ticker.C:
					s := m.Report()
					log.Printf("\t[periodic]  received=%d  processed=%d  failed=%d  avg=%v  min=%v  max=%v",
						s.Received, s.Processed, s.Failed,
						s.Avg.Round(time.Millisecond),
						s.Min.Round(time.Millisecond),
						s.Max.Round(time.Millisecond),
					)
				}
			}
		}()

		waitUntil(func() bool { return m.Total() >= int64(len(tasks)) }, 10*time.Second)
		close(stopReport)
		consumer.Close()

		s := m.Report()
		log.Printf("\tsummary: received=%d processed=%d failed=%d avg=%v min=%v max=%v",
			s.Received, s.Processed, s.Failed,
			s.Avg.Round(time.Millisecond),
			s.Min.Round(time.Millisecond),
			s.Max.Round(time.Millisecond),
		)

		if err := b.Delete(&topology); err != nil {
			log.Printf("cleanup: %v", err)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
	default:
	}

	log.Println("done")
}

// Metrics tracks per-handler outcome and latency using atomics for concurrency safety.
type Metrics struct {
	received  atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
	totalNs   atomic.Int64 // sum of durations in nanoseconds
	minNs     atomic.Int64 // minimum duration in nanoseconds
	maxNs     atomic.Int64 // maximum duration in nanoseconds
}

// MetricsReport is a snapshot of collected metrics.
type MetricsReport struct {
	Received  int64
	Processed int64
	Failed    int64
	Avg       time.Duration
	Min       time.Duration
	Max       time.Duration
}

// Record updates the metrics with a single observation.
func (m *Metrics) Record(action broker.HandlerAction, err error, d time.Duration) {
	m.received.Add(1)
	if err == nil && action == broker.HandlerActionAck {
		m.processed.Add(1)
	} else {
		m.failed.Add(1)
	}

	ns := d.Nanoseconds()
	m.totalNs.Add(ns)

	// update min with CAS loop
	for {
		cur := m.minNs.Load()
		if cur != 0 && cur <= ns {
			break
		}
		if m.minNs.CompareAndSwap(cur, ns) {
			break
		}
	}

	// update max with CAS loop
	for {
		cur := m.maxNs.Load()
		if cur >= ns {
			break
		}
		if m.maxNs.CompareAndSwap(cur, ns) {
			break
		}
	}
}

// Total returns the total number of recorded observations.
func (m *Metrics) Total() int64 { return m.received.Load() }

// Report returns a snapshot of the current metrics.
func (m *Metrics) Report() MetricsReport {
	n := m.received.Load()
	var avg time.Duration
	if n > 0 {
		avg = time.Duration(m.totalNs.Load() / n)
	}
	return MetricsReport{
		Received:  n,
		Processed: m.processed.Load(),
		Failed:    m.failed.Load(),
		Avg:       avg,
		Min:       time.Duration(m.minNs.Load()),
		Max:       time.Duration(m.maxNs.Load()),
	}
}

// waitUntil blocks until predicate returns true or the timeout elapses.
func waitUntil(predicate func() bool, d time.Duration) {
	deadline := time.After(d)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return
		case <-ticker.C:
			if predicate() {
				time.Sleep(50 * time.Millisecond)
				return
			}
		}
	}
}
