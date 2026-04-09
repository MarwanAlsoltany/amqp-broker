package broker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// testName generates a unique name for tests
func testName(prefix string) string {
	now := time.Now()
	// prefix-yyyymmdd-HHMMSS-microseconds
	return fmt.Sprintf("%s-%04d%02d%02d-%02d%02d%02d-%06d",
		prefix,
		now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(),
		now.Nanosecond()/1e3,
	)
}

// handler returns a handler that always returns the given HandlerAction.
func testHandler(ackAction HandlerAction) func(context.Context, *Message) (HandlerAction, error) {
	return func(ctx context.Context, msg *Message) (HandlerAction, error) {
		return ackAction, nil
	}
}

// countingHandler returns a handler that increments the given atomic counter and returns the given HandlerAction.
func testCountingHandler(ackAction HandlerAction, counter *atomic.Int32) func(context.Context, *Message) (HandlerAction, error) {
	return func(ctx context.Context, msg *Message) (HandlerAction, error) {
		counter.Add(1)
		return ackAction, nil
	}
}

// newTestBrokerWithConnection creates a Broker whose connection pool is seeded with conn.
// Pool is disabled (cacheTTL=0) to keep setup minimal.
// The broker must be closed by the caller.
func newTestBrokerWithConnection(conn Connection) *Broker {
	b, err := New(
		WithConnectionDialer(func(_ string, _ *Config) (Connection, error) {
			return conn, nil
		}),
		WithCache(0),
	)
	if err != nil {
		panic("newTestBrokerWithConnection: " + err.Error())
	}
	return b
}
