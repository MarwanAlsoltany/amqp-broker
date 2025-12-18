package broker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// testURL is an invalid AMQP URL for testing purposes.
const testURL = "amqp://invalid"

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

// testErrorHandler returns a handler that returns the given HandlerAction and error.
func testErrorHandler(ackAction HandlerAction, err error) func(context.Context, *Message) (HandlerAction, error) {
	return func(ctx context.Context, msg *Message) (HandlerAction, error) {
		return ackAction, err
	}
}

// testSleepHandler returns a handler that sleeps for the given duration before returning the given HandlerAction.
func testSleepHandler(ackAction HandlerAction, sleep time.Duration) func(context.Context, *Message) (HandlerAction, error) {
	return func(ctx context.Context, msg *Message) (HandlerAction, error) {
		time.Sleep(sleep)
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

// testProcessingHandler returns a handler that simulates processing by sleeping for the given duration
// and increments the given atomic counter when processing starts.
func testProcessingHandler(ackAction HandlerAction, delay time.Duration, counter *atomic.Int32) func(context.Context, *Message) (HandlerAction, error) {
	return func(ctx context.Context, msg *Message) (HandlerAction, error) {
		time.Sleep(delay)
		counter.Add(1)
		return ackAction, nil
	}
}
