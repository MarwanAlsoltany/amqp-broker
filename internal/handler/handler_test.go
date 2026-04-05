package handler

import (
	"context"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHandler returns a handler that always returns the given [Action].
func testHandler(action Action) Handler {
	return func(ctx context.Context, msg *message.Message) (Action, error) {
		return action, nil
	}
}

// testErrorHandler returns a handler that returns the given [Action] and error.
func testErrorHandler(action Action, err error) Handler {
	return func(ctx context.Context, msg *message.Message) (Action, error) {
		return action, err
	}
}

// testSleepHandler returns a handler that sleeps for the given duration before returning.
func testSleepHandler(action Action, sleep time.Duration) Handler {
	return func(ctx context.Context, msg *message.Message) (Action, error) {
		time.Sleep(sleep)
		return action, nil
	}
}

func TestActionString(t *testing.T) {
	tests := []struct {
		action   Action
		expected string
	}{
		{ActionAck, "ack"},
		{ActionNackRequeue, "nack.requeue"},
		{ActionNackDiscard, "nack.discard"},
		{ActionNoAction, ""},
		{Action(999), "unknown"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.action.String())
	}
}

func TestWrap(t *testing.T) {
	callOrder := []string{}

	middleware1 := func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			callOrder = append(callOrder, "m1-before")
			action, err := next(ctx, msg)
			callOrder = append(callOrder, "m1-after")
			return action, err
		}
	}

	middleware2 := func(next Handler) Handler {
		return func(ctx context.Context, msg *message.Message) (Action, error) {
			callOrder = append(callOrder, "m2-before")
			action, err := next(ctx, msg)
			callOrder = append(callOrder, "m2-after")
			return action, err
		}
	}

	handler := func(ctx context.Context, msg *message.Message) (Action, error) {
		callOrder = append(callOrder, "handler")
		return ActionAck, nil
	}

	wrapped := Wrap(handler, middleware1, middleware2)

	ctx := t.Context()
	msg := message.New([]byte("test"))
	action, err := wrapped(ctx, &msg)

	require.NoError(t, err)
	assert.Equal(t, ActionAck, action)
	assert.Equal(t, []string{"m1-before", "m2-before", "handler", "m2-after", "m1-after"}, callOrder)
}
