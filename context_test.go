package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromContext(t *testing.T) {
	t.Run("WhenBrokerInjected", func(t *testing.T) {
		b := &Broker{}
		ctx := context.WithValue(t.Context(), contextKey{}, b)

		got := FromContext(ctx)
		assert.Same(t, b, got)
	})

	t.Run("WhenNotInjected", func(t *testing.T) {
		got := FromContext(t.Context())
		assert.Nil(t, got)
	})

	t.Run("WhenWrongType", func(t *testing.T) {
		ctx := context.WithValue(t.Context(), contextKey{}, "not-a-broker")

		got := FromContext(ctx)
		assert.Nil(t, got)
	})
}

func TestContextWithBroker(t *testing.T) {
	t.Run("InjectsIntoContext", func(t *testing.T) {
		b := &Broker{}
		var got *Broker

		h := contextWithBroker(b, func(ctx context.Context, msg *Message) (HandlerAction, error) {
			got = FromContext(ctx)
			return HandlerActionAck, nil
		})

		_, err := h(t.Context(), &Message{})
		require.NoError(t, err)
		assert.Same(t, b, got)
	})

	t.Run("ForwardsReturnValues", func(t *testing.T) {
		b := &Broker{}
		sentinel := errors.New("sentinel")

		h := contextWithBroker(b, func(_ context.Context, _ *Message) (HandlerAction, error) {
			return HandlerActionNackRequeue, sentinel
		})

		action, err := h(t.Context(), &Message{})
		assert.Equal(t, HandlerActionNackRequeue, action)
		assert.ErrorIs(t, err, sentinel)
	})
}

func TestContextWithAnyCancel(t *testing.T) {
	t.Run("CancelByFunc", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		other, otherCancel := context.WithCancel(t.Context())
		defer otherCancel()
		ctx, cancel := contextWithAnyCancel(parent, other)
		cancel()
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by cancel()")
		}
	})

	t.Run("CancelByParent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		other, otherCancel := context.WithCancel(t.Context())
		defer otherCancel()
		ctx, cancel := contextWithAnyCancel(parent, other)
		defer cancel()
		parentCancel()
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by parent")
		}
	})

	t.Run("CancelByOther", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		other, otherCancel := context.WithCancel(t.Context())
		defer otherCancel()
		ctx, cancel := contextWithAnyCancel(parent, other)
		defer cancel()
		otherCancel()
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by other")
		}
	})

	t.Run("CancelByAnyOther", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		o1, o1Cancel := context.WithCancel(t.Context())
		defer o1Cancel()
		o2, o2Cancel := context.WithCancel(t.Context())
		defer o2Cancel()
		o3, o3Cancel := context.WithCancel(t.Context())
		defer o3Cancel()
		ctx, cancel := contextWithAnyCancel(parent, o1, o2, o3)
		defer cancel()
		o2Cancel()
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by second of multiple others")
		}
	})

	t.Run("WithNoOthers", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		ctx, cancel := contextWithAnyCancel(parent)
		defer cancel()
		// should not be done yet
		select {
		case <-ctx.Done():
			t.Error("context cancelled prematurely with no others")
		default:
		}
		parentCancel()
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by parent when no others")
		}
	})

	t.Run("WithNilOther", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		ctx, cancel := contextWithAnyCancel(parent, nil, nil)
		defer cancel()
		// should not be done yet
		select {
		case <-ctx.Done():
			t.Error("context cancelled prematurely with only nil others")
		default:
		}
		parentCancel()
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
			t.Error("context not cancelled by parent when others are nil")
		}
	})
}
