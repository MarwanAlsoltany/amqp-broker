package broker

import (
	"context"
)

// contextKey is the context key used to store a *Broker in consumer handler contexts.
type contextKey struct{}

// FromContext retrieves the *Broker injected into the context by the Broker when invoking consumer handlers.
// Returns nil if no Broker was injected.
//
// A Broker is automatically injected into every handler context when using
// [Broker.NewConsumer] or [Broker.Consume], enabling reply-to and RPC patterns.
//
// Example:
//
//	handler := func(ctx context.Context, msg *broker.Message) (broker.HandlerAction, error) {
//	    b := broker.FromContext(ctx)
//	    if b != nil {
//	        _ = b.Publish(ctx, "", msg.ReplyTo, broker.NewMessage([]byte("reply")))
//	    }
//	    return broker.HandlerActionAck, nil
//	}
func FromContext(ctx context.Context) *Broker {
	if b, ok := ctx.Value(contextKey{}).(*Broker); ok {
		return b
	}
	return nil
}

// contextWithBroker wraps h and injects b into the handler context on every invocation.
func contextWithBroker(b *Broker, h Handler) Handler {
	return func(ctx context.Context, msg *Message) (HandlerAction, error) {
		return h(context.WithValue(ctx, contextKey{}, b), msg)
	}
}

// contextWithAnyCancel returns a derived context and cancel func.
// The derived context is cancelled when either:
//   - The returned cancel() is called, or
//   - The parent context is cancelled, or
//   - Any of the other contexts is cancelled
func contextWithAnyCancel(parent context.Context, others ...context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)

	if len(others) == 0 {
		return ctx, cancel
	}

	for _, other := range others {
		if other == nil {
			continue
		}

		// watch for other cancelation, exit when either other or ctx is done
		go func(other context.Context) {
			select {
			case <-other.Done():
				// if other cancels, cancel our derived ctx
				cancel()
			case <-ctx.Done():
				// no-op: ctx already cancelled (either by caller or parent)
			}
		}(other)
	}

	return ctx, cancel
}
