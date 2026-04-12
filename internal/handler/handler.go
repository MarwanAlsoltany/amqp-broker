package handler

import (
	"context"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

var (
	// ErrHandler is the base error for handler operations.
	// All handler-related errors wrap this error for consistent error handling.
	ErrHandler = internal.ErrDomain.Sentinel("handler")
)

// Action controls how a consumed message is acknowledged.
type Action int

func (a Action) String() string {
	switch a {
	case ActionAck:
		return "ack"
	case ActionNackRequeue:
		return "nack.requeue"
	case ActionNackDiscard:
		return "nack.discard"
	case ActionNoAction:
		return ""
	default:
		return "unknown"
	}
}

const (
	// ActionAck acknowledges the message, removing it from the queue.
	// Use this when the message was processed successfully.
	ActionAck Action = iota
	// ActionNackRequeue negatively acknowledges and requeues the message.
	// The message will be redelivered, allowing retry of transient failures.
	// Use this for temporary errors like network timeouts or busy resources.
	ActionNackRequeue
	// ActionNackDiscard negatively acknowledges without requeuing.
	// The message is discarded (or sent to a dead-letter exchange if configured).
	// Use this for permanent errors like invalid message format.
	ActionNackDiscard
	// ActionNoAction performs no acknowledgment.
	// The message remains unacknowledged until manually ack'd or the consumer closes.
	// Use this when the handler delegates acknowledgment to another component.
	ActionNoAction
)

// Handler is the function type called for each consumed message.
// It processes the message and returns an action to control acknowledgment.
//
// The context is cancelled when the consumer is stopped, allowing
// handlers to gracefully abort long-running operations.
//
// Example:
//
//	handler := func(ctx context.Context, msg *message.Message) (Action, error) {
//		// process the message
//		if err := process(msg.Body); err != nil {
//			return ActionNackRequeue, err // retry later
//		}
//		return ActionAck, nil // success
//	}
type Handler func(ctx context.Context, msg *message.Message) (Action, error)

// Middleware wraps a [Handler] to add cross-cutting concerns.
// Middlewares execute in order: outermost first, innermost last.
//
// Use [Wrap] to compose multiple middlewares:
//
//	handler := Wrap(myHandler,
//		RecoveryMiddleware(...),  // outermost
//		LoggingMiddleware(...),
//		TimeoutMiddleware(...),   // innermost
//	)
type Middleware func(Handler) Handler

// Wrap wraps a handler with multiple middlewares.
// Middlewares are applied in the order provided: the first middleware
// in the list becomes the outermost wrapper.
//
// Execution order:
//  1. First middleware's pre-handler logic
//  2. Second middleware's pre-handler logic
//  3. ... (additional middlewares)
//  4. Innermost handler execution
//  5. ... (middleware post-handler logic in reverse)
//  6. Second middleware's post-handler logic
//  7. First middleware's post-handler logic
//
// Example:
//
//	handler := Wrap(
//		myHandler,
//		RecoveryMiddleware(&RecoveryMiddlewareConfig{}), // catches panics (outermost)
//		LoggingMiddleware(&LoggingMiddlewareConfig{}),   // logs execution
//		TimeoutMiddleware(&TimeoutMiddlewareConfig{      // enforces timeout (innermost)
//			Timeout: 30 * time.Second,
//		}),
//	)
func Wrap(handler Handler, middlewares ...Middleware) Handler {
	// apply in reverse so first middleware is outermost
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// ActionHandler returns a Handler that always returns the specified action.
// Useful for creating simple handlers or testing.
//
// Example:
//
//	handler := ActionHandler(ActionAck) // always acks
func ActionHandler(action Action) Handler {
	return func(_ context.Context, _ *message.Message) (Action, error) {
		return action, nil
	}
}
