package broker

import (
	"context"
	"fmt"
	"time"
)

// Middleware wraps a Handler to provide additional functionality.
type Middleware func(Handler) Handler

func BoundaryMiddlewareHandler(next Handler) Handler {
	return func(ctx context.Context, msg *Message) (AckAction, error) {
		// Pre-processing logic here
		fmt.Println("Before handling message:", msg.MessageID)

		// Call the next handler
		action, err := next(ctx, msg)

		// Post-processing logic here
		fmt.Println("After handling message:", msg.MessageID)

		return action, err
	}
}

func TimeMiddlewareHandler(next Handler) Handler {
	return func(ctx context.Context, msg *Message) (AckAction, error) {
		// Start metrics collection
		startTime := time.Now()

		// Call the next handler
		action, err := next(ctx, msg)

		// End metrics collection
		duration := time.Since(startTime)
		fmt.Printf("Message %q processed in %vms\n", msg.MessageID, duration/time.Millisecond)

		return action, err
	}
}

func LoggingMiddlewareHandler(next Handler) Handler {
	return func(ctx context.Context, msg *Message) (AckAction, error) {
		// Log message receipt
		fmt.Printf("Message received: %s\n", msg.MessageID)

		// Call the next handler
		action, err := next(ctx, msg)

		// Log message completion
		if err != nil {
			fmt.Printf("Message %s processing failed: %v\n", msg.MessageID, err)
		} else {
			fmt.Printf("Message %s processed successfully\n", msg.MessageID)
		}

		return action, err
	}
}

func RetryMiddleware(next Handler, retries int, delay time.Duration) Handler {
	return func(ctx context.Context, msg *Message) (AckAction, error) {
		var action AckAction
		var err error
		for attempt := 0; attempt <= retries; attempt++ {
			action, err = next(ctx, msg)
			if err == nil {
				return action, nil
			}
			fmt.Printf("Retry %d for message %s after error: %v\n", attempt+1, msg.MessageID, err)
			time.Sleep(delay)
		}
		return action, err
	}
}
