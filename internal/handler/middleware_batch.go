package handler

import (
	"context"
	"iter"
	"sync"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
)

// BatchHandler processes a batch of messages delivered as a lazy indexed iterator.
//
// Two acknowledgment patterns are supported:
//
//  1. Uniform action: return [ActionAck], [ActionNackRequeue], or [ActionNackDiscard].
//     The consumer applies that action to every message in the batch.
//     Do NOT call msg.Ack/Nack/Reject() on any message in this mode.
//
//  2. Per-message acking: call [message.Message.Ack], [message.Message.Nack], or
//     [message.Message.Reject] on each visited message, then return [ActionNoAction].
//     The consumer will not touch acknowledgments.
//     Messages skipped via an early break from the range loop are automatically
//     handled according to [BatchConfig.ErrorAction].
type BatchHandler func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error)

// BatchConfig configures [BatchMiddleware].
type BatchConfig struct {
	// Size is the number of messages that triggers a flush (default: 10).
	//
	// Sync mode (Async=false): the consumer's PrefetchCount MUST be >= Size.
	// If it is smaller, the consumer will never deliver enough messages to fill
	// a batch while earlier goroutines are still blocking, causing a deadlock.
	Size int
	// FlushTimeout flushes a partial batch after this duration (default: 1s).
	// Prevents messages from being held indefinitely during quiet periods.
	FlushTimeout time.Duration
	// Async controls the execution model (default: false).
	//
	// false - sync/blocking:
	//   Consumer goroutines block until the batch is flushed. Provides
	//   natural back-pressure. Requires PrefetchCount >= Size.
	//
	// true - async/non-blocking:
	//   Consumer goroutines return immediately after enqueue; a background
	//   goroutine processes batches. No PrefetchCount constraint when
	//   EnqueueAction is ActionAck.
	Async bool
	// EnqueueAction is the action returned to the consumer goroutine on
	// successful enqueue in async mode (default: [ActionAck], at-most-once).
	//
	//   ActionAck (default): pre-ack on enqueue; at-most-once guarantee.
	//     A crash before Process completes loses the batch.
	//     No special PrefetchCount constraint. Suitable for logging/analytics.
	//
	//   ActionNoAction: hold-ack; at-least-once guarantee.
	//     Messages stay unacked in the broker until Process completes,
	//     then acked/nacked individually.
	//     Requires PrefetchCount >= BufferSize to avoid stalling the consumer.
	//     Suitable for durable pipelines where loss is unacceptable.
	//
	// Ignored when Async is false.
	EnqueueAction Action
	// ErrorAction is applied to all messages when Process returns an error,
	// or to unvisited messages when Process breaks the iterator early
	// (default: [ActionNackRequeue]).
	//
	//   [ActionNackRequeue]: Nack with requeue=true (retry).
	//   [ActionNackDiscard]: Reject, sending the message to dead-letter.
	//
	// Ignored when Process returns [ActionNoAction]; in that case Process
	// owns all acknowledgments.
	ErrorAction Action
	// BufferSize is the capacity of the internal queue channel (default: 10*Size).
	// When the buffer is full, incoming messages are handled according to ErrorAction.
	// Only relevant when Async is true.
	BufferSize int
	// OnError is called when Process returns an error or the buffer is full (optional).
	// count is the number of messages in the affected batch.
	OnError func(ctx context.Context, err error, count int)
}

// BatchMiddleware accumulates messages into batches and processes them with a [BatchHandler].
// It is a terminal middleware: the next handler is never called. Use ActionHandler(ActionNoAction) as the base handler.
//
// The ctx parameter controls the async background goroutine's lifetime (Async=true only).
// When cancelled, the goroutine drains its buffer and flushes a final batch before exiting.
// Pass the consumer's lifecycle context.
//
// Example (sync, blocking: transactional DB writes):
//
//	Wrap(ActionHandler(ActionNoAction),
//	    BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
//	            batch := &pgx.Batch{}
//	            for _, msg := range msgs {
//	                batch.Queue("INSERT INTO events ...", msg.Body)
//	            }
//	            if err := pool.SendBatch(ctx, batch).Close(); err != nil {
//	                return ActionNackRequeue, err
//	            }
//	            return ActionAck, nil
//	        },
//	        // PrefetchCount must be >= BatchMiddlewareConfig.Size
//	        &BatchMiddlewareConfig{Size: 50, FlushTimeout: 200 * time.Millisecond},
//	    ),
//	),
//
// Example (async, at-most-once - fire-and-forget - non-durable pipeline):
//
//	Wrap(ActionHandler(ActionNoAction),
//	    BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
//	            for _, msg := range msgs {
//	                analytics.Record(msg)
//	            }
//	            return ActionAck, nil
//	        },
//	        &BatchMiddlewareConfig{Async: true, Size: 100, FlushTimeout: 500 * time.Millisecond},
//	    ),
//	),
//
// Example (async, at-least-once - durable pipeline):
//
//	Wrap(ActionHandler(ActionNoAction),
//	    BatchMiddleware(ctx,
//	        func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
//	            batch := &pgx.Batch{}
//	            for i, msg := range msgs {
//	                var row Row
//	                if err := json.Unmarshal(msg.Body, &row); err != nil {
//	                    return ActionNackRequeue, fmt.Errorf("msg %d: %w", i, err)
//	                }
//	                batch.Queue("INSERT INTO events ...", row.Fields()...)
//	            }
//	            return ActionAck, pool.SendBatch(ctx, batch).Close()
//	        },
//	        &BatchMiddlewareConfig{
//	            Async:         true,
//	            EnqueueAction: ActionNoAction,
//	            ErrorAction:   ActionNackRequeue,
//	            Size:          50, // BufferSize defaults to 10*Size=500; PrefetchCount must be >= BufferSize
//	            FlushTimeout:  200 * time.Millisecond,
//	            OnError: func(ctx context.Context, err error, count int) {
//	                log.Printf("batch of %d failed: %v", count, err)
//	            },
//	        },
//	    ),
//	),
func BatchMiddleware(ctx context.Context, bh BatchHandler, cfg *BatchConfig) Middleware {
	if cfg == nil {
		cfg = &BatchConfig{}
	}
	if cfg.Size <= 0 {
		cfg.Size = 10
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = time.Second
	}
	if cfg.ErrorAction == 0 {
		cfg.ErrorAction = ActionNackRequeue
	}

	if cfg.Async {
		return batchAsync(ctx, bh, cfg)
	}

	return batchSync(ctx, bh, cfg)
}

// batchSync implements the blocking execution model for [BatchMiddleware].
// Consumer goroutines park until the batch fills or times out; all receive the
// outcome simultaneously. Requires PrefetchCount >= cfg.Size.
func batchSync(ctx context.Context, bh BatchHandler, cfg *BatchConfig) Middleware {
	type (
		outcome struct {
			action Action
			err    error
		}
		waiter struct {
			msg    *message.Message
			result chan outcome
		}
	)

	var (
		mu      sync.Mutex
		pending []waiter
		timer   *time.Timer
	)

	// flush drains the pending list and invokes bh,
	// bust be called with mu held; releases mu before calling bh
	flush := func() {
		batch := pending
		pending = nil
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		mu.Unlock()

		// visited tracks how many messages were actually yielded to bh
		// if bh breaks early (yield returns false), batch[visited:] are unvisited
		visited := 0
		seq := func(yield func(int, *message.Message) bool) {
			for i, w := range batch {
				if !yield(i, w.msg) {
					visited = i + 1
					return
				}
				visited = i + 1
			}
		}

		action, err := bh(ctx, seq)

		var o outcome
		switch {
		case action == ActionNoAction:
			// bh owns acking for visited messages;
			// apply ErrorAction to any messages it never visited (early break)
			for _, w := range batch[visited:] {
				applyAction(cfg.ErrorAction, w.msg)
			}
			o = outcome{ActionNoAction, err}
		case err != nil:
			o = outcome{cfg.ErrorAction, err}
		default:
			o = outcome{action, nil}
		}

		for _, w := range batch {
			w.result <- o
		}
	}

	return func(next Handler) Handler {
		return func(msgCtx context.Context, msg *message.Message) (Action, error) {
			ch := make(chan outcome, 1)

			mu.Lock()
			pending = append(pending, waiter{msg, ch})
			switch {
			case len(pending) >= cfg.Size:
				flush() // releases mu
			case len(pending) == 1:
				// first message in a new batch: arm the flush timeout
				timer = time.AfterFunc(cfg.FlushTimeout, func() {
					mu.Lock()
					if len(pending) > 0 {
						flush() // releases mu
					} else {
						mu.Unlock()
					}
				})
				mu.Unlock()
			default:
				mu.Unlock()
			}

			// block until the batch is flushed;
			// FlushTimeout ensures this always resolves
			select {
			case o := <-ch:
				return o.action, o.err
			case <-msgCtx.Done():
				return ActionNackRequeue, ErrMiddleware.Detailf("%w", msgCtx.Err())
			}
		}
	}
}

// batchAsync implements the non-blocking execution model for [BatchMiddleware].
// Consumer goroutines return immediately after enqueueing; a background goroutine
// accumulates and flushes batches. The background goroutine drains on ctx cancellation.
func batchAsync(ctx context.Context, bh BatchHandler, cfg *BatchConfig) Middleware {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = cfg.Size * 10
	}

	queue := make(chan *message.Message, cfg.BufferSize)

	flushBatch := func(batch []*message.Message) {
		if len(batch) == 0 {
			return
		}

		visited := 0
		seq := func(yield func(int, *message.Message) bool) {
			for i, msg := range batch {
				if !yield(i, msg) {
					visited = i + 1
					return
				}
				visited = i + 1
			}
		}

		action, err := bh(ctx, seq)

		if cfg.EnqueueAction == ActionNoAction {
			// hold-ack mode: middleware owns all acks
			earlyBreak := visited < len(batch)
			switch {
			case action == ActionNoAction:
				// bh owns visited acks; clean up unvisited
				for _, msg := range batch[visited:] {
					applyAction(cfg.ErrorAction, msg)
				}
			case earlyBreak:
				// bh returned a uniform action but broke early;
				// apply it to visited, ErrorAction to unvisited
				for _, msg := range batch[:visited] {
					applyAction(action, msg)
				}
				for _, msg := range batch[visited:] {
					applyAction(cfg.ErrorAction, msg)
				}
			case err != nil:
				for _, msg := range batch {
					applyAction(cfg.ErrorAction, msg)
				}
			default:
				for _, msg := range batch {
					applyAction(action, msg)
				}
			}
		}

		if err != nil && cfg.OnError != nil {
			cfg.OnError(ctx, err, len(batch))
		}
	}

	// drain queue into batches and flush them
	go func() {
		batch := make([]*message.Message, 0, cfg.Size)
		ticker := time.NewTicker(cfg.FlushTimeout)
		defer ticker.Stop()

		for {
			select {
			case msg, ok := <-queue:
				_ = ok // dead check: channel is never closed
				batch = append(batch, msg)
				if len(batch) >= cfg.Size {
					flushBatch(batch)
					batch = batch[:0]
					// reset the timeout window after a size-triggered flush
					ticker.Reset(cfg.FlushTimeout)
				}
			case <-ticker.C:
				if len(batch) > 0 {
					flushBatch(batch)
					batch = batch[:0]
				}
			case <-ctx.Done():
				// drain any remaining buffered messages into a final batch, then exit
				for {
					select {
					case msg := <-queue:
						batch = append(batch, msg)
					default:
						flushBatch(batch)
						return
					}
				}
			}
		}
	}()

	return func(next Handler) Handler {
		return func(hCtx context.Context, msg *message.Message) (Action, error) {
			select {
			case queue <- msg:
				return cfg.EnqueueAction, nil
			default:
				// buffer full: cannot enqueue
				bufErr := ErrMiddleware.Detail("batching buffer full")
				if cfg.EnqueueAction == ActionNoAction {
					// hold-ack mode: we own this message's ack
					applyAction(cfg.ErrorAction, msg)
					if cfg.OnError != nil {
						cfg.OnError(hCtx, bufErr, 1)
					}
					return ActionNoAction, bufErr
				}
				if cfg.OnError != nil {
					cfg.OnError(hCtx, bufErr, 1)
				}
				return cfg.ErrorAction, bufErr
			}
		}
	}
}

// applyAction applies a uniform ack action directly to a message.
func applyAction(a Action, msg *message.Message) {
	switch a {
	case ActionAck:
		_ = msg.Ack()
	case ActionNackRequeue:
		_ = msg.Nack(true)
	case ActionNackDiscard:
		_ = msg.Reject()
	}
}
