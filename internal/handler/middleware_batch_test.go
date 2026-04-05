package handler

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAcknowledger is a thread-safe Acknowledger for testing.
type mockAcknowledger struct {
	mu       sync.Mutex
	acked    int
	nacked   int
	rejected int
	requeue  bool
}

var _ message.Acknowledger = (*mockAcknowledger)(nil)

func (m *mockAcknowledger) Ack(multiple bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked++
	return nil
}

func (m *mockAcknowledger) Nack(multiple, requeue bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked++
	m.requeue = requeue
	return nil
}

func (m *mockAcknowledger) Reject(requeue bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rejected++
	return nil
}

func (m *mockAcknowledger) counts() (acked, nacked, rejected int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acked, m.nacked, m.rejected
}

// testConsumedMessage creates a message.Message backed by a mockAck so msg.Ack/Nack/Reject work.
func testConsumedMessage(body []byte) (message.Message, *mockAcknowledger) {
	ack := &mockAcknowledger{}
	msg := message.NewConsumedMessage(message.New(body), ack, message.DeliveryInfo{})
	return msg, ack
}

// noopNext is a placeholder next handler; BatchMiddleware never calls next.
var noopNext = testHandler(ActionAck)

func TestBatchMiddleware(t *testing.T) {
	t.Run("Sync", func(t *testing.T) {
		t.Run("FullBatchFlushesImmediately", func(t *testing.T) {
			const size = 3
			var called atomic.Bool
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					called.Store(true)
					assert.Len(t, slices.Collect(maps.Values(maps.Collect(msgs))), size)
					return ActionAck, nil
				},
				&BatchConfig{Size: size, FlushTimeout: 10 * time.Second},
			)
			wrapped := mw(noopNext)

			var wg sync.WaitGroup
			results := make([]Action, size)
			errs := make([]error, size)
			for i := range size {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					msg := message.New([]byte("m"))
					results[i], errs[i] = wrapped(t.Context(), &msg)
				}(i)
			}
			wg.Wait()

			assert.True(t, called.Load())
			for i := range size {
				require.NoError(t, errs[i])
				assert.Equal(t, ActionAck, results[i])
			}
		})

		t.Run("PartialBatchFlushesOnTimeout", func(t *testing.T) {
			var called atomic.Bool
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					called.Store(true)
					return ActionAck, nil
				},
				&BatchConfig{Size: 10, FlushTimeout: 30 * time.Millisecond},
			)
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
			assert.True(t, called.Load())
		})

		t.Run("MultipleBatchesSequential", func(t *testing.T) {
			const size = 2
			var calls atomic.Int32
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					calls.Add(1)
					for range msgs {
					}
					return ActionAck, nil
				},
				&BatchConfig{Size: size, FlushTimeout: 30 * time.Millisecond},
			)
			wrapped := mw(noopNext)

			// first batch: fills immediately
			var wg sync.WaitGroup
			for range size {
				wg.Add(1)
				go func() {
					defer wg.Done()
					msg := message.New([]byte("m"))
					wrapped(t.Context(), &msg)
				}()
			}
			wg.Wait()
			assert.EqualValues(t, 1, calls.Load())

			// second batch: 1 message, flushes on timeout
			msg := message.New([]byte("m"))
			wrapped(t.Context(), &msg)
			assert.EqualValues(t, 2, calls.Load())
		})

		t.Run("ErrorActionDefaultOnFailure", func(t *testing.T) {
			handlerErr := errors.New("batch failed")
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					return ActionNackRequeue, handlerErr
				},
				&BatchConfig{Size: 1, FlushTimeout: time.Second},
			)
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			assert.Equal(t, ActionNackRequeue, action)
			assert.ErrorIs(t, err, handlerErr)
		})

		t.Run("CustomErrorAction", func(t *testing.T) {
			handlerErr := errors.New("batch failed")
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					return ActionNackDiscard, handlerErr
				},
				&BatchConfig{Size: 1, FlushTimeout: time.Second, ErrorAction: ActionNackDiscard},
			)
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			assert.Equal(t, ActionNackDiscard, action)
			assert.ErrorIs(t, err, handlerErr)
		})

		t.Run("NoActionForPerMessageAck", func(t *testing.T) {
			const size = 3
			acks := make([]*mockAcknowledger, size)
			msgs := make([]message.Message, size)
			for i := range size {
				msgs[i], acks[i] = testConsumedMessage([]byte("m"))
			}

			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, seq iter.Seq2[int, *message.Message]) (Action, error) {
					for _, msg := range seq {
						_ = msg.Ack()
					}
					return ActionNoAction, nil
				},
				&BatchConfig{Size: size, FlushTimeout: time.Second},
			)
			wrapped := mw(noopNext)

			var wg sync.WaitGroup
			results := make([]Action, size)
			for i := range size {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					results[i], _ = wrapped(t.Context(), &msgs[i])
				}(i)
			}
			wg.Wait()

			for i := range size {
				assert.Equal(t, ActionNoAction, results[i])
				a, n, r := acks[i].counts()
				assert.Equal(t, 1, a, "msg %d: expected Ack", i)
				assert.Equal(t, 0, n, "msg %d: unexpected Nack", i)
				assert.Equal(t, 0, r, "msg %d: unexpected Reject", i)
			}
		})

		t.Run("NoActionEarlyBreakAppliesErrorActionToUnvisited", func(t *testing.T) {
			const size = 5
			acks := make([]*mockAcknowledger, size)
			msgs := make([]message.Message, size)
			for i := range size {
				msgs[i], acks[i] = testConsumedMessage([]byte("m"))
			}

			// process only first 2 (ack them manually), then break;
			// unvisited messages should get ErrorAction (default: ActionNackRequeue)
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, seq iter.Seq2[int, *message.Message]) (Action, error) {
					for i, msg := range seq {
						_ = msg.Ack()
						if i == 1 {
							break // visited: 0,1; unvisited: 2,3,4
						}
					}
					return ActionNoAction, nil
				},
				&BatchConfig{Size: size, FlushTimeout: time.Second},
			)
			wrapped := mw(noopNext)

			var wg sync.WaitGroup
			for i := range size {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					wrapped(t.Context(), &msgs[i])
				}(i)
			}
			wg.Wait()

			totalAcked, totalNacked := 0, 0
			for i := range size {
				a, n, _ := acks[i].counts()
				totalAcked += a
				totalNacked += n
			}
			assert.Equal(t, 2, totalAcked)
			assert.Equal(t, 3, totalNacked)
		})

		t.Run("NoActionEarlyBreakCustomErrorActionReject", func(t *testing.T) {
			const size = 3
			acks := make([]*mockAcknowledger, size)
			msgs := make([]message.Message, size)
			for i := range size {
				msgs[i], acks[i] = testConsumedMessage([]byte("m"))
			}

			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, seq iter.Seq2[int, *message.Message]) (Action, error) {
					for i, msg := range seq {
						_ = msg.Ack()
						if i == 0 {
							break // visited: 0; unvisited: 1,2
						}
					}
					return ActionNoAction, nil
				},
				&BatchConfig{Size: size, FlushTimeout: time.Second, ErrorAction: ActionNackDiscard},
			)
			wrapped := mw(noopNext)

			var wg sync.WaitGroup
			for i := range size {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					wrapped(t.Context(), &msgs[i])
				}(i)
			}
			wg.Wait()

			totalAcked, totalRejected := 0, 0
			for i := range size {
				a, _, r := acks[i].counts()
				totalAcked += a
				totalRejected += r
			}
			assert.Equal(t, 1, totalAcked)
			assert.Equal(t, 2, totalRejected)
		})

		t.Run("ContextCancelledWhileBlocking", func(t *testing.T) {
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					return ActionAck, nil
				},
				&BatchConfig{Size: 5, FlushTimeout: 10 * time.Second},
			)
			wrapped := mw(noopNext)

			ctx, cancel := context.WithCancel(t.Context())
			go func() {
				time.Sleep(20 * time.Millisecond)
				cancel()
			}()

			msg := message.New([]byte("m"))
			action, err := wrapped(ctx, &msg)

			assert.Equal(t, ActionNackRequeue, action)
			assert.ErrorIs(t, err, ErrMiddleware)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("WithNilConfig", func(t *testing.T) {
			var called atomic.Bool
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					called.Store(true)
					return ActionAck, nil
				},
				nil,
			)
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
			assert.True(t, called.Load())
		})

		t.Run("WithBadConfig", func(t *testing.T) {
			var called atomic.Bool
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					called.Store(true)
					return ActionAck, nil
				},
				&BatchConfig{Size: -1, FlushTimeout: -1, ErrorAction: 0},
			)
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
			assert.True(t, called.Load())
		})

		t.Run("TimerFiresAfterSizeFlush", func(t *testing.T) {
			// exercises the defensive else { mu.Unlock() } in the timer callback:
			// timer fires after a concurrent size flush has already cleared pending
			const size = 2
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					return ActionAck, nil
				},
				&BatchConfig{Size: size, FlushTimeout: time.Nanosecond},
			)
			wrapped := mw(noopNext)

			for range 500 {
				var wg sync.WaitGroup
				for range size {
					wg.Go(func() {
						msg := message.New([]byte("m"))
						wrapped(t.Context(), &msg)
					})
				}
				wg.Wait()
			}
		})
	})

	t.Run("Async", func(t *testing.T) {
		t.Run("AtMostOnceAcksImmediately", func(t *testing.T) {
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					return ActionAck, nil
				},
				&BatchConfig{Async: true, Size: 10, FlushTimeout: 10 * time.Second},
			)
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			require.NoError(t, err)
			assert.Equal(t, ActionAck, action) // pre-acked on enqueue
		})

		t.Run("AtMostOnceFlushesOnSize", func(t *testing.T) {
			const size = 3
			processed := make(chan int, 1)
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					n := 0
					for range msgs {
						n++
					}
					processed <- n
					return ActionAck, nil
				},
				&BatchConfig{Async: true, Size: size, FlushTimeout: 10 * time.Second},
			)
			wrapped := mw(noopNext)

			for range size {
				msg := message.New([]byte("m"))
				wrapped(t.Context(), &msg)
			}

			select {
			case n := <-processed:
				assert.Equal(t, size, n)
			case <-time.After(time.Second):
				t.Fatal("timeout: Process not called after size flush")
			}
		})

		t.Run("AtMostOnceFlushesOnTimeout", func(t *testing.T) {
			processed := make(chan struct{}, 1)
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					processed <- struct{}{}
					return ActionAck, nil
				},
				&BatchConfig{Async: true, Size: 10, FlushTimeout: 30 * time.Millisecond},
			)
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			wrapped(t.Context(), &msg)

			select {
			case <-processed:
			case <-time.After(time.Second):
				t.Fatal("timeout: Process not called after FlushTimeout")
			}
		})

		t.Run("HoldAckReturnsNoAction", func(t *testing.T) {
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					return ActionAck, nil
				},
				&BatchConfig{
					Async:         true,
					EnqueueAction: ActionNoAction,
					Size:          10,
					FlushTimeout:  10 * time.Second,
				},
			)
			wrapped := mw(noopNext)

			msg, ack := testConsumedMessage([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			require.NoError(t, err)
			assert.Equal(t, ActionNoAction, action)
			// ack not called yet (async)
			a, n, r := ack.counts()
			assert.Equal(t, 0, a+n+r)
		})

		t.Run("HoldAckAcksOnSuccess", func(t *testing.T) {
			done := make(chan struct{})
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					close(done)
					return ActionAck, nil
				},
				&BatchConfig{
					Async:         true,
					EnqueueAction: ActionNoAction,
					Size:          1,
					FlushTimeout:  time.Second,
				},
			)
			wrapped := mw(noopNext)

			msg, ack := testConsumedMessage([]byte("m"))
			wrapped(t.Context(), &msg)

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for Process")
			}
			time.Sleep(10 * time.Millisecond)

			a, n, r := ack.counts()
			assert.Equal(t, 1, a, "expected Ack")
			assert.Equal(t, 0, n+r)
		})

		t.Run("HoldAckNacksOnError", func(t *testing.T) {
			processErr := errors.New("process failed")
			done := make(chan struct{})
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					close(done)
					return ActionNackRequeue, processErr
				},
				&BatchConfig{
					Async:         true,
					EnqueueAction: ActionNoAction,
					ErrorAction:   ActionNackRequeue,
					Size:          1,
					FlushTimeout:  time.Second,
				},
			)
			wrapped := mw(noopNext)

			msg, ack := testConsumedMessage([]byte("m"))
			wrapped(t.Context(), &msg)

			<-done
			time.Sleep(10 * time.Millisecond)

			a, n, _ := ack.counts()
			assert.Equal(t, 0, a)
			assert.Equal(t, 1, n, "expected Nack")
			ack.mu.Lock()
			assert.True(t, ack.requeue, "expected requeue=true")
			ack.mu.Unlock()
		})

		t.Run("HoldAckRejectsOnError", func(t *testing.T) {
			processErr := errors.New("process failed")
			done := make(chan struct{})
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					close(done)
					return ActionNackDiscard, processErr
				},
				&BatchConfig{
					Async:         true,
					EnqueueAction: ActionNoAction,
					ErrorAction:   ActionNackDiscard,
					Size:          1,
					FlushTimeout:  time.Second,
				},
			)
			wrapped := mw(noopNext)

			msg, ack := testConsumedMessage([]byte("m"))
			wrapped(t.Context(), &msg)

			<-done
			time.Sleep(10 * time.Millisecond)

			a, n, r := ack.counts()
			assert.Equal(t, 0, a+n)
			assert.Equal(t, 1, r, "expected Reject")
		})

		t.Run("HoldAckEarlyBreakAppliesErrorActionToUnvisited", func(t *testing.T) {
			const size = 4
			done := make(chan struct{})
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for i, msg := range msgs {
						_ = msg.Ack()
						if i == 1 {
							break // ack 0,1; unvisited 2,3 -> ErrorAction cleanup
						}
					}
					close(done)
					return ActionNoAction, nil
				},
				&BatchConfig{
					Async:         true,
					EnqueueAction: ActionNoAction,
					ErrorAction:   ActionNackRequeue,
					Size:          size,
					FlushTimeout:  time.Second,
				},
			)
			wrapped := mw(noopNext)

			acks := make([]*mockAcknowledger, size)
			for i := range size {
				var msg message.Message
				msg, acks[i] = testConsumedMessage([]byte("m"))
				wrapped(t.Context(), &msg)
			}

			<-done
			time.Sleep(10 * time.Millisecond)

			totalAcked, totalNacked := 0, 0
			for i := range size {
				a, n, _ := acks[i].counts()
				totalAcked += a
				totalNacked += n
			}
			assert.Equal(t, 2, totalAcked)
			assert.Equal(t, 2, totalNacked)
		})

		t.Run("HoldAckUniformActionEarlyBreak", func(t *testing.T) {
			// bh returns a uniform action (not ActionNoAction) but breaks early.
			// visited messages -> ActionAck; unvisited -> ErrorAction (ActionNackDiscard).
			const size = 4
			done := make(chan struct{})
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for i := range msgs {
						if i == 1 {
							break // visited: 0,1; unvisited: 2,3
						}
					}
					close(done)
					return ActionAck, nil
				},
				&BatchConfig{
					Async:         true,
					EnqueueAction: ActionNoAction,
					ErrorAction:   ActionNackDiscard,
					Size:          size,
					FlushTimeout:  time.Second,
				},
			)
			wrapped := mw(noopNext)

			acks := make([]*mockAcknowledger, size)
			for i := range size {
				var msg message.Message
				msg, acks[i] = testConsumedMessage([]byte("m"))
				wrapped(t.Context(), &msg)
			}

			<-done
			time.Sleep(10 * time.Millisecond)

			totalAcked, totalRejected := 0, 0
			for i := range size {
				a, _, r := acks[i].counts()
				totalAcked += a
				totalRejected += r
			}
			assert.Equal(t, 2, totalAcked)
			assert.Equal(t, 2, totalRejected)
		})

		t.Run("AtMostOnceBufferFullCallsOnError", func(t *testing.T) {
			var onErrCalled atomic.Bool
			// bhEntered signals that the background goroutine consumed msg1 and is now
			// blocked inside the batch handler at this point the batch queue is empty,
			// bhRelease unblocks it after the overflow assertions are done
			bhEntered := make(chan struct{}, 1)
			bhRelease := make(chan struct{})
			defer close(bhRelease)

			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					select {
					case bhEntered <- struct{}{}:
					default:
					}
					<-bhRelease
					for range msgs {
						// drain ...
					}
					return ActionAck, nil
				},
				&BatchConfig{
					Async:        true,
					Size:         1, // flush after 1 message so the goroutine enters bh immediately
					FlushTimeout: 10 * time.Second,
					BufferSize:   1,
					OnError: func(ctx context.Context, err error, count int) {
						onErrCalled.Store(true)
						assert.ErrorIs(t, err, ErrMiddleware)
						assert.Equal(t, 1, count)
					},
				},
			)
			wrapped := mw(noopNext)

			// msg1 fills the queue; the goroutine reads it and blocks inside the batch handler
			msg1 := message.New([]byte("first"))
			wrapped(t.Context(), &msg1)
			<-bhEntered // goroutine is now blocked in bh; channel is empty

			// msg2 refills the queue, so the next send hits the overflow path
			msg2 := message.New([]byte("fill"))
			wrapped(t.Context(), &msg2)

			// msg3 overflows the queue; expect OnError callback with ErrMiddleware
			msg3 := message.New([]byte("overflow"))
			action, err := wrapped(t.Context(), &msg3)

			assert.Equal(t, ActionNackRequeue, action) // ErrorAction default
			assert.ErrorIs(t, err, ErrMiddleware)
			assert.True(t, onErrCalled.Load())
		})

		t.Run("HoldAckBufferFullRejectsAndCallsOnError", func(t *testing.T) {
			var onErrCalled atomic.Bool
			// bhEntered signals that the background goroutine consumed msg1 and is now
			// blocked inside the batch handler at this point the batch queue is empty,
			// bhRelease unblocks it after the overflow assertions are done
			bhEntered := make(chan struct{}, 1)
			bhRelease := make(chan struct{})
			defer close(bhRelease)

			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					select {
					case bhEntered <- struct{}{}:
					default:
					}
					<-bhRelease
					for range msgs {
						// drain ...
					}
					return ActionAck, nil
				},
				&BatchConfig{
					Async:         true,
					EnqueueAction: ActionNoAction,
					ErrorAction:   ActionNackDiscard,
					Size:          1, // flush after 1 message so the goroutine enters bh immediately
					FlushTimeout:  10 * time.Second,
					BufferSize:    1,
					OnError: func(ctx context.Context, err error, count int) {
						onErrCalled.Store(true)
					},
				},
			)
			wrapped := mw(noopNext)

			// msg1 fills the queue; the goroutine reads it and blocks inside the batch handler
			msg1, _ := testConsumedMessage([]byte("initial"))
			wrapped(t.Context(), &msg1)
			<-bhEntered // goroutine is now blocked in bh; queue is empty

			// msg2 refills the queue, so the next send hits the overflow path
			msg2, _ := testConsumedMessage([]byte("fill"))
			wrapped(t.Context(), &msg2)

			// msg3 overflows the queue; expect Reject and OnError callback
			msg3, ack3 := testConsumedMessage([]byte("overflow"))
			action, err := wrapped(t.Context(), &msg3)

			assert.Equal(t, ActionNoAction, action)
			assert.ErrorIs(t, err, ErrMiddleware)
			assert.True(t, onErrCalled.Load())
			_, _, r := ack3.counts()
			assert.Equal(t, 1, r, "expected Reject on overflow")
		})

		t.Run("HoldAckBufferFullNacksWithRequeue", func(t *testing.T) {
			// bhEntered signals that the background goroutine consumed msg1 and is now
			// blocked inside the batch handler at this point the batch queue is empty,
			// bhRelease unblocks it after the overflow assertions are done
			bhEntered := make(chan struct{}, 1)
			bhRelease := make(chan struct{})
			defer close(bhRelease)

			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					select {
					case bhEntered <- struct{}{}:
					default:
					}
					<-bhRelease
					for range msgs {
						// drain ...
					}
					return ActionAck, nil
				},
				&BatchConfig{
					Async:         true,
					EnqueueAction: ActionNoAction,
					ErrorAction:   ActionNackRequeue,
					Size:          1, // flush after 1 message so the goroutine enters bh immediately
					FlushTimeout:  10 * time.Second,
					BufferSize:    1,
				},
			)
			wrapped := mw(noopNext)

			// msg1 fills the queue; the goroutine reads it and blocks inside the batch handler
			msg1, _ := testConsumedMessage([]byte("initial"))
			wrapped(t.Context(), &msg1)
			<-bhEntered // goroutine is now blocked in bh; queue is empty

			// msg2 refills the queue, so the next send hits the overflow path
			msg2, _ := testConsumedMessage([]byte("fill"))
			wrapped(t.Context(), &msg2)

			// msg3 overflows the queue; expect Reject and OnError callback
			msg3, ack3 := testConsumedMessage([]byte("overflow"))
			action, _ := wrapped(t.Context(), &msg3)

			assert.Equal(t, ActionNoAction, action)
			_, n, _ := ack3.counts()
			assert.Equal(t, 1, n, "expected Nack on overflow")
			ack3.mu.Lock()
			assert.True(t, ack3.requeue)
			ack3.mu.Unlock()
		})

		t.Run("OnErrorCalledWithCorrectArgs", func(t *testing.T) {
			processErr := errors.New("oops")
			var capturedCount int
			var capturedErr error
			done := make(chan struct{})
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					return ActionNackRequeue, processErr
				},
				&BatchConfig{
					Async:        true,
					Size:         2,
					FlushTimeout: time.Second,
					OnError: func(ctx context.Context, err error, count int) {
						capturedErr = err
						capturedCount = count
						close(done)
					},
				},
			)
			wrapped := mw(noopNext)

			for range 2 {
				msg := message.New([]byte("m"))
				wrapped(t.Context(), &msg)
			}

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for OnError")
			}

			assert.ErrorIs(t, capturedErr, processErr)
			assert.Equal(t, 2, capturedCount)
		})

		t.Run("DrainOnContextCancel", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())

			processed := make(chan int, 1)
			mw := BatchMiddleware(ctx,
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					n := 0
					for range msgs {
						n++
					}
					processed <- n
					return ActionAck, nil
				},
				&BatchConfig{
					Async:        true,
					Size:         100,
					FlushTimeout: 10 * time.Second,
				},
			)
			wrapped := mw(noopNext)

			const count = 3
			for range count {
				msg := message.New([]byte("m"))
				wrapped(t.Context(), &msg)
			}

			cancel()

			select {
			case n := <-processed:
				assert.Equal(t, count, n)
			case <-time.After(time.Second):
				t.Fatal("timeout: drain did not flush on context cancel")
			}
		})

		t.Run("WithNilConfig", func(t *testing.T) {
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					return ActionAck, nil
				},
				&BatchConfig{Async: true},
			)
			// BatchConfig with only Async=true; all other fields default
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			require.NoError(t, err)
			assert.Equal(t, ActionAck, action) // EnqueueAction defaults to ActionAck
		})

		t.Run("WithBadConfig", func(t *testing.T) {
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					for range msgs {
					}
					return ActionAck, nil
				},
				&BatchConfig{Async: true, Size: -1, FlushTimeout: -1, BufferSize: -1},
			)
			wrapped := mw(noopNext)

			msg := message.New([]byte("m"))
			action, err := wrapped(t.Context(), &msg)

			require.NoError(t, err)
			assert.Equal(t, ActionAck, action)
		})

		t.Run("TickerResetAfterSizeFlush", func(t *testing.T) {
			// verifies that after a size-triggered flush, the ticker is reset
			// so the next partial batch doesn't flush prematurely.
			flushed := make(chan int, 10)
			mw := BatchMiddleware(t.Context(),
				func(ctx context.Context, msgs iter.Seq2[int, *message.Message]) (Action, error) {
					n := 0
					for range msgs {
						n++
					}
					flushed <- n
					return ActionAck, nil
				},
				&BatchConfig{
					Async:        true,
					Size:         2,
					FlushTimeout: 50 * time.Millisecond,
				},
			)
			wrapped := mw(noopNext)

			// send 2 messages -> size flush
			for range 2 {
				msg := message.New([]byte("m"))
				wrapped(t.Context(), &msg)
			}
			select {
			case n := <-flushed:
				assert.Equal(t, 2, n)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for size flush")
			}

			// send 1 more -> should flush on timeout
			msg := message.New([]byte("m"))
			wrapped(t.Context(), &msg)
			select {
			case n := <-flushed:
				assert.Equal(t, 1, n)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for timeout flush after ticker reset")
			}
		})
	})
}

func TestApplyAction(t *testing.T) {
	t.Run("Ack", func(t *testing.T) {
		msg, ack := testConsumedMessage([]byte("m"))
		applyAction(ActionAck, &msg)
		a, n, r := ack.counts()
		assert.Equal(t, 1, a)
		assert.Equal(t, 0, n+r)
	})

	t.Run("NackRequeue", func(t *testing.T) {
		msg, ack := testConsumedMessage([]byte("m"))
		applyAction(ActionNackRequeue, &msg)
		a, n, r := ack.counts()
		assert.Equal(t, 0, a+r)
		assert.Equal(t, 1, n)
		ack.mu.Lock()
		assert.True(t, ack.requeue)
		ack.mu.Unlock()
	})

	t.Run("NackDiscard", func(t *testing.T) {
		msg, ack := testConsumedMessage([]byte("m"))
		applyAction(ActionNackDiscard, &msg)
		a, n, r := ack.counts()
		assert.Equal(t, 0, a+n)
		assert.Equal(t, 1, r)
	})

	t.Run("NoAction", func(t *testing.T) {
		msg, ack := testConsumedMessage([]byte("m"))
		applyAction(ActionNoAction, &msg)
		a, n, r := ack.counts()
		assert.Equal(t, 0, a+n+r)
	})
}
