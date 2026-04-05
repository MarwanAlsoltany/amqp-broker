package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDeduplicationCache is a simple in-memory DeduplicationCache for testing.
type testDeduplicationCache struct {
	seen map[string]bool
}

func newTestDeduplicationCache() *testDeduplicationCache {
	return &testDeduplicationCache{seen: make(map[string]bool)}
}

func (c *testDeduplicationCache) Seen(id string) bool {
	if c.seen[id] {
		return false
	}
	c.seen[id] = true
	return true
}

func TestDeduplicationMiddleware(t *testing.T) {
	t.Run("NewMessage", func(t *testing.T) {
		cache := newTestDeduplicationCache()
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache: cache,
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "unique-id-1"
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("DuplicateMessageDefaultAction", func(t *testing.T) {
		cache := newTestDeduplicationCache()
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache: cache,
		})

		handlerCalled := false
		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			handlerCalled = true
			return ActionAck, nil
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		msg.MessageID = "dup-id"

		wrapped(t.Context(), &msg)

		handlerCalled = false
		action, err := wrapped(t.Context(), &msg) // duplicate

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.False(t, handlerCalled, "handler must not be called for duplicate")
	})

	t.Run("DuplicateMessageCustomAction", func(t *testing.T) {
		cache := newTestDeduplicationCache()
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache:  cache,
			Action: ActionNackDiscard,
		})

		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "dup-id-2"

		// first call
		action1, err1 := wrapped(t.Context(), &msg)
		require.NoError(t, err1)
		assert.Equal(t, ActionAck, action1)

		// duplicate call
		action2, err2 := wrapped(t.Context(), &msg)
		require.NoError(t, err2)
		assert.Equal(t, ActionNackDiscard, action2)
	})

	t.Run("DuplicateMessageRequeueAction", func(t *testing.T) {
		cache := newTestDeduplicationCache()
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache:  cache,
			Action: ActionNackRequeue,
		})

		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "dup-id-3"

		wrapped(t.Context(), &msg)

		action, err := wrapped(t.Context(), &msg) // duplicate
		require.NoError(t, err)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := DeduplicationMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithNilCache", func(t *testing.T) {
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache: nil, // no cache
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "any-id"

		// first call
		action1, err1 := wrapped(t.Context(), &msg)
		require.NoError(t, err1)
		assert.Equal(t, ActionAck, action1)

		// "duplicate" call - but no cache, so should still call handler
		action2, err2 := wrapped(t.Context(), &msg)
		require.NoError(t, err2)
		assert.Equal(t, ActionAck, action2)
	})

	t.Run("WithCustomIdentifyFromHeader", func(t *testing.T) {
		cache := newTestDeduplicationCache()
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache: cache,
			Identify: func(msg *message.Message) string {
				if id, ok := msg.Headers["x-dedup-id"].(string); ok {
					return id
				}
				return msg.MessageID
			},
		})

		wrapped := mw(testHandler(ActionAck))

		msg1 := message.New([]byte("test"))
		msg1.MessageID = "id-1"
		msg1.Headers = map[string]any{"x-dedup-id": "custom-123"}

		msg2 := message.New([]byte("test"))
		msg2.MessageID = "id-2"                                   // different message ID
		msg2.Headers = map[string]any{"x-dedup-id": "custom-123"} // same custom ID

		// first call
		action1, err1 := wrapped(t.Context(), &msg1)
		require.NoError(t, err1)
		assert.Equal(t, ActionAck, action1)

		// second call with different MessageID but same custom ID - should be duplicate
		action2, err2 := wrapped(t.Context(), &msg2)
		require.NoError(t, err2)
		assert.Equal(t, ActionAck, action2) // duplicate action
	})

	t.Run("WithCustomIdentifyFromBody", func(t *testing.T) {
		cache := newTestDeduplicationCache()
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache: cache,
			Identify: func(msg *message.Message) string {
				// use body as ID
				return string(msg.Body)
			},
		})

		callCount := 0
		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			callCount++
			return ActionAck, nil
		}
		wrapped := mw(handler)

		msg1 := message.New([]byte("same-body"))
		msg1.MessageID = "id-1"

		msg2 := message.New([]byte("same-body"))
		msg2.MessageID = "id-2" // different message ID

		// first call
		wrapped(t.Context(), &msg1)

		// second call with same body - should be duplicate
		action, err := wrapped(t.Context(), &msg2)
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.Equal(t, 1, callCount, "handler should only be called once")
	})

	t.Run("WithZeroActionUsesDefault", func(t *testing.T) {
		cache := newTestDeduplicationCache()
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache:  cache,
			Action: 0, // zero value should use default Ack
		})

		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.MessageID = "dup-id-4"

		wrapped(t.Context(), &msg)

		action, err := wrapped(t.Context(), &msg) // duplicate
		require.NoError(t, err)
		assert.Equal(t, ActionAck, action) // default
	})

	t.Run("HandlerNotCalledForDuplicate", func(t *testing.T) {
		cache := newTestDeduplicationCache()
		mw := DeduplicationMiddleware(&DeduplicationMiddlewareConfig{
			Cache: cache,
		})

		callCount := 0
		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			callCount++
			return ActionAck, nil
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		msg.MessageID = "test-id"

		// call 3 times
		wrapped(t.Context(), &msg)
		wrapped(t.Context(), &msg)
		wrapped(t.Context(), &msg)

		assert.Equal(t, 1, callCount, "handler should only be called once for duplicates")
	})
}

func TestValidationMiddleware(t *testing.T) {
	t.Run("ValidMessage", func(t *testing.T) {
		mw := ValidationMiddleware(&ValidationMiddlewareConfig{
			Validate: func(msg *message.Message) error { return nil },
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("InvalidMessageDefaultAction", func(t *testing.T) {
		validationErr := errors.New("body is empty")
		mw := ValidationMiddleware(&ValidationMiddlewareConfig{
			Validate: func(msg *message.Message) error { return validationErr },
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New(nil)
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.ErrorIs(t, err, validationErr)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("InvalidMessageCustomAction", func(t *testing.T) {
		validationErr := errors.New("invalid content")
		mw := ValidationMiddleware(&ValidationMiddlewareConfig{
			Validate: func(msg *message.Message) error { return validationErr },
			Action:   ActionNackRequeue,
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.ErrorIs(t, err, validationErr)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := ValidationMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithNilValidate", func(t *testing.T) {
		mw := ValidationMiddleware(&ValidationMiddlewareConfig{
			Validate: nil, // no validation function
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithZeroActionUsesDefault", func(t *testing.T) {
		validationErr := errors.New("invalid")
		mw := ValidationMiddleware(&ValidationMiddlewareConfig{
			Validate: func(msg *message.Message) error { return validationErr },
			Action:   0, // zero value should use default
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action) // default
	})

	t.Run("WithCustomActionNoAction", func(t *testing.T) {
		validationErr := errors.New("bad format")
		mw := ValidationMiddleware(&ValidationMiddlewareConfig{
			Validate: func(msg *message.Message) error { return validationErr },
			Action:   ActionNoAction,
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNoAction, action)
	})
}

func TestTransformMiddleware(t *testing.T) {
	t.Run("TransformsBodySuccessfully", func(t *testing.T) {
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				return append([]byte("prefix:"), b...), nil
			},
		}
		mw := TransformMiddleware(cfg)

		var received []byte
		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			received = msg.Body
			return ActionAck, nil
		}
		wrapped := mw(handler)

		msg := message.New([]byte("body"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.Equal(t, []byte("prefix:body"), received)
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := TransformMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("body"))
		action, err := wrapped(t.Context(), &msg)

		assert.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.Equal(t, []byte("body"), msg.Body) // unchanged
	})

	t.Run("WithNilTransformFunction", func(t *testing.T) {
		cfg := &TransformMiddlewareConfig{Transform: nil}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("body"))
		action, err := wrapped(t.Context(), &msg)

		assert.NoError(t, err)
		assert.Equal(t, ActionAck, action)
		assert.Equal(t, []byte("body"), msg.Body) // unchanged (no-op)
	})

	t.Run("TransformErrorUsesDefaultAction", func(t *testing.T) {
		transformErr := errors.New("decompression failed")
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				return nil, transformErr
			},
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("body"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.ErrorIs(t, err, transformErr)
		assert.Equal(t, ActionNackDiscard, action) // default
	})

	t.Run("TransformErrorUsesCustomAction", func(t *testing.T) {
		transformErr := errors.New("decryption failed")
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				return nil, transformErr
			},
			Action: ActionNackRequeue,
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("body"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, transformErr)
		assert.Equal(t, ActionNackRequeue, action) // custom action
	})

	t.Run("WithZeroActionUsesDefault", func(t *testing.T) {
		transformErr := errors.New("encoding failed")
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				return nil, transformErr
			},
			Action: 0, // zero value
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("body"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action) // default when Action is zero
	})

	t.Run("PassesContextToTransform", func(t *testing.T) {
		type ctxKey string
		key := ctxKey("ctx-key")
		expectedValue := "ctx-value"

		var receivedCtx context.Context
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				receivedCtx = ctx
				return b, nil
			},
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		ctx := context.WithValue(context.Background(), key, expectedValue)
		msg := message.New([]byte("body"))
		_, _ = wrapped(ctx, &msg)

		assert.NotNil(t, receivedCtx)
		assert.Equal(t, expectedValue, receivedCtx.Value(key))
	})

	t.Run("ContextCancellationInTransform", func(t *testing.T) {
		cancelErr := errors.New("cancelled")
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				select {
				case <-ctx.Done():
					return nil, cancelErr
				default:
					return b, nil
				}
			},
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		msg := message.New([]byte("body"))
		action, err := wrapped(ctx, &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, cancelErr)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("TransformModifiesBody", func(t *testing.T) {
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				// simulate base64 encoding
				return []byte("encoded:" + string(b)), nil
			},
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("original"))
		_, _ = wrapped(t.Context(), &msg)

		assert.Equal(t, []byte("encoded:original"), msg.Body)
	})

	t.Run("BodyUnchangedOnTransformError", func(t *testing.T) {
		originalBody := []byte("original-body")
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				return nil, errors.New("transform failed")
			},
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New(originalBody)
		_, _ = wrapped(t.Context(), &msg)

		// body should remain unchanged when transform fails
		assert.Equal(t, originalBody, msg.Body)
	})

	t.Run("HandlerErrorStillPropagates", func(t *testing.T) {
		handlerErr := errors.New("handler error")
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				return b, nil
			},
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("body"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, handlerErr)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("ErrorWrappingPreservesContext", func(t *testing.T) {
		transformErr := errors.New("specific transform error")
		cfg := &TransformMiddlewareConfig{
			Transform: func(ctx context.Context, b []byte) ([]byte, error) {
				return nil, transformErr
			},
		}
		mw := TransformMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("body"))
		_, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.ErrorIs(t, err, transformErr)
		assert.Contains(t, err.Error(), "body transform failed")
	})
}

func TestDeadlineMiddleware(t *testing.T) {
	t.Run("NoDeadlineHeaderPassesThrough", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test")) // Headers is nil
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("UnexpiredDeadlinePassesThrough", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["x-deadline"] = time.Now().Add(time.Hour) // future
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("ExpiredDeadlineDefaultAction", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["x-deadline"] = time.Now().Add(-time.Minute) // past
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Contains(t, err.Error(), "expired")
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("ExpiredDeadlineCustomAction", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{
			Action: ActionNackRequeue,
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["x-deadline"] = time.Now().Add(-time.Second) // past
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("NonTimeHeaderIsIgnored", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["x-deadline"] = "not-a-time-value" // wrong type
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := DeadlineMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["x-deadline"] = time.Now().Add(-time.Hour) // expired
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("WithCustomTimestampFromUnixTimestamp", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{
			Timestamp: func(msg *message.Message) (time.Time, bool) {
				if ts, ok := msg.Headers["expires"].(int64); ok {
					return time.Unix(ts, 0), true
				}
				return time.Time{}, false
			},
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["expires"] = time.Now().Add(-time.Hour).Unix() // expired Unix timestamp
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("WithCustomTimestampFromTTL", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{
			Timestamp: func(msg *message.Message) (time.Time, bool) {
				if ttlSec, ok := msg.Headers["ttl"].(int); ok {
					return msg.Timestamp.Add(time.Duration(ttlSec) * time.Second), true
				}
				return time.Time{}, false
			},
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Timestamp = time.Now().Add(-10 * time.Second) // 10 seconds ago
		msg.Headers = make(message.Arguments)
		msg.Headers["ttl"] = 5 // 5 second TTL - expired
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("WithCustomTimestampReturningFalse", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{
			Timestamp: func(msg *message.Message) (time.Time, bool) {
				// never returns a deadline
				return time.Time{}, false
			},
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithZeroActionUsesDefault", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{
			Action: 0, // zero value should use default NackDiscard
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["x-deadline"] = time.Now().Add(-time.Minute) // expired
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action) // default
	})

	t.Run("WithCustomHeaderName", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{
			Timestamp: func(msg *message.Message) (time.Time, bool) {
				if raw, ok := msg.Headers["expires-at"]; ok {
					if deadline, ok := raw.(time.Time); ok {
						return deadline, true
					}
				}
				return time.Time{}, false
			},
		})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["expires-at"] = time.Now().Add(-time.Second) // expired
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("ErrorMessageIncludesDeadline", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{})
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["x-deadline"] = time.Now().Add(-time.Hour)
		_, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expired at")
	})

	t.Run("HandlerNotCalledWhenExpired", func(t *testing.T) {
		mw := DeadlineMiddleware(&DeadlineMiddlewareConfig{})

		handlerCalled := false
		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			handlerCalled = true
			return ActionAck, nil
		}
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		msg.Headers = make(message.Arguments)
		msg.Headers["x-deadline"] = time.Now().Add(-time.Minute) // expired
		wrapped(t.Context(), &msg)

		assert.False(t, handlerCalled, "handler should not be called for expired message")
	})
}

func TestTimeoutMiddleware(t *testing.T) {
	t.Run("TimesOutWithDefaultAction", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{Timeout: 50 * time.Millisecond}
		mw := TimeoutMiddleware(cfg)

		handler := testSleepHandler(ActionAck, 100*time.Millisecond)
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("CompletesInTime", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{Timeout: 100 * time.Millisecond}
		mw := TimeoutMiddleware(cfg)

		handler := testSleepHandler(ActionAck, 10*time.Millisecond)
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithNilConfig", func(t *testing.T) {
		mw := TimeoutMiddleware(nil)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithZeroTimeout", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{Timeout: 0}
		mw := TimeoutMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithNegativeTimeout", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{Timeout: -1 * time.Second}
		mw := TimeoutMiddleware(cfg)
		wrapped := mw(testHandler(ActionAck))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		require.NoError(t, err)
		assert.Equal(t, ActionAck, action)
	})

	t.Run("WithCustomAction", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{
			Timeout: 50 * time.Millisecond,
			Action:  ActionNackDiscard,
		}
		mw := TimeoutMiddleware(cfg)

		handler := testSleepHandler(ActionAck, 100*time.Millisecond)
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackDiscard, action)
	})

	t.Run("WithZeroActionUsesDefault", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{
			Timeout: 50 * time.Millisecond,
			Action:  0,
		}
		mw := TimeoutMiddleware(cfg)

		handler := testSleepHandler(ActionAck, 100*time.Millisecond)
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("ContextCancellationPropagates", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{Timeout: 100 * time.Millisecond}
		mw := TimeoutMiddleware(cfg)

		handler := func(ctx context.Context, msg *message.Message) (Action, error) {
			<-ctx.Done()
			return ActionAck, ctx.Err()
		}
		wrapped := mw(handler)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		msg := message.New([]byte("test"))
		action, err := wrapped(ctx, &msg)

		assert.Error(t, err)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("HandlerErrorStillPropagates", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{Timeout: 100 * time.Millisecond}
		mw := TimeoutMiddleware(cfg)

		handlerErr := errors.New("handler error")
		wrapped := mw(testErrorHandler(ActionNackRequeue, handlerErr))

		msg := message.New([]byte("test"))
		action, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, handlerErr)
		assert.Equal(t, ActionNackRequeue, action)
	})

	t.Run("ErrorWrappingPreservesContext", func(t *testing.T) {
		cfg := &TimeoutMiddlewareConfig{Timeout: 50 * time.Millisecond}
		mw := TimeoutMiddleware(cfg)

		handler := testSleepHandler(ActionAck, 100*time.Millisecond)
		wrapped := mw(handler)

		msg := message.New([]byte("test"))
		_, err := wrapped(t.Context(), &msg)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMiddleware)
		assert.Contains(t, err.Error(), "message processing timeout after")
	})
}
