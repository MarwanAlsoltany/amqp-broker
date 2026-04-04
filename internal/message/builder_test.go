package message

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuilder(t *testing.T) {
	builder := NewBuilder()

	assert.NotNil(t, builder)
	assert.NotNil(t, builder.msg)
	assert.Equal(t, DefaultContentType, builder.msg.ContentType)
	assert.Equal(t, DefaultDeliveryMode, builder.msg.DeliveryMode)
	assert.WithinDuration(t, time.Now(), builder.msg.Timestamp, time.Second)
}

func TestMessageBuilderBody(t *testing.T) {
	builder := NewBuilder()
	body := []byte("test body")

	msg, err := builder.Body(body).Build()

	require.NoError(t, err)
	assert.Equal(t, body, msg.Body)
}

func TestMessageBuilderBodyString(t *testing.T) {
	builder := NewBuilder()
	body := "test"

	msg, err := builder.BodyString(body).Build()

	require.NoError(t, err)
	assert.Equal(t, []byte(body), msg.Body)
}

func TestMessageBuilderBodyJSON(t *testing.T) {
	tests := []struct {
		name      string
		input     interface{}
		wantError bool
	}{
		{
			name: "SerializableStruct",
			input: struct {
				Str string `json:"str"`
				Num int    `json:"num"`
			}{
				Str: "value",
				Num: 123,
			},
			wantError: false,
		},
		{
			name:      "SerializableSlice",
			input:     []string{"a", "b", "c"},
			wantError: false,
		},
		{
			name:      "UnserializableType",
			input:     make(chan int),
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewBuilder()
			msg, err := builder.BodyJSON(tt.input).Build()

			if tt.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "application/json", msg.ContentType)
				assert.NotEmpty(t, msg.Body)
			}
		})
	}
}

func TestMessageBuilderContentType(t *testing.T) {
	builder := NewBuilder()
	ct := "text/plain"

	msg, err := builder.ContentType(ct).Build()

	require.NoError(t, err)
	assert.Equal(t, ct, msg.ContentType)
}

func TestMessageBuilderDeliveryMode(t *testing.T) {
	builder := NewBuilder()
	dm := uint8(2)

	msg, err := builder.DeliveryMode(dm).Build()

	require.NoError(t, err)
	assert.Equal(t, dm, msg.DeliveryMode)
}

func TestMessageBuilderPriority(t *testing.T) {
	builder := NewBuilder()
	p := uint8(5)

	msg, err := builder.Priority(p).Build()

	require.NoError(t, err)
	assert.Equal(t, p, msg.Priority)
}

func TestMessageBuilderTimestamp(t *testing.T) {
	builder := NewBuilder()
	ts := time.Now().Add(-1 * time.Hour)

	msg, err := builder.Timestamp(ts).Build()

	require.NoError(t, err)
	assert.Equal(t, ts, msg.Timestamp)
}

func TestMessageBuilderHeader(t *testing.T) {
	builder := NewBuilder()

	msg, err := builder.
		Header("key1", "value1").
		Header("key2", 123).
		Build()

	require.NoError(t, err)
	assert.Equal(t, "value1", msg.Headers["key1"])
	assert.Equal(t, 123, msg.Headers["key2"])
}

func TestMessageBuilderHeaders(t *testing.T) {
	builder := NewBuilder()
	headers := Arguments{
		"x-custom": "value",
		"x-count":  10,
	}

	msg, err := builder.Headers(headers).Build()

	require.NoError(t, err)
	assert.Equal(t, headers, msg.Headers)
}

func TestMessageBuilderCorrelationID(t *testing.T) {
	builder := NewBuilder()
	corrID := "correlation-123"

	msg, err := builder.CorrelationID(corrID).Build()

	require.NoError(t, err)
	assert.Equal(t, corrID, msg.CorrelationID)
}

func TestMessageBuilderReplyTo(t *testing.T) {
	builder := NewBuilder()
	replyQueue := "reply.queue"

	msg, err := builder.ReplyTo(replyQueue).Build()

	require.NoError(t, err)
	assert.Equal(t, replyQueue, msg.ReplyTo)
}

func TestMessageBuilderMessageID(t *testing.T) {
	builder := NewBuilder()
	msgID := "msg-123"

	msg, err := builder.MessageID(msgID).Build()

	require.NoError(t, err)
	assert.Equal(t, msgID, msg.MessageID)
}

func TestMessageBuilderExpiration(t *testing.T) {
	builder := NewBuilder()
	expiration := "60000"

	msg, err := builder.Expiration(expiration).Build()

	require.NoError(t, err)
	assert.Equal(t, expiration, msg.Expiration)
}

func TestMessageBuilderExpirationDuration(t *testing.T) {
	builder := NewBuilder()
	duration := 5 * time.Second

	msg, err := builder.ExpirationDuration(duration).Build()

	require.NoError(t, err)
	assert.Equal(t, "5000", msg.Expiration)
}

func TestMessageBuilderType(t *testing.T) {
	builder := NewBuilder()
	msgType := "user.created"

	msg, err := builder.Type(msgType).Build()

	require.NoError(t, err)
	assert.Equal(t, msgType, msg.Type)
}

func TestMessageBuilderAppID(t *testing.T) {
	builder := NewBuilder()
	appID := "my-app"

	msg, err := builder.AppID(appID).Build()

	require.NoError(t, err)
	assert.Equal(t, appID, msg.AppID)
}

func TestMessageBuilderUserID(t *testing.T) {
	builder := NewBuilder()
	userID := "user-123"

	msg, err := builder.UserID(userID).Build()

	require.NoError(t, err)
	assert.Equal(t, userID, msg.UserID)
}

func TestMessageBuilderNow(t *testing.T) {
	builder := NewBuilder()
	before := time.Now()

	msg, err := builder.Now().Build()

	after := time.Now()
	require.NoError(t, err)
	assert.True(t, msg.Timestamp.After(before) || msg.Timestamp.Equal(before))
	assert.True(t, msg.Timestamp.Before(after) || msg.Timestamp.Equal(after))
}

func TestMessageBuilderPersistent(t *testing.T) {
	builder := NewBuilder()

	msg, err := builder.Persistent().Build()

	require.NoError(t, err)
	assert.Equal(t, uint8(2), msg.DeliveryMode)
}

func TestMessageBuilderTransient(t *testing.T) {
	builder := NewBuilder()

	msg, err := builder.Transient().Build()

	require.NoError(t, err)
	assert.Equal(t, uint8(1), msg.DeliveryMode)
}

func TestMessageBuilderJSON(t *testing.T) {
	builder := NewBuilder()

	msg, err := builder.JSON().Build()

	require.NoError(t, err)
	assert.Equal(t, "application/json", msg.ContentType)
}

func TestMessageBuilderText(t *testing.T) {
	builder := NewBuilder()

	msg, err := builder.Text().Build()

	require.NoError(t, err)
	assert.Equal(t, "text/plain", msg.ContentType)
}

func TestMessageBuilderBinary(t *testing.T) {
	builder := NewBuilder()

	msg, err := builder.Binary().Build()

	require.NoError(t, err)
	assert.Equal(t, "application/octet-stream", msg.ContentType)
}

func TestMessageBuilderReset(t *testing.T) {
	builder := NewBuilder()
	builder.BodyString("test").
		MessageID("msg-123").
		Priority(5)

	builder.Reset()

	msg, err := builder.Build()
	require.NoError(t, err)
	assert.Nil(t, msg.Body)
	assert.Empty(t, msg.MessageID)
	assert.Equal(t, uint8(0), msg.Priority)
	assert.Equal(t, DefaultContentType, msg.ContentType)
}

func TestMessageBuilderBuild(t *testing.T) {
	t.Run("WithChaining", func(t *testing.T) {
		ts := time.Now()

		msg, err := NewBuilder().
			BodyString("test body").
			ContentType("text/plain").
			DeliveryMode(2).
			Priority(5).
			Timestamp(ts).
			CorrelationID("corr-123").
			ReplyTo("reply-queue").
			MessageID("msg-456").
			Header("x-custom", "value").
			Type("test.event").
			AppID("test-app").
			UserID("user-1").
			Expiration("30000").
			Build()

		require.NoError(t, err)
		assert.Equal(t, []byte("test body"), msg.Body)
		assert.Equal(t, "text/plain", msg.ContentType)
		assert.Equal(t, uint8(2), msg.DeliveryMode)
		assert.Equal(t, uint8(5), msg.Priority)
		assert.Equal(t, ts, msg.Timestamp)
		assert.Equal(t, "corr-123", msg.CorrelationID)
		assert.Equal(t, "reply-queue", msg.ReplyTo)
		assert.Equal(t, "msg-456", msg.MessageID)
		assert.Equal(t, "value", msg.Headers["x-custom"])
		assert.Equal(t, "test.event", msg.Type)
		assert.Equal(t, "test-app", msg.AppID)
		assert.Equal(t, "user-1", msg.UserID)
		assert.Equal(t, "30000", msg.Expiration)
	})

	t.Run("WithErrors", func(t *testing.T) {
		builder := NewBuilder()

		// create an invalid JSON scenario
		invalidJSON := make(chan int)
		msg, err := builder.BodyJSON(invalidJSON).Build()

		assert.ErrorIs(t, err, ErrMessageBuild)
		assert.Empty(t, msg.Body)
	})

	t.Run("Reusability", func(t *testing.T) {
		// Build() returns a copy of the message, so the builder is safe to reuse
		// without Reset(). This test verifies that behavior.
		builder := NewBuilder().
			ContentType("application/json").
			DeliveryMode(2)

		msg1, err := builder.BodyString("message 1").Build()
		require.NoError(t, err)

		msg2, err := builder.BodyString("message 2").Build()
		require.NoError(t, err)

		// both should have the same settings but different bodies
		// Build() returns a copy, so msg1 is not affected by subsequent calls
		assert.Equal(t, "application/json", msg1.ContentType)
		assert.Equal(t, "application/json", msg2.ContentType)
		assert.Equal(t, uint8(2), msg1.DeliveryMode)
		assert.Equal(t, uint8(2), msg2.DeliveryMode)
		assert.Equal(t, []byte("message 1"), msg1.Body)
		assert.Equal(t, []byte("message 2"), msg2.Body)
	})
}
