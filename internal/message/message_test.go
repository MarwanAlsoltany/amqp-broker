package message

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAcknowledger implements Acknowledger for testing.
type mockAcknowledger struct {
	ackErr    error
	nackErr   error
	rejectErr error
	acked     bool
	nacked    bool
	rejected  bool
}

var _ Acknowledger = (*mockAcknowledger)(nil)

func (m *mockAcknowledger) Ack(_ bool) error {
	m.acked = true
	return m.ackErr
}

func (m *mockAcknowledger) Nack(_ bool, _ bool) error {
	m.nacked = true
	return m.nackErr
}

func (m *mockAcknowledger) Reject(_ bool) error {
	m.rejected = true
	return m.rejectErr
}

func TestNew(t *testing.T) {
	body := []byte("test")
	msg := New(body)

	assert.Equal(t, body, msg.Body)
	assert.Equal(t, DefaultContentType, msg.ContentType)
	assert.Equal(t, DefaultDeliveryMode, msg.DeliveryMode)
	assert.WithinDuration(t, time.Now(), msg.Timestamp, time.Second)
	assert.Nil(t, msg.metadata)

	t.Run("WithNilBody", func(t *testing.T) {
		msg := New(nil)

		assert.Nil(t, msg.Body)
		assert.Equal(t, DefaultContentType, msg.ContentType)
	})
}

func TestMessageData(t *testing.T) {
	t.Run("NilBody", func(t *testing.T) {
		msg := New(nil)
		assert.Nil(t, msg.Data())
	})

	t.Run("JSON", func(t *testing.T) {
		msg := New([]byte(`{"key":"value"}`))
		msg.ContentType = "application/json"

		data := msg.Data()
		require.NotNil(t, data)
		m, ok := data.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "value", m["key"])
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		msg := New([]byte(`not json`))
		msg.ContentType = "application/json"

		assert.Nil(t, msg.Data())
	})

	t.Run("PlainText", func(t *testing.T) {
		msg := New([]byte("hello"))
		msg.ContentType = "text/plain"

		assert.Equal(t, "hello", msg.Data())
	})

	t.Run("BinaryDefault", func(t *testing.T) {
		body := []byte{0x01, 0x02, 0x03}
		msg := New(body)

		assert.Equal(t, body, msg.Data())
	})
}

func TestMessageCopy(t *testing.T) {
	original := New([]byte("original"))
	original.ContentType = "text/plain"
	original.MessageID = "msg-123"
	original.Headers = Arguments{"x-name": "value"}

	copy := original.Copy()

	assert.Equal(t, original.Body, copy.Body)
	assert.Equal(t, original.ContentType, copy.ContentType)
	assert.Equal(t, original.MessageID, copy.MessageID)
	assert.Equal(t, original.Headers, copy.Headers)

	// verify headers are deep copied
	original.Headers["x-name"] = "changed"
	assert.Equal(t, "value", copy.Headers["x-name"])
}

func TestMessageCopyNilHeaders(t *testing.T) {
	original := New([]byte("body"))
	assert.Nil(t, original.Headers)

	copy := original.Copy()
	assert.Nil(t, copy.Headers)
}

func TestMetadataIsPublished(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		assert.True(t, msg.IsPublished())
	})

	t.Run("Consumed", func(t *testing.T) {
		msg := NewConsumedMessage(New([]byte("test")), &mockAcknowledger{}, DeliveryInfo{})
		assert.False(t, msg.IsPublished())
	})

	t.Run("Returned", func(t *testing.T) {
		msg := NewReturnedMessage(New([]byte("test")), ReturnInfo{})
		assert.False(t, msg.IsPublished())
	})
}

func TestMetadataIsConsumed(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		assert.False(t, msg.IsConsumed())
	})

	t.Run("Consumed", func(t *testing.T) {
		msg := NewConsumedMessage(New([]byte("test")), &mockAcknowledger{}, DeliveryInfo{})
		assert.True(t, msg.IsConsumed())
	})

	t.Run("Returned", func(t *testing.T) {
		msg := NewReturnedMessage(New([]byte("test")), ReturnInfo{})
		assert.False(t, msg.IsConsumed())
	})
}

func TestMetadataIsRedelivered(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		assert.False(t, msg.IsRedelivered())
	})

	t.Run("NotRedelivered", func(t *testing.T) {
		msg := NewConsumedMessage(
			New([]byte("test")),
			&mockAcknowledger{},
			DeliveryInfo{Redelivered: false},
		)
		assert.False(t, msg.IsRedelivered())
	})

	t.Run("Redelivered", func(t *testing.T) {
		msg := NewConsumedMessage(
			New([]byte("test")),
			&mockAcknowledger{},
			DeliveryInfo{Redelivered: true},
		)
		assert.True(t, msg.IsRedelivered())
	})
}

func TestMetadataIsReturned(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		assert.False(t, msg.IsReturned())
	})

	t.Run("NotReturned", func(t *testing.T) {
		msg := NewConsumedMessage(New([]byte("test")), &mockAcknowledger{}, DeliveryInfo{})
		assert.False(t, msg.IsReturned())
	})

	t.Run("Returned", func(t *testing.T) {
		msg := NewReturnedMessage(New([]byte("test")), ReturnInfo{})
		assert.True(t, msg.IsReturned())
	})
}

func TestMetadataAck(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		err := msg.Ack()
		assert.ErrorIs(t, err, ErrMessageNotConsumed)
	})

	t.Run("Returned", func(t *testing.T) {
		msg := NewReturnedMessage(New([]byte("test")), ReturnInfo{})
		err := msg.Ack()
		assert.ErrorIs(t, err, ErrMessageNotConsumed)
	})

	t.Run("Consumed", func(t *testing.T) {
		ack := &mockAcknowledger{}
		msg := NewConsumedMessage(New([]byte("test")), ack, DeliveryInfo{})
		err := msg.Ack()
		require.NoError(t, err)
		assert.True(t, ack.acked)
	})

	t.Run("ConsumedWithError", func(t *testing.T) {
		ack := &mockAcknowledger{ackErr: errors.New("ack failed")}
		msg := NewConsumedMessage(New([]byte("test")), ack, DeliveryInfo{})
		err := msg.Ack()
		assert.EqualError(t, err, "ack failed")
		assert.NotErrorIs(t, err, ErrMessageNotConsumed)
	})
}

func TestMetadataNack(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		err := msg.Nack(false)
		assert.ErrorIs(t, err, ErrMessageNotConsumed)
	})

	t.Run("Consumed", func(t *testing.T) {
		ack := &mockAcknowledger{}
		msg := NewConsumedMessage(New([]byte("test")), ack, DeliveryInfo{})
		err := msg.Nack(true)
		require.NoError(t, err)
		assert.True(t, ack.nacked)
	})

	t.Run("ConsumedWithError", func(t *testing.T) {
		ack := &mockAcknowledger{nackErr: errors.New("nack failed")}
		msg := NewConsumedMessage(New([]byte("test")), ack, DeliveryInfo{})
		err := msg.Nack(false)
		assert.EqualError(t, err, "nack failed")
		assert.NotErrorIs(t, err, ErrMessageNotConsumed)
	})
}

func TestMetadataReject(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		err := msg.Reject()
		assert.ErrorIs(t, err, ErrMessageNotConsumed)
	})

	t.Run("Consumed", func(t *testing.T) {
		ack := &mockAcknowledger{}
		msg := NewConsumedMessage(New([]byte("test")), ack, DeliveryInfo{})
		err := msg.Reject()
		require.NoError(t, err)
		assert.True(t, ack.rejected)
	})

	t.Run("ConsumedWithError", func(t *testing.T) {
		ack := &mockAcknowledger{rejectErr: errors.New("reject failed")}
		msg := NewConsumedMessage(New([]byte("test")), ack, DeliveryInfo{})
		err := msg.Reject()
		assert.EqualError(t, err, "reject failed")
		assert.NotErrorIs(t, err, ErrMessageNotConsumed)
	})
}

func TestMetadataDeliveryDetails(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		_, err := msg.DeliveryInfo()
		assert.ErrorIs(t, err, ErrMessageNotConsumed)
	})

	t.Run("Returned", func(t *testing.T) {
		msg := NewReturnedMessage(New([]byte("test")), ReturnInfo{})
		_, err := msg.DeliveryInfo()
		assert.ErrorIs(t, err, ErrMessageNotConsumed)
	})

	t.Run("Consumed", func(t *testing.T) {
		info := DeliveryInfo{
			DeliveryTag:  42,
			ConsumerTag:  "consumer-1",
			Exchange:     "test-exchange",
			RoutingKey:   "test-key",
			Redelivered:  true,
			MessageCount: 3,
		}
		msg := NewConsumedMessage(New([]byte("test")), &mockAcknowledger{}, info)
		got, err := msg.DeliveryInfo()
		require.NoError(t, err)
		assert.Equal(t, info, got)
	})
}

func TestMetadataReturnDetails(t *testing.T) {
	t.Run("NilMetadata", func(t *testing.T) {
		msg := New([]byte("test"))
		_, err := msg.ReturnInfo()
		assert.ErrorIs(t, err, ErrMessageNotPublished)
	})

	t.Run("Consumed", func(t *testing.T) {
		msg := NewConsumedMessage(New([]byte("test")), &mockAcknowledger{}, DeliveryInfo{})
		_, err := msg.ReturnInfo()
		assert.ErrorIs(t, err, ErrMessageNotPublished)
	})

	t.Run("Returned", func(t *testing.T) {
		info := ReturnInfo{
			ReplyCode:  312,
			ReplyText:  "NO_ROUTE",
			Exchange:   "test-exchange",
			RoutingKey: "test-key",
		}
		msg := NewReturnedMessage(New([]byte("test")), info)
		got, err := msg.ReturnInfo()
		require.NoError(t, err)
		assert.Equal(t, info, got)
	})
}

func TestNewConsumedMessage(t *testing.T) {
	body := []byte("consumed body")
	message := Message{
		Body:          body,
		ContentType:   "application/json",
		MessageID:     "msg-1",
		CorrelationID: "corr-1",
	}
	info := DeliveryInfo{
		DeliveryTag: 7,
		ConsumerTag: "c-tag",
		Exchange:    "events",
		RoutingKey:  "user.created",
		Redelivered: false,
	}
	ack := &mockAcknowledger{}

	msg := NewConsumedMessage(message, ack, info)

	assert.Equal(t, body, msg.Body)
	assert.Equal(t, "application/json", msg.ContentType)
	assert.Equal(t, "msg-1", msg.MessageID)
	assert.True(t, msg.IsConsumed())
	assert.False(t, msg.IsReturned())
	assert.False(t, msg.IsPublished())
	assert.False(t, msg.IsRedelivered())
	gotInfo, err := msg.DeliveryInfo()
	require.NoError(t, err)
	assert.Equal(t, info, gotInfo)
}

func TestNewReturnedMessage(t *testing.T) {
	body := []byte("returned body")
	message := Message{
		Body:      body,
		MessageID: "msg-2",
	}
	info := ReturnInfo{
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE",
		Exchange:   "events",
		RoutingKey: "missing.key",
	}

	msg := NewReturnedMessage(message, info)

	assert.Equal(t, body, msg.Body)
	assert.Equal(t, "msg-2", msg.MessageID)
	assert.False(t, msg.IsConsumed())
	assert.True(t, msg.IsReturned())
	assert.False(t, msg.IsPublished())
	_, errDel := msg.DeliveryInfo()
	assert.ErrorIs(t, errDel, ErrMessageNotConsumed)
	gotInfo, err := msg.ReturnInfo()
	require.NoError(t, err)
	assert.Equal(t, info, gotInfo)
}
