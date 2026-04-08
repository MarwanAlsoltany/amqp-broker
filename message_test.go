package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessage(t *testing.T) {
	t.Run("NewMessage", func(t *testing.T) {
		body := []byte("hello")
		msg := NewMessage(body)

		assert.Equal(t, body, msg.Body)
		assert.NotEmpty(t, msg.ContentType)
		assert.NotZero(t, msg.DeliveryMode)
	})

	t.Run("NewMessageBuilder", func(t *testing.T) {
		b := NewMessageBuilder()

		require.NotNil(t, b)
	})
}
