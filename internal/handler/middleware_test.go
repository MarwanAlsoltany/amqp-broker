package handler

import (
	"testing"

	"github.com/MarwanAlsoltany/amqp-broker/internal/message"
	"github.com/stretchr/testify/assert"
)

func TestNoopMiddleware(t *testing.T) {
	mw := noopMiddleware()
	wrapped := mw(ActionHandler(ActionNoAction))

	action, err := wrapped(t.Context(), &message.Message{})

	assert.NoError(t, err)
	assert.Equal(t, ActionNoAction, action)
}
