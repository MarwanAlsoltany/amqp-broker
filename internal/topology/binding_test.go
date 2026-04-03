package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBinding(t *testing.T) {
	binding := NewBinding("test-exchange", "test-queue", "test-key")

	assert.Equal(t, "test-exchange", binding.Source)
	assert.Equal(t, "test-queue", binding.Destination)
	assert.Equal(t, "test-key", binding.Key)
	assert.Equal(t, BindingTypeQueue, binding.Type)
	assert.Nil(t, binding.Arguments)
}

func TestBindingValidate(t *testing.T) {
	tests := []struct {
		name    string
		binding Binding
		want    error
	}{
		{
			name:    "WithSourceAndDestination",
			binding: Binding{Source: "exchange", Destination: "queue"},
			want:    nil,
		},
		{
			name:    "WithSourceAndDestinationAndKey",
			binding: Binding{Source: "exchange", Destination: "queue", Key: "key"},
			want:    nil,
		},
		{
			name:    "WithSourceAndDestinationAndArguments",
			binding: Binding{Source: "exchange", Destination: "queue", Arguments: Arguments{"x-match": "all"}},
			want:    nil,
		},
		{
			name:    "EmptyFields",
			binding: Binding{},
			want:    ErrTopologyBindingFieldsEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.binding.Validate()
			assert.Equal(t, tt.want, err)
			if err != nil {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestBindingMatches(t *testing.T) {
	binding1 := Binding{Source: "exchange", Destination: "queue", Key: "test"}
	binding2 := Binding{Source: "exchange", Destination: "queue", Key: "test"}
	binding3 := Binding{Source: "exchange", Destination: "queue", Key: "other"}
	binding4 := Binding{Source: "other", Destination: "queue", Key: "test"}

	assert.True(t, binding1.Matches(binding2), "bindings with same source, destination, key should match")
	assert.False(t, binding1.Matches(binding3), "bindings with different keys should not match")
	assert.False(t, binding1.Matches(binding4), "bindings with different sources should not match")
}

func TestBindingDeclare(t *testing.T) {
	binding := Binding{Source: "test-exchange", Destination: "test-queue", Key: "test-key"}

	err := binding.Declare(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)
	assert.ErrorContains(t, err, "binding")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Binding{}.Declare(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyBindingFieldsEmpty)
	})
}

func TestBindingDelete(t *testing.T) {
	binding := Binding{Source: "test-exchange", Destination: "test-queue", Key: "test-key"}

	err := binding.Delete(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)
	assert.ErrorContains(t, err, "binding")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Binding{}.Delete(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyBindingFieldsEmpty)
	})
}

func TestBindingWithMethods(t *testing.T) {
	t.Run("WithType", func(t *testing.T) {
		b := NewBinding("source", "destination", "key").WithType(BindingTypeExchange)
		assert.Equal(t, BindingTypeExchange, b.Type)
	})

	t.Run("WithArguments", func(t *testing.T) {
		args := Arguments{"x-match": "all"}
		b := NewBinding("source", "destination", "key").WithArguments(args)
		assert.Equal(t, args, b.Arguments)
	})

	t.Run("WithArgument", func(t *testing.T) {
		b := NewBinding("source", "destination", "key").
			WithArgument("x-match", "all").
			WithArgument("nameX", "valueX")

		assert.Equal(t, "all", b.Arguments["x-match"])
		assert.Equal(t, "valueX", b.Arguments["nameX"])
	})

	t.Run("Chaining", func(t *testing.T) {
		b := NewBinding("events", "notifications", "user.*").
			WithType(BindingTypeQueue).
			WithArgument("x-match", "any").
			WithArgument("priority", 5)

		assert.Equal(t, "events", b.Source)
		assert.Equal(t, "notifications", b.Destination)
		assert.Equal(t, "user.*", b.Key)
		assert.Equal(t, BindingTypeQueue, b.Type)
		assert.Equal(t, "any", b.Arguments["x-match"])
		assert.Equal(t, 5, b.Arguments["priority"])
	})
}
