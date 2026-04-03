package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRoutingKey(t *testing.T) {
	key := NewRoutingKey("test-key", nil)
	assert.Equal(t, RoutingKey("test-key"), key)
}

func TestRoutingKeyString(t *testing.T) {
	key := RoutingKey("test-key")
	assert.Equal(t, "test-key", key.String())
}

func TestRoutingKeyReplace(t *testing.T) {
	tests := []struct {
		name string
		key  RoutingKey
		args map[string]string
		want RoutingKey
	}{
		{
			name: "SinglePlaceholder",
			key:  RoutingKey("user.{id}.created"),
			args: map[string]string{"id": "123"},
			want: RoutingKey("user.123.created"),
		},
		{
			name: "MultiplePlaceholders",
			key:  RoutingKey("user.{id}.{action}"),
			args: map[string]string{"id": "123", "action": "updated"},
			want: RoutingKey("user.123.updated"),
		},
		{
			name: "NoPlaceholders",
			key:  RoutingKey("user.created"),
			args: map[string]string{"id": "123"},
			want: RoutingKey("user.created"),
		},
		{
			name: "UnusedArguments",
			key:  RoutingKey("user.{id}.created"),
			args: map[string]string{"id": "123", "unused": "value"},
			want: RoutingKey("user.123.created"),
		},
		{
			name: "EmptyArguments",
			key:  RoutingKey("user.{id}.created"),
			args: map[string]string{},
			want: RoutingKey("user.{id}.created"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.key.Replace(tt.args)
			assert.Equal(t, tt.want, tt.key)
		})
	}
}

func TestRoutingKeyValidate(t *testing.T) {
	tests := []struct {
		name string
		key  RoutingKey
		want error
	}{
		{
			name: "ValidKey",
			key:  RoutingKey("user.created"),
			want: nil,
		},
		{
			name: "ValidTopicPattern",
			key:  RoutingKey("user.*.created"),
			want: nil,
		},
		{
			name: "ValidHashPattern",
			key:  RoutingKey("user.#"),
			want: nil,
		},
		{
			name: "ValidWithPlaceholders",
			key:  RoutingKey("user.{id}.created"),
			want: nil,
		},
		{
			name: "EmptyKey",
			key:  RoutingKey(""),
			want: ErrTopologyRoutingKeyEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.key.Validate()
			assert.Equal(t, tt.want, err)
			if err != nil {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}
