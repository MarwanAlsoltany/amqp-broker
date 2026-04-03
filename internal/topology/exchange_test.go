package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewExchange(t *testing.T) {
	exchange := NewExchange("test-exchange")

	assert.Equal(t, "test-exchange", exchange.Name)
	assert.Equal(t, DefaultExchangeType, exchange.Type)
	assert.Equal(t, DefaultExchangeDurable, exchange.Durable)
	assert.False(t, exchange.AutoDelete)
	assert.False(t, exchange.Internal)
	assert.Nil(t, exchange.Arguments)
}

func TestExchangeValidate(t *testing.T) {
	tests := []struct {
		name     string
		exchange Exchange
		want     error
	}{
		{
			name:     "DirectExchange",
			exchange: Exchange{Name: "test.direct", Type: "direct", Durable: true},
			want:     nil,
		},
		{
			name:     "TopicExchange",
			exchange: Exchange{Name: "test.topic", Type: "topic", Durable: true},
			want:     nil,
		},
		{
			name:     "FanoutExchange",
			exchange: Exchange{Name: "test.fanout", Type: "fanout"},
			want:     nil,
		},
		{
			name:     "HeadersExchange",
			exchange: Exchange{Name: "test.headers", Type: "headers"},
			want:     nil,
		},
		{
			name:     "WithArguments",
			exchange: Exchange{Name: "test.args", Type: "direct", Arguments: Arguments{"x-delayed-type": "direct"}},
			want:     nil,
		},
		{
			name:     "EmptyFields",
			exchange: Exchange{},
			want:     ErrTopologyExchangeNameEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.exchange.Validate()
			assert.Equal(t, tt.want, err)
			if err != nil {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestExchangeMatches(t *testing.T) {
	exchange1 := Exchange{Name: "test", Type: "direct", Durable: true}
	exchange2 := Exchange{Name: "test", Type: "fanout", Durable: false}
	exchange3 := Exchange{Name: "other", Type: "direct", Durable: true}

	assert.True(t, exchange1.Matches(exchange2), "exchanges with same name should match")
	assert.False(t, exchange1.Matches(exchange3), "exchanges with different names should not match")
}

func TestExchangeDeclare(t *testing.T) {
	exchange := Exchange{Name: "test-exchange", Type: "direct", Durable: true}

	err := exchange.Declare(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)
	assert.ErrorContains(t, err, "exchange")

	t.Run("WhenTypeIsEmpty", func(t *testing.T) {
		err := Exchange{Name: "test-exchange"}.Declare(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyDeclareFailed)
	})

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Exchange{}.Declare(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyExchangeNameEmpty)
	})
}

func TestExchangeVerify(t *testing.T) {
	exchange := Exchange{Name: "test-exchange", Type: "direct", Durable: true}

	err := exchange.Verify(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)
	assert.ErrorContains(t, err, "exchange")

	t.Run("WhenTypeIsEmpty", func(t *testing.T) {
		err := Exchange{Name: "test-exchange"}.Verify(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyVerifyFailed)
	})

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Exchange{}.Verify(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyExchangeNameEmpty)
	})
}

func TestExchangeDelete(t *testing.T) {
	exchange := Exchange{Name: "test-exchange", Type: "direct", Durable: true}

	err := exchange.Delete(nil, false)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)
	assert.ErrorContains(t, err, "exchange")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Exchange{}.Delete(nil, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyExchangeNameEmpty)
	})
}

func TestExchangeWithMethods(t *testing.T) {
	t.Run("WithType", func(t *testing.T) {
		ex := NewExchange("test").WithType("topic")
		assert.Equal(t, "topic", ex.Type)
	})

	t.Run("WithDurable", func(t *testing.T) {
		ex := NewExchange("test").WithDurable(false)
		assert.False(t, ex.Durable)

		ex2 := NewExchange("test").WithDurable(true)
		assert.True(t, ex2.Durable)
	})

	t.Run("WithAutoDelete", func(t *testing.T) {
		ex := NewExchange("test").WithAutoDelete(true)
		assert.True(t, ex.AutoDelete)
	})

	t.Run("WithInternal", func(t *testing.T) {
		ex := NewExchange("test").WithInternal(true)
		assert.True(t, ex.Internal)
	})

	t.Run("WithArguments", func(t *testing.T) {
		args := Arguments{"x-delayed-type": "direct"}
		ex := NewExchange("test").WithArguments(args)
		assert.Equal(t, args, ex.Arguments)
	})

	t.Run("WithArgument", func(t *testing.T) {
		ex := NewExchange("test").
			WithArgument("x-max-length", 1000).
			WithArgument("x-message-ttl", 60000)

		assert.Equal(t, 1000, ex.Arguments["x-max-length"])
		assert.Equal(t, 60000, ex.Arguments["x-message-ttl"])
	})

	t.Run("Chaining", func(t *testing.T) {
		ex := NewExchange("events").
			WithType("topic").
			WithDurable(true).
			WithAutoDelete(false).
			WithArgument("x-delayed-type", "topic")

		assert.Equal(t, "events", ex.Name)
		assert.Equal(t, "topic", ex.Type)
		assert.True(t, ex.Durable)
		assert.False(t, ex.AutoDelete)
		assert.Equal(t, "topic", ex.Arguments["x-delayed-type"])
	})
}
