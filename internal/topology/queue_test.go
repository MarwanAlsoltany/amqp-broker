package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
)

func TestNewQueue(t *testing.T) {
	queue := NewQueue("test-queue")

	assert.Equal(t, "test-queue", queue.Name)
	assert.Equal(t, DefaultQueueDurable, queue.Durable)
	assert.False(t, queue.AutoDelete)
	assert.False(t, queue.Exclusive)
	assert.Nil(t, queue.Arguments)
}

func TestQueueValidate(t *testing.T) {
	tests := []struct {
		name  string
		queue Queue
		want  error
	}{
		{
			name:  "DurableQueue",
			queue: Queue{Name: "test.durable", Durable: true},
			want:  nil,
		},
		{
			name:  "AutoDeleteQueue",
			queue: Queue{Name: "test.autodelete", AutoDelete: true},
			want:  nil,
		},
		{
			name:  "ExclusiveQueue",
			queue: Queue{Name: "test.exclusive", Exclusive: true},
			want:  nil,
		},
		{
			name:  "WithArguments",
			queue: Queue{Name: "test.args", Arguments: Arguments{"x-message-ttl": 60000}},
			want:  nil,
		},
		{
			name:  "EmptyFields",
			queue: Queue{},
			want:  ErrTopologyQueueNameEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.queue.Validate()
			assert.Equal(t, tt.want, err)
			if err != nil {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestQueueMatches(t *testing.T) {
	queue1 := Queue{Name: "test", Durable: true}
	queue2 := Queue{Name: "test", Durable: false}
	queue3 := Queue{Name: "other", Durable: true}

	assert.True(t, queue1.Matches(queue2), "queues with same name should match")
	assert.False(t, queue1.Matches(queue3), "queues with different names should not match")
}

func TestQueueDeclare(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	err := queue.Declare(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeclareFailed)
	assert.ErrorContains(t, err, "queue")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Queue{}.Declare(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyQueueNameEmpty)
	})
}

func TestQueueVerify(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	err := queue.Verify(nil)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyVerifyFailed)
	assert.ErrorContains(t, err, "queue")

	t.Run("WhenInvalid", func(t *testing.T) {
		err := Queue{}.Verify(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyQueueNameEmpty)
	})
}

func TestQueueDelete(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	_, err := queue.Delete(nil, false, false)
	assert.Error(t, err) // expected with nil channel
	assert.ErrorIs(t, err, ErrTopologyDeleteFailed)
	assert.ErrorContains(t, err, "queue")

	t.Run("WhenInvalid", func(t *testing.T) {
		_, err := Queue{}.Delete(nil, false, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyQueueNameEmpty)
	})
}

func TestQueuePurge(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	_, err := queue.Purge(nil)
	assert.Error(t, err)                       // expected with nil channel
	assert.ErrorIs(t, err, internal.ErrBroker) // has no specific error

	t.Run("WhenInvalid", func(t *testing.T) {
		_, err := Queue{}.Purge(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyQueueNameEmpty)
	})
}

func TestQueueInspect(t *testing.T) {
	queue := Queue{Name: "test-queue", Durable: true}

	_, err := queue.Inspect(nil)
	assert.Error(t, err)                       // expected with nil channel
	assert.ErrorIs(t, err, internal.ErrBroker) // has no specific error

	t.Run("WhenInvalid", func(t *testing.T) {
		_, err := Queue{}.Inspect(nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTopologyQueueNameEmpty)
	})
}

func TestQueueWithMethods(t *testing.T) {
	t.Run("WithDurable", func(t *testing.T) {
		q := NewQueue("test").WithDurable(false)
		assert.False(t, q.Durable)

		q2 := NewQueue("test").WithDurable(true)
		assert.True(t, q2.Durable)
	})

	t.Run("WithAutoDelete", func(t *testing.T) {
		q := NewQueue("test").WithAutoDelete(true)
		assert.True(t, q.AutoDelete)
	})

	t.Run("WithExclusive", func(t *testing.T) {
		q := NewQueue("test").WithExclusive(true)
		assert.True(t, q.Exclusive)
	})

	t.Run("WithArguments", func(t *testing.T) {
		args := Arguments{"x-message-ttl": 60000}
		q := NewQueue("test").WithArguments(args)
		assert.Equal(t, args, q.Arguments)
	})

	t.Run("WithArgument", func(t *testing.T) {
		q := NewQueue("test").
			WithArgument("x-message-ttl", 60000).
			WithArgument("x-max-length", 10000)

		assert.Equal(t, 60000, q.Arguments["x-message-ttl"])
		assert.Equal(t, 10000, q.Arguments["x-max-length"])
	})

	t.Run("Chaining", func(t *testing.T) {
		q := NewQueue("notifications").
			WithDurable(true).
			WithAutoDelete(false).
			WithExclusive(false).
			WithArgument("x-message-ttl", 60000).
			WithArgument("x-dead-letter-exchange", "dlx")

		assert.Equal(t, "notifications", q.Name)
		assert.True(t, q.Durable)
		assert.False(t, q.AutoDelete)
		assert.False(t, q.Exclusive)
		assert.Equal(t, 60000, q.Arguments["x-message-ttl"])
		assert.Equal(t, "dlx", q.Arguments["x-dead-letter-exchange"])
	})
}
