package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopology(t *testing.T) {
	t.Run("NewExchange", func(t *testing.T) {
		e := NewExchange("test-exchange")

		assert.Equal(t, "test-exchange", e.Name)
	})

	t.Run("NewQueue", func(t *testing.T) {
		q := NewQueue("test-queue")

		assert.Equal(t, "test-queue", q.Name)
	})

	t.Run("NewBinding", func(t *testing.T) {
		b := NewBinding("test-exchange", "test-queue", "test-key")

		assert.Equal(t, "test-exchange", b.Source)
		assert.Equal(t, "test-queue", b.Destination)
		assert.Equal(t, "test-key", b.Key)
	})

	t.Run("NewTopology", func(t *testing.T) {
		exchanges := []Exchange{NewExchange("exchange")}
		queues := []Queue{NewQueue("queue")}
		bindings := []Binding{NewBinding("exchange", "queue", "routing.key")}

		topology := NewTopology(exchanges, queues, bindings)

		assert.Len(t, topology.Exchanges, 1)
		assert.Len(t, topology.Queues, 1)
		assert.Len(t, topology.Bindings, 1)
	})

	t.Run("NewRoutingKey", func(t *testing.T) {
		rk := NewRoutingKey("orders.{region}.placed", map[string]string{"region": "eu"})

		assert.Equal(t, RoutingKey("orders.eu.placed"), rk)
	})
}
