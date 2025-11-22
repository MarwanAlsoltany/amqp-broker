package broker

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer is a handle to a running consumer. It provides access to the last known underlying channel/connection.
type Consumer interface {
	endpoint
	Wait()
	Unregister()
}

// consumerEntry stores consumer registration + runtime state
type consumerEntry struct {
	id      string
	queue   Queue
	opts    ConsumeOptions
	handler Handler

	runningMu sync.Mutex
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// last seen runtime channel & conn (nil if not running); read-only for user
	lastMu   sync.RWMutex
	lastCh   *amqp.Channel
	lastConn *amqp.Connection
}

func (ce *consumerEntry) stop() error {
	ce.runningMu.Lock()
	cancel := ce.cancel
	ce.runningMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

type consumerHandle struct {
	entry  *consumerEntry
	broker *Broker
}

var _ Consumer = (*consumerHandle)(nil)

func (h *consumerHandle) Close() error {
	if h.entry != nil {
		return h.entry.stop()
	}
	return nil
}
func (h *consumerHandle) Wait() {
	if h.entry != nil {
		h.entry.wg.Wait()
	}
}
func (h *consumerHandle) Unregister() {
	if h.entry == nil || h.broker == nil {
		return
	}
	h.broker.consumersMu.Lock()
	delete(h.broker.consumers, h.entry.id)
	h.broker.consumersMu.Unlock()
	_ = h.entry.stop()
}
func (h *consumerHandle) Connection() *amqp.Connection {
	if h.entry == nil {
		return nil
	}
	h.entry.lastMu.RLock()
	defer h.entry.lastMu.RUnlock()
	return h.entry.lastConn
}
func (h *consumerHandle) Channel() *amqp.Channel {
	if h.entry == nil {
		return nil
	}
	h.entry.lastMu.RLock()
	defer h.entry.lastMu.RUnlock()
	return h.entry.lastCh
}
