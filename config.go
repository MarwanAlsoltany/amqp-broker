package broker

import (
	"fmt"
	"os"
	"time"
)

var defaultBrokerID string

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()

	defaultBrokerID = fmt.Sprintf("%s-%d", hostname, pid)
}

const (
	// Default AMQP Broker URL
	defaultBrokerURL = "amqp://guest:guest@localhost:5672/"

	// Default connection pool size
	// Size 2: One for publishers/control, one for consumers (recommended)
	defaultConnPoolSize = 1

	// Default reconnection backoff
	defaultReconnectMin = 500 * time.Millisecond
	defaultReconnectMax = 30 * time.Second
	// Default reconnection delay between attempts
	defaultReconnectDelay = 1 * time.Second

	// Default ready timeout
	defaultReadyTimeout = 10 * time.Second

	// Default confirmation timeout
	defaultConfirmTimeout = 5 * time.Second

	// Default concurrent handlers for consumers
	defaultConcurrentHandlers = 0 // unlimited

	// Default exchange kind
	defaultExchangeType = "direct"

	// Default prefetch count for consumers
	defaultPrefetchCount = 1

	// Default delivery mode (2 = persistent)
	defaultDeliveryMode = 2

	// Default content type
	defaultContentType = "application/octet-stream"
)

func defaultExchange(name string) Exchange {
	return Exchange{
		Name:       name,
		Type:       defaultExchangeType,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

func defaultQueue(name string) Queue {
	return Queue{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}

// defaultPublishOptions returns PublishOptions with defaults applied.
func defaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		ConfirmMode:     false,
		ConfirmTimeout:  defaultConfirmTimeout,
		Mandatory:       false,
		Immediate:       false,
		NoWaitForReady:  false,
		ReadyTimeout:    defaultReadyTimeout,
		NoAutoReconnect: false,
		ReconnectDelay:  defaultReconnectDelay,
	}
}

// defaultConsumerOptions returns ConsumeOptions with defaults applied.
func defaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		AutoAck:               false,
		PrefetchCount:         defaultPrefetchCount,
		NoWait:                false,
		Exclusive:             false,
		NoWaitForReady:        false,
		ReadyTimeout:          defaultReadyTimeout,
		NoAutoReconnect:       false,
		ReconnectDelay:        defaultReconnectDelay,
		MaxConcurrentHandlers: defaultConcurrentHandlers,
	}
}
