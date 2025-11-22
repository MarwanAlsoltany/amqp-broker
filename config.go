package broker

import (
	"fmt"
	"os"
	"time"
)

func generateBrokerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", hostname, pid)
}

const (
	// Default AMQP Broker URL
	defaultBrokerURL = "amqp://guest:guest@localhost:5672/"

	// Default exchange kind
	defaultExchangeType = "direct"

	// Default connection pool size
	defaultConnPoolSize = 4

	// Default channel pool size
	defaultChPoolSize = 8

	// Default reconnection backoff
	defaultReconnectMin = 500 * time.Millisecond
	defaultReconnectMax = 30 * time.Second

	// Default reconnection delay for publishers/consumers
	defaultReconnectDelay = 1 * time.Second

	// Default ready timeout
	defaultReadyTimeout = 10 * time.Second

	// Default confirmation timeout
	defaultConfirmTimeout = 5 * time.Second

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

func defaultBinding(exchange, queue string) Binding {
	return Binding{
		Source:      exchange,
		Destination: queue,
		Pattern:     "",
		Args:        nil,
	}
}

// defaultPublishOptions returns PublishOptions with defaults applied.
func defaultPublishOptions() PublishOptions {
	return PublishOptions{
		Mandatory:      false,
		Immediate:      false,
		WaitForConfirm: false,
		ConfirmTimeout: defaultConfirmTimeout,
	}
}

func defaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		ConfirmMode:     false,
		NoWaitForReady:  false,
		ReadyTimeout:    defaultReadyTimeout,
		NoAutoReconnect: false,
		ReconnectDelay:  defaultReconnectDelay,
	}
}

// defaultConsumerOptions returns ConsumeOptions with defaults applied.
func defaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		AutoAck:         false,
		PrefetchCount:   defaultPrefetchCount,
		NoWaitForReady:  false,
		ReadyTimeout:    defaultReadyTimeout,
		NoAutoReconnect: false,
		ReconnectDelay:  defaultReconnectDelay,
	}
}
