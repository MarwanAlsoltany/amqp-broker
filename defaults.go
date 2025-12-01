package broker

import (
	"fmt"
	"os"
	"time"
)

var defaultBrokerID string // read-only

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
	defaultConnectionPoolSize = 1

	// Default cache TTL for endpoints
	defaultCacheTTL = 5 * time.Minute

	// Default reconnection backoff
	defaultReconnectMin = 500 * time.Millisecond
	defaultReconnectMax = 30 * time.Second
	// Default reconnection delay between attempts
	defaultReconnectDelay = 1 * time.Second

	// Default ready timeout
	defaultReadyTimeout = 10 * time.Second

	// Default confirmation timeout for publishers
	defaultConfirmTimeout = 5 * time.Second

	// Default prefetch count for consumers
	defaultPrefetchCount = 1

	// Default concurrent handlers for consumers
	defaultConcurrentHandlers = 0 // unlimited

	// Default exchange type
	defaultExchangeType = "direct"

	// Default exchange durability
	defaultExchangeDurable = true

	// Default queue durability
	defaultQueueDurable = true

	// Default delivery mode for messages (1=transient, 2=persistent)
	defaultDeliveryMode = 2

	// Default content type for messages
	defaultContentType = "application/octet-stream"
)

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
		OnConfirm:       nil,
		OnReturn:        nil,
		OnFlow:          nil,
		OnError:         nil,
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
		OnCancel:              nil,
		OnError:               nil,
	}
}
