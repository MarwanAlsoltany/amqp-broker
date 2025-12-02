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

	// Default cache TTL for Broker endpoints
	defaultCacheTTL = 5 * time.Minute

	// Default endpoint reconnection backoff min
	defaultReconnectMin = 500 * time.Millisecond
	// Default endpoint reconnection backoff max
	defaultReconnectMax = 30 * time.Second

	// Default endpoint ready timeout
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

// defaultEndpointOptions returns EndpointOptions with defaults applied.
func defaultEndpointOptions() EndpointOptions {
	return EndpointOptions{
		NoAutoReconnect: false,
		ReconnectMin:    defaultReconnectMin,
		ReconnectMax:    defaultReconnectMax,
		NoWaitReady:     false,
		ReadyTimeout:    defaultReadyTimeout,
	}
}

// mergeEndpointOptions merges user-supplied options with defaults.
func mergeEndpointOptions(user, defaults EndpointOptions) EndpointOptions {
	if user.ReconnectMin == 0 {
		user.ReconnectMin = defaults.ReconnectMin
	}
	if user.ReconnectMax == 0 {
		user.ReconnectMax = defaults.ReconnectMax
	}
	if user.ReadyTimeout == 0 {
		user.ReadyTimeout = defaults.ReadyTimeout
	}
	// NoAutoReconnect and NoWaitReady are bools,
	// so user value is always explicit (false is a valid override)
	return user
}

// defaultPublishOptions returns PublishOptions with defaults applied.
func defaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		EndpointOptions: defaultEndpointOptions(),
		ConfirmMode:     false,
		ConfirmTimeout:  defaultConfirmTimeout,
		Mandatory:       false,
		Immediate:       false,
		OnConfirm:       nil,
		OnReturn:        nil,
		OnFlow:          nil,
		OnError:         nil,
	}
}

// mergePublisherOptions merges user-supplied options with defaults.
func mergePublisherOptions(user, defaults PublisherOptions) PublisherOptions {
	user.EndpointOptions = mergeEndpointOptions(user.EndpointOptions, defaults.EndpointOptions)
	if user.ConfirmTimeout == 0 {
		user.ConfirmTimeout = defaults.ConfirmTimeout
	}
	// other fields are bools or function pointers,
	// so user value is always explicit (false/nil are valid overrides)
	return user
}

// defaultConsumerOptions returns ConsumeOptions with defaults applied.
func defaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		EndpointOptions:       defaultEndpointOptions(),
		AutoAck:               false,
		PrefetchCount:         defaultPrefetchCount,
		NoWait:                false,
		Exclusive:             false,
		MaxConcurrentHandlers: defaultConcurrentHandlers,
		OnCancel:              nil,
		OnError:               nil,
	}
}

// mergeConsumerOptions merges user-supplied options with defaults.
func mergeConsumerOptions(user, defaults ConsumerOptions) ConsumerOptions {
	user.EndpointOptions = mergeEndpointOptions(user.EndpointOptions, defaults.EndpointOptions)
	if user.PrefetchCount == 0 {
		user.PrefetchCount = defaults.PrefetchCount
	}
	if user.MaxConcurrentHandlers == 0 {
		user.MaxConcurrentHandlers = defaults.MaxConcurrentHandlers
	}
	// other fields are bools or function pointers,
	// so user value is always explicit (false/nil are valid overrides)
	return user
}
