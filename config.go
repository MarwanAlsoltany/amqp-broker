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

func validateTimeBounds(min, max time.Duration) error {
	if min <= 0 {
		return fmt.Errorf("min must be greater than zero")
	}
	if max <= min {
		return fmt.Errorf("max must be greater than min (%s)", min)
	}
	return nil
}

// defaultConnectionManagerOptions returns the default connection manager configuration.
func defaultConnectionManagerOptions() ConnectionManagerOptions {
	return ConnectionManagerOptions{
		Config:          nil,
		Size:            defaultConnectionPoolSize,
		NoAutoReconnect: false,
		ReconnectMin:    defaultReconnectMin,
		ReconnectMax:    defaultReconnectMax,
		OnOpen:          nil,
		OnClose:         nil,
		OnBlock:         nil,
	}
}

// mergeConnectionManagerOptions merges user options with defaults.
// User-specified values take precedence; zero values are replaced with defaults.
func mergeConnectionManagerOptions(user, defaults ConnectionManagerOptions) ConnectionManagerOptions {
	if user.Size <= 0 {
		user.Size = defaults.Size
	}
	if user.ReconnectMin == 0 {
		user.ReconnectMin = defaults.ReconnectMin
	}
	if user.ReconnectMax == 0 {
		user.ReconnectMax = defaults.ReconnectMax
	}
	// for other fields user value is always explicit ...
	return user
}

// validateConnectionManagerOptions validates connection manager configuration.
func validateConnectionManagerOptions(opts ConnectionManagerOptions) error {
	if opts.Size <= 0 {
		return fmt.Errorf("pool size must be positive, got %d", opts.Size)
	}

	if err := validateTimeBounds(opts.ReconnectMin, opts.ReconnectMax); err != nil {
		return fmt.Errorf("reconnect %w", err)
	}

	return nil
}

// defaultEndpointOptions returns EndpointOptions with defaults applied.
func defaultEndpointOptions() EndpointOptions {
	return EndpointOptions{
		NoAutoDeclare:   false,
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
	// for other fields user value is always explicit ...
	return user
}

// validateEndpointOptions validates endpoint configuration.
func validateEndpointOptions(opts EndpointOptions) error {
	if err := validateTimeBounds(opts.ReconnectMin, opts.ReconnectMax); err != nil {
		return fmt.Errorf("reconnect %w", err)
	}
	// other fields require no validation ...
	return nil
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
	// for other fields user value is always explicit ...
	return user
}

func validatePublisherOptions(opts PublisherOptions) error {
	if err := validateEndpointOptions(opts.EndpointOptions); err != nil {
		return err
	}
	// other fields require no validation ...
	return nil
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
	// for other fields user value is always explicit ...
	return user
}

func validateConsumerOptions(opts ConsumerOptions) error {
	if err := validateEndpointOptions(opts.EndpointOptions); err != nil {
		return err
	}
	// other fields require no validation ...
	return nil
}
