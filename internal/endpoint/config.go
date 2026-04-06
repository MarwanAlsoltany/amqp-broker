package endpoint

import (
	"fmt"
	"time"
)

const (
	// DefaultReconnectMin is the minimum delay between reconnection attempts.
	DefaultReconnectMin = 500 * time.Millisecond
	// DefaultReconnectMax is the maximum delay between reconnection attempts.
	DefaultReconnectMax = 30 * time.Second
	// DefaultReadyTimeout is the maximum wait time for an endpoint to become ready.
	DefaultReadyTimeout = 10 * time.Second
	// DefaultConfirmTimeout is the maximum wait time for publisher confirmations.
	DefaultConfirmTimeout = 5 * time.Second
	// DefaultPrefetchCount is the default QoS prefetch count for consumers.
	DefaultPrefetchCount = 1
	// DefaultConcurrentHandlers is the default MaxConcurrentHandlers for consumers (0 = capped by PrefetchCount).
	DefaultConcurrentHandlers = 0
)

// validateTimeBounds validates that min > 0 and max > min.
func validateTimeBounds(min, max time.Duration) error {
	if min <= 0 {
		return fmt.Errorf("min must be greater than zero")
	}
	if max <= min {
		return fmt.Errorf("max must be greater than min (%s)", min)
	}
	return nil
}

func DefaultEndpointOptions() EndpointOptions {
	return EndpointOptions{
		NoAutoDeclare:   false,
		NoAutoReconnect: false,
		ReconnectMin:    DefaultReconnectMin,
		ReconnectMax:    DefaultReconnectMax,
		NoWaitReady:     false,
		ReadyTimeout:    DefaultReadyTimeout,
	}
}

func MergeEndpointOptions(user, defaults EndpointOptions) EndpointOptions {
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

func ValidateEndpointOptions(opts EndpointOptions) error {
	if err := validateTimeBounds(opts.ReconnectMin, opts.ReconnectMax); err != nil {
		return fmt.Errorf("reconnect %w", err)
	}
	return nil
}

func DefaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		EndpointOptions: DefaultEndpointOptions(),
		ConfirmMode:     false,
		ConfirmTimeout:  DefaultConfirmTimeout,
		Mandatory:       false,
		Immediate:       false,
	}
}

func MergePublisherOptions(user, defaults PublisherOptions) PublisherOptions {
	user.EndpointOptions = MergeEndpointOptions(user.EndpointOptions, defaults.EndpointOptions)
	if user.ConfirmTimeout == 0 {
		user.ConfirmTimeout = defaults.ConfirmTimeout
	}
	// for other fields user value is always explicit ...
	return user
}

func ValidatePublisherOptions(opts PublisherOptions) error {
	if err := ValidateEndpointOptions(opts.EndpointOptions); err != nil {
		return err
	}
	return nil
}

func DefaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		EndpointOptions:       DefaultEndpointOptions(),
		AutoAck:               false,
		PrefetchCount:         DefaultPrefetchCount,
		NoWait:                false,
		Exclusive:             false,
		MaxConcurrentHandlers: DefaultConcurrentHandlers,
	}
}

func MergeConsumerOptions(user, defaults ConsumerOptions) ConsumerOptions {
	user.EndpointOptions = MergeEndpointOptions(user.EndpointOptions, defaults.EndpointOptions)
	if user.PrefetchCount == 0 {
		user.PrefetchCount = defaults.PrefetchCount
	}
	if user.MaxConcurrentHandlers == 0 {
		user.MaxConcurrentHandlers = defaults.MaxConcurrentHandlers
	}
	// for other fields user value is always explicit ...
	return user
}

func ValidateConsumerOptions(opts ConsumerOptions) error {
	if err := ValidateEndpointOptions(opts.EndpointOptions); err != nil {
		return err
	}
	return nil
}
