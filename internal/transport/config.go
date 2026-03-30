package transport

import (
	"fmt"
	"time"
)

const (
	DefaultConnectionPoolSize = 1
	DefaultReconnectMin       = 500 * time.Millisecond
	DefaultReconnectMax       = 30 * time.Second
)

// validateTimeBounds validates time duration bounds.
func validateTimeBounds(min, max time.Duration) error {
	if min <= 0 {
		return fmt.Errorf("min must be greater than zero")
	}
	if max <= min {
		return fmt.Errorf("max must be greater than min (%s)", min)
	}
	return nil
}

// DefaultConnectionManagerOptions returns the default connection manager configuration.
func DefaultConnectionManagerOptions() ConnectionManagerOptions {
	return ConnectionManagerOptions{
		Size:            DefaultConnectionPoolSize,
		NoAutoReconnect: false,
		ReconnectMin:    DefaultReconnectMin,
		ReconnectMax:    DefaultReconnectMax,
		OnOpen:          nil,
		OnClose:         nil,
		OnBlock:         nil,
		Config:          nil,
		Dialer:          nil,
	}
}

// MergeConnectionManagerOptions merges user options with defaults.
// User-specified values take precedence; zero values are replaced with defaults.
func MergeConnectionManagerOptions(user, defaults ConnectionManagerOptions) ConnectionManagerOptions {
	if user.Size <= 0 {
		user.Size = defaults.Size
	}
	if user.ReconnectMin == 0 {
		user.ReconnectMin = defaults.ReconnectMin
	}
	if user.ReconnectMax == 0 {
		user.ReconnectMax = defaults.ReconnectMax
	}
	if user.Dialer == nil {
		user.Dialer = defaults.Dialer
	}
	// for other fields user value is always explicit ...
	return user
}

// ValidateConnectionManagerOptions validates connection manager configuration.
func ValidateConnectionManagerOptions(opts ConnectionManagerOptions) error {
	if opts.Size <= 0 {
		return fmt.Errorf("pool size must be positive, got %d", opts.Size)
	}

	if err := validateTimeBounds(opts.ReconnectMin, opts.ReconnectMax); err != nil {
		return fmt.Errorf("reconnect %w", err)
	}

	return nil
}
