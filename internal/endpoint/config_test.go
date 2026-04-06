package endpoint

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultsConstants(t *testing.T) {
	// these tests serve as documentation; if they fail an intentional change is needed
	// and all references to the constant should be reviewed
	t.Run("DefaultReconnectMin", func(t *testing.T) {
		assert.Equal(t, 500*time.Millisecond, DefaultReconnectMin)
	})

	t.Run("DefaultReconnectMax", func(t *testing.T) {
		assert.Equal(t, 30*time.Second, DefaultReconnectMax)
	})

	t.Run("DefaultReadyTimeout", func(t *testing.T) {
		assert.Equal(t, 10*time.Second, DefaultReadyTimeout)
	})

	t.Run("DefaultConfirmTimeout", func(t *testing.T) {
		assert.Equal(t, 5*time.Second, DefaultConfirmTimeout)
	})

	t.Run("DefaultPrefetchCount", func(t *testing.T) {
		assert.Equal(t, 1, DefaultPrefetchCount)
	})

	t.Run("DefaultConcurrentHandlers", func(t *testing.T) {
		assert.Equal(t, 0, DefaultConcurrentHandlers)
	})
}

func TestValidateTimeBounds(t *testing.T) {
	t.Run("ValidBounds", func(t *testing.T) {
		err := validateTimeBounds(1*time.Second, 10*time.Second)
		assert.NoError(t, err)
	})

	t.Run("MinIsZero", func(t *testing.T) {
		err := validateTimeBounds(0, 10*time.Second)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "min must be greater than zero")
	})

	t.Run("MinIsNegative", func(t *testing.T) {
		err := validateTimeBounds(-1*time.Second, 10*time.Second)
		assert.Error(t, err)
	})

	t.Run("MaxEqualsMin", func(t *testing.T) {
		err := validateTimeBounds(5*time.Second, 5*time.Second)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max must be greater than min")
	})

	t.Run("MaxLessThanMin", func(t *testing.T) {
		err := validateTimeBounds(10*time.Second, 5*time.Second)
		assert.Error(t, err)
	})
}

func TestMergeEndpointOptions(t *testing.T) {
	defaults := DefaultEndpointOptions()

	t.Run("MergesZeroValues", func(t *testing.T) {
		merged := MergeEndpointOptions(EndpointOptions{}, defaults)
		assert.Equal(t, defaults.ReconnectMin, merged.ReconnectMin)
		assert.Equal(t, defaults.ReconnectMax, merged.ReconnectMax)
		assert.Equal(t, defaults.ReadyTimeout, merged.ReadyTimeout)
	})

	t.Run("KeepsOverrideValues", func(t *testing.T) {
		overrides := EndpointOptions{
			ReconnectMin:    1 * time.Second,
			ReconnectMax:    5 * time.Second,
			ReadyTimeout:    7 * time.Second,
			NoAutoDeclare:   true,
			NoAutoReconnect: true,
			NoWaitReady:     true,
		}
		merged := MergeEndpointOptions(overrides, defaults)
		assert.Equal(t, 1*time.Second, merged.ReconnectMin)
		assert.Equal(t, 5*time.Second, merged.ReconnectMax)
		assert.Equal(t, 7*time.Second, merged.ReadyTimeout)
		assert.True(t, merged.NoAutoDeclare)
		assert.True(t, merged.NoAutoReconnect)
		assert.True(t, merged.NoWaitReady)
	})
}

func TestValidateEndpointOptions(t *testing.T) {
	t.Run("ValidOptions", func(t *testing.T) {
		opts := EndpointOptions{
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 10 * time.Second,
		}
		assert.NoError(t, ValidateEndpointOptions(opts))
	})

	t.Run("InvalidReconnectMinZero", func(t *testing.T) {
		opts := EndpointOptions{ReconnectMin: 0, ReconnectMax: 10 * time.Second}
		err := ValidateEndpointOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reconnect")
	})

	t.Run("InvalidReconnectBoundsMaxLessThanMin", func(t *testing.T) {
		opts := EndpointOptions{ReconnectMin: 10 * time.Second, ReconnectMax: 5 * time.Second}
		assert.Error(t, ValidateEndpointOptions(opts))
	})

	t.Run("ZeroReadyTimeoutIsValid", func(t *testing.T) {
		opts := EndpointOptions{ReconnectMin: 1 * time.Second, ReconnectMax: 10 * time.Second, ReadyTimeout: 0}
		assert.NoError(t, ValidateEndpointOptions(opts))
	})
}

func TestMergePublisherOptions(t *testing.T) {
	defaults := DefaultPublisherOptions()

	t.Run("MergesZeroValues", func(t *testing.T) {
		merged := MergePublisherOptions(PublisherOptions{}, defaults)
		assert.Equal(t, defaults.EndpointOptions, merged.EndpointOptions)
		assert.Equal(t, defaults.ConfirmTimeout, merged.ConfirmTimeout)
	})

	t.Run("KeepsOverrideValues", func(t *testing.T) {
		overrides := PublisherOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: 1 * time.Second, ReconnectMax: 5 * time.Second},
			ConfirmTimeout:  9 * time.Second,
			ConfirmMode:     true,
			Mandatory:       true,
			Immediate:       true,
		}
		merged := MergePublisherOptions(overrides, defaults)
		assert.Equal(t, 9*time.Second, merged.ConfirmTimeout)
		assert.True(t, merged.ConfirmMode)
		assert.True(t, merged.Mandatory)
		assert.True(t, merged.Immediate)
	})
}

func TestValidatePublisherOptions(t *testing.T) {
	t.Run("ValidOptions", func(t *testing.T) {
		opts := PublisherOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: 1 * time.Second, ReconnectMax: 10 * time.Second},
		}
		assert.NoError(t, ValidatePublisherOptions(opts))
	})

	t.Run("InvalidEndpointOptions", func(t *testing.T) {
		opts := PublisherOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: 0, ReconnectMax: 10 * time.Second},
		}
		assert.Error(t, ValidatePublisherOptions(opts))
	})
}

func TestMergeConsumerOptions(t *testing.T) {
	defaults := DefaultConsumerOptions()

	t.Run("MergesZeroValues", func(t *testing.T) {
		merged := MergeConsumerOptions(ConsumerOptions{}, defaults)
		assert.Equal(t, defaults.EndpointOptions, merged.EndpointOptions)
		assert.Equal(t, defaults.PrefetchCount, merged.PrefetchCount)
		assert.Equal(t, defaults.MaxConcurrentHandlers, merged.MaxConcurrentHandlers)
	})

	t.Run("KeepsOverrideValues", func(t *testing.T) {
		overrides := ConsumerOptions{
			PrefetchCount:         5,
			MaxConcurrentHandlers: 3,
			AutoAck:               true,
			Exclusive:             true,
			NoWait:                true,
		}
		merged := MergeConsumerOptions(overrides, defaults)
		assert.Equal(t, 5, merged.PrefetchCount)
		assert.Equal(t, 3, merged.MaxConcurrentHandlers)
		assert.True(t, merged.AutoAck)
		assert.True(t, merged.Exclusive)
		assert.True(t, merged.NoWait)
	})

	t.Run("KeepsUnlimitedConcurrency", func(t *testing.T) {
		// -1 (explicit unlimited) must survive merging and not be treated as zero-value
		overrides := ConsumerOptions{MaxConcurrentHandlers: -1}
		merged := MergeConsumerOptions(overrides, defaults)
		assert.Equal(t, -1, merged.MaxConcurrentHandlers)
	})
}

func TestValidateConsumerOptions(t *testing.T) {
	t.Run("ValidOptions", func(t *testing.T) {
		opts := ConsumerOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: 1 * time.Second, ReconnectMax: 10 * time.Second},
		}
		assert.NoError(t, ValidateConsumerOptions(opts))
	})

	t.Run("InvalidEndpointOptions", func(t *testing.T) {
		opts := ConsumerOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: 0, ReconnectMax: 10 * time.Second},
		}
		assert.Error(t, ValidateConsumerOptions(opts))
	})
}
