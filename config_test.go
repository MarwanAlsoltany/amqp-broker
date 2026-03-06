package broker

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultsConstants(t *testing.T) {
	// these tests serve as documentation and ensure constants don't accidentally change,
	// if any of these tests fail, it indicates an intentional change is needed, therefore
	// all references to these constants in the codebase should be reviewed.

	t.Run("DefaultBrokerID", func(t *testing.T) {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		pid := os.Getpid()

		assert.Contains(t, fmt.Sprintf("%s-%d", hostname, pid), defaultBrokerID)
	})

	t.Run("DefaultBrokerURL", func(t *testing.T) {
		assert.Equal(t, "amqp://guest:guest@localhost:5672/", defaultBrokerURL)
	})

	t.Run("DefaultConnectionPoolSize", func(t *testing.T) {
		assert.Equal(t, 1, defaultConnectionPoolSize)
	})

	t.Run("DefaultCacheTTL", func(t *testing.T) {
		assert.Equal(t, 5*time.Minute, defaultCacheTTL)
	})

	t.Run("DefaultReconnectMin", func(t *testing.T) {
		assert.Equal(t, 500*time.Millisecond, defaultReconnectMin)
	})

	t.Run("DefaultReconnectMax", func(t *testing.T) {
		assert.Equal(t, 30*time.Second, defaultReconnectMax)
	})

	t.Run("DefaultReadyTimeout", func(t *testing.T) {
		assert.Equal(t, 10*time.Second, defaultReadyTimeout)
	})

	t.Run("DefaultConfirmTimeout", func(t *testing.T) {
		assert.Equal(t, 5*time.Second, defaultConfirmTimeout)
	})

	t.Run("DefaultPrefetchCount", func(t *testing.T) {
		assert.Equal(t, 1, defaultPrefetchCount)
	})

	t.Run("DefaultConcurrentHandlers", func(t *testing.T) {
		assert.Equal(t, 0, defaultConcurrentHandlers)
	})

	t.Run("DefaultExchangeType", func(t *testing.T) {
		assert.Equal(t, "direct", defaultExchangeType)
	})

	t.Run("DefaultExchangeDurable", func(t *testing.T) {
		assert.True(t, defaultExchangeDurable)
	})

	t.Run("DefaultQueueDurable", func(t *testing.T) {
		assert.True(t, defaultQueueDurable)
	})

	t.Run("DefaultDeliveryMode", func(t *testing.T) {
		assert.Equal(t, 2, defaultDeliveryMode)
	})

	t.Run("DefaultContentType", func(t *testing.T) {
		assert.Equal(t, "application/octet-stream", defaultContentType)
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
		assert.Contains(t, err.Error(), "min must be greater than zero")
	})

	t.Run("MaxEqualsMin", func(t *testing.T) {
		err := validateTimeBounds(5*time.Second, 5*time.Second)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max must be greater than min")
	})

	t.Run("MaxLessThanMin", func(t *testing.T) {
		err := validateTimeBounds(10*time.Second, 5*time.Second)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max must be greater than min")
	})
}

func TestMergeConnectionManagerOptions(t *testing.T) {
	defaults := defaultConnectionManagerOptions()

	t.Run("MergesZeroValues", func(t *testing.T) {
		overrides := ConnectionManagerOptions{}
		merged := mergeConnectionManagerOptions(overrides, defaults)
		assert.Equal(t, defaults.Size, merged.Size)
		assert.Equal(t, defaults.ReconnectMin, merged.ReconnectMin)
		assert.Equal(t, defaults.ReconnectMax, merged.ReconnectMax)
	})

	t.Run("KeepsOverrideValues", func(t *testing.T) {
		overrides := ConnectionManagerOptions{
			Size:         3,
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 5 * time.Second,
		}
		merged := mergeConnectionManagerOptions(overrides, defaults)
		assert.Equal(t, 3, merged.Size)
		assert.Equal(t, 1*time.Second, merged.ReconnectMin)
		assert.Equal(t, 5*time.Second, merged.ReconnectMax)
	})
}

func TestValidateConnectionManagerOptions(t *testing.T) {
	t.Run("ValidOptions", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         2,
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 10 * time.Second,
		}
		err := validateConnectionManagerOptions(opts)
		assert.NoError(t, err)
	})

	t.Run("ZeroPoolSize", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         0,
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 10 * time.Second,
		}
		err := validateConnectionManagerOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pool size must be positive")
	})

	t.Run("NegativePoolSize", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         -1,
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 10 * time.Second,
		}
		err := validateConnectionManagerOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pool size must be positive")
	})

	t.Run("InvalidReconnectMinZero", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         2,
			ReconnectMin: 0,
			ReconnectMax: 10 * time.Second,
		}
		err := validateConnectionManagerOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reconnect")
	})

	t.Run("InvalidReconnectBoundsMaxLessThanMin", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         2,
			ReconnectMin: 10 * time.Second,
			ReconnectMax: 5 * time.Second,
		}
		err := validateConnectionManagerOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reconnect")
	})

	t.Run("InvalidReconnectBoundsMaxEqualsMin", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         2,
			ReconnectMin: 5 * time.Second,
			ReconnectMax: 5 * time.Second,
		}
		err := validateConnectionManagerOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reconnect")
	})
}

func TestMergeEndpointOptions(t *testing.T) {
	defaults := defaultEndpointOptions()

	t.Run("MergesZeroValues", func(t *testing.T) {
		overrides := EndpointOptions{}
		merged := mergeEndpointOptions(overrides, defaults)
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
		merged := mergeEndpointOptions(overrides, defaults)
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
			ReadyTimeout: 5 * time.Second,
		}
		err := validateEndpointOptions(opts)
		assert.NoError(t, err)
	})

	t.Run("InvalidReconnectMinZero", func(t *testing.T) {
		opts := EndpointOptions{
			ReconnectMin: 0,
			ReconnectMax: 10 * time.Second,
		}
		err := validateEndpointOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reconnect")
	})

	t.Run("InvalidReconnectBoundsMaxLessThanMin", func(t *testing.T) {
		opts := EndpointOptions{
			ReconnectMin: 10 * time.Second,
			ReconnectMax: 5 * time.Second,
		}
		err := validateEndpointOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reconnect")
	})

	t.Run("ZeroReadyTimeoutIsValid", func(t *testing.T) {
		opts := EndpointOptions{
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 10 * time.Second,
			ReadyTimeout: 0, // valid, means no timeout
		}
		err := validateEndpointOptions(opts)
		assert.NoError(t, err)
	})
}

func TestMergePublisherOptions(t *testing.T) {
	defaults := defaultPublisherOptions()

	t.Run("MergesZeroValues", func(t *testing.T) {
		overrides := PublisherOptions{}
		merged := mergePublisherOptions(overrides, defaults)
		assert.Equal(t, defaults.EndpointOptions, merged.EndpointOptions)
		assert.Equal(t, defaults.ConfirmTimeout, merged.ConfirmTimeout)
	})

	t.Run("KeepsOverrideValues", func(t *testing.T) {
		overrides := PublisherOptions{
			EndpointOptions: EndpointOptions{ReconnectMin: 1 * time.Second},
			ConfirmTimeout:  9 * time.Second,
			ConfirmMode:     true,
			Mandatory:       true,
			Immediate:       true,
		}
		merged := mergePublisherOptions(overrides, defaults)
		assert.Equal(t, 1*time.Second, merged.EndpointOptions.ReconnectMin)
		assert.Equal(t, 9*time.Second, merged.ConfirmTimeout)
		assert.True(t, merged.ConfirmMode)
		assert.True(t, merged.Mandatory)
		assert.True(t, merged.Immediate)
	})
}

func TestValidatePublisherOptions(t *testing.T) {
	t.Run("ValidOptions", func(t *testing.T) {
		opts := PublisherOptions{
			EndpointOptions: EndpointOptions{
				ReconnectMin: 1 * time.Second,
				ReconnectMax: 10 * time.Second,
			},
			ConfirmTimeout: 5 * time.Second,
		}
		err := validatePublisherOptions(opts)
		assert.NoError(t, err)
	})

	t.Run("InvalidEndpointOptions", func(t *testing.T) {
		opts := PublisherOptions{
			EndpointOptions: EndpointOptions{
				ReconnectMin: 0,
				ReconnectMax: 10 * time.Second,
			},
		}
		err := validatePublisherOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reconnect")
	})

	t.Run("ZeroConfirmTimeoutIsValid", func(t *testing.T) {
		opts := PublisherOptions{
			EndpointOptions: EndpointOptions{
				ReconnectMin: 1 * time.Second,
				ReconnectMax: 10 * time.Second,
			},
			ConfirmTimeout: 0, // valid, user can set explicit zero
		}
		err := validatePublisherOptions(opts)
		assert.NoError(t, err)
	})
}

func TestMergeConsumerOptions(t *testing.T) {
	defaults := defaultConsumerOptions()

	t.Run("MergesZeroValues", func(t *testing.T) {
		overrides := ConsumerOptions{}
		merged := mergeConsumerOptions(overrides, defaults)
		assert.Equal(t, defaults.EndpointOptions, merged.EndpointOptions)
		assert.Equal(t, defaults.PrefetchCount, merged.PrefetchCount)
		assert.Equal(t, defaults.MaxConcurrentHandlers, merged.MaxConcurrentHandlers)
	})

	t.Run("KeepsOverrideValues", func(t *testing.T) {
		overrides := ConsumerOptions{
			EndpointOptions:       EndpointOptions{ReconnectMax: 7 * time.Second},
			PrefetchCount:         3,
			MaxConcurrentHandlers: 5,
			AutoAck:               true,
			NoWait:                true,
			Exclusive:             true,
		}
		merged := mergeConsumerOptions(overrides, defaults)
		assert.Equal(t, 7*time.Second, merged.EndpointOptions.ReconnectMax)
		assert.Equal(t, 3, merged.PrefetchCount)
		assert.Equal(t, 5, merged.MaxConcurrentHandlers)
		assert.True(t, merged.AutoAck)
		assert.True(t, merged.NoWait)
		assert.True(t, merged.Exclusive)
	})
}

func TestValidateConsumerOptions(t *testing.T) {
	t.Run("ValidOptions", func(t *testing.T) {
		opts := ConsumerOptions{
			EndpointOptions: EndpointOptions{
				ReconnectMin: 1 * time.Second,
				ReconnectMax: 10 * time.Second,
			},
			PrefetchCount:         10,
			MaxConcurrentHandlers: 5,
		}
		err := validateConsumerOptions(opts)
		assert.NoError(t, err)
	})

	t.Run("InvalidEndpointOptions", func(t *testing.T) {
		opts := ConsumerOptions{
			EndpointOptions: EndpointOptions{
				ReconnectMin: 10 * time.Second,
				ReconnectMax: 5 * time.Second,
			},
		}
		err := validateConsumerOptions(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reconnect")
	})

	t.Run("ZeroPrefetchCountIsValid", func(t *testing.T) {
		opts := ConsumerOptions{
			EndpointOptions: EndpointOptions{
				ReconnectMin: 1 * time.Second,
				ReconnectMax: 10 * time.Second,
			},
			PrefetchCount: 0, // valid, user can set explicit zero
		}
		err := validateConsumerOptions(opts)
		assert.NoError(t, err)
	})

	t.Run("ZeroMaxConcurrentHandlersIsValid", func(t *testing.T) {
		opts := ConsumerOptions{
			EndpointOptions: EndpointOptions{
				ReconnectMin: 1 * time.Second,
				ReconnectMax: 10 * time.Second,
			},
			MaxConcurrentHandlers: 0, // valid, means unlimited
		}
		err := validateConsumerOptions(opts)
		assert.NoError(t, err)
	})
}
