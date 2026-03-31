package transport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidateConnectionManagerOptions(t *testing.T) {
	t.Run("ValidOptions", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         5,
			ReconnectMin: 100 * time.Millisecond,
			ReconnectMax: 1 * time.Second,
		}
		err := ValidateConnectionManagerOptions(opts)
		assert.NoError(t, err)
	})

	t.Run("InvalidSize", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         0,
			ReconnectMin: 100 * time.Millisecond,
			ReconnectMax: 1 * time.Second,
		}
		err := ValidateConnectionManagerOptions(opts)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "pool size must be positive")
	})

	t.Run("InvalidReconnectTimes", func(t *testing.T) {
		opts := ConnectionManagerOptions{
			Size:         5,
			ReconnectMin: 0,
			ReconnectMax: 1 * time.Second,
		}
		err := ValidateConnectionManagerOptions(opts)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "reconnect")
	})
}

func TestValidateTimeBounds(t *testing.T) {
	t.Run("ValidBounds", func(t *testing.T) {
		err := validateTimeBounds(100*time.Millisecond, 1*time.Second)
		assert.NoError(t, err)
	})

	t.Run("MinZero", func(t *testing.T) {
		err := validateTimeBounds(0, 1*time.Second)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "min must be greater than zero")
	})

	t.Run("MinNegative", func(t *testing.T) {
		err := validateTimeBounds(-1*time.Second, 1*time.Second)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "min must be greater than zero")
	})

	t.Run("MaxLessThanMin", func(t *testing.T) {
		err := validateTimeBounds(2*time.Second, 1*time.Second)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "max must be greater than min")
	})

	t.Run("MaxEqualToMin", func(t *testing.T) {
		err := validateTimeBounds(1*time.Second, 1*time.Second)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "max must be greater than min")
	})
}

func TestDefaultConnectionManagerOptions(t *testing.T) {
	opts := DefaultConnectionManagerOptions()

	assert.Equal(t, DefaultConnectionPoolSize, opts.Size)
	assert.Equal(t, DefaultReconnectMin, opts.ReconnectMin)
	assert.Equal(t, DefaultReconnectMax, opts.ReconnectMax)
	assert.False(t, opts.NoAutoReconnect)
	assert.Nil(t, opts.Config)
	assert.Nil(t, opts.OnOpen)
	assert.Nil(t, opts.OnClose)
	assert.Nil(t, opts.OnBlock)
}

func TestMergeConnectionManagerOptions(t *testing.T) {
	t.Run("MergesZeroValues", func(t *testing.T) {
		user := ConnectionManagerOptions{
			Size:         0, // should use default
			ReconnectMin: 0, // should use default
			ReconnectMax: 0, // should use default
		}
		defaults := DefaultConnectionManagerOptions()

		merged := MergeConnectionManagerOptions(user, defaults)

		assert.Equal(t, defaults.Size, merged.Size)
		assert.Equal(t, defaults.ReconnectMin, merged.ReconnectMin)
		assert.Equal(t, defaults.ReconnectMax, merged.ReconnectMax)
	})

	t.Run("PreservesUserValues", func(t *testing.T) {
		customSize := 10
		customMin := 200 * time.Millisecond
		customMax := 2 * time.Second

		user := ConnectionManagerOptions{
			Size:         customSize,
			ReconnectMin: customMin,
			ReconnectMax: customMax,
		}
		defaults := DefaultConnectionManagerOptions()

		merged := MergeConnectionManagerOptions(user, defaults)

		assert.Equal(t, customSize, merged.Size)
		assert.Equal(t, customMin, merged.ReconnectMin)
		assert.Equal(t, customMax, merged.ReconnectMax)
	})

	t.Run("MergesNegativeSize", func(t *testing.T) {
		user := ConnectionManagerOptions{
			Size: -5, // should use default
		}
		defaults := DefaultConnectionManagerOptions()

		merged := MergeConnectionManagerOptions(user, defaults)

		assert.Equal(t, defaults.Size, merged.Size)
	})
}
