package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBrokerOptions(t *testing.T) {
	t.Run("WithURL", func(t *testing.T) {
		b := &Broker{}

		customURL := "amqp://invalid"
		WithURL(customURL)(b)
		assert.Equal(t, customURL, b.url)
	})

	t.Run("WithIdentifier", func(t *testing.T) {
		b := &Broker{}

		customID := "test-broker-123"
		WithIdentifier(customID)(b)
		assert.Equal(t, customID, b.id)
	})

	t.Run("WithContext", func(t *testing.T) {
		b := &Broker{}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		WithContext(ctx)(b)

		assert.NotNil(t, b.ctx)
		assert.NotNil(t, b.cancel)

		// verify context is derived from parent
		cancel()
		select {
		case <-b.ctx.Done():
			// expected - child context cancelled when parent cancelled
		case <-time.After(100 * time.Millisecond):
			t.Fatal("child context not cancelled when parent cancelled")
		}
	})

	t.Run("WithCache", func(t *testing.T) {
		b := &Broker{}
		// when positive
		WithCache(10 * time.Minute)(b)
		assert.Equal(t, 10*time.Minute, b.cacheTTL)
		// when zero
		WithCache(0)(b)
		assert.Equal(t, time.Duration(0), b.cacheTTL)
		// when negative
		WithCache(-1 * time.Minute)(b)
		assert.Equal(t, time.Duration(0), b.cacheTTL)
	})

	t.Run("WithEndpointOptions", func(t *testing.T) {
		b := &Broker{}

		opts := EndpointOptions{
			NoWaitReady:     true,
			ReadyTimeout:    123 * time.Second,
			NoAutoReconnect: true,
			ReconnectMin:    100 * time.Millisecond,
			ReconnectMax:    10 * time.Second,
		}
		WithEndpointOptions(opts)(b)
		assert.True(t, b.endpointOpts.NoWaitReady)
		assert.Equal(t, 123*time.Second, b.endpointOpts.ReadyTimeout)
		assert.True(t, b.endpointOpts.NoAutoReconnect)
		assert.Equal(t, 100*time.Millisecond, b.endpointOpts.ReconnectMin)
		assert.Equal(t, 10*time.Second, b.endpointOpts.ReconnectMax)
	})

	t.Run("WithConnectionManagerOptions", func(t *testing.T) {
		b := &Broker{}

		t.Run("PoolSize", func(t *testing.T) {
			opts := ConnectionManagerOptions{Size: 3}
			WithConnectionManagerOptions(opts)(b)
			assert.Equal(t, 3, b.connectionMgrOpts.Size)
		})

		t.Run("ConnectionConfig", func(t *testing.T) {
			config := Config{
				Heartbeat: 30 * time.Second,
				Locale:    "en_US",
				Vhost:     "/custom",
			}
			opts := ConnectionManagerOptions{Config: &config}
			WithConnectionManagerOptions(opts)(b)
			require.NotNil(t, b.connectionMgrOpts.Config)
			assert.Equal(t, 30*time.Second, b.connectionMgrOpts.Config.Heartbeat)
			assert.Equal(t, "en_US", b.connectionMgrOpts.Config.Locale)
			assert.Equal(t, "/custom", b.connectionMgrOpts.Config.Vhost)
		})

		t.Run("OnOpen", func(t *testing.T) {
			var capturedIdx int
			handler := func(idx int) {
				capturedIdx = idx
			}
			opts := ConnectionManagerOptions{OnOpen: handler}
			WithConnectionManagerOptions(opts)(b)

			b.connectionMgrOpts.OnOpen(3)
			assert.Equal(t, 3, capturedIdx)
		})

		t.Run("OnClose", func(t *testing.T) {
			var capturedIdx, capturedCode int
			var capturedReason string
			var capturedServer, capturedRecover bool

			handler := func(idx, code int, reason string, server, recover bool) {
				capturedIdx = idx
				capturedCode = code
				capturedReason = reason
				capturedServer = server
				capturedRecover = recover
			}
			opts := ConnectionManagerOptions{OnClose: handler}
			WithConnectionManagerOptions(opts)(b)

			b.connectionMgrOpts.OnClose(2, 404, "NOT_FOUND", true, false)
			assert.Equal(t, 2, capturedIdx)
			assert.Equal(t, 404, capturedCode)
			assert.Equal(t, "NOT_FOUND", capturedReason)
			assert.True(t, capturedServer)
			assert.False(t, capturedRecover)
		})

		t.Run("OnBlocked", func(t *testing.T) {
			var capturedIdx int
			var capturedActive bool
			var capturedReason string

			handler := func(idx int, active bool, reason string) {
				capturedIdx = idx
				capturedActive = active
				capturedReason = reason
			}

			opts := ConnectionManagerOptions{OnBlock: handler}
			WithConnectionManagerOptions(opts)(b)

			b.connectionMgrOpts.OnBlock(1, true, "alarm")
			assert.Equal(t, 1, capturedIdx)
			assert.True(t, capturedActive)
			assert.Equal(t, "alarm", capturedReason)
		})

		t.Run("ReconnectConfig", func(t *testing.T) {
			opts := ConnectionManagerOptions{
				NoAutoReconnect: true,
				ReconnectMin:    1 * time.Second,
				ReconnectMax:    10 * time.Second,
			}
			WithConnectionManagerOptions(opts)(b)
			assert.True(t, b.connectionMgrOpts.NoAutoReconnect)
			assert.Equal(t, 1*time.Second, b.connectionMgrOpts.ReconnectMin)
			assert.Equal(t, 10*time.Second, b.connectionMgrOpts.ReconnectMax)
		})
	})

	t.Run("WithConnectionPoolSize", func(t *testing.T) {
		b := &Broker{}
		WithConnectionPoolSize(3)(b)
		assert.Equal(t, 3, b.connectionMgrOpts.Size)
	})

	t.Run("WithConnectionConfig", func(t *testing.T) {
		b := &Broker{}
		config := Config{
			Heartbeat: 30 * time.Second,
			Locale:    "en_US",
			Vhost:     "/custom",
		}
		WithConnectionConfig(config)(b)
		require.NotNil(t, b.connectionMgrOpts.Config)
		assert.Equal(t, 30*time.Second, b.connectionMgrOpts.Config.Heartbeat)
		assert.Equal(t, "en_US", b.connectionMgrOpts.Config.Locale)
		assert.Equal(t, "/custom", b.connectionMgrOpts.Config.Vhost)
	})

	t.Run("WithConnectionDialer", func(t *testing.T) {
		b := &Broker{}
		assert.Nil(t, b.connectionMgrOpts.Dialer)

		var calledURL string
		dialer := func(url string, config *Config) (Connection, error) {
			calledURL = url
			return nil, nil
		}
		WithConnectionDialer(dialer)(b)

		require.NotNil(t, b.connectionMgrOpts.Dialer)
		_, _ = b.connectionMgrOpts.Dialer("amqp://invalid", nil)
		assert.Equal(t, "amqp://invalid", calledURL)
	})

	t.Run("WithConnectionOnOpen", func(t *testing.T) {
		b := &Broker{}
		var capturedIdx int
		handler := func(idx int) {
			capturedIdx = idx
		}
		WithConnectionOnOpen(handler)(b)

		b.connectionMgrOpts.OnOpen(3)
		assert.Equal(t, 3, capturedIdx)
	})

	t.Run("WithConnectionOnClose", func(t *testing.T) {
		b := &Broker{}
		var capturedIdx, capturedCode int
		var capturedReason string
		var capturedServer, capturedRecover bool

		handler := func(idx, code int, reason string, server, recover bool) {
			capturedIdx = idx
			capturedCode = code
			capturedReason = reason
			capturedServer = server
			capturedRecover = recover
		}
		WithConnectionOnClose(handler)(b)

		b.connectionMgrOpts.OnClose(2, 404, "NOT_FOUND", true, false)
		assert.Equal(t, 2, capturedIdx)
		assert.Equal(t, 404, capturedCode)
		assert.Equal(t, "NOT_FOUND", capturedReason)
		assert.True(t, capturedServer)
		assert.False(t, capturedRecover)
	})

	t.Run("WithConnectionOnBlocked", func(t *testing.T) {
		b := &Broker{}
		var capturedIdx int
		var capturedActive bool
		var capturedReason string

		handler := func(idx int, active bool, reason string) {
			capturedIdx = idx
			capturedActive = active
			capturedReason = reason
		}
		WithConnectionOnBlocked(handler)(b)

		b.connectionMgrOpts.OnBlock(1, true, "alarm")
		assert.Equal(t, 1, capturedIdx)
		assert.True(t, capturedActive)
		assert.Equal(t, "alarm", capturedReason)
	})

	t.Run("WithConnectionReconnectConfig", func(t *testing.T) {
		b := &Broker{}
		WithConnectionReconnectConfig(true, 2*time.Second, 20*time.Second)(b)
		assert.True(t, b.connectionMgrOpts.NoAutoReconnect)
		assert.Equal(t, 2*time.Second, b.connectionMgrOpts.ReconnectMin)
		assert.Equal(t, 20*time.Second, b.connectionMgrOpts.ReconnectMax)
	})

	t.Run("Behavior", func(t *testing.T) {
		t.Run("MultipleOptions", func(t *testing.T) {
			b := &Broker{}

			customURL := "amqp://invalid"
			customID := "multi-test"
			poolSize := 4
			min := 1 * time.Second
			max := 60 * time.Second
			cacheTTL := 15 * time.Minute

			opts := []Option{
				WithURL(customURL),
				WithIdentifier(customID),
				WithConnectionManagerOptions(ConnectionManagerOptions{Size: poolSize}),
				WithEndpointOptions(EndpointOptions{ReconnectMin: min, ReconnectMax: max}),
				WithCache(cacheTTL),
			}

			for _, opt := range opts {
				opt(b)
			}

			assert.Equal(t, customURL, b.url)
			assert.Equal(t, customID, b.id)
			assert.Equal(t, poolSize, b.connectionMgrOpts.Size)
			assert.Equal(t, min, b.endpointOpts.ReconnectMin)
			assert.Equal(t, max, b.endpointOpts.ReconnectMax)
			assert.Equal(t, cacheTTL, b.cacheTTL)
		})

		t.Run("OptionsOrderIndependent", func(t *testing.T) {
			customID := "order-test"
			cacheTTL := 20 * time.Minute

			b1 := &Broker{}
			WithIdentifier(customID)(b1)
			WithCache(cacheTTL)(b1)

			b2 := &Broker{}
			WithCache(cacheTTL)(b2)
			WithIdentifier(customID)(b2)

			assert.Equal(t, b1.id, b2.id)
			assert.Equal(t, b1.cacheTTL, b2.cacheTTL)
		})

		t.Run("LaterOptionsOverride", func(t *testing.T) {
			b := &Broker{}

			WithIdentifier("old")(b)
			WithIdentifier("new")(b)
			assert.Equal(t, "new", b.id)

			WithConnectionManagerOptions(ConnectionManagerOptions{Size: 1})(b)
			WithConnectionManagerOptions(ConnectionManagerOptions{Size: 5})(b)
			assert.Equal(t, 5, b.connectionMgrOpts.Size)
		})
	})
}
