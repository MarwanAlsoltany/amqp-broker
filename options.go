package broker

import (
	"context"
	"time"
)

// Option configures a Broker instance.
type Option func(*Broker)

// WithURL sets the AMQP URL for the Broker.
// Defaults to [defaultAMQPURL] ("amqp://guest:guest@localhost:5672/").
func WithURL(url string) Option {
	return func(b *Broker) {
		b.url = url
	}
}

// WithIdentifier sets a custom identifier for the Broker.
// It is used in managed publisher and consumer IDs.
// Defaults to {hostname}-{processID}.
func WithIdentifier(id string) Option {
	return func(b *Broker) {
		b.id = id
	}
}

// WithContext sets the base context for the Broker.
// The context is used for managing the lifecycle of connections and endpoints.
// Defaults to context.WithCancel(context.Background()), which is canceled on [Broker.Close].
func WithContext(ctx context.Context) Option {
	return func(b *Broker) {
		b.ctx, b.cancel = context.WithCancel(ctx)
	}
}

// WithCache enables pooling of one-off endpoints (e.g. [Broker.Publish] calls).
// Publishers are reused across calls with the same parameters, improving performance.
// TTL (time-to-live) controls how long idle pooled endpoints are kept.
// Defaults to [defaultCacheTTL] (5 minutes). Set to <=0 to disable caching.
func WithCache(ttl time.Duration) Option {
	return func(b *Broker) {
		if ttl < 0 {
			ttl = 0
		}
		b.cacheTTL = ttl
	}
}

// WithEndpointOptions sets the default EndpointOptions for all endpoints (publishers/consumers).
//
// NOTE: Due to Go's inability to distinguish between a zero value and an absent value,
// only non-zero fields in opts are propagated to endpoints. Boolean flags and numeric fields
// that default to zero (false/0) cannot be reliably inherited. For example, if opts has
// NoAutoDeclare=true, an endpoint created via [Broker.NewPublisher] or [Broker.NewConsumer]
// with no explicit options will still have NoAutoDeclare=false, because the zero value of
// the endpoint's own options overwrites the broker default during merging.
//
// Fields reliably inherited: ReconnectMin, ReconnectMax, ReadyTimeout (duration fields,
// only when non-zero in opts).
//
// Fields NOT reliably inherited: NoAutoDeclare, NoWaitReady, NoAutoReconnect, and any other
// boolean, integer, or string field whose meaningful value is false/0/"".
//
// To set boolean defaults, always pass explicit options to [Broker.NewPublisher]/[Broker.NewConsumer].
func WithEndpointOptions(opts EndpointOptions) Option {
	return func(b *Broker) {
		b.endpointOpts = opts
	}
}

// WithConnectionManagerOptions sets the connection pool configuration.
// This controls pool size, dialer and dial config, event handlers, and retry behavior.
func WithConnectionManagerOptions(opts ConnectionManagerOptions) Option {
	return func(b *Broker) {
		b.connectionMgrOpts = opts
	}
}

// WithConnectionDialer sets a custom Dialer for AMQP connection creation.
// Use this option in tests to inject a mock connection without a real RabbitMQ server.
//
// Same as [WithConnectionManagerOptions] with:
//
//	WithConnectionManagerOptions(ConnectionManagerOptions{
//	    Dialer: dialer,
//	})
func WithConnectionDialer(dialer Dialer) Option {
	return func(b *Broker) {
		b.connectionMgrOpts.Dialer = dialer
	}
}

// WithConnectionPoolSize sets the number of managed connections.
//   - Size 1: All operations share one connection
//   - Size 2: Publishers/Control use one, Consumers use another (recommended for most cases)
//   - Size 3+: Dedicated connections for publishers, consumers, and control
//
// Same as [WithConnectionManagerOptions] with:
//
//	WithConnectionManagerOptions(ConnectionManagerOptions{
//	    Size: size,
//	})
func WithConnectionPoolSize(size int) Option {
	return func(b *Broker) {
		b.connectionMgrOpts.Size = size
	}
}

// WithConnectionConfig sets the AMQP connection configuration.
//
// Same as [WithConnectionManagerOptions] with:
//
//	WithConnectionManagerOptions(ConnectionManagerOptions{
//	    Config: &config,
//	})
func WithConnectionConfig(config Config) Option {
	return func(b *Broker) {
		b.connectionMgrOpts.Config = &config
	}
}

// WithConnectionOnOpen registers a callback for connection open events.
//
// Same as [WithConnectionManagerOptions] with:
//
//	WithConnectionManagerOptions(ConnectionManagerOptions{
//	    OnOpen: handler,
//	})
func WithConnectionOnOpen(handler ConnectionOnOpenHandler) Option {
	return func(b *Broker) {
		b.connectionMgrOpts.OnOpen = handler
	}
}

// WithConnectionOnClose registers a callback for connection close events.
//
// Same as [WithConnectionManagerOptions] with:
//
//	WithConnectionManagerOptions(ConnectionManagerOptions{
//	    OnClose: handler,
//	})
func WithConnectionOnClose(handler ConnectionOnCloseHandler) Option {
	return func(b *Broker) {
		b.connectionMgrOpts.OnClose = handler
	}
}

// WithConnectionOnBlocked registers a callback for connection flow control events.
//
// Same as [WithConnectionManagerOptions] with:
//
//	WithConnectionManagerOptions(ConnectionManagerOptions{
//	    OnBlock: handler,
//	})
func WithConnectionOnBlocked(handler ConnectionOnBlockHandler) Option {
	return func(b *Broker) {
		b.connectionMgrOpts.OnBlock = handler
	}
}

// WithConnectionReconnectConfig sets the connection pool reconnection configuration.
// This controls automatic reconnection behavior when connections fail.
// Defaults to auto-reconnect enabled with 500ms min and 30s max backoff.
//
// Same as [WithConnectionManagerOptions] with:
//
//	WithConnectionManagerOptions(ConnectionManagerOptions{
//	    NoAutoReconnect: false,
//	    ReconnectMin:    500 * time.Millisecond,
//	    ReconnectMax:    30 * time.Second,
//	})
func WithConnectionReconnectConfig(noAutoReconnect bool, reconnectMin, reconnectMax time.Duration) Option {
	return func(b *Broker) {
		b.connectionMgrOpts.NoAutoReconnect = noAutoReconnect
		b.connectionMgrOpts.ReconnectMin = reconnectMin
		b.connectionMgrOpts.ReconnectMax = reconnectMax
	}
}
