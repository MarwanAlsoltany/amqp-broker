package transport

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrConnection is the base error for connection operations.
	// All connection-related errors wrap this error.
	ErrConnection = &internal.Error{Op: "connection"}

	// ErrConnectionClosed indicates the connection is closed.
	// This error is returned when operations are attempted on a closed connection.
	ErrConnectionClosed = fmt.Errorf("%w: closed", ErrConnection)

	// ErrConnectionManager is the base error for connection manager operations.
	// All connection manager errors wrap this error.
	ErrConnectionManager = fmt.Errorf("%w: manager", ErrConnection)

	// ErrConnectionManagerClosed indicates the connection manager is closed.
	// This error is returned when operations are attempted on a closed manager.
	ErrConnectionManagerClosed = fmt.Errorf("%w: closed", ErrConnectionManager)
)

// Dialer is a function type for creating AMQP connections.
// It enables dependency injection for testing and custom connection logic
// (e.g. for testing, instrumentation, or alternative AMQP libraries).
type Dialer func(url string, config *Config) (Connection, error)

// DefaultDialer is the default function used to create AMQP connections.
func DefaultDialer(url string, config *Config) (Connection, error) {
	var conn *amqp091.Connection
	var err error

	if config == nil {
		conn, err = amqp091.Dial(url)
	} else {
		conn, err = amqp091.DialConfig(url, *config)
	}

	if err != nil {
		return nil, err
	}

	return &connectionAdapter{Connection: conn}, nil
}

// ConnectionPurpose indicates the intended use of a connection.
type ConnectionPurpose uint8

const (
	// ConnectionPurposeControl indicates the connection is for control operations (topology management).
	ConnectionPurposeControl ConnectionPurpose = iota
	// ConnectionPurposePublish indicates the connection is for publishers.
	ConnectionPurposePublish
	// ConnectionPurposeConsume indicates the connection is for consumers.
	ConnectionPurposeConsume
)

// Handler types for connection events
type (
	// ConnectionOnOpenHandler is called when a connection is (re-)established.
	// Parameters: idx (connection pool index)
	ConnectionOnOpenHandler func(idx int)

	// ConnectionOnCloseHandler is called when a connection closes.
	// Parameters: idx (connection pool index), code (AMQP error code), reason (error description),
	// server (true if initiated by server), recover (true if recoverable)
	ConnectionOnCloseHandler func(idx int, code int, reason string, server bool, recover bool)

	// ConnectionOnBlockHandler is called when connection flow control changes.
	// Parameters: idx (connection pool index), active (true=blocked, false=unblocked),
	// reason (blocking reason, only set when active=true)
	ConnectionOnBlockHandler func(idx int, active bool, reason string)
)

// ConnectionManagerOptions configures the connection manager.
// It includes pool size, reconnection strategy, and lifecycle hooks.
type ConnectionManagerOptions struct {
	// Size determines the number of managed connections in the pool.
	// Default: 1
	Size int

	// Config sets the AMQP connection configuration.
	// Default: nil
	Config *Config

	// Dialer is the function used to establish AMQP connections.
	// This option allows for custom connection logic, such as
	// using a different AMQP library or adding instrumentation.
	// Default: DefaultDialer
	Dialer Dialer

	// OnOpen is called when a connection is established or re-established.
	// Parameters: idx (pool index)
	// Default: none
	OnOpen ConnectionOnOpenHandler

	// OnClose is called when a connection closes unexpectedly.
	// Parameters: idx (pool index), code (AMQP error code), reason (description),
	// server (true if server-initiated), recover (true if recoverable)
	// Default: none
	OnClose ConnectionOnCloseHandler

	// OnBlock is called when RabbitMQ flow control activates/deactivates.
	// Parameters: idx (pool index), active (true=blocked, false=unblocked),
	// reason (only set when active=true)
	// Default: none
	OnBlock ConnectionOnBlockHandler

	// NoAutoReconnect disables automatic reconnection on connection failure.
	// Default: false
	NoAutoReconnect bool

	// ReconnectMin is the minimum delay between reconnection attempts.
	// Only used if NoAutoReconnect is false.
	// Default: [DefaultReconnectMin] (500ms)
	ReconnectMin time.Duration

	// ReconnectMax is the maximum delay between reconnection attempts.
	// Only used if NoAutoReconnect is false.
	// Default: [DefaultReconnectMax] (30s)
	ReconnectMax time.Duration
}

// ConnectionManager manages a fixed set of long-lived AMQP connections.
// Connections are assigned to endpoints based on their role and held for their lifetime.
type ConnectionManager struct {
	url  string
	opts ConnectionManagerOptions

	pool   []Connection
	poolMu sync.RWMutex

	// round-robin counters for assignment within role groups
	publishersCount atomic.Uint32
	consumersCount  atomic.Uint32

	// cancellation for monitoring goroutines
	ctx    context.Context
	cancel context.CancelFunc
	// closed state
	closed atomic.Bool
}

// NewConnectionManager creates a new connection manager with the specified options.
// Options are merged with defaults for any unspecified fields.
func NewConnectionManager(url string, opts *ConnectionManagerOptions) *ConnectionManager {
	cn := &ConnectionManager{url: url}

	if opts != nil {
		cn.opts = *opts
	}

	// merge with defaults to ensure all fields have valid values
	cn.opts = MergeConnectionManagerOptions(cn.opts, DefaultConnectionManagerOptions())

	cn.pool = make([]Connection, cn.opts.Size)

	return cn
}

// Init creates all managed connections.
func (cm *ConnectionManager) Init(ctx context.Context) error {
	// check if context is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := ValidateConnectionManagerOptions(cm.opts); err != nil {
		return fmt.Errorf("%w: %w", ErrConnectionManager, err)
	}

	cm.poolMu.Lock()
	defer cm.poolMu.Unlock()

	// set up context for monitoring goroutines
	cm.ctx, cm.cancel = context.WithCancel(ctx)

	for i := 0; i < len(cm.pool); i++ {
		conn, err := newConnection(cm.url, cm.opts.Config, cm.opts.Dialer)
		if err != nil {
			// close any connections already opened
			for j := 0; j < i; j++ {
				if cm.pool[j] != nil {
					_ = cm.pool[j].Close()
				}
			}
			return fmt.Errorf("%w: init connection %d: %w", ErrConnectionManager, i+1, err)
		}

		cm.pool[i] = conn

		go cm.Monitor(conn)
	}

	return nil
}

// Replace replaces a failed connection at the given index with a new one.
// It retries with exponential backoff until successful or the manager is closed.
func (cm *ConnectionManager) Replace(idx int) error {
	if cm.closed.Load() {
		return ErrConnectionManagerClosed
	}

	cm.poolMu.Lock()
	defer cm.poolMu.Unlock()

	if idx < 0 || idx >= len(cm.pool) {
		return fmt.Errorf("%w: replace connection %d: out of range", ErrConnectionManager, idx)
	}

	// close old connection if still open
	if cm.pool[idx] != nil {
		_ = cm.pool[idx].Close()
		cm.pool[idx] = nil // explicitly clear the slot
	}

	autoReconnect := !cm.opts.NoAutoReconnect
	reconnectMin := cm.opts.ReconnectMin
	reconnectMax := cm.opts.ReconnectMax
	// safeguards for min/max
	if reconnectMin <= 0 {
		reconnectMin = DefaultReconnectMin
	}
	if reconnectMax < reconnectMin {
		reconnectMax = reconnectMin
	}

	// if auto-reconnect is disabled, try once and fail
	if !autoReconnect {
		conn, err := newConnection(cm.url, cm.opts.Config, cm.opts.Dialer)
		if err != nil {
			return fmt.Errorf("%w: replace connection %d: %w", ErrConnectionManager, idx, err)
		}
		cm.pool[idx] = conn
		go cm.Monitor(conn)
		return nil
	}

	attempt := 0
	delay := reconnectMin

	for {
		if cm.closed.Load() {
			return ErrConnectionManagerClosed
		}
		// guard against nil context (e.g., when called before init())
		if cm.ctx != nil && cm.ctx.Err() != nil {
			return fmt.Errorf("%w: replace connection %d: context cancelled: %w", ErrConnectionManager, idx, cm.ctx.Err())
		}

		attempt++
		conn, err := newConnection(cm.url, cm.opts.Config, cm.opts.Dialer)
		if err == nil {
			// success: install new connection and start monitoring
			cm.pool[idx] = conn
			go cm.Monitor(conn)
			return nil
		}

		// failed: exponential backoff with jitter
		backoff := delay + time.Duration(rand.Int64N(int64(delay/4))) // +0-25% jitter

		if cm.ctx != nil {
			select {
			case <-cm.ctx.Done():
				return fmt.Errorf("%w: replace connection %d (attempt %d): context cancelled: %w", ErrConnectionManager, idx, attempt, cm.ctx.Err())
			case <-time.After(backoff):
				delay = min(delay*2, reconnectMax)
			}
		} else {
			// no context available (e.g. replace called before init()), use simple sleep
			time.Sleep(backoff)
			delay = min(delay*2, reconnectMax)
		}
	}
}

// Monitor starts watching a connection for close and block events, replacing it when necessary.
//
// This function MUST be called in a goroutine, e.g. `go cm.Monitor(conn)`, otherwise it will block the caller.
// It calls the open handler (if registered) and then enters a select loop to handle close and block events.
func (cm *ConnectionManager) Monitor(conn Connection) {
	idx := cm.Index(conn)
	if idx < 0 {
		return // connection not found in pool
	}

	isStale := func(conn Connection) bool {
		cm.poolMu.RLock()
		defer cm.poolMu.RUnlock()
		return conn != cm.pool[idx]
	}

	// call open handler if registered
	if cm.opts.OnOpen != nil {
		go cm.opts.OnOpen(idx)
	}

	// create a buffered channel to avoid deadlock
	// library sends notification once, then closes the channel
	closeCh := conn.NotifyClose(make(chan *Error, 1))
	blockCh := conn.NotifyBlocked(make(chan Blocking, 1))

	for {
		select {
		case <-cm.ctx.Done():
			return
		case err, ok := <-closeCh:
			// channel closed without error: graceful shutdown
			if graceful := !ok && err == nil; graceful {
				err = &Error{
					Reason:  "graceful shutdown",
					Recover: true,
				}
			}

			if isStale(conn) {
				return // ignore: stale event for a replaced connection
			}

			if err != nil && cm.opts.OnClose != nil {
				go cm.opts.OnClose(idx, err.Code, err.Reason, err.Server, err.Recover) // prevent blocking
			}

			// connection closed, attempt to replace it if manager is still open
			// replace even if err is nil (graceful close) to maintain pool size
			if !cm.closed.Load() {
				_ = cm.Replace(idx)
			}

			return
		case block := <-blockCh:
			// due to the nature of how blocking happens in RabbitMQ,
			// there is no reliable way to test this code path
			if isStale(conn) {
				return // ignore: stale event for a blocked connection
			}

			if cm.opts.OnBlock != nil {
				go cm.opts.OnBlock(idx, block.Active, block.Reason) // prevent blocking
			}
		}
	}
}

// Index returns the index of a connection in the pool, or -1 if not found.
func (cm *ConnectionManager) Index(conn Connection) int {
	if conn == nil {
		return -1
	}

	cm.poolMu.RLock()
	defer cm.poolMu.RUnlock()

	for i, c := range cm.pool {
		if c == conn {
			return i
		}
	}
	return -1
}

// Assign returns a connection for the given role.
// The connection should be held by the endpoint (publisher/consumer) until it closes or fails.
//
// Assignment strategy:
//   - size 1: All roles (controller, publishers, consumers) share connection 0.
//   - size 2: Controller uses connection 0; publishers and consumers share connection 1.
//   - size 3: Controller uses connection 0; publishers use connection 1; consumers use connection 2.
//   - size 4+:
//   - Controller uses connection 0.
//   - The next N connections are dedicated to publishers (N scales with pool size, e.g., N = max(1, (size-1)/4)).
//   - The following M connections are dedicated to consumers (M scales with pool size, e.g., M = max(1, (size-1)/4)).
//   - Any remaining connections are shared between publishers and consumers using round-robin assignment.
//
// Examples
//   - N=2: controller=0, publisher=1, consumer=1
//   - N=4: controller=0, publisher=1, consumer=2, round-robin=3
//   - N=8: controller=0, publisher=1, consumer=2, round-robin=3-7
//   - N=10: controller=0, publishers=1-2, consumers=3-4, round-robin=5-9
//   - N=16: controller=0, publishers=1-3, consumers=4-6, round-robin=7-15
//
// This strategy ensures isolation for controller, dedicated connections for
// publishers and consumers, and efficient utilization of extra connections for high concurrency.
func (cm *ConnectionManager) Assign(role ConnectionPurpose) (Connection, error) {
	if cm.closed.Load() {
		return nil, ErrConnectionManagerClosed
	}

	cm.poolMu.RLock()
	defer cm.poolMu.RUnlock()

	var idx int
	var size = len(cm.pool)

	switch size {
	case 1:
		// all roles share the same connection
		idx = 0
	case 2:
		// control uses connection 0, publishers and consumers use connection 1
		if role == ConnectionPurposeControl {
			idx = 0
		} else {
			idx = 1
		}
	case 3:
		// control uses connection 0, publishers use connection 1, consumers use connection 2
		switch role {
		case ConnectionPurposeControl:
			idx = 0
		case ConnectionPurposePublish:
			idx = 1
		case ConnectionPurposeConsume:
			idx = 2
		}
	default: // size 4+
		// control uses connection 0, publishers and Consumers distributed across connections 1+
		// partition logic for 4+ connections
		// example: 10 connections: 0=control, 1-3=publishers, 4-6=consumers, 7-9=round-robin
		controllersCount := 1
		publishersCount := max(controllersCount, (size-1)/4)
		consumersCount := max(controllersCount, (size-1)/4)
		startController := controllersCount - 1
		startPublisher := startController + 1
		startConsumer := startPublisher + publishersCount
		startRoundRobin := startConsumer + consumersCount

		switch role {
		case ConnectionPurposeControl:
			idx = 0
		case ConnectionPurposePublish:
			if publishersCount > 0 && cm.publishersCount.Load() < uint32(publishersCount) {
				// dedicated publisher connections
				counter := cm.publishersCount.Add(1) - 1
				idx = startPublisher + int(counter%uint32(publishersCount))
			} else {
				// round-robin over remaining
				counter := cm.publishersCount.Add(1) - 1
				idx = startRoundRobin + int(counter%uint32(size-startRoundRobin))
			}
		case ConnectionPurposeConsume:
			if consumersCount > 0 && cm.consumersCount.Load() < uint32(consumersCount) {
				// dedicated consumer connections
				counter := cm.consumersCount.Add(1) - 1
				idx = startConsumer + int(counter%uint32(consumersCount))
			} else {
				// round-robin over remaining
				counter := cm.consumersCount.Add(1) - 1
				idx = startRoundRobin + int(counter%uint32(size-startRoundRobin))
			}
		}
	}

	conn := cm.pool[idx]
	if conn == nil {
		return nil, fmt.Errorf("%w: assign connection %d: not available", ErrConnectionManager, idx)
	}

	return conn, nil
}

// Close closes all managed connections.
func (cm *ConnectionManager) Close() error {
	if !cm.closed.CompareAndSwap(false, true) {
		return nil
	}

	// cancel monitoring goroutines
	if cm.cancel != nil {
		cm.cancel()
	}

	cm.poolMu.Lock()
	defer cm.poolMu.Unlock()

	var errs []error
	for i, conn := range cm.pool {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close connection %d: %w", i, err))
			}
			cm.pool[i] = nil
		}
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: close failed: %w", ErrConnectionManager, err)
	}

	return nil
}

// newConnection establishes a new AMQP connection using the provided config.
func newConnection(url string, config *Config, dialer Dialer) (Connection, error) {
	if dialer == nil {
		dialer = DefaultDialer // use default dialer if none provided
	}

	conn, err := dialer(url, config)
	if err != nil {
		return nil, fmt.Errorf("%w: dial failed: %w", ErrConnection, err)
	}

	return conn, nil
}
