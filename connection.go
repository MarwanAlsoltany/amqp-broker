package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler types for connection events
type (
	// ConnectionOpenHandler is called when a connection is established or re-established.
	// Parameters: idx (connection pool index)
	ConnectionOpenHandler func(idx int)

	// ConnectionCloseHandler is called when a connection closes.
	// Parameters: idx (connection pool index), code (AMQP error code), reason (error description),
	// server (true if initiated by server), recover (true if recoverable)
	ConnectionCloseHandler func(idx int, code int, reason string, server bool, recover bool)

	// ConnectionBlockHandler is called when connection flow control changes.
	// Parameters: idx (connection pool index), active (true=blocked, false=unblocked),
	// reason (blocking reason, only set when active=true)
	ConnectionBlockHandler func(idx int, active bool, reason string)
)

// newConnection establishes a new AMQP connection using the provided config.
func newConnection(url string, config *Config) (*Connection, error) {
	var conn *Connection
	var err error

	if config == nil {
		conn, err = amqp.Dial(url)
	} else {
		conn, err = amqp.DialConfig(url, *config)
	}

	if err != nil {
		return nil, wrapError("dial connection", err)
	}
	return conn, nil
}

// connectionManager manages a fixed set of long-lived AMQP connections.
// Connections are assigned to endpoints based on their role and held for their lifetime.
type connectionManager struct {
	url  string
	size int
	cfg  *Config

	pool []*Connection
	mu   sync.RWMutex

	// round-robin counters for assignment within role groups
	publishersCount atomic.Uint32
	consumersCount  atomic.Uint32

	// cancellation for monitoring goroutines
	ctx    context.Context
	cancel context.CancelFunc
	// closed state
	closed atomic.Bool

	// event handlers
	openHandler  ConnectionOpenHandler
	closeHandler ConnectionCloseHandler
	blockHandler ConnectionBlockHandler
	handlersMu   sync.RWMutex
}

// newConnectionManager creates a new connection manager with the specified size.
// Size determines how many long-lived connections are maintained.
func newConnectionManager(url string, cfg *Config, size int) *connectionManager {
	if size <= 0 {
		size = defaultConnectionPoolSize
	}
	return &connectionManager{
		url:  url,
		cfg:  cfg,
		size: size,
		pool: make([]*Connection, size),
	}
}

// onOpen sets the callback for connection open events.
// Handlers added later are chained after earlier ones.
// An added handler cannot be removed.
func (cm *connectionManager) onOpen(handler ConnectionOpenHandler) {
	cm.handlersMu.Lock()
	oldHandler := cm.openHandler
	cm.openHandler = func(idx int) {
		if oldHandler != nil {
			oldHandler(idx)
		}
		handler(idx)
	}
	cm.handlersMu.Unlock()
}

// onClose sets the callback for connection close events.
// Handlers added later are chained after earlier ones.
// An added handler cannot be removed.
func (cm *connectionManager) onClose(handler ConnectionCloseHandler) {
	cm.handlersMu.Lock()
	oldHandler := cm.closeHandler
	cm.closeHandler = func(idx int, code int, reason string, server bool, recover bool) {
		if oldHandler != nil {
			oldHandler(idx, code, reason, server, recover)
		}
		handler(idx, code, reason, server, recover)
	}
	cm.handlersMu.Unlock()
}

// onBlock sets the callback for connection blocked/unblocked events.
// Handlers added later are chained after earlier ones.
// An added handler cannot be removed.
func (cm *connectionManager) onBlock(handler ConnectionBlockHandler) {
	cm.handlersMu.Lock()
	oldHandler := cm.blockHandler
	cm.blockHandler = func(idx int, active bool, reason string) {
		if oldHandler != nil {
			oldHandler(idx, active, reason)
		}
		handler(idx, active, reason)
	}
	cm.handlersMu.Unlock()
}

// init creates all managed connections.
func (cm *connectionManager) init(ctx context.Context) error {
	// check if context is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// set up context for monitoring goroutines
	cm.ctx, cm.cancel = context.WithCancel(ctx)

	for i := 0; i < cm.size; i++ {
		conn, err := newConnection(cm.url, cm.cfg)
		if err != nil {
			// close any connections already opened
			for j := 0; j < i; j++ {
				if cm.pool[j] != nil {
					_ = cm.pool[j].Close()
				}
			}
			return wrapError(fmt.Sprintf("init connection %d", i+1), err)
		}

		cm.pool[i] = conn

		go cm.monitor(conn)
	}

	return nil
}

// replace replaces a failed connection at the given index with a new one.
func (cm *connectionManager) replace(idx int) error {
	if cm.closed.Load() {
		return ErrConnectionManagerClosed
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if idx < 0 || idx >= len(cm.pool) {
		return fmt.Errorf("%w: invalid connection index: %d", ErrConnectionNotAvailable, idx)
	}

	// create new connection
	conn, err := newConnection(cm.url, cm.cfg)
	if err != nil {
		// m.pool[idx] = nil
		return fmt.Errorf("%w: replace connection %d: %w", ErrConnectionManagerReplace, idx, err)
	}

	// close old connection if still open
	if cm.pool[idx] != nil {
		_ = cm.pool[idx].Close()
	}

	// replace with new connection
	cm.pool[idx] = conn

	go cm.monitor(conn)

	return nil
}

// monitor starts watching a connection for close and block events, replacing it when necessary.
//
// This function MUST be called in a goroutine, e.g. `go cm.monitor(conn)`, otherwise it will block the caller.
// It calls the open handler (if registered) and then enters a select loop to handle close and block events.
func (cm *connectionManager) monitor(conn *Connection) {
	idx := cm.index(conn)
	if idx < 0 {
		return // connection not found in pool
	}

	isStale := func(conn *Connection) bool {
		cm.mu.RLock()
		defer cm.mu.RUnlock()
		return conn != cm.pool[idx]
	}

	// call open handler if registered
	cm.handlersMu.RLock()
	openHandler := cm.openHandler
	cm.handlersMu.RUnlock()
	if openHandler != nil {
		go openHandler(idx)
	}

	// create a buffered channel to avoid deadlock
	// library sends notification once, then closes the channel
	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	blockCh := conn.NotifyBlocked(make(chan amqp.Blocking, 1))

	for {
		select {
		case <-cm.ctx.Done():
			return
		case err, ok := <-closeCh:
			if !ok {
				return
			}

			// ignore: stale event for a replaced connection
			if isStale(conn) {
				return
			}

			// call close handler if registered
			if err != nil {
				cm.handlersMu.RLock()
				handler := cm.closeHandler
				cm.handlersMu.RUnlock()
				if handler != nil {
					go handler(idx, err.Code, err.Reason, err.Server, err.Recover) // prevent blocking
				}
			}

			// connection closed, attempt to replace it if manager is still open
			// replace even if err is nil (graceful close) to maintain pool size
			if !cm.closed.Load() {
				_ = cm.replace(idx)
			}

			return
		case block, ok := <-blockCh:
			// due to the nature of how blocking happens in RabbitMQ,
			// there is no way to test this code path reliably in tests
			if !ok {
				return
			}

			// ignore: stale event for a blocked connection
			if isStale(conn) {
				return
			}

			cm.handlersMu.RLock()
			handler := cm.blockHandler
			cm.handlersMu.RUnlock()
			if handler != nil {
				go handler(idx, block.Active, block.Reason) // prevent blocking
			}
		}
	}
}

// index returns the index of a connection in the pool, or -1 if not found.
func (cm *connectionManager) index(conn *Connection) int {
	if conn == nil {
		return -1
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for i, c := range cm.pool {
		if c == conn {
			return i
		}
	}
	return -1
}

// assign returns a connection for the given role.
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
//   - N=4: controller=0, publisher=1, consumer=2, round robin=3
//   - N=8: controller=0, publisher=1, consumer=2, round robin=3-7
//   - N=10: controller=0, publishers=1-2, consumers=3-4, round robin=5-9
//   - N=16: controller=0, publishers=1-3, consumers=4-6, round robin=7-15
//
// This strategy ensures isolation for controller, dedicated connections for
// publishers and consumers, and efficient utilization of extra connections for high concurrency.
func (cm *connectionManager) assign(role endpointRole) (*Connection, error) {
	if cm.closed.Load() {
		return nil, ErrConnectionManagerClosed
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var idx int
	var size = len(cm.pool)

	switch size {
	case 1:
		// all roles share the same connection
		idx = 0
	case 2:
		// control uses connection 0, publishers and consumers use connection 1
		if role == roleController {
			idx = 0
		} else {
			idx = 1
		}
	case 3:
		// control uses connection 0, publishers use connection 1, consumers use connection 2
		switch role {
		case roleController:
			idx = 0
		case rolePublisher:
			idx = 1
		case roleConsumer:
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
		case roleController:
			idx = 0
		case rolePublisher:
			if publishersCount > 0 && cm.publishersCount.Load() < uint32(publishersCount) {
				// dedicated publisher connections
				counter := cm.publishersCount.Add(1) - 1
				idx = startPublisher + int(counter%uint32(publishersCount))
			} else {
				// round-robin over remaining
				counter := cm.publishersCount.Add(1) - 1
				idx = startRoundRobin + int(counter%uint32(size-startRoundRobin))
			}
		case roleConsumer:
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
		return nil, ErrConnectionNotAvailable
	}

	// check if connection is closed
	if conn.IsClosed() {
		return nil, ErrConnectionClosed
	}

	return conn, nil
}

// close closes all managed connections.
func (cm *connectionManager) Close() error {
	if !cm.closed.CompareAndSwap(false, true) {
		return nil
	}

	// cancel monitoring goroutines
	if cm.cancel != nil {
		cm.cancel()
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	var errs []error
	for i, conn := range cm.pool {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close connection %d: %w", i, err))
			}
			cm.pool[i] = nil
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrConnectionManagerClose, errors.Join(errs...))
	}

	return nil
}

// doSafeChannelActionWithReturn executes a channel operation while monitoring for channel closure.
// It registers a close notification handler and executes the provided operation function.
// If the channel closes during the operation, it returns both the operation error (if any)
// and a wrapped error indicating the channel was closed.
//
// This function is useful for topology operations that might trigger channel closure
// due to AMQP protocol errors (e.g., PreconditionFailed, AccessRefused, NotFound).
//
// Usage examples:
//
//	err := doSafeChannelAction(ch, func(ch *Channel) error {
//	    return ch.ExchangeDeclare(...)
//	})
func doSafeChannelAction(ch *Channel, op func(*Channel) error) error {
	_, err := doSafeChannelActionWithReturn(ch, func(ch *Channel) (struct{}, error) {
		return struct{}{}, op(ch)
	})
	return err
}

// doSafeChannelActionWithReturn is similar to [doSafeChannelAction] but supports operations that return a value. It is generic and can handle operations that return any type along with an error.
//
// Usage examples:
//
//	count, err := doSafeChannelActionWithReturn(ch, func(ch *Channel) (int, error) {
//	    return ch.QueueDelete(...)
//	})
//
//	// or for operations that don't return a value:
//	_, err := doSafeChannelActionWithReturn(ch, func(ch *Channel) (struct{}, error) {
//	    return struct{}{}, ch.ExchangeDeclare(...)
//	})
func doSafeChannelActionWithReturn[T any](ch *Channel, op func(*Channel) (T, error)) (T, error) {
	var zero T

	// create a buffered channel to avoid deadlock
	// library sends notification once, then closes the channel
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	// create channel to communicate results
	type result struct {
		value T
		err   error
	}
	resultCh := make(chan result, 1)

	// execute the operation in a goroutine
	go func() {
		val, err := op(ch)
		resultCh <- result{value: val, err: err}
	}()

	// wait for either operation completion or channel closure
	var opResult result
	var chCloseErr *amqp.Error

	select {
	case opResult = <-resultCh:
		// operation completed first, check if there's a close notification
		select {
		case chCloseErr = <-closeCh:
			// channel was closed during or immediately after the operation
		default:
			// channel is still open
		}
	case chCloseErr = <-closeCh:
		// channel closed before operation completed
		// wait briefly for operation to complete
		select {
		case opResult = <-resultCh:
			// operation completed after channel closed
		default:
			// operation didn't complete yet
		}
	}

	// combine operation error and channel close error
	if opResult.err == nil && chCloseErr == nil {
		return opResult.value, nil
	}

	if chCloseErr != nil {
		// return an error that lets callers inspect both errors
		return zero, wrapError("channel closed during operation", opResult.err, chCloseErr)
	}

	// only operation error
	return zero, opResult.err
}
