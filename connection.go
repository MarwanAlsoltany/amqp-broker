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
	url    string
	size   int
	cfg    *Config
	pool   []*Connection
	connMu sync.RWMutex
	// Round-robin counters for assignment within role groups
	publisherIdx atomic.Uint32
	consumerIdx  atomic.Uint32
	closed       atomic.Bool
	// Cancellation for monitoring goroutines
	ctx    context.Context
	cancel context.CancelFunc
	// Event handlers
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
func (m *connectionManager) onOpen(handler ConnectionOpenHandler) {
	m.handlersMu.Lock()
	oldHandler := m.openHandler
	m.openHandler = func(idx int) {
		if oldHandler != nil {
			oldHandler(idx)
		}
		handler(idx)
	}
	m.openHandler = handler
	m.handlersMu.Unlock()
}

// onClose sets the callback for connection close events.
// Handlers added later are chained after earlier ones.
// An added handler cannot be removed.
func (m *connectionManager) onClose(handler ConnectionCloseHandler) {
	m.handlersMu.Lock()
	oldHandler := m.closeHandler
	m.closeHandler = func(idx int, code int, reason string, server bool, recover bool) {
		if oldHandler != nil {
			oldHandler(idx, code, reason, server, recover)
		}
		handler(idx, code, reason, server, recover)
	}
	m.handlersMu.Unlock()
}

// onBlock sets the callback for connection blocked/unblocked events.
// Handlers added later are chained after earlier ones.
// An added handler cannot be removed.
func (m *connectionManager) onBlock(handler ConnectionBlockHandler) {
	m.handlersMu.Lock()
	oldHandler := m.blockHandler
	m.blockHandler = func(idx int, active bool, reason string) {
		if oldHandler != nil {
			oldHandler(idx, active, reason)
		}
		handler(idx, active, reason)
	}
	m.handlersMu.Unlock()
}

// init creates all managed connections.
func (m *connectionManager) init(ctx context.Context) error {
	m.connMu.Lock()
	defer m.connMu.Unlock()

	// Set up context for monitoring goroutines
	m.ctx, m.cancel = context.WithCancel(ctx)

	for i := 0; i < m.size; i++ {
		conn, err := newConnection(m.url, m.cfg)
		if err != nil {
			// Close any connections we've already opened
			for j := 0; j < i; j++ {
				if m.pool[j] != nil {
					_ = m.pool[j].Close()
				}
			}
			return wrapEntityError("initialize", fmt.Sprintf("connection %d", i+1), err)
		}
		m.pool[i] = conn // Call open handler if registered
		m.handlersMu.RLock()
		openHandler := m.openHandler
		m.handlersMu.RUnlock()
		if openHandler != nil {
			go openHandler(i)
		}

		// Start monitoring this connection
		go m.monitor(conn)
	}

	return nil
}

// assign returns a connection for the given role.
// The connection should be held by the endpoint (publisher/consumer) until it closes or fails.
// Assignment strategy:
//   - size 1: All roles share connection 0.
//   - size 2: Control and Publishers use connection 0, Consumers use connection 1.
//   - size 3: Control uses connection 0, Publishers use connection 1, Consumers use connection 2.
//   - size 4+: Control uses connection 0, Publishers and Consumers are distributed across connections 1+ using round-robin.
func (m *connectionManager) assign(_ context.Context, role endpointRole) (*Connection, error) {
	if m.closed.Load() {
		return nil, ErrConnectionManagerClosed
	}

	m.connMu.RLock()
	defer m.connMu.RUnlock()

	var idx int
	switch m.size {
	case 1:
		// All roles share the same connection
		idx = 0
	case 2:
		// Control and Publishers use connection 0, Consumers use connection 1
		if role == roleConsumer {
			idx = 1
		} else {
			idx = 0
		}
	case 3:
		// Control uses connection 0, Publishers use connection 1, Consumers use connection 2
		switch role {
		case roleControl:
			idx = 0
		case rolePublisher:
			idx = 1
		case roleConsumer:
			idx = 2
		}
	default:
		// size 4+: Control uses connection 0, Publishers and Consumers distributed across connections 1+
		switch role {
		case roleControl:
			idx = 0
		case rolePublisher:
			// Round-robin across connections 1 to size-1
			counter := m.publisherIdx.Add(1) - 1
			idx = int(counter%(uint32(m.size-1))) + 1
		case roleConsumer:
			// Round-robin across connections 1 to size-1
			counter := m.consumerIdx.Add(1) - 1
			idx = int(counter%(uint32(m.size-1))) + 1
		}
	}

	if idx >= len(m.pool) {
		return nil, ErrConnectionIndexRange
	}

	conn := m.pool[idx]
	if conn == nil {
		return nil, ErrConnectionNotInitialized
	}

	// Check if connection is closed
	if conn.IsClosed() {
		return nil, ErrConnectionClosed
	}

	return conn, nil
}

// replace replaces a failed connection at the given index with a new one.
func (m *connectionManager) replace(idx int) error {
	if m.closed.Load() {
		return ErrConnectionManagerClosed
	}

	m.connMu.Lock()
	defer m.connMu.Unlock()

	if idx < 0 || idx >= len(m.pool) {
		return fmt.Errorf("%w: invalid connection index: %d", ErrConnectionIndexRange, idx)
	}

	// Close old connection if still open
	if m.pool[idx] != nil {
		_ = m.pool[idx].Close()
	}

	// Create new connection
	conn, err := newConnection(m.url, m.cfg)
	if err != nil {
		m.pool[idx] = nil
		return fmt.Errorf("%w: replace connection %d: %v", ErrConnectionReplace, idx, err)
	}

	m.pool[idx] = conn

	// Call open handler if registered
	m.handlersMu.RLock()
	openHandler := m.openHandler
	m.handlersMu.RUnlock()
	if openHandler != nil {
		go openHandler(idx)
	}

	// Start monitoring the new connection
	go m.monitor(conn)

	return nil
}

// monitor watches a connection for close and block events, replacing it when necessary.
func (m *connectionManager) monitor(conn *Connection) {
	closeCh := make(chan *amqp.Error, 1)
	blockCh := make(chan amqp.Blocking, 1)

	conn.NotifyClose(closeCh)
	conn.NotifyBlocked(blockCh)

	for {
		select {
		case <-m.ctx.Done():
			return
		case err, ok := <-closeCh:
			if !ok {
				return
			}

			idx := m.index(conn)

			// Call close handler if registered
			if err != nil {
				m.handlersMu.RLock()
				handler := m.closeHandler
				m.handlersMu.RUnlock()

				if handler != nil {
					// Call handler in goroutine to prevent blocking
					go handler(idx, err.Code, err.Reason, err.Server, err.Recover)
				}
			}

			// Connection closed, attempt to replace it if manager is still open
			if err != nil && !m.closed.Load() {
				_ = m.replace(idx)
			}
			return
		case block, ok := <-blockCh:
			if !ok {
				return
			}

			// Call block handler if registered
			m.handlersMu.RLock()
			handler := m.blockHandler
			m.handlersMu.RUnlock()

			if handler != nil {
				// Call handler in goroutine to prevent blocking
				go handler(m.index(conn), block.Active, block.Reason)
			}
		}
	}
}

// index returns the index of a connection in the pool, or -1 if not found.
func (m *connectionManager) index(conn *Connection) int {
	if conn == nil {
		return -1
	}

	m.connMu.RLock()
	defer m.connMu.RUnlock()

	for i, c := range m.pool {
		if c == conn {
			return i
		}
	}
	return -1
}

// close closes all managed connections.
func (m *connectionManager) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return ErrConnectionManagerClosed
	}

	// Cancel monitoring goroutines
	m.cancel()

	m.connMu.Lock()
	defer m.connMu.Unlock()

	var errs []error
	for i, conn := range m.pool {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close connection %d: %w", i, err))
			}
			m.pool[i] = nil
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %v", ErrConnectionClose, errors.Join(errs...))
	}

	return nil
}
