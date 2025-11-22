package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// newConnection establishes a new AMQP connection to the given URL.
func newConnection(url string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dial connection: %w", err)
	}
	return conn, nil
}

// connectionManager manages a fixed set of long-lived AMQP connections.
// Connections are assigned to endpoints based on their role and held for their lifetime.
type connectionManager struct {
	url    string
	size   int
	conns  []*amqp.Connection
	connMu sync.RWMutex
	// Round-robin counters for assignment within role groups
	publisherIdx atomic.Uint32
	consumerIdx  atomic.Uint32
	closed       atomic.Bool
	// Cancellation for monitoring goroutines
	ctx    context.Context
	cancel context.CancelFunc
}

// newConnectionManager creates a new connection manager with the specified size.
// Size determines how many long-lived connections are maintained.
func newConnectionManager(url string, size int) *connectionManager {
	if size <= 0 {
		size = defaultConnPoolSize
	}
	return &connectionManager{
		url:   url,
		size:  size,
		conns: make([]*amqp.Connection, size),
	}
}

// init creates all managed connections.
func (m *connectionManager) init(ctx context.Context) error {
	m.connMu.Lock()
	defer m.connMu.Unlock()

	// Set up context for monitoring goroutines
	m.ctx, m.cancel = context.WithCancel(ctx)

	for i := 0; i < m.size; i++ {
		conn, err := newConnection(m.url)
		if err != nil {
			// Close any connections we've already opened
			for j := 0; j < i; j++ {
				if m.conns[j] != nil {
					_ = m.conns[j].Close()
				}
			}
			return fmt.Errorf("failed to initialize connection %d: %w", i+1, err)
		}
		m.conns[i] = conn

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
func (m *connectionManager) assign(_ context.Context, role endpointRole) (*amqp.Connection, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("connection manager closed")
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

	if idx >= len(m.conns) {
		return nil, fmt.Errorf("connection index out of range")
	}

	conn := m.conns[idx]
	if conn == nil {
		return nil, fmt.Errorf("connection not initialized")
	}

	return conn, nil
}

// replace replaces a failed connection at the given index with a new one.
func (m *connectionManager) replace(idx int) error {
	if m.closed.Load() {
		return fmt.Errorf("connection manager closed")
	}

	m.connMu.Lock()
	defer m.connMu.Unlock()

	if idx < 0 || idx >= len(m.conns) {
		return fmt.Errorf("invalid connection index: %d", idx)
	}

	// Close old connection if still open
	if m.conns[idx] != nil {
		_ = m.conns[idx].Close()
	}

	// Create new connection
	conn, err := newConnection(m.url)
	if err != nil {
		m.conns[idx] = nil
		return fmt.Errorf("replace connection %d: %w", idx, err)
	}

	m.conns[idx] = conn

	// Start monitoring the new connection
	go m.monitor(conn)

	return nil
}

// monitor watches a connection for close and block events, replacing it when necessary.
func (m *connectionManager) monitor(conn *amqp.Connection) {
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
			// Connection closed, attempt to replace it if manager is still open
			if err != nil && !m.closed.Load() {
				// Connection closed with error
				idx := m.index(conn)
				_ = m.replace(idx)
			}
			return
		case block, ok := <-blockCh:
			if !ok {
				return
			}
			if block.Active {
				// Connection blocked - for now just log/monitor
				// In the future, could implement more aggressive replacement
				_ = block
			}
			// Connection unblocked, continue monitoring
		}
	}
}

// index returns the index of a connection in the pool, or -1 if not found.
func (m *connectionManager) index(conn *amqp.Connection) int {
	if conn == nil {
		return -1
	}

	m.connMu.RLock()
	defer m.connMu.RUnlock()

	for i, c := range m.conns {
		if c == conn {
			return i
		}
	}
	return -1
}

// close closes all managed connections.
func (m *connectionManager) close() {
	if !m.closed.CompareAndSwap(false, true) {
		return
	}

	// Cancel monitoring goroutines
	m.cancel()

	m.connMu.Lock()
	defer m.connMu.Unlock()

	for i, conn := range m.conns {
		if conn != nil {
			_ = conn.Close()
			m.conns[i] = nil
		}
	}
}
