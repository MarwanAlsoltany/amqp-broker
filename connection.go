package broker

import (
	"context"
	"fmt"
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

// connectionPool manages a pool of AMQP connections for reuse.
type connectionPool struct {
	url     string
	current chan *resourceWrapper[amqp.Connection]
	max     atomic.Int32
	total   atomic.Int32
	closed  atomic.Bool
}

// newConnectionPool creates a new connection pool with the specified maximum size.
func newConnectionPool(url string, max int) *connectionPool {
	if max <= 0 {
		max = defaultConnPoolSize
	}
	cp := &connectionPool{
		url:     url,
		current: make(chan *resourceWrapper[amqp.Connection], max),
	}
	cp.max.Store(int32(max))
	return cp
}

// Get retrieves a connection from the pool or creates a new one.
// Returns the connection, a release function, and any error.
func (p *connectionPool) get(ctx context.Context) (*amqp.Connection, releaseFunc, error) {
	select {
	case rw := <-p.current:
		return rw.resource, p.makeRelease(rw), nil
	default:
		// Try to create a new connection if under limit
		for {
			curr := p.total.Load()
			if curr >= p.max.Load() {
				// Wait for available connection
				select {
				case w := <-p.current:
					return w.resource, p.makeRelease(w), nil
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			if p.total.CompareAndSwap(curr, curr+1) {
				conn, err := amqp.Dial(p.url)
				if err != nil {
					p.total.Add(-1)
					return nil, nil, fmt.Errorf("dial connection pool: %w", err)
				}
				rw := &resourceWrapper[amqp.Connection]{
					resource: conn,
					release:  func(bad bool) { /* noop */ },
				}
				return conn, p.makeRelease(rw), nil
			}
		}
	}
}

// makeRelease creates a release function for a connection wrapper.
func (p *connectionPool) makeRelease(rw *resourceWrapper[amqp.Connection]) releaseFunc {
	return func(bad bool) {
		if rw == nil || rw.resource == nil {
			return
		}
		if p.closed.Load() || bad {
			_ = rw.resource.Close()
			p.total.Add(-1)
			return
		}
		select {
		case p.current <- rw:
		default:
			_ = rw.resource.Close()
			p.total.Add(-1)
		}
	}
}

// close drains and closes all connections in the pool.
func (p *connectionPool) close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	for {
		select {
		case c := <-p.current:
			_ = c.resource.Close()
			p.total.Add(-1)
		default:
			return
		}
	}
}
