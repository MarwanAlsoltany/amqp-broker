package broker

import (
	"context"
	"fmt"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// newChannel creates a new AMQP channel from the given connection.
func newChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create channel: %w", err)
	}
	return ch, nil
}

// channelPool manages a pool of AMQP channels for reuse.
// Channels are created from connections borrowed from the connection pool.
type channelPool struct {
	connPool *connectionPool
	pool     chan *resourceWrapper[amqp.Channel]
	max      atomic.Int32
	total    atomic.Int32
	closed   atomic.Bool
}

// newChannelPool creates a new channel pool using the provided connection pool.
func newChannelPool(connPool *connectionPool, max int) *channelPool {
	if max <= 0 {
		max = defaultChPoolSize
	}
	cp := &channelPool{
		connPool: connPool,
		pool:     make(chan *resourceWrapper[amqp.Channel], max),
	}
	cp.max.Store(int32(max))
	return cp
}

// Get retrieves a channel from the pool or creates a new one.
// Returns the channel, a release function, and any error.
func (p *channelPool) get(ctx context.Context) (*amqp.Channel, releaseFunc, error) {
	select {
	case rw := <-p.pool:
		return rw.resource, p.makeRelease(rw), nil
	default:
		// Try to create a new channel if under limit
		for {
			curr := p.total.Load()
			if curr >= p.max.Load() {
				// Wait for available channel
				select {
				case w := <-p.pool:
					return w.resource, p.makeRelease(w), nil
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			if p.total.CompareAndSwap(curr, curr+1) {
				// Borrow connection from pool
				conn, relConn, err := p.connPool.get(ctx)
				if err != nil {
					p.total.Add(-1)
					return nil, nil, fmt.Errorf("channel pool: get connection: %w", err)
				}
				ch, err := conn.Channel()
				if err != nil {
					relConn(true)
					p.total.Add(-1)
					return nil, nil, fmt.Errorf("channel pool: create channel: %w", err)
				}
				rw := &resourceWrapper[amqp.Channel]{
					resource: ch,
					release:  relConn,
				}
				return ch, p.makeRelease(rw), nil
			}
		}
	}
}

// makeRelease creates a release function for a channel wrapper.
func (p *channelPool) makeRelease(rw *resourceWrapper[amqp.Channel]) releaseFunc {
	return func(bad bool) {
		if rw == nil || rw.resource == nil {
			return
		}
		if p.closed.Load() || bad {
			_ = rw.resource.Close()
			if rw.release != nil {
				rw.release(bad)
			}
			p.total.Add(-1)
			return
		}
		// Return to pool or close if full
		select {
		case p.pool <- rw:
		default:
			_ = rw.resource.Close()
			if rw.release != nil {
				rw.release(false)
			}
			p.total.Add(-1)
		}
	}
}

// close drains and closes all channels in the pool.
func (p *channelPool) close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	for {
		select {
		case w := <-p.pool:
			_ = w.resource.Close()
			if w.release != nil {
				w.release(false)
			}
			p.total.Add(-1)
		default:
			return
		}
	}
}
