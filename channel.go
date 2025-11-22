package broker

import (
	"context"
	"fmt"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// channelPool builds channels by borrowing connections from connPool.
// When a channel is returned the underlying connection release function is called only when channel is closed/bad.
type channelPool struct {
	connPool *connectionPool
	pool     chan *resourceWrapper[amqp.Channel]
	max      atomic.Int32
	total    atomic.Int32
	closed   atomic.Bool
}

// newChannelPool uses the provided connectionPool to create channels on demand.
func newChannelPool(connPool *connectionPool, max int) *channelPool {
	if max <= 0 {
		max = 8
	}
	cp := &channelPool{
		connPool: connPool,
		pool:     make(chan *resourceWrapper[amqp.Channel], max),
	}
	cp.max.Store(int32(max))
	return cp
}

// Get returns an *amqp.Channel and a release function (bad bool).
func (p *channelPool) Get(ctx context.Context) (*amqp.Channel, releaseFunc, error) {
	select {
	case rw := <-p.pool:
		return rw.resource, p.makeRelease(rw), nil
	default:
		for {
			curr := p.total.Load()
			if curr >= p.max.Load() {
				select {
				case w := <-p.pool:
					return w.resource, p.makeRelease(w), nil
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			if p.total.CompareAndSwap(curr, curr+1) {
				// create new channel by borrowing a connection
				conn, relConn, err := p.connPool.Get(ctx)
				if err != nil {
					p.total.Add(-1)
					return nil, nil, fmt.Errorf("channel pool: get connection: %w", err)
				}
				ch, err := conn.Channel()
				if err != nil {
					// release connection as bad
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

func (p *channelPool) makeRelease(rw *resourceWrapper[amqp.Channel]) releaseFunc {
	return func(bad bool) {
		if rw == nil || rw.resource == nil {
			return
		}
		if p.closed.Load() || bad {
			_ = rw.resource.Close()
			// release connection as bad if channel bad
			if rw.release != nil {
				rw.release(bad)
			}
			p.total.Add(-1)
			return
		}
		// return to pool or close if pool full
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

func (p *channelPool) Close() {
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
