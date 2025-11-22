package broker

import (
	"context"
	"fmt"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// connectionPool creates and reuses *amqp.Connection objects.
// It is used by publishers to get dedicated connections.
type connectionPool struct {
	url     string
	current chan *resourceWrapper[amqp.Connection] // changed from *amqp.Connection
	max     atomic.Int32
	total   atomic.Int32
	closed  atomic.Bool
}

func newConnectionPool(url string, max int) *connectionPool {
	if max <= 0 {
		max = 4
	}
	cp := &connectionPool{
		url:     url,
		current: make(chan *resourceWrapper[amqp.Connection], max),
	}
	cp.max.Store(int32(max))
	return cp
}

func (p *connectionPool) Get(ctx context.Context) (*amqp.Connection, releaseFunc, error) {
	select {
	case rw := <-p.current:
		return rw.resource, p.makeRelease(rw), nil
	default:
		for {
			curr := p.total.Load()
			if curr >= p.max.Load() {
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
					release:  func(bad bool) {},
				}
				return conn, p.makeRelease(rw), nil
			}
		}
	}
}

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

func (p *connectionPool) Close() {
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
