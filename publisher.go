package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher is a dedicated publisher object: it holds a connection and channel borrowed from the connPool.
// If created with confirm=true the publisher's channel is put into confirm mode and Publish waits for confirms.
type Publisher interface {
	endpoint
	Publish(ctx context.Context, ex Exchange, rk RoutingKey, body []byte, opts *PublishOptions) error
}

type publisher struct {
	conn    *amqp.Connection
	relConn func(bad bool)
	ch      *amqp.Channel
	confirm bool
	confCh  <-chan amqp.Confirmation
	mu      sync.Mutex
	closed  atomic.Bool
	broker  *Broker
}

var _ Publisher = (*publisher)(nil)

func (p *publisher) Connection() *amqp.Connection {
	return p.conn
}
func (p *publisher) Channel() *amqp.Channel {
	return p.ch
}

func (p *publisher) Publish(ctx context.Context, ex Exchange, rk RoutingKey, body []byte, opts *PublishOptions) error {
	if p.closed.Load() {
		return errors.New("publisher closed")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	po := PublishOptions{}
	if opts != nil {
		po = *opts
	}
	if po.DeliveryMode == 0 {
		po.DeliveryMode = 2
	}
	if po.ContentType == "" {
		po.ContentType = "application/octet-stream"
	}
	pub := amqp.Publishing{
		Headers:      po.Headers,
		ContentType:  po.ContentType,
		Body:         body,
		DeliveryMode: po.DeliveryMode,
		Timestamp:    time.Now(),
	}

	// ensure exchange declared if provided (best-effort)
	if ex.Name != "" {
		if _, loaded := p.broker.declaredExchanges.LoadOrStore(ex.Name, struct{}{}); !loaded {
			// try to declare using control connection
			conn := p.broker.Connection()
			if conn != nil {
				ch, rel, err := p.broker.NewChannel()
				if err == nil {
					_ = ch.ExchangeDeclare(ex.Name, defaultExchangeTypeFallback(ex.Kind), ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
					rel()
				} else {
					// fallback: declare on publisher channel
					_ = p.ch.ExchangeDeclare(ex.Name, defaultExchangeTypeFallback(ex.Kind), ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
				}
			} else {
				_ = p.ch.ExchangeDeclare(ex.Name, defaultExchangeTypeFallback(ex.Kind), ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
			}
		}
	}

	// publish
	if err := p.ch.PublishWithContext(ctx, ex.Name, string(rk), po.Mandatory, po.Immediate, pub); err != nil {
		// mark connection/channel bad and return
		p.release(true)
		return fmt.Errorf("publish: %w", err)
	}

	// optionally wait for confirm if publisher configured for confirm
	if p.confirm {
		timeout := po.ConfirmTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		select {
		case c := <-p.confCh:
			if !c.Ack {
				return errors.New("publish not acked by broker")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(timeout):
			p.release(true)
			return errors.New("confirm timeout")
		}
	}
	return nil
}

func (p *publisher) release(bad bool) {
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.relConn != nil {
		p.relConn(bad)
		p.relConn = nil
	}
}

func (p *publisher) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	// close channel and release connection
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.relConn != nil {
		p.relConn(false)
		p.relConn = nil
	}
	return nil
}
