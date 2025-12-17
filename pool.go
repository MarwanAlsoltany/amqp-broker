package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// pool provides TTL-based pooling for reusable endpoints.
// Uses reference counting to track active usage and automatic cleanup of idle entries.
type pool[T any] struct {
	once   sync.Once     // only start cleanup goroutine once
	items  sync.Map      // map[string]*poolItem[T]
	ttl    time.Duration // time-to-live for idle items
	closed atomic.Bool   // close state
}

// poolItem tracks usage metadata for pooled endpoints.
type poolItem[T any] struct {
	value    T
	refCount atomic.Int32
	lastUsed atomic.Int64
}

// newPool creates a new endpoint pool with the given TTL.
func newPool[T any](ttl time.Duration) *pool[T] {
	return &pool[T]{ttl: ttl}
}

// init begins the background cleanup goroutine.
// Can be called multiple times safely - only starts once.
func (p *pool[T]) init(ctx context.Context) error {
	p.once.Do(func() {
		go func() {
			ticker := time.NewTicker(p.ttl / 2)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p.cleanup()
				}
			}
		}()
	})

	return nil
}

// cleanup removes idle entries that have exceeded TTL.
func (p *pool[T]) cleanup() error {
	var errs []error

	p.items.Range(func(k, v any) bool {
		item := v.(*poolItem[T])
		if item.refCount.Load() == 0 {
			lastUsed := time.Unix(0, item.lastUsed.Load())
			if time.Since(lastUsed) > p.ttl {
				if value, ok := any(item.value).(io.Closer); ok {
					errs = append(errs, value.Close())
				}
				p.items.Delete(k)
			}
		}
		return true
	})

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: close failed: %w", ErrPool, err)
	}

	return nil
}

// Close marks the pool as closed and cleans up all idle items.
// After calling Close, acquire will return ErrPoolClosed.
func (p *pool[T]) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	return p.cleanup()
}

// acquire retrieves a pooled item or creates a new one using the provided factory.
// Returns the item and a release function that must be called when done.
func (p *pool[T]) acquire(key string, factory func() (T, error)) (T, func(), error) {
	var zero T

	if p.closed.Load() {
		return zero, nil, ErrPoolClosed
	}

	// fast path: reuse existing
	if v, ok := p.items.Load(key); ok {
		item := v.(*poolItem[T])
		item.refCount.Add(1)
		release := func() {
			item.refCount.Add(-1)
			item.lastUsed.Store(time.Now().UnixNano())
		}
		return item.value, release, nil
	}

	// slow path: create new
	value, err := factory()
	if err != nil {
		return zero, nil, err
	}

	item := &poolItem[T]{value: value}
	item.refCount.Store(1)
	item.lastUsed.Store(time.Now().UnixNano())

	// race-catcher: another goroutine may have created it
	if actual, loaded := p.items.LoadOrStore(key, item); loaded {
		// lost race, close ours and use the winner
		if closer, ok := any(value).(io.Closer); ok {
			closer.Close()
		}
		item = actual.(*poolItem[T])
		item.refCount.Add(1)
		release := func() {
			item.refCount.Add(-1)
			item.lastUsed.Store(time.Now().UnixNano())
		}
		return item.value, release, nil
	}

	release := func() {
		item.refCount.Add(-1)
		item.lastUsed.Store(time.Now().UnixNano())
	}

	return value, release, nil
}
