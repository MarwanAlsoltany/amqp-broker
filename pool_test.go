package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPool(t *testing.T) {
	ttl := 5 * time.Minute
	p := newPool[string](ttl)
	require.NotNil(t, p)
	assert.Equal(t, ttl, p.ttl)
}

func TestPoolInit(t *testing.T) {
	t.Run("Idempotency", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		p := newPool[string](time.Second)
		err := p.init(ctx)
		require.NoError(t, err)
		err = p.init(ctx)
		require.NoError(t, err)
		err = p.init(ctx)
		require.NoError(t, err)
	})

	t.Run("AutoCleanup", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		ttl := 50 * time.Millisecond
		p := newPool[*mockCloser](ttl)
		err := p.init(ctx)
		require.NoError(t, err)

		key := "test-key"

		// create and release an item
		value, release, err := p.acquire(key, func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		release()

		// wait for automatic cleanup (TTL + cleanup interval)
		time.Sleep(ttl + ttl/2 + 50*time.Millisecond)

		// item should be automatically removed
		_, ok := p.items.Load(key)
		assert.False(t, ok)
		assert.True(t, value.closed.Load())
	})

	t.Run("WhenContextCancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		ttl := 100 * time.Millisecond
		p := newPool[string](ttl)
		err := p.init(ctx)
		require.NoError(t, err)

		// cancel context
		cancel()

		// give goroutine time to exit
		time.Sleep(50 * time.Millisecond)

		// further operations should still work (cleanup goroutine stopped,
		// but pool functional), manual cleanup can be triggered via Close
		_, release, err := p.acquire("key", func() (string, error) {
			return "value", nil
		})
		require.NoError(t, err)
		release()
	})
}

func TestPoolAcquire(t *testing.T) {
	t.Run("WithActiveItems", func(t *testing.T) {
		p := newPool[string](time.Minute)
		key := "test-key"
		value1 := "first-value"
		// first acquisition - creates new
		val1, release1, err := p.acquire(key, func() (string, error) {
			return value1, nil
		})
		require.NoError(t, err)
		assert.Equal(t, value1, val1)
		// second acquisition - reuses existing
		factoryCalled := false
		val2, release2, err := p.acquire(key, func() (string, error) {
			factoryCalled = true
			return "should-not-be-called", nil
		})
		require.NoError(t, err)
		assert.Equal(t, value1, val2)
		assert.False(t, factoryCalled, "factory should not be called when reusing")
		// verify refCount increased
		raw, _ := p.items.Load(key)
		item := raw.(*poolItem[string])
		assert.Equal(t, int32(2), item.refCount.Load())
		release1()
		assert.Equal(t, int32(1), item.refCount.Load())
		release2()
		assert.Equal(t, int32(0), item.refCount.Load())
	})

	t.Run("WithMultipleItems", func(t *testing.T) {
		p := newPool[int](time.Minute)

		keys := []string{"key1", "key2", "key3"}
		values := []int{1, 2, 3}

		// acquire different items
		var releases []func()
		for i, key := range keys {
			value, release, err := p.acquire(key, func() (int, error) {
				return values[i], nil
			})
			require.NoError(t, err)
			assert.Equal(t, values[i], value)
			releases = append(releases, release)
		}

		// all should be in pool
		for _, key := range keys {
			_, ok := p.items.Load(key)
			assert.True(t, ok)
		}

		// release all
		for _, release := range releases {
			release()
		}

		// all refCounts should be 0
		for _, key := range keys {
			raw, _ := p.items.Load(key)
			item := raw.(*poolItem[int])
			assert.Equal(t, int32(0), item.refCount.Load())
		}
	})

	t.Run("UpdatesItemLastUsed", func(t *testing.T) {
		p := newPool[string](time.Minute)
		key := "test-key"

		before := time.Now()
		value, release, err := p.acquire(key, func() (string, error) {
			return "test", nil
		})
		require.NoError(t, err)
		after := time.Now()

		// check initial lastUsed is set
		raw, _ := p.items.Load(key)
		item := raw.(*poolItem[string])
		lastUsed := time.Unix(0, item.lastUsed.Load())
		assert.True(t, lastUsed.After(before) || lastUsed.Equal(before))
		assert.True(t, lastUsed.Before(after) || lastUsed.Equal(after))

		// wait a bit
		time.Sleep(50 * time.Millisecond)

		// release should update lastUsed
		beforeRelease := time.Now()
		release()
		afterRelease := time.Now()

		lastUsed = time.Unix(0, item.lastUsed.Load())
		assert.True(t, lastUsed.After(beforeRelease) || lastUsed.Equal(beforeRelease))
		assert.True(t, lastUsed.Before(afterRelease) || lastUsed.Equal(afterRelease))

		// suppress unused warning
		_ = value
	})

	t.Run("DecrementsItemRefCount", func(t *testing.T) {
		p := newPool[string](time.Minute)
		key := "test-key"

		_, release, err := p.acquire(key, func() (string, error) {
			return "test", nil
		})
		require.NoError(t, err)

		raw, ok := p.items.Load(key)
		require.True(t, ok)
		item := raw.(*poolItem[string])

		assert.Equal(t, int32(1), item.refCount.Load())
		release()
		assert.Equal(t, int32(0), item.refCount.Load())
	})

	t.Run("WhenFactoryErrors", func(t *testing.T) {
		p := newPool[string](time.Minute)
		key := "test-key"
		expectedErr := assert.AnError
		value, release, err := p.acquire(key, func() (string, error) {
			return "", expectedErr
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Equal(t, "", value)
		assert.Nil(t, release)
		// verify nothing was stored
		_, ok := p.items.Load(key)
		assert.False(t, ok)
	})

	t.Run("WhenClosed", func(t *testing.T) {
		p := newPool[string](time.Minute)

		// close the pool
		err := p.Close()
		require.NoError(t, err)

		// try to acquire after close
		value, release, err := p.acquire("test-key", func() (string, error) {
			return "test-value", nil
		})

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrPoolClosed)
		assert.Empty(t, value)
		assert.Nil(t, release)
	})

	// checks that concurrent access to an already-created item
	// is safe and that reference counting works as expected
	t.Run("ConcurrentAccess", func(t *testing.T) {
		p := newPool[string](time.Minute)
		key := "test-key"

		var wg sync.WaitGroup
		const goroutines = 10

		for range goroutines {
			wg.Go(func() {
				item, release, err := p.acquire(key, func() (string, error) {
					return "shared-value", nil
				})
				assert.NoError(t, err)
				assert.Equal(t, "shared-value", item)
				// simulate some work
				time.Sleep(10 * time.Millisecond)
				release()
			})
		}

		wg.Wait()

		// all goroutines should have released
		raw, ok := p.items.Load(key)
		require.True(t, ok)
		item := raw.(*poolItem[string])
		assert.Equal(t, int32(0), item.refCount.Load())
	})

	// stresses the pool's ability to handle simultaneous creation
	// attempts and ensures proper cleanup of redundant resources
	t.Run("ConcurrentRace", func(t *testing.T) {
		p := newPool[*mockCloser](time.Minute)
		key := "test-key"

		var wg sync.WaitGroup
		const goroutines = 10

		// barrier to synchronize goroutines
		// this ensures they all attempt to acquire at the same time
		// otherwise it is random and the coverage will not always happen
		startCh := make(chan struct{})

		// track all created items to verify losers are closed
		var itemsMu sync.Mutex
		createdItems := []*mockCloser{}
		createdCount := atomic.Int32{}

		for range goroutines {
			wg.Go(func() {
				<-startCh // wait for the signal to start together

				_, release, err := p.acquire(key, func() (*mockCloser, error) {
					createdCount.Add(1)
					item := &mockCloser{}
					itemsMu.Lock()
					createdItems = append(createdItems, item)
					itemsMu.Unlock()
					return item, nil
				})
				require.NoError(t, err)
				// simulate some work
				time.Sleep(10 * time.Millisecond)
				release()
			})
		}

		close(startCh) // release all goroutines at once
		wg.Wait()

		// multiple goroutines created items, but only 1 should be kept
		raw, ok := p.items.Load(key)
		require.True(t, ok)
		item := raw.(*poolItem[*mockCloser])
		assert.Equal(t, int32(0), item.refCount.Load(), "all should be released")

		// count how many items were closed (losers should be closed)
		itemsMu.Lock()
		closedCount := 0
		var winner *mockCloser
		for _, created := range createdItems {
			if created.closed.Load() {
				closedCount++
			} else {
				winner = created
			}
		}
		itemsMu.Unlock()

		// winner should be in the pool and not closed
		assert.Equal(t, item.value, winner, "winner should be in pool")
		assert.False(t, item.value.closed.Load(), "winner should not be closed")

		// losers should have been closed (all but the winner)
		assert.Equal(t, len(createdItems)-1, closedCount, "race losers should be closed")
	})
}

func TestPoolCleanup(t *testing.T) {
	t.Run("RemovesIdleItems", func(t *testing.T) {
		ttl := 100 * time.Millisecond
		p := newPool[*mockCloser](ttl)
		key := "test-key"

		// acquire and release immediately
		value, release, err := p.acquire(key, func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		release()

		// item should still exist
		_, ok := p.items.Load(key)
		assert.True(t, ok)

		// wait for TTL to expire
		time.Sleep(ttl + 50*time.Millisecond)

		// run cleanup
		p.cleanup()

		// item should be removed and closed
		_, ok = p.items.Load(key)
		assert.False(t, ok)
		assert.True(t, value.closed.Load())
	})

	t.Run("PreservesActiveItems", func(t *testing.T) {
		p := newPool[*mockCloser](10 * time.Millisecond)
		key := "test-key"

		closer := &mockCloser{}
		value, release, err := p.acquire(key, func() (*mockCloser, error) {
			return closer, nil
		})
		require.NoError(t, err)
		assert.NotNil(t, value)

		// item is active, cleanup shouldn't remove it
		time.Sleep(20 * time.Millisecond)
		p.cleanup()

		_, ok := p.items.Load(key)
		assert.True(t, ok, "active item should not be removed")
		assert.False(t, closer.closed.Load(), "active item should not be closed")

		// release and cleanup
		release()
		time.Sleep(20 * time.Millisecond)
		p.cleanup()

		_, ok = p.items.Load(key)
		assert.False(t, ok, "idle item should be removed after TTL")
		assert.True(t, closer.closed.Load(), "idle item should be closed")
	})

	t.Run("ClosesClosers", func(t *testing.T) {
		ttl := 100 * time.Millisecond
		p := newPool[*mockCloser](ttl)

		key := "test-key"
		value, release, err := p.acquire(key, func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		release()

		// item should still exist
		_, ok := p.items.Load(key)
		assert.True(t, ok)

		// wait for TTL to expire
		time.Sleep(ttl + 50*time.Millisecond)

		// run cleanup
		p.cleanup()

		// item should be removed and closed
		_, ok = p.items.Load(key)
		assert.False(t, ok)
		assert.True(t, value.closed.Load())

		t.Run("PropagatesCloseError", func(t *testing.T) {
			p := newPool[*mockCloser](10 * time.Millisecond)

			// add an item
			value := &mockCloser{
				closeErr: assert.AnError,
			}
			_, release, err := p.acquire("test-key", func() (*mockCloser, error) {
				return value, nil
			})
			require.NoError(t, err)

			// release and wait for TTL
			release()
			time.Sleep(20 * time.Millisecond)

			// cleanup should close the item and return error
			err = p.cleanup()
			assert.Error(t, err, "cleanup should return error from Close()")
			assert.ErrorIs(t, err, value.closeErr)
			assert.True(t, value.closed.Load())
		})
	})

	t.Run("IgnoresNonClosers", func(t *testing.T) {
		ttl := 100 * time.Millisecond
		p := newPool[string](ttl)
		key := "test-key"

		// acquire and release string (not io.Closer)
		_, release, err := p.acquire(key, func() (string, error) {
			return "test", nil
		})
		require.NoError(t, err)
		release()

		// wait for TTL
		time.Sleep(ttl + 50*time.Millisecond)

		// should not panic
		require.NotPanics(t, func() {
			p.cleanup()
		})

		// item should be removed
		_, ok := p.items.Load(key)
		assert.False(t, ok)
	})

	t.Run("RespectsTimingBeforeAndAfterTTL", func(t *testing.T) {
		ttl := 200 * time.Millisecond
		p := newPool[*mockCloser](ttl)
		key := "test-key"

		// acquire and release
		value, release, err := p.acquire(key, func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		release()

		// item should exist before TTL expires
		time.Sleep(ttl / 2)
		_, ok := p.items.Load(key)
		assert.True(t, ok, "item should exist at 50%% of TTL")
		assert.False(t, value.closed.Load(), "item should not be closed at 50%% of TTL")

		err = p.cleanup()
		require.NoError(t, err)
		_, ok = p.items.Load(key)
		assert.True(t, ok, "item should still exist after cleanup at 50%% of TTL")

		// wait for TTL to expire
		time.Sleep(ttl/2 + 50*time.Millisecond)

		// item should be removed after TTL + cleanup
		err = p.cleanup()
		require.NoError(t, err)
		_, ok = p.items.Load(key)
		assert.False(t, ok, "item should be removed after TTL expires")
		assert.True(t, value.closed.Load(), "item should be closed after TTL expires")
	})

	t.Run("TracksIndependentItemLifetimes", func(t *testing.T) {
		ttl := 100 * time.Millisecond
		p := newPool[*mockCloser](ttl)

		// create first item
		value1, release1, err := p.acquire("key1", func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		release1()

		// wait a bit, then create second item
		time.Sleep(50 * time.Millisecond)
		value2, release2, err := p.acquire("key2", func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		release2()

		// wait for first item's TTL to expire
		time.Sleep(60 * time.Millisecond) // total: 110ms for key1, 60ms for key2

		p.cleanup()

		// first item should be removed, second should still exist
		_, ok := p.items.Load("key1")
		assert.False(t, ok, "key1 should be removed after its TTL")
		assert.True(t, value1.closed.Load(), "key1 should be closed")

		_, ok = p.items.Load("key2")
		assert.True(t, ok, "key2 should still exist")
		assert.False(t, value2.closed.Load(), "key2 should not be closed yet")

		// wait for second item's TTL to expire
		time.Sleep(50 * time.Millisecond) // total: 110ms for key2

		p.cleanup()

		// both items should now be removed
		_, ok = p.items.Load("key2")
		assert.False(t, ok, "key2 should be removed after its TTL")
		assert.True(t, value2.closed.Load(), "key2 should be closed")
	})

	t.Run("ReacquireExtendsItemLifetime", func(t *testing.T) {
		ttl := 100 * time.Millisecond
		p := newPool[*mockCloser](ttl)
		key := "test-key"

		// first acquisition
		value, release1, err := p.acquire(key, func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		release1()

		// wait 75% of TTL
		time.Sleep(75 * time.Millisecond)

		// reacquire - this should reset the lastUsed timestamp
		_, release2, err := p.acquire(key, func() (*mockCloser, error) {
			t.Fatal("factory should not be called - item should be reused")
			return nil, nil
		})
		require.NoError(t, err)
		release2()

		// wait another 75ms (total 150ms from first acquisition, but only 75ms from second)
		time.Sleep(75 * time.Millisecond)

		// item should still exist because lastUsed was updated on second acquisition
		p.cleanup()
		_, ok := p.items.Load(key)
		assert.True(t, ok, "item should still exist - lifetime was reset on reacquisition")
		assert.False(t, value.closed.Load(), "item should not be closed - lifetime was reset")

		// wait for TTL to expire from the second release
		time.Sleep(50 * time.Millisecond)

		p.cleanup()
		_, ok = p.items.Load(key)
		assert.False(t, ok, "item should be removed after TTL from second release")
		assert.True(t, value.closed.Load(), "item should be closed")
	})
}

func TestPoolClose(t *testing.T) {
	t.Run("Idempotency", func(t *testing.T) {
		p := newPool[string](time.Minute)

		// first close
		err := p.Close()
		assert.NoError(t, err)
		assert.True(t, p.closed.Load())

		// second close should be safe
		err = p.Close()
		assert.NoError(t, err)
		assert.True(t, p.closed.Load())
	})

	t.Run("RemovesIdleItems", func(t *testing.T) {
		ttl := 50 * time.Millisecond
		p := newPool[*mockCloser](ttl)
		key := "test-key"

		// acquire and release
		value, release, err := p.acquire(key, func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		release()

		// wait for TTL to expire
		time.Sleep(ttl + 20*time.Millisecond)

		// close the pool
		err = p.Close()
		require.NoError(t, err)

		// verify closed flag is set
		assert.True(t, p.closed.Load())

		// verify idle item was cleaned up
		_, ok := p.items.Load(key)
		assert.False(t, ok)
		assert.True(t, value.closed.Load())
	})

	t.Run("PreservesActiveItems", func(t *testing.T) {
		p := newPool[*mockCloser](time.Minute)
		key := "test-key"

		// acquire but don't release
		value, release, err := p.acquire(key, func() (*mockCloser, error) {
			return &mockCloser{}, nil
		})
		require.NoError(t, err)
		defer release()

		// close the pool
		err = p.Close()
		require.NoError(t, err)

		// active item should NOT be cleaned up
		_, ok := p.items.Load(key)
		assert.True(t, ok)
		assert.False(t, value.closed.Load())
	})
}
