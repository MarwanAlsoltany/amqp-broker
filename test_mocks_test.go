package broker

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type mockError struct {
	msg string
}

var _ error = (*mockError)(nil)

func (m *mockError) Error() string {
	return m.msg
}

type mockCloser struct {
	closed   atomic.Bool
	closeErr error
}

var _ io.Closer = (*mockCloser)(nil)

func (m *mockCloser) Close() error {
	m.closed.Store(true)
	return m.closeErr
}

type mockEndpoint struct {
	closeErr error
}

var _ Endpoint = (*mockEndpoint)(nil)

func (m *mockEndpoint) Connection() *Connection {
	return nil
}

func (m *mockEndpoint) Channel() *Channel {
	return nil
}

func (m *mockEndpoint) Ready() bool {
	return m.closeErr == nil
}

func (m *mockEndpoint) Close() error {
	return m.closeErr
}

func (m *mockEndpoint) Release() {
	return
}

func (m *mockEndpoint) Exchange(e Exchange) error {
	return nil
}

func (m *mockEndpoint) Queue(q Queue) error {
	return nil
}

func (m *mockEndpoint) Binding(b Binding) error {
	return nil
}

type mockEndpointLifecycle struct {
	mu                 sync.RWMutex
	connectAttempts    *atomic.Int32
	connectDelay       time.Duration
	connectErr         error
	connectFailUntil   int32 // fail until this attempt number, then succeed
	disconnectAttempts *atomic.Int32
	disconnectDelay    time.Duration
	disconnectErr      error
	monitorAttempts    *atomic.Int32
	monitorDelay       time.Duration
	monitorErr         error
}

var _ endpointLifecycle = (*mockEndpointLifecycle)(nil)

func (m *mockEndpointLifecycle) init(ctx context.Context) error {
	return nil
}

func (m *mockEndpointLifecycle) connect(ctx context.Context) error {
	var currentAttempt int32
	if m.connectAttempts != nil {
		currentAttempt = m.connectAttempts.Add(1)
	}

	if m.connectDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.connectDelay):
		}
	}

	// support conditional failures for backoff testing
	m.mu.RLock()
	failUntil := m.connectFailUntil
	err := m.connectErr
	m.mu.RUnlock()

	if failUntil > 0 && currentAttempt <= failUntil {
		return err
	}

	if failUntil == 0 && err != nil {
		return err
	}

	return nil
}

func (m *mockEndpointLifecycle) disconnect(ctx context.Context) error {
	if m.disconnectAttempts != nil {
		defer m.disconnectAttempts.Add(1)
	}

	if m.disconnectDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.disconnectDelay):
		}
	}

	return m.disconnectErr
}

func (m *mockEndpointLifecycle) monitor(ctx context.Context) error {
	if m.monitorAttempts != nil {
		defer m.monitorAttempts.Add(1)
	}

	if m.monitorDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.monitorDelay):
		}
	}

	return m.monitorErr
}

type mockPublisher struct {
	mockEndpoint
}

var _ Publisher = (*mockPublisher)(nil)

func (m *mockPublisher) Publish(ctx context.Context, rk RoutingKey, msgs ...Message) error {
	return nil
}

type mockConsumer struct {
	mockEndpoint
}

var _ Consumer = (*mockConsumer)(nil)

func (m *mockConsumer) Consume(ctx context.Context) error {
	return nil
}

func (m *mockConsumer) Wait() {
	return
}

func (m *mockConsumer) Get() (*Message, error) {
	return nil, nil
}

func (m *mockConsumer) Cancel() error {
	return nil
}
