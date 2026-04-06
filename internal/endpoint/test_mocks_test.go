package endpoint

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

// mockConnection implements amqp.Connection for testing.
// Configuration methods (withXXX) must be called before concurrent use.
type mockConnection struct {
	ch          transport.Channel
	channelErr  error
	closeErr    error
	shouldClose bool
	closeDelay  time.Duration
	closeError  *transport.Error

	closed           atomic.Bool
	notifyCloseCh    chan *transport.Error
	notifyCloseCalls atomic.Int32

	mu sync.Mutex
}

var _ transport.Connection = (*mockConnection)(nil)

func newMockConnection() *mockConnection {
	return &mockConnection{}
}

func (m *mockConnection) withChannelError(err error) *mockConnection {
	m.channelErr = err
	return m
}

func (m *mockConnection) withChannel(ch transport.Channel) *mockConnection {
	m.ch = ch
	return m
}

func (m *mockConnection) withCloseError(err error) *mockConnection {
	m.closeErr = err
	return m
}

func (m *mockConnection) withAutoClose(err *transport.Error, delay time.Duration) *mockConnection {
	m.shouldClose = true
	m.closeError = err
	m.closeDelay = delay
	return m
}

func (m *mockConnection) triggerClose() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.notifyCloseCh != nil {
		if m.closeError != nil {
			m.notifyCloseCh <- m.closeError
		}
		close(m.notifyCloseCh)
		m.notifyCloseCh = nil
	}
}

func (m *mockConnection) Channel() (transport.Channel, error) {
	if m.channelErr != nil {
		return nil, m.channelErr
	}
	if m.ch != nil {
		return m.ch, nil
	}
	return newMockChannel(), nil
}

func (m *mockConnection) Close() error {
	m.closed.Store(true)
	return m.closeErr
}

func (m *mockConnection) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockConnection) NotifyClose(receiver chan *transport.Error) chan *transport.Error {
	m.notifyCloseCalls.Add(1)
	m.mu.Lock()
	m.notifyCloseCh = receiver
	m.mu.Unlock()
	if m.shouldClose {
		go func() {
			time.Sleep(m.closeDelay)
			m.triggerClose()
		}()
	}
	return receiver
}

func (m *mockConnection) NotifyBlocked(receiver chan transport.Blocking) chan transport.Blocking {
	return receiver
}

// mockChannel implements amqp.Channel for testing.
// Configuration methods (withXxx) must be called before concurrent use.
type mockChannel struct {
	publishErr         error
	closeErr           error
	qosErr             error
	consumeErr         error
	getErr             error
	cancelErr          error
	confirmErr         error
	queueDeclareErr    error
	exchangeDeclareErr error
	shouldClose        bool
	closeDelay         time.Duration
	closeError         *transport.Error

	publishedMsgs []transport.Publishing
	publishCalls  atomic.Int32
	closed        atomic.Bool
	getOk         bool

	notifyCloseCh     chan *transport.Error
	notifyCloseCalled atomic.Bool
	notifyFlowCh      chan bool
	notifyReturnCh    chan transport.Return
	notifyPublishCh   chan transport.Confirmation
	notifyCancelCh    chan string
	deliveryCh        chan transport.Delivery

	mu sync.Mutex
}

var _ transport.Channel = (*mockChannel)(nil)

func newMockChannel() *mockChannel {
	return &mockChannel{}
}

func (m *mockChannel) withPublishError(err error) *mockChannel {
	m.publishErr = err
	return m
}

func (m *mockChannel) withQosError(err error) *mockChannel {
	m.qosErr = err
	return m
}

func (m *mockChannel) withConsumeError(err error) *mockChannel {
	m.consumeErr = err
	return m
}

func (m *mockChannel) withGetOK(ok bool) *mockChannel {
	m.getOk = ok
	return m
}

func (m *mockChannel) withGetError(err error) *mockChannel {
	m.getErr = err
	return m
}

func (m *mockChannel) withCancelError(err error) *mockChannel {
	m.cancelErr = err
	return m
}

func (m *mockChannel) withExchangeDeclareError(err error) *mockChannel {
	m.exchangeDeclareErr = err
	return m
}

func (m *mockChannel) withQueueDeclareError(err error) *mockChannel {
	m.queueDeclareErr = err
	return m
}

func (m *mockChannel) withConfirmError(err error) *mockChannel {
	m.confirmErr = err
	return m
}

func (m *mockChannel) withCloseError(err error) *mockChannel {
	m.closeErr = err
	return m
}

func (m *mockChannel) withClose(err *transport.Error, delay time.Duration) *mockChannel {
	m.shouldClose = true
	m.closeError = err
	m.closeDelay = delay
	return m
}

func (m *mockChannel) triggerClose() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.notifyCloseCh != nil {
		if m.closeError != nil {
			m.notifyCloseCh <- m.closeError
		}
		close(m.notifyCloseCh)
		m.notifyCloseCh = nil
	}
}

func (m *mockChannel) sendDelivery(d transport.Delivery) {
	m.mu.Lock()
	ch := m.deliveryCh
	m.mu.Unlock()
	if ch != nil {
		ch <- d
	}
}

func (m *mockChannel) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg transport.Publishing) error {
	m.mu.Lock()
	m.publishedMsgs = append(m.publishedMsgs, msg)
	m.mu.Unlock()
	m.publishCalls.Add(1)
	return m.publishErr
}

func (m *mockChannel) PublishWithDeferredConfirmWithContext(_ context.Context, _, _ string, _, _ bool, msg transport.Publishing) (*transport.DeferredConfirmation, error) {
	m.mu.Lock()
	m.publishedMsgs = append(m.publishedMsgs, msg)
	m.mu.Unlock()
	m.publishCalls.Add(1)
	if m.publishErr != nil {
		return nil, m.publishErr
	}
	return &transport.DeferredConfirmation{}, nil
}

func (m *mockChannel) Confirm(_ bool) error {
	return m.confirmErr
}

func (m *mockChannel) NotifyPublish(confirm chan transport.Confirmation) chan transport.Confirmation {
	m.mu.Lock()
	m.notifyPublishCh = confirm
	m.mu.Unlock()
	return confirm
}

func (m *mockChannel) NotifyReturn(returns chan transport.Return) chan transport.Return {
	m.mu.Lock()
	m.notifyReturnCh = returns
	m.mu.Unlock()
	return returns
}

func (m *mockChannel) NotifyFlow(flow chan bool) chan bool {
	m.mu.Lock()
	m.notifyFlowCh = flow
	m.mu.Unlock()
	return flow
}

func (m *mockChannel) Consume(_, _ string, _, _, _, _ bool, _ transport.Arguments) (<-chan transport.Delivery, error) {
	if m.consumeErr != nil {
		return nil, m.consumeErr
	}
	ch := make(chan transport.Delivery, 16)
	m.mu.Lock()
	m.deliveryCh = ch
	m.mu.Unlock()
	return ch, nil
}

func (m *mockChannel) Get(_ string, _ bool) (transport.Delivery, bool, error) {
	return transport.Delivery{}, m.getOk, m.getErr
}

func (m *mockChannel) Qos(_, _ int, _ bool) error {
	return m.qosErr
}

func (m *mockChannel) Cancel(_ string, _ bool) error {
	return m.cancelErr
}

func (m *mockChannel) NotifyCancel(cancellations chan string) chan string {
	m.mu.Lock()
	m.notifyCancelCh = cancellations
	m.mu.Unlock()
	return cancellations
}

func (m *mockChannel) ExchangeDeclare(_, _ string, _, _, _, _ bool, _ transport.Arguments) error {
	return m.exchangeDeclareErr
}

func (m *mockChannel) ExchangeDeclarePassive(_, _ string, _, _, _, _ bool, _ transport.Arguments) error {
	return nil
}

func (m *mockChannel) ExchangeDelete(_ string, _, _ bool) error {
	return nil
}

func (m *mockChannel) ExchangeBind(_, _, _ string, _ bool, _ transport.Arguments) error {
	return nil
}

func (m *mockChannel) ExchangeUnbind(_, _, _ string, _ bool, _ transport.Arguments) error {
	return nil
}

func (m *mockChannel) QueueDeclare(_ string, _, _, _, _ bool, _ transport.Arguments) (transport.Queue, error) {
	return transport.Queue{}, m.queueDeclareErr
}

func (m *mockChannel) QueueDeclarePassive(_ string, _, _, _, _ bool, _ transport.Arguments) (transport.Queue, error) {
	return transport.Queue{}, nil
}

func (m *mockChannel) QueueDelete(_ string, _, _, _ bool) (int, error) {
	return 0, nil
}

func (m *mockChannel) QueueBind(_, _, _ string, _ bool, _ transport.Arguments) error {
	return nil
}

func (m *mockChannel) QueueUnbind(_, _, _ string, _ transport.Arguments) error {
	return nil
}

func (m *mockChannel) QueuePurge(_ string, _ bool) (int, error) {
	return 0, nil
}

func (m *mockChannel) Tx() error {
	return nil
}

func (m *mockChannel) TxCommit() error {
	return nil
}

func (m *mockChannel) TxRollback() error {
	return nil
}

func (m *mockChannel) Close() error {
	m.closed.Store(true)
	m.mu.Lock()
	if m.deliveryCh != nil {
		close(m.deliveryCh)
		m.deliveryCh = nil
	}
	m.mu.Unlock()
	return m.closeErr
}

func (m *mockChannel) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockChannel) NotifyClose(receiver chan *transport.Error) chan *transport.Error {
	m.notifyCloseCalled.Store(true)
	m.mu.Lock()
	m.notifyCloseCh = receiver
	m.mu.Unlock()
	if m.shouldClose {
		go func() {
			time.Sleep(m.closeDelay)
			m.triggerClose()
		}()
	}
	return receiver
}

// newTestConnectionManager creates a ConnectionManager whose pool slot 0 holds conn.
// If conn is nil the slot is left empty (nil), so Assign() returns ErrConnectionManager.
// The caller must NOT call Init(), the pool is populated directly via the dialer.
func newTestConnectionManager(conn transport.Connection) *transport.ConnectionManager {
	dialer := func(_ string, _ *transport.Config) (transport.Connection, error) {
		if conn == nil {
			return nil, errors.New("dialer: no connection")
		}
		return conn, nil
	}
	cm := transport.NewConnectionManager("amqp://invalid", &transport.ConnectionManagerOptions{
		Size:   1,
		Dialer: dialer,
	})
	_ = cm.Init(context.Background())
	return cm
}

// mockEndpointLifecycle implements endpointLifecycle for testing.
// Configuration fields must be set before concurrent use.
type mockEndpointLifecycle struct {
	mu sync.RWMutex

	connectAttempts  *atomic.Int32
	connectDelay     time.Duration
	connectErr       error
	connectFailUntil int32 // fail until attempt number, then succeed

	disconnectAttempts *atomic.Int32
	disconnectDelay    time.Duration
	disconnectErr      error

	monitorAttempts *atomic.Int32
	monitorDelay    time.Duration
	monitorErr      error
}

var _ endpointLifecycle = (*mockEndpointLifecycle)(nil)

func (m *mockEndpointLifecycle) init(_ context.Context) error {
	return nil
}

func (m *mockEndpointLifecycle) connect(ctx context.Context) error {
	var currentAttempt int32
	if m.connectAttempts != nil {
		currentAttempt = m.connectAttempts.Add(1)
	}

	m.mu.RLock()
	delay := m.connectDelay
	err := m.connectErr
	failUntil := m.connectFailUntil
	m.mu.RUnlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	if err != nil && (failUntil == 0 || currentAttempt <= failUntil) {
		return err
	}

	return nil
}

func (m *mockEndpointLifecycle) disconnect(ctx context.Context) error {
	if m.disconnectAttempts != nil {
		m.disconnectAttempts.Add(1)
	}

	m.mu.RLock()
	delay := m.disconnectDelay
	err := m.disconnectErr
	m.mu.RUnlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return err
}

func (m *mockEndpointLifecycle) monitor(ctx context.Context) error {
	if m.monitorAttempts != nil {
		m.monitorAttempts.Add(1)
	}

	m.mu.RLock()
	delay := m.monitorDelay
	err := m.monitorErr
	m.mu.RUnlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return err
}

// mockAcknowledger implements amqp091.Acknowledger for testing.
type mockAcknowledger struct {
	ackErr    error
	nackErr   error
	rejectErr error
	ackCalls  atomic.Int32
}

var _ transport.Acknowledger = (*mockAcknowledger)(nil)

func (m *mockAcknowledger) Ack(_ uint64, _ bool) error {
	m.ackCalls.Add(1)
	return m.ackErr
}

func (m *mockAcknowledger) Nack(_ uint64, _, _ bool) error {
	m.ackCalls.Add(1)
	return m.nackErr
}

func (m *mockAcknowledger) Reject(_ uint64, _ bool) error {
	m.ackCalls.Add(1)
	return m.rejectErr
}
