package broker

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

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

func (m *mockEndpoint) Connection() Connection {
	return nil
}

func (m *mockEndpoint) Channel() Channel {
	return nil
}

func (m *mockEndpoint) Ready() bool {
	return m.closeErr == nil
}

func (m *mockEndpoint) Close() error {
	return m.closeErr
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

type mockConnection struct {
	channelErr error
	channelFn  func() (Channel, error)
	closeErr   error
	closed     atomic.Bool
}

var _ Connection = (*mockConnection)(nil)

func (m *mockConnection) Channel() (Channel, error) {
	if m.channelFn != nil {
		return m.channelFn()
	}
	if m.channelErr != nil {
		return nil, m.channelErr
	}
	return &mockChannel{}, nil
}

func (m *mockConnection) Close() error {
	m.closed.Store(true)
	return m.closeErr
}

func (m *mockConnection) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockConnection) NotifyClose(receiver chan *transport.Error) chan *transport.Error {
	return receiver
}

func (m *mockConnection) NotifyBlocked(receiver chan transport.Blocking) chan transport.Blocking {
	return receiver
}

type mockChannel struct {
	txErr         error
	txCommitErr   error
	txRollbackErr error
	closeErr      error
	closed        atomic.Bool
}

var _ Channel = (*mockChannel)(nil)

func (m *mockChannel) PublishWithContext(_ context.Context, _, _ string, _, _ bool, _ transport.Publishing) error {
	return nil
}

func (m *mockChannel) PublishWithDeferredConfirmWithContext(_ context.Context, _, _ string, _, _ bool, _ transport.Publishing) (*transport.DeferredConfirmation, error) {
	return &transport.DeferredConfirmation{}, nil
}

func (m *mockChannel) Confirm(_ bool) error {
	return nil
}

func (m *mockChannel) NotifyPublish(c chan transport.Confirmation) chan transport.Confirmation {
	return c
}

func (m *mockChannel) NotifyReturn(c chan transport.Return) chan transport.Return {
	return c
}

func (m *mockChannel) NotifyFlow(c chan bool) chan bool {
	return c
}

func (m *mockChannel) Consume(_, _ string, _, _, _, _ bool, _ transport.Arguments) (<-chan transport.Delivery, error) {
	return make(chan transport.Delivery), nil
}

func (m *mockChannel) Get(_ string, _ bool) (transport.Delivery, bool, error) {
	return transport.Delivery{}, false, nil
}

func (m *mockChannel) Qos(_, _ int, _ bool) error {
	return nil
}

func (m *mockChannel) Cancel(_ string, _ bool) error {
	return nil
}

func (m *mockChannel) NotifyCancel(c chan string) chan string {
	return c
}

func (m *mockChannel) ExchangeDeclare(_, _ string, _, _, _, _ bool, _ transport.Arguments) error {
	return nil
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
	return transport.Queue{}, nil
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
	return m.txErr
}

func (m *mockChannel) TxCommit() error {
	return m.txCommitErr
}

func (m *mockChannel) TxRollback() error {
	return m.txRollbackErr
}

func (m *mockChannel) Close() error {
	m.closed.Store(true)
	return m.closeErr
}

func (m *mockChannel) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockChannel) NotifyClose(receiver chan *transport.Error) chan *transport.Error {
	return receiver
}
