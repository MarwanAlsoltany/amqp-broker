package broker

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// topologyManager manages topology state with internal synchronization.
// It wraps a Topology and provides thread-safe operations.
type topologyManager struct {
	exchanges []Exchange
	queues    []Queue
	bindings  []Binding

	// Cache of declared exchanges, queues, and bindings
	declarations sync.Map // map[string]struct{}

	// Synchronization mutex
	mu sync.RWMutex
}

// newTopologyManager creates a new topology manager.
func newTopologyManager() *topologyManager {
	return &topologyManager{}
}

// exchange returns a copy of the exchange with the given name, or nil if not found.
func (tm *topologyManager) exchange(name string) *Exchange {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if idx := slices.IndexFunc(tm.exchanges, func(e Exchange) bool { return e.Name == name }); idx >= 0 {
		exCopy := tm.exchanges[idx]
		return &exCopy
	}
	return nil
}

// queue returns a copy of the queue with the given name, or nil if not found.
func (tm *topologyManager) queue(name string) *Queue {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if idx := slices.IndexFunc(tm.queues, func(q Queue) bool { return q.Name == name }); idx >= 0 {
		qCopy := tm.queues[idx]
		return &qCopy
	}
	return nil
}

// binding returns a copy of the binding with the given source, destination, and pattern, or nil if not found.
func (tm *topologyManager) binding(source, destination, pattern string) *Binding {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if idx := slices.IndexFunc(tm.bindings, func(b Binding) bool {
		return b.Source == source && b.Destination == destination && b.Pattern == pattern
	}); idx >= 0 {
		bCopy := tm.bindings[idx]
		return &bCopy
	}
	return nil
}

// declare declares topology on channel and merges it into the manager atomically.
// Uses the declaration cache to prevent re-declarations.
// For exchanges and queues, if an entity with the same name exists, it's replaced.
// For bindings, if a binding with the same source+destination+pattern exists, it's replaced.
func (tm *topologyManager) declare(ch *Channel, t *Topology) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Declare exchanges
	for _, e := range t.Exchanges {
		if err := e.Validate(); err != nil {
			return err
		}

		if _, loaded := tm.declarations.LoadOrStore(hash(e), e); loaded {
			continue // already declared
		}

		if err := e.Declare(ch); err != nil {
			tm.declarations.Delete(hash(e))
			return err
		}

		if idx := slices.IndexFunc(tm.exchanges, func(oe Exchange) bool { return oe.Matches(e) }); idx >= 0 {
			tm.exchanges[idx] = e
		} else {
			tm.exchanges = append(tm.exchanges, e)
		}
	}

	// Declare queues
	for _, q := range t.Queues {
		if err := q.Validate(); err != nil {
			return err
		}

		if _, loaded := tm.declarations.LoadOrStore(hash(q), q); loaded {
			continue // already declared
		}

		if err := q.Declare(ch); err != nil {
			tm.declarations.Delete(hash(q))
			return err
		}

		if idx := slices.IndexFunc(tm.queues, func(oe Queue) bool { return oe.Matches(q) }); idx >= 0 {
			tm.queues[idx] = q
		} else {
			tm.queues = append(tm.queues, q)
		}
	}

	// Declare bindings
	for _, b := range t.Bindings {
		if err := b.Validate(); err != nil {
			return err
		}

		if _, loaded := tm.declarations.LoadOrStore(hash(b), b); loaded {
			continue // already declared
		}

		if err := b.Declare(ch); err != nil {
			tm.declarations.Delete(hash(b))
			return err
		}

		if idx := slices.IndexFunc(tm.bindings, func(ob Binding) bool { return ob.Matches(b) }); idx >= 0 {
			tm.bindings[idx] = b
		} else {
			tm.bindings = append(tm.bindings, b)
		}
	}

	return nil
}

// delete deletes topology from channel and removes it from the manager atomically.
// Bindings are removed first, then queues, then exchanges (reverse order of declaration).
// Entities are removed from internal state and cleared from the declaration cache.
func (tm *topologyManager) delete(ch *Channel, t *Topology) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Delete bindings
	for _, b := range t.Bindings {
		if err := b.Validate(); err != nil {
			return err
		}
		if err := b.Delete(ch, nil); err != nil {
			return err
		}
		tm.declarations.Delete(hash(b))
		tm.bindings = slices.DeleteFunc(tm.bindings, func(existing Binding) bool {
			return existing.Matches(b)
		})
	}

	// Delete queues
	for _, q := range t.Queues {
		if err := q.Validate(); err != nil {
			return err
		}
		if _, err := q.Delete(ch, false, false); err != nil {
			return err
		}
		tm.declarations.Delete(hash(q))
		tm.queues = slices.DeleteFunc(tm.queues, func(existing Queue) bool {
			return existing.Matches(q)
		})
	}

	// Delete exchanges
	for _, e := range t.Exchanges {
		if err := e.Validate(); err != nil {
			return err
		}
		if err := e.Delete(ch, false); err != nil {
			return err
		}
		tm.declarations.Delete(hash(e))
		tm.exchanges = slices.DeleteFunc(tm.exchanges, func(existing Exchange) bool {
			return existing.Matches(e)
		})
	}

	return nil
}

// verify verifies that topology exists on the channel with correct configuration.
// For exchanges, redeclares them (idempotent) to ensure type/args match.
// For queues, uses passive declaration to check existence.
// Note: AMQP doesn't provide a way to verify bindings - they're verified through routing.
func (tm *topologyManager) verify(ch *Channel, t *Topology) error {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	// Verify exchanges
	for _, e := range t.Exchanges {
		if err := e.Validate(); err != nil {
			return err
		}
		if err := e.Verify(ch); err != nil {
			return err
		}
	}

	// Verify queues
	for _, q := range t.Queues {
		if err := q.Validate(); err != nil {
			return err
		}
		if err := q.Verify(ch); err != nil {
			return err
		}
	}

	// Verify bindings (no passive verify, redeclare idempotently)
	for _, b := range t.Bindings {
		if err := b.Validate(); err != nil {
			return err
		}
		if err := b.Declare(ch); err != nil {
			return fmt.Errorf("%w: %w", ErrTopologyVerify, err)
		}
	}

	return nil
}

// sync synchronizes the desired topology with the actual AMQP state.
// It deletes entities that exist in the manager but not in the desired topology,
// then declares entities from the desired topology.
// This provides declarative topology management - specify desired state and sync makes it so.
// Note that sync is aware of the topology declared on this Broker, it does not inspect the server directly.
func (tm *topologyManager) sync(ch *Channel, desired *Topology) error {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	t := &Topology{}

	// Bindings to delete
	for _, existing := range tm.bindings {
		if slices.IndexFunc(desired.Bindings, func(d Binding) bool { return existing.Matches(d) }) < 0 {
			t.Bindings = append(t.Bindings, existing)
		}
	}

	// Queues to delete
	for _, existing := range tm.queues {
		if slices.IndexFunc(desired.Queues, func(d Queue) bool { return existing.Matches(d) }) < 0 {
			t.Queues = append(t.Queues, existing)
		}
	}

	// Exchanges to delete
	for _, existing := range tm.exchanges {
		if slices.IndexFunc(desired.Exchanges, func(d Exchange) bool { return existing.Matches(d) }) < 0 {
			t.Exchanges = append(t.Exchanges, existing)
		}
	}

	// Delete entities not in desired topology
	if len(t.Bindings) > 0 || len(t.Queues) > 0 || len(t.Exchanges) > 0 {
		if err := tm.delete(ch, t); err != nil {
			return err
		}
	}

	// Declare desired topology
	return tm.declare(ch, desired)
}

// Topology represents a complete AMQP topology declaration (DTO).
type Topology struct {
	manager   topologyManager
	Exchanges []Exchange
	Queues    []Queue
	Bindings  []Binding
}

// NewTopology creates a new Topology instance.
func NewTopology(exchanges []Exchange, queues []Queue, bindings []Binding) Topology {
	return Topology{
		Exchanges: exchanges,
		Queues:    queues,
		Bindings:  bindings,
	}
}

func (t *Topology) Declare(ch *Channel) error {
	// internal re-use, can be inlined if behavior changes
	return newTopologyManager().declare(ch, t)
}

func (t *Topology) Verify(ch *Channel) error {
	// internal re-use, can be inlined if behavior changes
	return newTopologyManager().verify(ch, t)
}

func (t *Topology) Delete(ch *Channel) error {
	// internal re-use, can be inlined if behavior changes
	return newTopologyManager().delete(ch, t)
}

// Empty returns true if the topology has no exchanges, queues, or bindings.
func (t *Topology) Empty() bool {
	return len(t.Exchanges) == 0 && len(t.Queues) == 0 && len(t.Bindings) == 0
}

// Exchange returns the exchange with the given name, or nil if not found.
func (t *Topology) Exchange(name string) *Exchange {
	for i := range t.Exchanges {
		e := &t.Exchanges[i]
		if e.Name == name {
			return e
		}
	}
	return nil
}

func (t *Topology) Queue(name string) *Queue {
	for i := range t.Queues {
		q := &t.Queues[i]
		if q.Name == name {
			return q
		}
	}
	return nil
}

func (t *Topology) Binding(source, destination, pattern string) *Binding {
	for i := range t.Bindings {
		b := &t.Bindings[i]
		if b.Source == source && b.Destination == destination && b.Pattern == pattern {
			return b
		}
	}
	return nil
}

// Merge creates a new Topology with all elements from both topologies.
func (t *Topology) Merge(other *Topology) *Topology {
	merged := &Topology{}

	// Merge exchanges
	for _, e := range t.Exchanges {
		if oe := other.Exchange(e.Name); oe != nil {
			merged.Exchanges = append(merged.Exchanges, *oe)
		} else {
			merged.Exchanges = append(merged.Exchanges, e)
		}
	}

	// Merge queues
	for _, q := range t.Queues {
		if oq := other.Queue(q.Name); oq != nil {
			merged.Queues = append(merged.Queues, *oq)
		} else {
			merged.Queues = append(merged.Queues, q)
		}
	}

	// Merge bindings
	for _, b := range t.Bindings {
		if ob := other.Binding(b.Source, b.Destination, b.Pattern); ob != nil {
			merged.Bindings = append(merged.Bindings, *ob)
		} else {
			merged.Bindings = append(merged.Bindings, b)
		}
	}

	return merged
}

// Exchange represents an AMQP exchange declaration.
type Exchange struct {
	// Exchange name
	Name string
	// Exchange type:direct, fanout, topic, headers (default: direct)
	Type string
	// Survives broker restart
	Durable bool
	// Deleted when no bindings remain
	AutoDelete bool
	// Cannot be published to directly
	Internal bool
	// Additional AMQP arguments
	Arguments Arguments
}

// NewExchange creates a new Exchange with sensible defaults.
func NewExchange(name string) Exchange {
	return Exchange{
		Name:       name,
		Type:       defaultExchangeType,
		Durable:    defaultExchangeDurable,
		AutoDelete: false,
		Internal:   false,
		Arguments:  nil,
	}
}

// Matches returns true if this exchange matches another by name.
func (e Exchange) Matches(other Exchange) bool {
	return e.Name == other.Name
}

// Validate returns an error if the exchange is invalid.
func (e Exchange) Validate() error {
	if e.Name == "" {
		return ErrEmptyExchangeName
	}
	return nil
}

// Declare declares this exchange using the provided channel.
// If the operation causes a channel-level error (e.g., precondition failed),
// the error will include information about the channel closure reason.
func (e Exchange) Declare(ch *Channel) error {
	if e.Name == "" {
		return ErrEmptyExchangeName
	}

	kind := e.Type
	if kind == "" {
		kind = defaultExchangeType
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		return ch.ExchangeDeclare(
			e.Name,
			kind,
			e.Durable,
			e.AutoDelete,
			e.Internal,
			false, // no-wait
			e.Arguments,
		)
	})

	if err != nil {
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyDeclare, e.Name, err)
	}

	return nil
}

// Verify checks if this exchange exists without creating it (passive declaration).
// If the operation causes a channel-level error (e.g., not found),
// the error will include information about the channel closure reason.
func (e Exchange) Verify(ch *Channel) error {
	if e.Name == "" {
		return ErrEmptyExchangeName
	}

	kind := e.Type
	if kind == "" {
		kind = defaultExchangeType
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		return ch.ExchangeDeclarePassive(
			e.Name,
			kind,
			e.Durable,
			e.AutoDelete,
			e.Internal,
			false, // no-wait
			e.Arguments,
		)
	})

	if err != nil {
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyVerify, e.Name, err)
	}

	return nil
}

// Delete removes this exchange.
// If the operation causes a channel-level error (e.g., not found),
// the error will include information about the channel closure reason.
//
// When ifUnused is true, the exchange will not be deleted if there are any
// queues or exchanges bound to it. If there are bindings, an error will be
// returned and the channel will be closed.
//
// If this exchange does not exist, the channel will be closed with an error.
func (e Exchange) Delete(ch *Channel, ifUnused bool) error {
	if e.Name == "" {
		return ErrEmptyExchangeName
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		return ch.ExchangeDelete(e.Name, ifUnused, false /* no-wait */)
	})

	if err != nil {
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyDelete, e.Name, err)
	}

	return nil
}

// Queue represents an AMQP queue declaration.
type Queue struct {
	// Queue name
	Name string
	// Survives broker restart
	Durable bool
	// Deleted when no consumers remain
	AutoDelete bool
	// Used by only one connection
	Exclusive bool
	// Additional AMQP arguments
	Arguments Arguments
}

// NewQueue creates a new Queue with sensible defaults.
func NewQueue(name string) Queue {
	return Queue{
		Name:       name,
		Durable:    defaultQueueDurable,
		AutoDelete: false,
		Exclusive:  false,
		Arguments:  nil,
	}
}

// Matches returns true if this queue matches another by name.
func (q Queue) Matches(other Queue) bool {
	return q.Name == other.Name
}

// Validate returns an error if the queue is invalid.
func (q Queue) Validate() error {
	if q.Name == "" {
		return ErrEmptyQueueName
	}
	return nil
}

// Declare declares this queue using the provided channel.
// If the operation causes a channel-level error (e.g., precondition failed),
// the error will include information about the channel closure reason.
//
// When the error return value is not nil, you can assume the queue could not be
// declared with these parameters, and the channel will be closed.
func (q Queue) Declare(ch *Channel) error {
	if q.Name == "" {
		return ErrEmptyQueueName
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		_, err := ch.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false, // no-wait
			q.Arguments,
		)
		return err
	})

	if err != nil {
		return fmt.Errorf("%w: queue %q: %w", ErrTopologyDeclare, q.Name, err)
	}

	return nil
}

// Verify checks if this queue exists without creating it (passive declaration).
// If the operation causes a channel-level error (e.g., not found),
// the error will include information about the channel closure reason.
func (q Queue) Verify(ch *Channel) error {
	if q.Name == "" {
		return ErrEmptyQueueName
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		_, err := ch.QueueDeclarePassive(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false, // no-wait
			q.Arguments,
		)
		return err
	})

	if err != nil {
		return fmt.Errorf("%w: queue %q: %w", ErrTopologyVerify, q.Name, err)
	}

	return nil
}

// Delete removes this queue and returns the number of purged messages.
// If the operation causes a channel-level error (e.g., not found),
// the error will include information about the channel closure reason.
//
// When this queue does not exist, the channel will be closed with an error.
//
// When ifUnused is true, the queue will not be deleted if there are any
// consumers on the queue. If there are consumers, an error will be returned and
// the channel will be closed.
//
// When ifEmpty is true, the queue will not be deleted if there are any messages
// remaining on the queue. If there are messages, an error will be returned and
// the channel will be closed.
func (q Queue) Delete(ch *Channel, ifUnused, ifEmpty bool) (int, error) {
	if q.Name == "" {
		return 0, ErrEmptyQueueName
	}

	count, err := doSafeChannelActionWithReturn(ch, func(ch *Channel) (int, error) {
		return ch.QueueDelete(q.Name, ifUnused, ifEmpty, false /* no-wait */)
	})

	if err != nil {
		return count, fmt.Errorf("%w: queue %q: %w", ErrTopologyDelete, q.Name, err)
	}

	return count, nil
}

// Purge removes all messages from this queue.
// If the operation causes a channel-level error (e.g., not found),
// the error will include information about the channel closure reason.
func (q Queue) Purge(ch *Channel) (int, error) {
	if q.Name == "" {
		return 0, ErrEmptyQueueName
	}

	return doSafeChannelActionWithReturn(ch, func(ch *Channel) (int, error) {
		return ch.QueuePurge(q.Name, false /* no-wait */)
	})
}

// Inspect retrieves queue information including message and consumer counts.
// If the operation causes a channel-level error (e.g., not found),
// the error will include information about the channel closure reason.
//
// If a queue by this name does not exist, an error will be returned and the channel will be closed.
func (q Queue) Inspect(ch *Channel) (*struct{ Messages, Consumers int }, error) {
	if q.Name == "" {
		return nil, ErrEmptyQueueName
	}

	return doSafeChannelActionWithReturn(ch, func(ch *Channel) (*struct{ Messages, Consumers int }, error) {
		info, err := ch.QueueDeclarePassive(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false, // no-wait
			q.Arguments,
		)
		if err != nil {
			return nil, err
		}
		return &struct{ Messages, Consumers int }{
			Messages:  info.Messages,
			Consumers: info.Consumers,
		}, nil
	})
}

// Binding represents a queue-to-exchange binding.
type Binding struct {
	// Type of binding destination,
	// either "queue" or "exchange", defaults to: "queue"
	Type BindingType
	// Source name
	Source string
	// Destination name
	Destination string
	// Pattern for routing keys,
	// dot-separated words, * = single word, # = zero or more words
	Pattern string
	// Additional AMQP arguments
	Arguments Arguments
}

// NewBinding creates a new Binding with sensible defaults.
func NewBinding(source, destination string) Binding {
	return Binding{
		Type:        BindingTypeQueue,
		Source:      source,
		Destination: destination,
		Pattern:     "",
		Arguments:   nil,
	}
}

// Matches returns true if this binding matches another by source, destination, and pattern.
func (b Binding) Matches(other Binding) bool {
	return b.Source == other.Source && b.Destination == other.Destination && b.Pattern == other.Pattern
}

// Validate returns an error if the binding is invalid.
func (b Binding) Validate() error {
	if b.Source == "" || b.Destination == "" {
		return ErrEmptyBindingFields
	}
	return nil
}

// Declare declares (binds) this binding using the provided channel.
// If the operation causes a channel-level error (e.g., source/destination not found),
// the error will include information about the channel closure reason.
func (bn Binding) Declare(ch *Channel) error {
	if bn.Source == "" || bn.Destination == "" {
		return ErrEmptyBindingFields
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		if bn.Type == BindingTypeExchange {
			return ch.ExchangeBind(
				bn.Destination,
				bn.Pattern,
				bn.Source,
				false, // no-wait
				bn.Arguments,
			)
		}

		return ch.QueueBind(
			bn.Destination,
			bn.Pattern,
			bn.Source,
			false, // no-wait
			bn.Arguments,
		)
	})

	if err != nil {
		return fmt.Errorf("%w: %s binding %q -> %q: %w", ErrTopologyDeclare, bn.Type, bn.Source, bn.Destination, err)
	}

	return nil
}

// Delete removes (unbinds) this binding using the provided channel.
// If the operation causes a channel-level error (e.g., binding not found),
// the error will include information about the channel closure reason.
func (bn Binding) Delete(ch *Channel, args Arguments) error {
	if bn.Source == "" || bn.Destination == "" {
		return ErrEmptyBindingFields
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		if bn.Type == BindingTypeExchange {
			return ch.ExchangeUnbind(
				bn.Destination,
				bn.Pattern,
				bn.Source,
				false, // no-wait
				args,
			)
		}

		return ch.QueueUnbind(
			bn.Destination,
			bn.Pattern,
			bn.Source,
			args,
		)
	})

	if err != nil {
		return fmt.Errorf("%w: %s binding %q -> %q: %w", ErrTopologyDelete, bn.Type, bn.Source, bn.Destination, err)
	}

	return nil
}

// BindingType specifies the type of binding destination.
type BindingType string

const (
	BindingTypeQueue    BindingType = "queue"
	BindingTypeExchange BindingType = "exchange"
)

// RoutingKey represents an AMQP routing key for message routing.
type RoutingKey string

// Inject replaces placeholders in the routing key with values from the provided map.
// Placeholders are in the format {key}. For example, given a routing key "user.{id}.update"
// and args map {"id": "123"}, their Inject method will produce "user.123.update".
func (rk *RoutingKey) Inject(args map[string]string) {
	// replace {} placeholders with values from args
	pattern := string(*rk)
	for k, v := range args {
		placeholder := "{" + k + "}"
		pattern = strings.ReplaceAll(pattern, placeholder, v)
	}
	*rk = RoutingKey(pattern)
}

func (rk RoutingKey) String() string {
	return string(rk)
}

// doSafeChannelActionWithReturn executes a channel operation while monitoring for channel closure.
// It registers a close notification handler and executes the provided operation function.
// If the channel closes during the operation, it returns both the operation error (if any)
// and a wrapped error indicating the channel was closed.
//
// This function is useful for topology operations that might trigger channel closure
// due to AMQP protocol errors (e.g., PreconditionFailed, AccessRefused, NotFound).
//
// Usage examples:
//
//	err := doSafeChannelAction(ch, func(ch *Channel) error {
//	    return ch.ExchangeDeclare(...)
//	})
func doSafeChannelAction(ch *Channel, op func(*Channel) error) error {
	_, err := doSafeChannelActionWithReturn(ch, func(ch *Channel) (struct{}, error) {
		return struct{}{}, op(ch)
	})
	return err
}

// doSafeChannelActionWithReturn is similar to [doSafeChannelAction] but supports operations that return a value. It is generic and can handle operations that return any type along with an error.
//
// Usage examples:
//
//	count, err := doSafeChannelActionWithReturn(ch, func(ch *Channel) (int, error) {
//	    return ch.QueueDelete(...)
//	})
//
//	// or for operations that don't return a value:
//	_, err := doSafeChannelActionWithReturn(ch, func(ch *Channel) (struct{}, error) {
//	    return struct{}{}, ch.ExchangeDeclare(...)
//	})
func doSafeChannelActionWithReturn[T any](ch *Channel, op func(*Channel) (T, error)) (T, error) {
	var zero T

	// Create a buffered channel to avoid deadlock
	// Library sends notification once, then closes the channel
	closeCh := make(chan *amqp.Error, 1)
	ch.NotifyClose(closeCh)

	// Create channel to communicate results
	type result struct {
		value T
		err   error
	}
	resultCh := make(chan result, 1)

	// Execute the operation in a goroutine
	go func() {
		val, err := op(ch)
		resultCh <- result{value: val, err: err}
	}()

	// Wait for either operation completion or channel closure
	var opResult result
	var chCloseErr *amqp.Error

	select {
	case opResult = <-resultCh:
		// Operation completed first, check if there's a close notification
		select {
		case chCloseErr = <-closeCh:
			// Channel was closed during or immediately after the operation
		default:
			// Channel is still open
		}
	case chCloseErr = <-closeCh:
		// Channel closed before operation completed
		// Wait briefly for operation to complete
		select {
		case opResult = <-resultCh:
			// Operation completed after channel closed
		default:
			// Operation didn't complete yet
		}
	}

	// Combine operation error and channel close error
	if opResult.err == nil && chCloseErr == nil {
		return opResult.value, nil
	}

	if chCloseErr != nil {
		// Channel was closed - this is critical information
		amqpErr := newAMQPError("channel closed during operation", chCloseErr)
		if opResult.err != nil {
			// Both errors present
			return zero, fmt.Errorf("%w (operation error: %v)", amqpErr, opResult.err)
		}
		return zero, amqpErr
	}

	// Only operation error
	return zero, opResult.err
}
