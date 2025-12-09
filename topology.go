package broker

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// topologyManager manages topology state with internal synchronization.
// It wraps declaration, deletion, verification, and mutex for thread-safety usage.
type topologyManager struct {
	exchanges []Exchange
	queues    []Queue
	bindings  []Binding

	// cache of declared exchanges, queues, and bindings
	declarations sync.Map // map[string]struct{}

	// synchronization mutex for internal state
	stateMu sync.RWMutex
}

// newTopologyManager creates a new topology manager.
func newTopologyManager() *topologyManager {
	return &topologyManager{
		exchanges: make([]Exchange, 0),
		queues:    make([]Queue, 0),
		bindings:  make([]Binding, 0),
	}
}

// exchange returns a copy of the exchange with the given name, or nil if not found.
func (tm *topologyManager) exchange(name string) *Exchange {
	tm.stateMu.RLock()
	defer tm.stateMu.RUnlock()

	if idx := slices.IndexFunc(tm.exchanges, func(e Exchange) bool { return e.Name == name }); idx >= 0 {
		exCopy := tm.exchanges[idx]
		return &exCopy
	}
	return nil
}

// queue returns a copy of the queue with the given name, or nil if not found.
func (tm *topologyManager) queue(name string) *Queue {
	tm.stateMu.RLock()
	defer tm.stateMu.RUnlock()

	if idx := slices.IndexFunc(tm.queues, func(q Queue) bool { return q.Name == name }); idx >= 0 {
		qCopy := tm.queues[idx]
		return &qCopy
	}
	return nil
}

// binding returns a copy of the binding with the given source, destination, and key, or nil if not found.
func (tm *topologyManager) binding(source, destination, key string) *Binding {
	tm.stateMu.RLock()
	defer tm.stateMu.RUnlock()

	if idx := slices.IndexFunc(tm.bindings, func(b Binding) bool {
		return b.Source == source && b.Destination == destination && b.Key == key
	}); idx >= 0 {
		bCopy := tm.bindings[idx]
		return &bCopy
	}
	return nil
}

// declare declares topology on channel and merges it into the manager atomically.
// It makes use of the declarations cache to prevent unnecessary re-declarations.
// For exchanges and queues, if an entity with the same name exists, it's replaced.
// For bindings, if a binding with the same source+destination+key exists, it's replaced.
func (tm *topologyManager) declare(ch *Channel, t *Topology) error {
	tm.stateMu.Lock()
	defer tm.stateMu.Unlock()

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
			tm.exchanges[idx] = e // replace definition in case non-identity fields changed
		} else {
			tm.exchanges = append(tm.exchanges, e)
		}
	}

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
			tm.queues[idx] = q // replace definition in case non-identity fields changed
		} else {
			tm.queues = append(tm.queues, q)
		}
	}

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
			tm.bindings[idx] = b // replace definition in case non-identity fields changed
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
	tm.stateMu.Lock()
	defer tm.stateMu.Unlock()

	for _, b := range t.Bindings {
		if err := b.Validate(); err != nil {
			return err
		}
		if err := b.Delete(ch); err != nil {
			return err
		}
		tm.declarations.Delete(hash(b))
		tm.bindings = slices.DeleteFunc(tm.bindings, func(existing Binding) bool {
			return existing.Matches(b)
		})
	}

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
// For exchanges and queues, uses passive declaration to ensure type/args match.
// Note: AMQP doesn't provide a way to verify bindings, they're verified through routing.
// A verify for bindings will simply re-declare them idempotently.
func (tm *topologyManager) verify(ch *Channel, t *Topology) error {
	tm.stateMu.RLock()
	defer tm.stateMu.RUnlock()

	for _, e := range t.Exchanges {
		if err := e.Validate(); err != nil {
			return err
		}
		if err := e.Verify(ch); err != nil {
			return err
		}
	}

	for _, q := range t.Queues {
		if err := q.Validate(); err != nil {
			return err
		}
		if err := q.Verify(ch); err != nil {
			return err
		}
	}

	// verify bindings (no passive verify, redeclare idempotently)
	for _, b := range t.Bindings {
		if err := b.Validate(); err != nil {
			return err
		}
		if err := b.Declare(ch); err != nil {
			return err
		}
	}

	return nil
}

// sync synchronizes the desired topology with the actual topology state.
// It deletes entities that exist in the manager but not in the desired topology,
// then declares entities from the desired topology.
// This provides declarative topology management, specify desired state and sync makes it so.
// Note: sync is aware of the topology declared on this instance, it does not inspect the server directly.
func (tm *topologyManager) sync(ch *Channel, t *Topology) error {
	diff := &Topology{}

	tm.stateMu.RLock()

	for _, b := range tm.bindings {
		if slices.IndexFunc(t.Bindings, func(db Binding) bool { return b.Matches(db) }) < 0 {
			diff.Bindings = append(diff.Bindings, b)
		}
	}

	for _, q := range tm.queues {
		if slices.IndexFunc(t.Queues, func(dq Queue) bool { return q.Matches(dq) }) < 0 {
			diff.Queues = append(diff.Queues, q)
		}
	}

	for _, e := range tm.exchanges {
		if slices.IndexFunc(t.Exchanges, func(de Exchange) bool { return e.Matches(de) }) < 0 {
			diff.Exchanges = append(diff.Exchanges, e)
		}
	}

	tm.stateMu.RUnlock()

	// delete entities not in desired topology (delete() acquires its own lock)
	if !diff.Empty() {
		if err := tm.delete(ch, diff); err != nil {
			return err
		}
	}

	// declare desired topology (declare() acquires its own lock)
	err := tm.declare(ch, t)

	return err
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

// Declare declares the topology on the provided channel.
func (t *Topology) Declare(ch *Channel) error {
	// internal re-use, can be inlined if behavior changes
	return newTopologyManager().declare(ch, t)
}

// Verify verifies the topology on the provided channel.
func (t *Topology) Verify(ch *Channel) error {
	// internal re-use, can be inlined if behavior changes
	return newTopologyManager().verify(ch, t)
}

// Delete deletes the topology from the provided channel.
func (t *Topology) Delete(ch *Channel) error {
	// internal re-use, can be inlined if behavior changes
	return newTopologyManager().delete(ch, t)
}

// Exchange returns a copy of the exchange with the given name, or nil if not found.
func (t *Topology) Exchange(name string) *Exchange {
	for _, e := range t.Exchanges {
		if e.Name == name {
			eCopy := e
			return &eCopy
		}
	}
	return nil
}

// Queue returns a copy of the queue with the given name, or nil if not found.
func (t *Topology) Queue(name string) *Queue {
	for _, q := range t.Queues {
		if q.Name == name {
			qCopy := q
			return &qCopy
		}
	}
	return nil
}

// Binding returns a copy of the binding with the given source, destination, and key, or nil if not found.
func (t *Topology) Binding(source, destination, key string) *Binding {
	for _, b := range t.Bindings {
		if b.Source == source && b.Destination == destination && b.Key == key {
			bCopy := b
			return &bCopy
		}
	}
	return nil
}

// Empty returns true if the topology has no exchanges, queues, or bindings.
func (t *Topology) Empty() bool {
	return len(t.Exchanges) == 0 && len(t.Queues) == 0 && len(t.Bindings) == 0
}

// Merge creates a new Topology with all elements from both topologies (deep merge).
func (t *Topology) Merge(other *Topology) *Topology {
	merged := &Topology{}

	// merge exchanges from current topology
	for _, e := range t.Exchanges {
		if oe := other.Exchange(e.Name); oe != nil {
			merged.Exchanges = append(merged.Exchanges, *oe)
		} else {
			merged.Exchanges = append(merged.Exchanges, e)
		}
	}
	// add exchanges from other that don't exist in current topology
	for _, e := range other.Exchanges {
		if t.Exchange(e.Name) == nil {
			merged.Exchanges = append(merged.Exchanges, e)
		}
	}

	// merge queues from current topology
	for _, q := range t.Queues {
		if oq := other.Queue(q.Name); oq != nil {
			merged.Queues = append(merged.Queues, *oq)
		} else {
			merged.Queues = append(merged.Queues, q)
		}
	}
	// add queues from other that don't exist in current topology
	for _, q := range other.Queues {
		if t.Queue(q.Name) == nil {
			merged.Queues = append(merged.Queues, q)
		}
	}

	// merge bindings from current topology
	for _, b := range t.Bindings {
		if ob := other.Binding(b.Source, b.Destination, b.Key); ob != nil {
			merged.Bindings = append(merged.Bindings, *ob)
		} else {
			merged.Bindings = append(merged.Bindings, b)
		}
	}
	// add bindings from other that don't exist in current topology
	for _, b := range other.Bindings {
		if t.Binding(b.Source, b.Destination, b.Key) == nil {
			merged.Bindings = append(merged.Bindings, b)
		}
	}

	return merged
}

// Validate returns an error if the topology is invalid by validating all entities.
// The returned error will aggregate all validation errors found (joint error).
func (t *Topology) Validate() error {
	var errs []error

	for _, e := range t.Exchanges {
		if err := e.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, q := range t.Queues {
		if err := q.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, b := range t.Bindings {
		if err := b.Validate(); err != nil {
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", ErrTopologyValidation, err)
	}

	return nil
}

// Exchange represents an AMQP exchange declaration.
type Exchange struct {
	// Exchange name
	Name string
	// Exchange type: direct, fanout, topic, headers (default: direct)
	Type string
	// Survives broker restart
	Durable bool
	// Deleted when no bindings remain
	AutoDelete bool
	// Cannot be published to directly (for inter-exchange topologies only)
	Internal bool
	// Additional AMQP arguments
	Arguments Arguments
}

// NewExchange creates a new Exchange with sensible defaults (Type="direct", Durable=true).
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
		return ErrExchangeNameEmpty
	}
	return nil
}

// Declare declares this exchange using the provided channel.
// If the operation causes a channel-level error (e.g. precondition failed),
// the error will include information about the channel closure reason.
func (e Exchange) Declare(ch *Channel) error {
	if err := e.Validate(); err != nil {
		return err
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
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyDeclareFailed, e.Name, err)
	}

	return nil
}

// Verify checks if this exchange exists without creating it (passive declaration).
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
func (e Exchange) Verify(ch *Channel) error {
	if err := e.Validate(); err != nil {
		return err
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
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyVerifyFailed, e.Name, err)
	}

	return nil
}

// Delete removes this exchange.
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
//
// When ifUnused is true, the exchange will not be deleted if there are any
// queues or exchanges bound to it. If there are bindings, an error will be
// returned and the channel will be closed.
//
// If this exchange does not exist, the channel will be closed with an error.
func (e Exchange) Delete(ch *Channel, ifUnused bool) error {
	if err := e.Validate(); err != nil {
		return err
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		return ch.ExchangeDelete(e.Name, ifUnused, false /* no-wait */)
	})

	if err != nil {
		return fmt.Errorf("%w: exchange %q: %w", ErrTopologyDeleteFailed, e.Name, err)
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

// NewQueue creates a new Queue with sensible defaults (Durable=true).
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
		return ErrQueueNameEmpty
	}
	return nil
}

// Declare declares this queue using the provided channel.
// If the operation causes a channel-level error (e.g. precondition failed),
// the error will include information about the channel closure reason.
//
// When the error return value is not nil, you can assume the queue could not be
// declared with these parameters, and the channel will be closed.
func (q Queue) Declare(ch *Channel) error {
	if err := q.Validate(); err != nil {
		return err
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
		return fmt.Errorf("%w: queue %q: %w", ErrTopologyDeclareFailed, q.Name, err)
	}

	return nil
}

// Verify checks if this queue exists without creating it (passive declaration).
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
func (q Queue) Verify(ch *Channel) error {
	if err := q.Validate(); err != nil {
		return err
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
		return fmt.Errorf("%w: queue %q: %w", ErrTopologyVerifyFailed, q.Name, err)
	}

	return nil
}

// Delete removes this queue and returns the number of purged messages.
// If the operation causes a channel-level error (e.g. not found),
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
	if err := q.Validate(); err != nil {
		return 0, err
	}

	count, err := doSafeChannelActionWithReturn(ch, func(ch *Channel) (int, error) {
		return ch.QueueDelete(q.Name, ifUnused, ifEmpty, false /* no-wait */)
	})

	if err != nil {
		return count, fmt.Errorf("%w: queue %q: %w", ErrTopologyDeleteFailed, q.Name, err)
	}

	return count, nil
}

// Purge removes all messages from this queue.
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
func (q Queue) Purge(ch *Channel) (int, error) {
	if err := q.Validate(); err != nil {
		return 0, err
	}

	return doSafeChannelActionWithReturn(ch, func(ch *Channel) (int, error) {
		return ch.QueuePurge(q.Name, false /* no-wait */)
	})
}

// Inspect retrieves queue information including message and consumer counts.
// If the operation causes a channel-level error (e.g. not found),
// the error will include information about the channel closure reason.
//
// If a queue by this name does not exist, an error will be returned and the channel will be closed.
func (q Queue) Inspect(ch *Channel) (*struct{ Messages, Consumers int }, error) {
	if err := q.Validate(); err != nil {
		return nil, err
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

// Binding represents a queue-to-exchange or an exchange-to-exchange binding.
type Binding struct {
	// Type of binding destination,
	// either "queue" or "exchange", defaults to: "queue"
	Type BindingType
	// Source exchange name
	Source string
	// Destination queue or exchange name
	Destination string
	// Key used to match against message routing keys for filtering.
	// The matching behavior depends on the source exchange type:
	//   - direct: exact match (e.g. "orders.created")
	//   - topic: pattern match with wildcards (e.g. "orders.*" or "*.created" or "orders.#")
	//     where * matches exactly one word and # matches zero or more words
	//   - fanout: ignored, all messages are routed regardless of key
	//   - headers: ignored, matching is based on message headers and Arguments
	// For queue bindings, this filters which messages from the source exchange
	// are routed to the destination queue. For exchange-to-exchange bindings,
	// this filters which messages are forwarded to the destination exchange.
	Key string
	// Additional AMQP arguments for headers exchange matching or custom behavior
	Arguments Arguments
}

// NewBinding creates a new Binding with sensible defaults (Type="queue").
func NewBinding(source, destination, key string) Binding {
	return Binding{
		Type:        BindingTypeQueue,
		Source:      source,
		Destination: destination,
		Key:         key,
		Arguments:   nil,
	}
}

// Matches returns true if this binding matches another by source, destination, and key.
func (b Binding) Matches(other Binding) bool {
	return b.Source == other.Source && b.Destination == other.Destination && b.Key == other.Key
}

// Validate returns an error if the binding is invalid.
func (b Binding) Validate() error {
	if b.Source == "" || b.Destination == "" {
		return ErrBindingFieldsEmpty
	}
	return nil
}

// Declare declares (binds) this binding using the provided channel.
// If the operation causes a channel-level error (e.g. source/destination not found),
// the error will include information about the channel closure reason.
func (b Binding) Declare(ch *Channel) error {
	if err := b.Validate(); err != nil {
		return err
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		if b.Type == BindingTypeExchange {
			return ch.ExchangeBind(
				b.Destination,
				b.Key,
				b.Source,
				false, // no-wait
				b.Arguments,
			)
		}

		return ch.QueueBind(
			b.Destination,
			b.Key,
			b.Source,
			false, // no-wait
			b.Arguments,
		)
	})

	if err != nil {
		return fmt.Errorf("%w: %s binding %q -> %q: %w", ErrTopologyDeclareFailed, b.Type, b.Source, b.Destination, err)
	}

	return nil
}

// Delete removes (unbinds) this binding using the provided channel.
// If the operation causes a channel-level error (e.g. binding not found),
// the error will include information about the channel closure reason.
func (b Binding) Delete(ch *Channel) error {
	if err := b.Validate(); err != nil {
		return err
	}

	err := doSafeChannelAction(ch, func(ch *Channel) error {
		if b.Type == BindingTypeExchange {
			return ch.ExchangeUnbind(
				b.Destination,
				b.Key,
				b.Source,
				false, // no-wait
				b.Arguments,
			)
		}

		return ch.QueueUnbind(
			b.Destination,
			b.Key,
			b.Source,
			b.Arguments,
		)
	})

	if err != nil {
		return fmt.Errorf("%w: %s binding %q -> %q: %w", ErrTopologyDeleteFailed, b.Type, b.Source, b.Destination, err)
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

// NewRoutingKey creates a new RoutingKey instance.
// It injects any provided placeholders using the Substitute method.
func NewRoutingKey(key string, placeholders map[string]string) RoutingKey {
	rk := RoutingKey(key)
	rk.Replace(placeholders)
	return rk
}

// Replace replaces placeholders in the routing key with values from the provided map.
// Placeholders are in the format {key}. For example, given a routing key "user.{id}.update"
// and placeholders map {"id": "123"}, their Replace method will produce "user.123.update".
func (rk *RoutingKey) Replace(placeholders map[string]string) {
	// replace {} placeholders with values from args
	pattern := string(*rk)
	for k, v := range placeholders {
		placeholder := "{" + k + "}"
		pattern = strings.ReplaceAll(pattern, placeholder, v)
	}
	*rk = RoutingKey(pattern)
}

// Validate returns an error if the routing key is invalid.
func (rk *RoutingKey) Validate() error {
	if rk != nil && string(*rk) == "" {
		return ErrRoutingKeyEmpty
	}
	return nil
}

// String returns the string representation of the routing key.
func (rk RoutingKey) String() string {
	return string(rk)
}
