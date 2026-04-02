package topology

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"

	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

// Registry manages topology state with internal synchronization.
// It wraps declaration, deletion, verification, and mutex for thread-safety usage.
type Registry struct {
	exchanges []Exchange
	queues    []Queue
	bindings  []Binding

	// cache of declared exchanges, queues, and bindings
	declarations sync.Map // map[string]struct{}

	// synchronization mutex for internal state
	stateMu sync.RWMutex
}

// NewRegistry creates a new [Registry].
func NewRegistry() *Registry {
	return &Registry{
		exchanges: make([]Exchange, 0),
		queues:    make([]Queue, 0),
		bindings:  make([]Binding, 0),
	}
}

// Exchange returns a copy of the exchange with the given name, or nil if not found.
func (tm *Registry) Exchange(name string) *Exchange {
	tm.stateMu.RLock()
	defer tm.stateMu.RUnlock()

	if idx := slices.IndexFunc(tm.exchanges, func(e Exchange) bool { return e.Name == name }); idx >= 0 {
		exCopy := tm.exchanges[idx]
		return &exCopy
	}
	return nil
}

// Queue returns a copy of the queue with the given name, or nil if not found.
func (tm *Registry) Queue(name string) *Queue {
	tm.stateMu.RLock()
	defer tm.stateMu.RUnlock()

	if idx := slices.IndexFunc(tm.queues, func(q Queue) bool { return q.Name == name }); idx >= 0 {
		qCopy := tm.queues[idx]
		return &qCopy
	}
	return nil
}

// Binding returns a copy of the binding with the given source, destination, and key, or nil if not found.
func (tm *Registry) Binding(source, destination, key string) *Binding {
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

// Declare declares topology on channel and merges it into the manager atomically.
// It makes use of the declarations cache to prevent unnecessary re-declarations.
// For exchanges and queues, if an entity with the same name exists, it's replaced.
// For bindings, if a binding with the same source+destination+key exists, it's replaced.
func (tm *Registry) Declare(ch transport.Channel, t *Topology) error {
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

// Delete deletes topology from channel and removes it from the manager atomically.
// Bindings are removed first, then queues, then exchanges (reverse order of declaration).
// Entities are removed from internal state and cleared from the declaration cache.
func (tm *Registry) Delete(ch transport.Channel, t *Topology) error {
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

// Verify verifies that topology exists on the channel with correct configuration.
// For exchanges and queues, uses passive declaration to ensure type/args match.
// Note: AMQP doesn't provide a way to verify bindings, they're verified through routing.
// A verify for bindings will simply re-declare them idempotently.
func (tm *Registry) Verify(ch transport.Channel, t *Topology) error {
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

// Sync synchronizes the desired topology with the actual topology state.
// It deletes entities that exist in the manager but not in the desired topology,
// then declares entities from the desired topology.
// This provides declarative topology management, specify desired state and sync makes it so.
// Note: Sync is aware of the topology declared on this instance, it does not inspect the server directly.
func (tm *Registry) Sync(ch transport.Channel, t *Topology) error {
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

	// delete entities not in desired topology (Delete() acquires its own lock)
	if !diff.Empty() {
		if err := tm.Delete(ch, diff); err != nil {
			return err
		}
	}

	// declare desired topology (Declare() acquires its own lock)
	err := tm.Declare(ch, t)

	return err
}

// hash computes an MD5 hash of the given value. The value is encoded using gob before hashing.
// If the value is nil, it hashes the string representation of nil.
// Returns the hash as a hexadecimal string.
func hash(value interface{}) string {
	var buffer bytes.Buffer

	if value == nil {
		fmt.Fprintf(&buffer, "%#v", value)
	} else {
		// safe to ignore error since value is expected to be gob-encodable
		_ = gob.NewEncoder(&buffer).Encode(value)
	}

	hash := md5.Sum(buffer.Bytes())

	return hex.EncodeToString(hash[:])
}
