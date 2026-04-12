package topology

import (
	"errors"

	"github.com/MarwanAlsoltany/amqp-broker/internal"
)

var (
	// ErrTopology is the base error for topology operations.
	// All topology-related errors wrap this error.
	ErrTopology = internal.ErrDomain.Sentinel("topology")

	// ErrTopologyDeclareFailed indicates a topology declaration failed.
	// This occurs when exchange, queue, or binding declaration is rejected by the broker.
	ErrTopologyDeclareFailed = ErrTopology.Derive("declare failed")

	// ErrTopologyDeleteFailed indicates a topology deletion failed.
	// This occurs when exchange, queue, or binding deletion is rejected by the broker.
	ErrTopologyDeleteFailed = ErrTopology.Derive("delete failed")

	// ErrTopologyVerifyFailed indicates a topology verification failed.
	// This occurs when passive declaration fails, meaning the entity doesn't exist.
	ErrTopologyVerifyFailed = ErrTopology.Derive("verify failed")

	// ErrTopologyValidation is the base error for topology validation errors.
	// It wraps specific validation failures like empty names or missing fields.
	ErrTopologyValidation = ErrTopology.Derive("validation")
)

// Topology is a DTO that groups exchanges, queues, and bindings for bulk operations.
// It provides methods for declaring, verifying, and deleting complete topologies.
type Topology struct {
	Exchanges []Exchange
	Queues    []Queue
	Bindings  []Binding
}

// New creates a new [Topology] grouping exchanges, queues, and bindings.
// The topology can be used for bulk declare, verify, delete, and sync operations.
//
// Example:
//
//	topology := New(
//		[]Exchange{NewExchange("events").WithType("topic")},
//		[]Queue{NewQueue("events.orders")},
//		[]Binding{NewBinding("events", "events.orders", "orders.*")},
//	)
func New(exchanges []Exchange, queues []Queue, bindings []Binding) Topology {
	return Topology{
		Exchanges: exchanges,
		Queues:    queues,
		Bindings:  bindings,
	}
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
		return ErrTopologyValidation.Detailf("%w", err)
	}

	return nil
}
